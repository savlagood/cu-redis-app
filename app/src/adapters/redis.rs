use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use redis::{
    AsyncConnectionConfig, Cmd, FromRedisValue,
    aio::MultiplexedConnection,
    sentinel::{SentinelClient, SentinelServerType},
    streams::{StreamAutoClaimReply, StreamId, StreamReadReply},
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::watch,
    time::{Instant, sleep, timeout},
};

use crate::{
    domain::{Notification, Player, Score},
    error::{AppError, Result},
    ports::{GameRepository, Notifications, ProfileCache},
};

const STREAM: &str = "notifications";
const GROUP: &str = "notifications-group";
const CONSUMER: &str = "gamehub-worker";
const CACHE_TTL: u64 = 60;
const LOGIN_TTL: u64 = 86_400;
const RETENTION_MILLIS: u64 = 7 * 24 * 60 * 60 * 1_000;
const BLOCK_MILLIS: u64 = 2_000;

const LOGIN_SCRIPT: &str = r#"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"#;

pub struct RedisSettings {
    pub sentinel_urls: Vec<String>,
    pub master_name: String,
    pub connect_timeout: Duration,
    pub response_timeout: Duration,
    pub connect_attempts: usize,
    pub retry_delay: Duration,
}

pub struct RedisStore {
    settings: RedisSettings,
}

impl RedisStore {
    pub fn new(settings: RedisSettings) -> Result<Self> {
        if settings.sentinel_urls.is_empty()
            || settings.master_name.trim().is_empty()
            || settings.connect_attempts == 0
            || settings.connect_timeout.is_zero()
            || settings.response_timeout.is_zero()
        {
            return Err(AppError::Validation(
                "invalid Redis connection settings".into(),
            ));
        }
        SentinelClient::build(
            settings.sentinel_urls.clone(),
            settings.master_name.clone(),
            None,
            SentinelServerType::Master,
        )
        .map_err(storage_error)?;
        Ok(Self { settings })
    }

    async fn connect(
        &self,
        server_type: SentinelServerType,
        response_timeout: Duration,
    ) -> Result<MultiplexedConnection> {
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.settings.connect_timeout))
            .set_response_timeout(Some(response_timeout));
        let mut last_error = "no reachable Sentinel".to_owned();

        for attempt in 0..self.settings.connect_attempts {
            for url in &self.settings.sentinel_urls {
                let mut client = SentinelClient::build(
                    vec![url.clone()],
                    self.settings.master_name.clone(),
                    None,
                    server_type.clone(),
                )
                .map_err(storage_error)?;
                match timeout(
                    self.settings.connect_timeout,
                    client.get_async_connection_with_config(&config),
                )
                .await
                {
                    Ok(Ok(connection)) => return Ok(connection),
                    Ok(Err(error)) => last_error = error.to_string(),
                    Err(_) => last_error = "Sentinel discovery/connection timed out".into(),
                }
            }
            if attempt + 1 < self.settings.connect_attempts {
                sleep(self.settings.retry_delay).await;
            }
        }
        Err(AppError::Unavailable(last_error))
    }

    async fn master(&self) -> Result<MultiplexedConnection> {
        self.connect(SentinelServerType::Master, self.settings.response_timeout)
            .await
    }

    async fn reader(&self) -> Result<MultiplexedConnection> {
        match self
            .connect(SentinelServerType::Replica, self.settings.response_timeout)
            .await
        {
            Ok(connection) => Ok(connection),
            Err(error) => {
                tracing::warn!(%error, "replica unavailable; reading from master");
                self.master().await
            }
        }
    }

    async fn query<T: FromRedisValue>(&self, command: &mut Cmd) -> Result<T> {
        command
            .query_async(&mut self.master().await?)
            .await
            .map_err(storage_error)
    }

    async fn read<T: FromRedisValue>(&self, command: &mut Cmd) -> Result<T> {
        command
            .query_async(&mut self.reader().await?)
            .await
            .map_err(storage_error)
    }

    pub async fn initialize(&self) -> Result<()> {
        let mut connection = self.master().await?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(storage_error)?;
        Self::ensure_group(&mut connection).await
    }

    async fn ensure_group(connection: &mut MultiplexedConnection) -> Result<()> {
        match redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(STREAM)
            .arg(GROUP)
            .arg("$")
            .arg("MKSTREAM")
            .query_async::<()>(connection)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
            Err(error) => Err(storage_error(error)),
        }
    }

    pub async fn consume_notifications(&self, mut shutdown: watch::Receiver<bool>) {
        let mut next_maintenance = Instant::now();
        let mut claim_cursor = "0-0".to_owned();
        loop {
            if *shutdown.borrow() {
                return;
            }
            let outcome = tokio::select! {
                _ = shutdown.changed() => return,
                outcome = self.consume_once(&mut next_maintenance, &mut claim_cursor) => outcome,
            };
            if let Err(error) = outcome {
                tracing::warn!(%error, "notification worker will reconnect through Sentinel");
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = sleep(self.settings.retry_delay) => {}
                }
            }
        }
    }

    async fn consume_once(
        &self,
        next_maintenance: &mut Instant,
        claim_cursor: &mut String,
    ) -> Result<()> {
        let response_timeout = self.settings.response_timeout.max(Duration::from_secs(5));
        let mut connection = self
            .connect(SentinelServerType::Master, response_timeout)
            .await?;
        Self::ensure_group(&mut connection).await?;

        if Instant::now() >= *next_maintenance {
            let (seconds, micros): (u64, u64) = redis::cmd("TIME")
                .query_async(&mut connection)
                .await
                .map_err(storage_error)?;
            let cutoff = (seconds * 1_000 + micros / 1_000).saturating_sub(RETENTION_MILLIS);
            redis::cmd("XTRIM")
                .arg(STREAM)
                .arg("MINID")
                .arg("=")
                .arg(format!("{cutoff}-0"))
                .query_async::<u64>(&mut connection)
                .await
                .map_err(storage_error)?;
            *next_maintenance = Instant::now() + Duration::from_secs(60);
        }

        let claimed: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
            .arg(STREAM)
            .arg(GROUP)
            .arg(CONSUMER)
            .arg(60_000)
            .arg(claim_cursor.as_str())
            .arg("COUNT")
            .arg(100)
            .query_async(&mut connection)
            .await
            .map_err(storage_error)?;
        *claim_cursor = claimed.next_stream_id;
        Self::process_entries(&mut connection, claimed.claimed).await?;

        let pending: StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(CONSUMER)
            .arg("COUNT")
            .arg(100)
            .arg("STREAMS")
            .arg(STREAM)
            .arg("0-0")
            .query_async(&mut connection)
            .await
            .map_err(storage_error)?;

        let has_pending = pending.keys.iter().any(|key| !key.ids.is_empty());
        for key in pending.keys {
            Self::process_entries(&mut connection, key.ids).await?;
        }
        if has_pending {
            return Ok(());
        }

        let messages: StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(CONSUMER)
            .arg("COUNT")
            .arg(100)
            .arg("BLOCK")
            .arg(BLOCK_MILLIS)
            .arg("STREAMS")
            .arg(STREAM)
            .arg(">")
            .query_async(&mut connection)
            .await
            .map_err(storage_error)?;
        for key in messages.keys {
            Self::process_entries(&mut connection, key.ids).await?;
        }
        Ok(())
    }

    async fn process_entries(
        connection: &mut MultiplexedConnection,
        entries: Vec<StreamId>,
    ) -> Result<()> {
        for entry in entries {
            if !entry.map.is_empty() {
                tracing::info!(stream_id = %entry.id, fields = ?entry.map, "notification delivered");
            }
            redis::cmd("XACK")
                .arg(STREAM)
                .arg(GROUP)
                .arg(&entry.id)
                .query_async::<u64>(connection)
                .await
                .map_err(storage_error)?;
        }
        Ok(())
    }
}

#[async_trait]
impl GameRepository for RedisStore {
    async fn save_player(&self, player: &Player) -> Result<()> {
        let mut command = redis::cmd("HSET");
        player_fields(&mut command, player);
        let mut pipeline = redis::pipe();
        pipeline.atomic().add_command(command).ignore();
        preserve_creation_time(&mut pipeline, player);
        pipeline
            .query_async::<()>(&mut self.master().await?)
            .await
            .map_err(storage_error)
    }

    async fn find_player(&self, id: u64) -> Result<Option<Player>> {
        let mut fields: HashMap<String, String> = self
            .query(redis::cmd("HGETALL").arg(format!("player:{id}")))
            .await?;
        if fields.is_empty() {
            return Ok(None);
        }
        let name = required_field(&mut fields, "name")?;
        let region = required_field(&mut fields, "region")?;
        let level = required_field(&mut fields, "level")?
            .parse()
            .map_err(|error| AppError::Internal(format!("invalid stored player level: {error}")))?;
        let created_at = required_field(&mut fields, "created_at")?
            .parse()
            .map_err(|error| {
                AppError::Internal(format!("invalid stored creation time: {error}"))
            })?;
        Ok(Some(Player {
            id,
            name,
            level,
            region,
            created_at,
        }))
    }

    async fn increase_level(&self, id: u64, delta: i64) -> Result<i64> {
        self.query(
            redis::cmd("HINCRBY")
                .arg(format!("player:{id}"))
                .arg("level")
                .arg(delta),
        )
        .await
    }

    async fn record_login(&self, id: u64) -> Result<u64> {
        self.query(
            redis::cmd("EVAL")
                .arg(LOGIN_SCRIPT)
                .arg(1)
                .arg(format!("logins:{id}"))
                .arg(LOGIN_TTL),
        )
        .await
    }

    async fn increment_score(&self, id: u64, delta: f64) -> Result<f64> {
        self.query(
            redis::cmd("ZINCRBY")
                .arg("tournament:main")
                .arg(delta)
                .arg(id),
        )
        .await
    }

    async fn leaderboard(&self, limit: usize) -> Result<Vec<Score>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let scores: Vec<(u64, f64)> = self
            .read(
                redis::cmd("ZREVRANGE")
                    .arg("tournament:main")
                    .arg(0)
                    .arg(limit - 1)
                    .arg("WITHSCORES"),
            )
            .await?;
        Ok(scores
            .into_iter()
            .map(|(player_id, score)| Score { player_id, score })
            .collect())
    }

    async fn rank(&self, id: u64) -> Result<Option<usize>> {
        self.read(redis::cmd("ZRANK").arg("tournament:main").arg(id))
            .await
    }

    async fn add_achievement(&self, id: u64, achievement: &str) -> Result<bool> {
        self.query(
            redis::cmd("SADD")
                .arg(format!("achievements:{id}"))
                .arg(achievement),
        )
        .await
    }

    async fn has_achievement(&self, id: u64, achievement: &str) -> Result<bool> {
        self.read(
            redis::cmd("SISMEMBER")
                .arg(format!("achievements:{id}"))
                .arg(achievement),
        )
        .await
    }

    async fn common_achievements(&self, first: u64, second: u64) -> Result<Vec<String>> {
        let mut achievements: Vec<String> = self
            .read(
                redis::cmd("SINTER")
                    .arg(format!("achievements:{first}"))
                    .arg(format!("achievements:{second}")),
            )
            .await?;
        achievements.sort();
        Ok(achievements)
    }

    async fn save_batch(&self, players: &[Player]) -> Result<()> {
        if players.is_empty() {
            return Ok(());
        }
        let mut pipeline = redis::pipe();
        for player in players {
            let mut command = redis::cmd("HSET");
            player_fields(&mut command, player);
            pipeline.add_command(command).ignore();
            preserve_creation_time(&mut pipeline, player);
        }
        pipeline
            .query_async::<()>(&mut self.master().await?)
            .await
            .map_err(storage_error)
    }
}

#[async_trait]
impl ProfileCache for RedisStore {
    async fn get(&self, id: u64) -> Result<Option<Player>> {
        let json: Option<String> = self
            .query(redis::cmd("GET").arg(format!("cache:player:{id}")))
            .await?;
        json.map(|value| {
            serde_json::from_str::<CachedPlayer>(&value)
                .map(Player::from)
                .map_err(|error| AppError::Internal(format!("invalid cached profile: {error}")))
        })
        .transpose()
    }

    async fn set(&self, player: &Player) -> Result<()> {
        let json = serde_json::to_string(&CachedPlayer::from(player))
            .map_err(|error| AppError::Internal(error.to_string()))?;
        self.query(
            redis::cmd("SET")
                .arg(format!("cache:player:{}", player.id))
                .arg(json)
                .arg("EX")
                .arg(CACHE_TTL),
        )
        .await
    }

    async fn invalidate(&self, id: u64) -> Result<()> {
        self.query::<u64>(redis::cmd("DEL").arg(format!("cache:player:{id}")))
            .await?;
        Ok(())
    }

    async fn invalidate_many(&self, ids: &[u64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut pipeline = redis::pipe();
        for id in ids {
            pipeline
                .cmd("DEL")
                .arg(format!("cache:player:{id}"))
                .ignore();
        }
        pipeline
            .query_async::<()>(&mut self.master().await?)
            .await
            .map_err(storage_error)
    }
}

#[async_trait]
impl Notifications for RedisStore {
    async fn publish(&self, notification: &Notification) -> Result<()> {
        self.query::<String>(&mut notification_command(notification))
            .await?;
        Ok(())
    }

    async fn publish_batch(&self, notifications: &[Notification]) -> Result<()> {
        if notifications.is_empty() {
            return Ok(());
        }
        let mut pipeline = redis::pipe();
        for notification in notifications {
            pipeline
                .add_command(notification_command(notification))
                .ignore();
        }
        pipeline
            .query_async::<()>(&mut self.master().await?)
            .await
            .map_err(storage_error)
    }
}

fn notification_command(notification: &Notification) -> Cmd {
    let mut command = redis::cmd("XADD");
    command
        .arg(STREAM)
        .arg("*")
        .arg("player_id")
        .arg(notification.player_id)
        .arg("type")
        .arg(&notification.kind)
        .arg("message")
        .arg(&notification.message)
        .arg("timestamp")
        .arg(notification.timestamp);
    command
}

fn player_fields(command: &mut Cmd, player: &Player) {
    command
        .arg(format!("player:{}", player.id))
        .arg("name")
        .arg(&player.name)
        .arg("level")
        .arg(player.level)
        .arg("region")
        .arg(&player.region);
}

fn preserve_creation_time(pipeline: &mut redis::Pipeline, player: &Player) {
    pipeline
        .cmd("HSETNX")
        .arg(format!("player:{}", player.id))
        .arg("created_at")
        .arg(player.created_at)
        .ignore();
}

fn required_field(fields: &mut HashMap<String, String>, name: &str) -> Result<String> {
    fields
        .remove(name)
        .ok_or_else(|| AppError::Internal(format!("stored profile has no {name} field")))
}

fn storage_error(error: redis::RedisError) -> AppError {
    AppError::Unavailable(error.to_string())
}

#[derive(Serialize, Deserialize)]
struct CachedPlayer {
    id: u64,
    name: String,
    level: i64,
    region: String,
    created_at: u64,
}

impl From<&Player> for CachedPlayer {
    fn from(player: &Player) -> Self {
        Self {
            id: player.id,
            name: player.name.clone(),
            level: player.level,
            region: player.region.clone(),
            created_at: player.created_at,
        }
    }
}

impl From<CachedPlayer> for Player {
    fn from(player: CachedPlayer) -> Self {
        Self {
            id: player.id,
            name: player.name,
            level: player.level,
            region: player.region,
            created_at: player.created_at,
        }
    }
}
