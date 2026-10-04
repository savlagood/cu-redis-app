use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    domain::{Notification, Player, PlayerInput, Profile, Score},
    error::{AppError, Result},
    ports::{GameRepository, Notifications, ProfileCache},
};

pub struct GameHub {
    repository: Arc<dyn GameRepository>,
    cache: Arc<dyn ProfileCache>,
    notifications: Arc<dyn Notifications>,
}

impl GameHub {
    pub fn new(
        repository: Arc<dyn GameRepository>,
        cache: Arc<dyn ProfileCache>,
        notifications: Arc<dyn Notifications>,
    ) -> Self {
        Self {
            repository,
            cache,
            notifications,
        }
    }

    pub async fn upsert_player(&self, input: PlayerInput) -> Result<Player> {
        validate_player(&input)?;
        let created_at = match self.repository.find_player(input.id).await? {
            Some(player) => player.created_at,
            None => now()?,
        };
        let player = into_player(input, created_at);
        self.repository.save_player(&player).await?;
        self.cache.invalidate(player.id).await?;
        self.notify(
            player.id,
            "profile_updated",
            "Player profile updated".into(),
        )
        .await?;
        Ok(player)
    }

    pub async fn profile(&self, id: u64) -> Result<Profile> {
        validate_id(id)?;
        if let Some(player) = self.cache.get(id).await? {
            return Ok(Profile {
                player,
                cache_hit: true,
            });
        }
        let player = self.require_player(id).await?;
        self.cache.set(&player).await?;
        Ok(Profile {
            player,
            cache_hit: false,
        })
    }

    pub async fn increase_level(&self, id: u64, delta: i64) -> Result<i64> {
        self.require_player(id).await?;
        let level = self.repository.increase_level(id, delta).await?;
        self.cache.invalidate(id).await?;
        self.notify(
            id,
            "level_changed",
            format!("Player level changed by {delta} to {level}"),
        )
        .await?;
        Ok(level)
    }

    pub async fn login(&self, id: u64) -> Result<u64> {
        self.require_player(id).await?;
        self.repository.record_login(id).await
    }

    pub async fn add_score(&self, id: u64, score: f64) -> Result<f64> {
        if !score.is_finite() {
            return Err(AppError::Validation("score must be finite".into()));
        }
        self.require_player(id).await?;
        self.repository.increment_score(id, score).await
    }

    pub async fn top(&self, limit: usize) -> Result<Vec<Score>> {
        if !(1..=1000).contains(&limit) {
            return Err(AppError::Validation(
                "limit must be between 1 and 1000".into(),
            ));
        }
        self.repository.leaderboard(limit).await
    }

    pub async fn rank(&self, id: u64) -> Result<Option<usize>> {
        validate_id(id)?;
        self.repository.rank(id).await
    }

    pub async fn add_achievement(&self, id: u64, name: String) -> Result<bool> {
        validate_text(&name, "achievement", 100)?;
        self.require_player(id).await?;
        self.repository.add_achievement(id, name.trim()).await
    }

    pub async fn has_achievement(&self, id: u64, name: String) -> Result<bool> {
        validate_id(id)?;
        validate_text(&name, "achievement", 100)?;
        self.repository.has_achievement(id, name.trim()).await
    }

    pub async fn common_achievements(&self, first: u64, second: u64) -> Result<Vec<String>> {
        validate_id(first)?;
        validate_id(second)?;
        let mut names = self.repository.common_achievements(first, second).await?;
        names.sort();
        Ok(names)
    }

    pub async fn batch(&self, inputs: Vec<PlayerInput>) -> Result<usize> {
        if inputs.is_empty() || inputs.len() > 1000 {
            return Err(AppError::Validation(
                "batch must contain between 1 and 1000 players".into(),
            ));
        }
        let mut ids = HashSet::with_capacity(inputs.len());
        for input in &inputs {
            validate_player(input)?;
            if !ids.insert(input.id) {
                return Err(AppError::Validation(format!(
                    "duplicate player id: {}",
                    input.id
                )));
            }
        }
        let timestamp = now()?;
        let players: Vec<_> = inputs
            .into_iter()
            .map(|input| into_player(input, timestamp))
            .collect();

        self.repository.save_batch(&players).await?;
        self.cache
            .invalidate_many(&players.iter().map(|player| player.id).collect::<Vec<_>>())
            .await?;

        let notifications: Vec<_> = players
            .iter()
            .map(|player| Notification {
                player_id: player.id,
                kind: "profile_updated".into(),
                message: "Player profile updated in batch".into(),
                timestamp,
            })
            .collect();
        self.notifications.publish_batch(&notifications).await?;

        Ok(players.len())
    }

    async fn require_player(&self, id: u64) -> Result<Player> {
        validate_id(id)?;
        self.repository
            .find_player(id)
            .await?
            .ok_or(AppError::NotFound(id))
    }

    async fn notify(&self, player_id: u64, kind: &str, message: String) -> Result<()> {
        self.notifications
            .publish(&Notification {
                player_id,
                kind: kind.into(),
                message,
                timestamp: now()?,
            })
            .await
    }
}

fn validate_id(id: u64) -> Result<()> {
    if id == 0 {
        return Err(AppError::Validation("player id must be positive".into()));
    }
    Ok(())
}

fn validate_player(player: &PlayerInput) -> Result<()> {
    validate_id(player.id)?;
    validate_text(&player.name, "name", 100)?;
    validate_text(&player.region, "region", 32)
}

fn validate_text(value: &str, field: &str, maximum: usize) -> Result<()> {
    let length = value.trim().chars().count();
    if length == 0 || length > maximum {
        return Err(AppError::Validation(format!(
            "{field} must contain between 1 and {maximum} characters"
        )));
    }
    Ok(())
}

fn into_player(input: PlayerInput, created_at: u64) -> Player {
    Player {
        id: input.id,
        name: input.name.trim().into(),
        level: input.level,
        region: input.region.trim().into(),
        created_at,
    }
}

fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| AppError::Internal(error.to_string()))
}
