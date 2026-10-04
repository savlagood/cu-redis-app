use async_trait::async_trait;

use crate::{
    domain::{Notification, Player, Score},
    error::Result,
};

#[async_trait]
pub trait GameRepository: Send + Sync {
    async fn save_player(&self, player: &Player) -> Result<()>;
    async fn find_player(&self, id: u64) -> Result<Option<Player>>;
    async fn increase_level(&self, id: u64, delta: i64) -> Result<i64>;
    async fn record_login(&self, id: u64) -> Result<u64>;
    async fn increment_score(&self, id: u64, delta: f64) -> Result<f64>;
    async fn leaderboard(&self, limit: usize) -> Result<Vec<Score>>;

    async fn rank(&self, id: u64) -> Result<Option<usize>>;
    async fn add_achievement(&self, id: u64, achievement: &str) -> Result<bool>;
    async fn has_achievement(&self, id: u64, achievement: &str) -> Result<bool>;
    async fn common_achievements(&self, first: u64, second: u64) -> Result<Vec<String>>;

    async fn save_batch(&self, players: &[Player]) -> Result<()>;
}

#[async_trait]
pub trait ProfileCache: Send + Sync {
    async fn get(&self, id: u64) -> Result<Option<Player>>;
    async fn set(&self, player: &Player) -> Result<()>;
    async fn invalidate(&self, id: u64) -> Result<()>;
    async fn invalidate_many(&self, ids: &[u64]) -> Result<()>;
}

#[async_trait]
pub trait Notifications: Send + Sync {
    async fn publish(&self, notification: &Notification) -> Result<()>;
    async fn publish_batch(&self, notifications: &[Notification]) -> Result<()>;
}
