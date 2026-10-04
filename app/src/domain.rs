#[derive(Debug, Clone)]
pub struct Player {
    pub id: u64,
    pub name: String,
    pub level: i64,
    pub region: String,
    /// Unix time in seconds.
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct PlayerInput {
    pub id: u64,
    pub name: String,
    pub level: i64,
    pub region: String,
}

#[derive(Debug)]
pub struct Profile {
    pub player: Player,
    pub cache_hit: bool,
}

#[derive(Debug)]
pub struct Score {
    pub player_id: u64,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub player_id: u64,
    pub kind: String,
    pub message: String,
    pub timestamp: u64,
}
