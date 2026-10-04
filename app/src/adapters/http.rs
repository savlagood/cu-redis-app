use std::{sync::Arc, time::Instant};

use axum::{
    Json, Router,
    extract::{FromRequest, Path, Query, Request, State},
    http::{HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;

use crate::{
    application::GameHub,
    domain::{Player, PlayerInput, Score},
    error::AppError,
};

type Service = State<Arc<GameHub>>;
type HttpResult<T> = Result<T, HttpError>;

pub fn router(service: Arc<GameHub>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/players/{id}", post(upsert_player).get(profile))
        .route("/api/players/{id}/level", patch(increase_level))
        .route("/api/players/{id}/login", post(login))
        .route("/api/leaderboard/score", post(add_score))
        .route("/api/leaderboard/top", get(top))
        .route("/api/leaderboard/rank/{playerId}", get(rank))
        .route("/api/players/{id}/achievements", post(add_achievement))
        .route(
            "/api/players/{id}/achievements/{name}",
            get(has_achievement),
        )
        .route(
            "/api/players/{id1}/achievements/common/{id2}",
            get(common_achievements),
        )
        .route("/api/players/batch", post(batch))
        .with_state(service)
}

struct HttpError(AppError);

impl From<AppError> for HttpError {
    fn from(error: AppError) -> Self {
        Self(error)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            AppError::Validation(message) => (StatusCode::BAD_REQUEST, message),
            AppError::NotFound(id) => (StatusCode::NOT_FOUND, format!("player {id} was not found")),
            AppError::Unavailable(error) => {
                tracing::warn!(%error, "request could not reach storage");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage is temporarily unavailable".to_owned(),
                )
            }
            AppError::Internal(error) => {
                tracing::error!(%error, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_owned(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

/// Keep malformed JSON responses in the same format as application errors.
struct Body<T>(T);

impl<S, T> FromRequest<S> for Body<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request(request: Request, state: &S) -> HttpResult<Self> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|error| HttpError(AppError::Validation(error.body_text())))
    }
}

#[derive(Deserialize)]
struct PlayerBody {
    name: String,
    level: i64,
    region: String,
}

#[derive(Serialize)]
struct PlayerResponse {
    id: u64,
    name: String,
    level: i64,
    region: String,
    created_at: u64,
}

impl From<Player> for PlayerResponse {
    fn from(player: Player) -> Self {
        Self {
            id: player.id,
            name: player.name,
            level: player.level,
            region: player.region,
            created_at: player.created_at,
        }
    }
}

#[derive(Serialize)]
struct ProfileResponse {
    #[serde(flatten)]
    player: PlayerResponse,
    cache_hit: bool,
}

#[derive(Deserialize)]
struct LevelBody {
    delta: i64,
}

#[derive(Deserialize)]
struct ScoreBody {
    player_id: u64,
    score: f64,
}

#[derive(Deserialize)]
struct TopQuery {
    #[serde(default = "default_top_limit")]
    limit: usize,
}

fn default_top_limit() -> usize {
    10
}

#[derive(Serialize)]
struct ScoreResponse {
    player_id: u64,
    score: f64,
}

impl From<Score> for ScoreResponse {
    fn from(score: Score) -> Self {
        Self {
            player_id: score.player_id,
            score: score.score,
        }
    }
}

#[derive(Deserialize)]
struct AchievementBody {
    #[serde(alias = "achievement", alias = "achievement_name")]
    name: String,
}

#[derive(Deserialize)]
struct BatchPlayer {
    id: u64,
    #[serde(flatten)]
    player: PlayerBody,
}

#[derive(Deserialize)]
struct BatchBody {
    players: Vec<BatchPlayer>,
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "running" }))
}

async fn upsert_player(
    State(service): Service,
    Path(id): Path<u64>,
    Body(input): Body<PlayerBody>,
) -> HttpResult<Json<PlayerResponse>> {
    let player = service
        .upsert_player(PlayerInput {
            id,
            name: input.name,
            level: input.level,
            region: input.region,
        })
        .await?;
    Ok(Json(player.into()))
}

async fn profile(State(service): Service, Path(id): Path<u64>) -> HttpResult<Response> {
    let profile = service.profile(id).await?;
    let cache_status = if profile.cache_hit { "HIT" } else { "MISS" };
    let response = ProfileResponse {
        player: profile.player.into(),
        cache_hit: profile.cache_hit,
    };
    Ok((
        [(
            HeaderName::from_static("x-cache"),
            HeaderValue::from_static(cache_status),
        )],
        Json(response),
    )
        .into_response())
}

async fn increase_level(
    State(service): Service,
    Path(id): Path<u64>,
    Body(input): Body<LevelBody>,
) -> HttpResult<Json<serde_json::Value>> {
    let level = service.increase_level(id, input.delta).await?;
    Ok(Json(json!({ "player_id": id, "level": level })))
}

async fn login(
    State(service): Service,
    Path(id): Path<u64>,
) -> HttpResult<Json<serde_json::Value>> {
    let logins = service.login(id).await?;
    Ok(Json(json!({ "player_id": id, "logins": logins })))
}

async fn add_score(
    State(service): Service,
    Body(input): Body<ScoreBody>,
) -> HttpResult<Json<ScoreResponse>> {
    let score = service.add_score(input.player_id, input.score).await?;
    Ok(Json(ScoreResponse {
        player_id: input.player_id,
        score,
    }))
}

async fn top(
    State(service): Service,
    Query(query): Query<TopQuery>,
) -> HttpResult<Json<Vec<ScoreResponse>>> {
    let scores = service.top(query.limit).await?;
    Ok(Json(scores.into_iter().map(ScoreResponse::from).collect()))
}

async fn rank(State(service): Service, Path(id): Path<u64>) -> HttpResult<Json<serde_json::Value>> {
    let rank = service.rank(id).await?;
    Ok(Json(json!({ "player_id": id, "rank": rank })))
}

async fn add_achievement(
    State(service): Service,
    Path(id): Path<u64>,
    Body(input): Body<AchievementBody>,
) -> HttpResult<Json<serde_json::Value>> {
    let added = service.add_achievement(id, input.name).await?;
    Ok(Json(json!({ "added": added })))
}

async fn has_achievement(
    State(service): Service,
    Path((id, name)): Path<(u64, String)>,
) -> HttpResult<Json<serde_json::Value>> {
    let exists = service.has_achievement(id, name).await?;
    Ok(Json(json!({ "exists": exists })))
}

async fn common_achievements(
    State(service): Service,
    Path((first, second)): Path<(u64, u64)>,
) -> HttpResult<Json<serde_json::Value>> {
    let achievements = service.common_achievements(first, second).await?;
    Ok(Json(json!({ "achievements": achievements })))
}

async fn batch(
    State(service): Service,
    Body(input): Body<BatchBody>,
) -> HttpResult<Json<serde_json::Value>> {
    let players = input
        .players
        .into_iter()
        .map(|input| PlayerInput {
            id: input.id,
            name: input.player.name,
            level: input.player.level,
            region: input.player.region,
        })
        .collect();
    let started = Instant::now();
    let created = service.batch(players).await?;
    Ok(Json(
        json!({ "created": created, "elapsed_ms": started.elapsed().as_millis() }),
    ))
}
