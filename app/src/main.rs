mod adapters;
mod application;
mod config;
mod domain;
mod error;
mod ports;

use std::{error::Error, sync::Arc};

use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;

use crate::{
    adapters::{http, redis::RedisStore},
    application::GameHub,
    config::Config,
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::new("gamehub=info"),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(std::io::Error::other)?;

    let config = Config::from_env()?;

    let store = Arc::new(RedisStore::new(config.redis)?);
    store.initialize().await?;

    let service = Arc::new(GameHub::new(store.clone(), store.clone(), store.clone()));

    let listener = TcpListener::bind(config.listen).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_store = store.clone();
    let worker = tokio::spawn(async move {
        worker_store.consume_notifications(shutdown_rx).await;
    });

    tracing::info!(address = %config.listen, "GameHub is listening");
    let signal_tx = shutdown_tx.clone();

    let result = axum::serve(listener, http::router(service))
        .with_graceful_shutdown(async move {
            if let Err(error) = shutdown_signal().await {
                tracing::error!(%error, "Failed to listen for shutdown signal");
            }
            let _ = signal_tx.send(true);
        })
        .await;

    let _ = shutdown_tx.send(true);
    worker.await?;
    result?;
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
