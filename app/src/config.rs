use std::{env, net::SocketAddr, str::FromStr, time::Duration};

use crate::{
    adapters::redis::RedisSettings,
    error::{AppError, Result},
};

pub struct Config {
    pub listen: SocketAddr,
    pub redis: RedisSettings,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let sentinel_urls: Vec<_> = value(
            "REDIS_SENTINELS",
            "redis://sentinel-1:26379,redis://sentinel-2:26379,redis://sentinel-3:26379",
        )?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
        if sentinel_urls.is_empty() {
            return Err(AppError::Validation(
                "REDIS_SENTINELS must contain at least one URL".into(),
            ));
        }

        let master_name = value("REDIS_MASTER_NAME", "mymaster")?;
        if master_name.trim().is_empty() {
            return Err(AppError::Validation(
                "REDIS_MASTER_NAME must not be empty".into(),
            ));
        }

        Ok(Self {
            listen: parse("HTTP_ADDR", "0.0.0.0:8080")?,
            redis: RedisSettings {
                sentinel_urls,
                master_name,
                connect_timeout: Duration::from_millis(positive(
                    "REDIS_CONNECT_TIMEOUT_MS",
                    "2000",
                )?),
                response_timeout: Duration::from_millis(positive(
                    "REDIS_RESPONSE_TIMEOUT_MS",
                    "5000",
                )?),
                connect_attempts: parse_positive_usize("REDIS_CONNECT_ATTEMPTS", "5")?,
                retry_delay: Duration::from_millis(positive("REDIS_RETRY_DELAY_MS", "500")?),
            },
        })
    }
}

fn value(name: &str, default: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.into()),
        Err(error) => Err(AppError::Validation(format!("{name}: {error}"))),
    }
}

fn parse<T: FromStr>(name: &str, default: &str) -> Result<T> {
    value(name, default)?
        .parse()
        .map_err(|_| AppError::Validation(format!("invalid {name}")))
}

fn positive(name: &str, default: &str) -> Result<u64> {
    let number = parse(name, default)?;
    if number == 0 {
        return Err(AppError::Validation(format!("{name} must be positive")));
    }
    Ok(number)
}

fn parse_positive_usize(name: &str, default: &str) -> Result<usize> {
    usize::try_from(positive(name, default)?)
        .map_err(|_| AppError::Validation(format!("{name} is too large")))
}
