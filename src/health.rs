#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use tracing::info;
use tracing::warn;

use crate::config::AppConfig;

#[derive(Debug, Clone)]
enum SystemdNotifySocket {
    FilesystemPath(String),
    #[cfg(target_os = "linux")]
    AbstractName(Vec<u8>),
}

pub async fn run_health_reporter(config: AppConfig) -> Result<()> {
    let mut health_ticker = tokio::time::interval(Duration::from_secs(config.health_report_interval_secs));
    let systemd_notify_socket = if config.systemd_notify_enabled {
        systemd_notify_socket_target()
    } else {
        None
    };
    let watchdog_ping_interval = if systemd_notify_socket.is_some() {
        systemd_watchdog_ping_interval()
    } else {
        None
    };

    if let Some(socket_target) = systemd_notify_socket.as_ref() {
        notify_systemd(socket_target, "READY=1");
        if let Some(interval) = watchdog_ping_interval {
            info!(
                watchdog_ping_interval_secs = interval.as_secs_f64(),
                "systemd watchdog ping loop enabled"
            );
        }
    }

    let mut watchdog_ticker = watchdog_ping_interval.map(tokio::time::interval);

    loop {
        if let Some(ticker) = watchdog_ticker.as_mut() {
            tokio::select! {
                _ = health_ticker.tick() => {
                    emit_health_heartbeat(&config);
                }
                _ = ticker.tick() => {
                    if let Some(socket_target) = systemd_notify_socket.as_ref() {
                        notify_systemd(socket_target, "WATCHDOG=1");
                    }
                }
            }
        } else {
            health_ticker.tick().await;
            emit_health_heartbeat(&config);
        }
    }
}

fn emit_health_heartbeat(config: &AppConfig) {
    info!(
        exchange = %config.exchange,
        market = %config.market,
        canonical_symbol = %config.canonical_symbol,
        exchange_symbol = %config.symbol,
        depth_stream_enabled = config.enable_depth_stream,
        book_ticker_stream_enabled = config.enable_book_ticker_stream,
        agg_trade_stream_enabled = config.enable_agg_trade_stream,
        cancel_heuristic = ?config.cancel_heuristic,
        metrics_backend = ?config.metrics_backend,
        "service health heartbeat"
    );
}

fn systemd_watchdog_ping_interval() -> Option<Duration> {
    let watchdog_usec = std::env::var("WATCHDOG_USEC")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    if watchdog_usec == 0 {
        return None;
    }

    let ping_usec = (watchdog_usec / 2).max(1_000_000);
    Some(Duration::from_micros(ping_usec))
}

fn systemd_notify_socket_target() -> Option<SystemdNotifySocket> {
    let notify_socket = std::env::var("NOTIFY_SOCKET").ok()?;
    parse_systemd_notify_socket(notify_socket.trim())
}

fn parse_systemd_notify_socket(notify_socket: &str) -> Option<SystemdNotifySocket> {
    if notify_socket.is_empty() {
        return None;
    }

    if let Some(abstract_name) = notify_socket.strip_prefix('@') {
        if abstract_name.is_empty() {
            warn!(
                notify_socket,
                "NOTIFY_SOCKET abstract namespace address is empty; systemd notify is disabled"
            );
            return None;
        }

        #[cfg(target_os = "linux")]
        {
            return Some(SystemdNotifySocket::AbstractName(
                abstract_name.as_bytes().to_vec(),
            ));
        }

        #[cfg(not(target_os = "linux"))]
        {
            warn!(
                notify_socket,
                "NOTIFY_SOCKET abstract namespace addresses are unsupported on this platform"
            );
            return None;
        }
    }

    Some(SystemdNotifySocket::FilesystemPath(
        notify_socket.to_string(),
    ))
}

fn notify_systemd(socket_target: &SystemdNotifySocket, payload: &str) {
    let socket = match UnixDatagram::unbound() {
        Ok(socket) => socket,
        Err(error) => {
            warn!(%error, "failed to create UNIX datagram for systemd notify");
            return;
        }
    };

    match socket_target {
        SystemdNotifySocket::FilesystemPath(socket_path) => {
            if let Err(error) = socket.send_to(payload.as_bytes(), Path::new(socket_path)) {
                warn!(
                    %error,
                    socket_path,
                    payload,
                    "failed to emit systemd notify payload"
                );
            }
        }
        #[cfg(target_os = "linux")]
        SystemdNotifySocket::AbstractName(socket_name) => {
            let socket_addr = match SocketAddr::from_abstract_name(socket_name) {
                Ok(socket_addr) => socket_addr,
                Err(error) => {
                    warn!(
                        %error,
                        socket_name = %String::from_utf8_lossy(socket_name),
                        "failed to build abstract namespace socket address for systemd notify"
                    );
                    return;
                }
            };

            if let Err(error) = socket.send_to_addr(payload.as_bytes(), &socket_addr) {
                warn!(
                    %error,
                    socket_name = %String::from_utf8_lossy(socket_name),
                    payload,
                    "failed to emit systemd notify payload"
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        _ => {
            warn!(payload, "systemd notify target is unsupported on this platform");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_systemd_notify_socket, SystemdNotifySocket};

    #[test]
    fn parses_filesystem_notify_socket() {
        let target = parse_systemd_notify_socket("/run/systemd/notify")
            .expect("filesystem notify socket should parse");

        match target {
            SystemdNotifySocket::FilesystemPath(path) => {
                assert_eq!(path, "/run/systemd/notify");
            }
            #[cfg(target_os = "linux")]
            SystemdNotifySocket::AbstractName(_) => {
                panic!("expected filesystem socket target");
            }
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn parses_abstract_notify_socket() {
        let target = parse_systemd_notify_socket("@systemd/notify")
            .expect("abstract notify socket should parse");

        match target {
            SystemdNotifySocket::AbstractName(name) => {
                assert_eq!(name, b"systemd/notify");
            }
            SystemdNotifySocket::FilesystemPath(_) => {
                panic!("expected abstract socket target");
            }
        }
    }

    #[test]
    fn rejects_empty_notify_socket() {
        assert!(parse_systemd_notify_socket("").is_none());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn rejects_empty_abstract_notify_socket() {
        assert!(parse_systemd_notify_socket("@").is_none());
    }
}
