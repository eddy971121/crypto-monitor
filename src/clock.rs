use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::types::now_utc_ms;

const NTP_PACKET_LEN: usize = 48;
const NTP_UNIX_EPOCH_OFFSET_SECS: i64 = 2_208_988_800;

static CLOCK_OFFSET_MS: AtomicI64 = AtomicI64::new(0);

pub fn corrected_utc_ms() -> i64 {
    now_utc_ms().saturating_add(CLOCK_OFFSET_MS.load(Ordering::Relaxed))
}

pub async fn run_ntp_slew(config: AppConfig) -> Result<()> {
    if !config.ntp_slew_enabled {
        info!("ntp slew is disabled; recv_ts_ms uses local wall clock");
        return Ok(());
    }

    info!(
        ntp_server = %config.ntp_server,
        poll_interval_secs = config.ntp_poll_interval_secs,
        timeout_ms = config.ntp_timeout_ms,
        slew_alpha = config.ntp_slew_alpha,
        slew_max_step_ms = config.ntp_slew_max_step_ms,
        "starting ntp slew loop"
    );

    loop {
        match query_ntp_offset_ms(
            config.ntp_server.as_str(),
            Duration::from_millis(config.ntp_timeout_ms),
        )
        .await
        {
            Ok(measured_offset_ms) => {
                let applied_offset_ms = apply_slew_update(
                    measured_offset_ms,
                    config.ntp_slew_alpha,
                    config.ntp_slew_max_step_ms,
                );

                info!(
                    ntp_server = %config.ntp_server,
                    measured_offset_ms,
                    applied_offset_ms,
                    "updated local recv timestamp slew offset"
                );
            }
            Err(error) => {
                warn!(
                    ntp_server = %config.ntp_server,
                    %error,
                    "failed to sample NTP offset"
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(config.ntp_poll_interval_secs)).await;
    }
}

async fn query_ntp_offset_ms(server: &str, timeout_duration: Duration) -> Result<i64> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("failed to bind local UDP socket for NTP")?;

    socket
        .connect(server)
        .await
        .with_context(|| format!("failed to connect to NTP server {server}"))?;

    let mut request = [0_u8; NTP_PACKET_LEN];
    request[0] = 0x23; // LI=0, VN=4, Mode=3(client)

    let t1_system = SystemTime::now();
    let (t1_ntp_secs, t1_ntp_frac) = system_time_to_ntp_parts(t1_system)?;
    request[40..44].copy_from_slice(&t1_ntp_secs.to_be_bytes());
    request[44..48].copy_from_slice(&t1_ntp_frac.to_be_bytes());

    timeout(timeout_duration, socket.send(&request))
        .await
        .context("timed out while sending NTP request")?
        .context("failed to send NTP request")?;

    let mut response = [0_u8; NTP_PACKET_LEN];
    let recv_len = timeout(timeout_duration, socket.recv(&mut response))
        .await
        .context("timed out while waiting for NTP response")?
        .context("failed to receive NTP response")?;

    let t4_system = SystemTime::now();

    if recv_len < NTP_PACKET_LEN {
        return Err(anyhow::anyhow!(
            "NTP response too short: expected at least {NTP_PACKET_LEN} bytes, got {recv_len}"
        ));
    }

    let t1_ms = system_time_to_unix_ms(t1_system)?;
    let t4_ms = system_time_to_unix_ms(t4_system)?;
    let t2_ms = read_ntp_unix_ms(&response[32..40])?;
    let t3_ms = read_ntp_unix_ms(&response[40..48])?;

    // NTP clock offset estimate: ((t2 - t1) + (t3 - t4)) / 2
    let offset_sum = (i128::from(t2_ms) - i128::from(t1_ms))
        + (i128::from(t3_ms) - i128::from(t4_ms));
    let offset_ms = offset_sum / 2;

    i64::try_from(offset_ms).context("NTP offset conversion overflow")
}

fn apply_slew_update(measured_offset_ms: i64, alpha: f64, max_step_ms: i64) -> i64 {
    let previous = CLOCK_OFFSET_MS.load(Ordering::Relaxed);
    let target_delta = measured_offset_ms.saturating_sub(previous);

    let blended_delta = ((target_delta as f64) * alpha).round() as i64;
    let bounded_delta = if blended_delta == 0 && target_delta != 0 {
        target_delta.signum()
    } else {
        blended_delta
    }
    .clamp(-max_step_ms, max_step_ms);

    let next = previous.saturating_add(bounded_delta);
    CLOCK_OFFSET_MS.store(next, Ordering::Relaxed);
    next
}

fn system_time_to_unix_ms(time: SystemTime) -> Result<i64> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .context("system clock appears before UNIX epoch")?;
    i64::try_from(duration.as_millis()).context("UNIX millisecond conversion overflow")
}

fn system_time_to_ntp_parts(time: SystemTime) -> Result<(u32, u32)> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .context("system clock appears before UNIX epoch")?;

    let ntp_secs = duration
        .as_secs()
        .checked_add(u64::try_from(NTP_UNIX_EPOCH_OFFSET_SECS).expect("constant is positive"))
        .context("NTP seconds conversion overflow")?;
    let ntp_frac = ((u128::from(duration.subsec_nanos())) << 32) / 1_000_000_000_u128;

    Ok((
        u32::try_from(ntp_secs).context("NTP seconds overflow u32")?,
        u32::try_from(ntp_frac).context("NTP fraction overflow u32")?,
    ))
}

fn read_ntp_unix_ms(bytes: &[u8]) -> Result<i64> {
    if bytes.len() != 8 {
        return Err(anyhow::anyhow!(
            "invalid NTP timestamp length: expected 8 bytes, got {}",
            bytes.len()
        ));
    }

    let secs = u32::from_be_bytes(bytes[0..4].try_into().expect("slice length checked"));
    let frac = u32::from_be_bytes(bytes[4..8].try_into().expect("slice length checked"));

    let unix_secs = i64::from(secs) - NTP_UNIX_EPOCH_OFFSET_SECS;
    let frac_ms = ((u64::from(frac) * 1000_u64) >> 32) as i64;

    Ok(unix_secs.saturating_mul(1000).saturating_add(frac_ms))
}

#[cfg(test)]
mod tests {
    use super::apply_slew_update;

    #[test]
    fn slew_update_moves_toward_target_with_bounds() {
        super::CLOCK_OFFSET_MS.store(0, std::sync::atomic::Ordering::Relaxed);

        let offset_1 = apply_slew_update(300, 0.5, 25);
        assert_eq!(offset_1, 25);

        let offset_2 = apply_slew_update(300, 0.5, 25);
        assert_eq!(offset_2, 50);
    }

    #[test]
    fn slew_update_moves_negative_direction() {
        super::CLOCK_OFFSET_MS.store(100, std::sync::atomic::Ordering::Relaxed);

        let offset = apply_slew_update(-100, 1.0, 30);
        assert_eq!(offset, 70);
    }
}