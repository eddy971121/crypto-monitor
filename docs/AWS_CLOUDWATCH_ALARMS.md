# AWS CloudWatch Metric Filters and Alarms

This guide defines practical CloudWatch metric filters and alarms for `crypto-monitor` running on EC2.

It assumes:

- systemd unit `crypto-monitor@.service` is used
- logs are shipped to CloudWatch Logs (CloudWatch Agent or equivalent)
- runtime uses `APP_LOG_FORMAT=json`

## 1. Prepare Variables

Set these variables in your shell before creating filters and alarms:

```bash
LOG_GROUP="/aws/ec2/crypto-monitor"
NAMESPACE="CryptoMonitor"
ALARM_TOPIC_ARN="arn:aws:sns:ap-northeast-1:123456789012:crypto-monitor-alerts"
```

## 2. Metric Filter: Reconnect Storm

This filter increments when a telemetry line reports reconnect scheduling activity.

```bash
aws logs put-metric-filter \
  --log-group-name "$LOG_GROUP" \
  --filter-name "crypto-monitor-reconnect-scheduled" \
  --filter-pattern '{ $.fields.message = "runtime telemetry" && $.fields.ws_reconnect_scheduled > 0 }' \
  --metric-transformations metricName=ReconnectScheduled,metricNamespace="$NAMESPACE",metricValue='$.fields.ws_reconnect_scheduled',defaultValue=0
```

Create alarm:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "crypto-monitor-reconnect-storm" \
  --metric-name ReconnectScheduled \
  --namespace "$NAMESPACE" \
  --statistic Sum \
  --period 60 \
  --evaluation-periods 5 \
  --datapoints-to-alarm 3 \
  --threshold 25 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALARM_TOPIC_ARN"
```

## 3. Metric Filter: Idle Timeout Events

This filter tracks dead-man timeout triggers.

```bash
aws logs put-metric-filter \
  --log-group-name "$LOG_GROUP" \
  --filter-name "crypto-monitor-ws-idle-timeouts" \
  --filter-pattern '{ $.fields.message = "runtime telemetry" && $.fields.ws_idle_timeouts > 0 }' \
  --metric-transformations metricName=WsIdleTimeouts,metricNamespace="$NAMESPACE",metricValue='$.fields.ws_idle_timeouts',defaultValue=0
```

Create alarm:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "crypto-monitor-ws-idle-timeouts" \
  --metric-name WsIdleTimeouts \
  --namespace "$NAMESPACE" \
  --statistic Sum \
  --period 60 \
  --evaluation-periods 5 \
  --datapoints-to-alarm 3 \
  --threshold 5 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALARM_TOPIC_ARN"
```

## 4. Metric Filter: S3 Upload Failures

```bash
aws logs put-metric-filter \
  --log-group-name "$LOG_GROUP" \
  --filter-name "crypto-monitor-s3-upload-failures" \
  --filter-pattern '{ $.fields.message = "runtime telemetry" && $.fields.s3_upload_failures > 0 }' \
  --metric-transformations metricName=S3UploadFailures,metricNamespace="$NAMESPACE",metricValue='$.fields.s3_upload_failures',defaultValue=0
```

Create alarm:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "crypto-monitor-s3-upload-failures" \
  --metric-name S3UploadFailures \
  --namespace "$NAMESPACE" \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALARM_TOPIC_ARN"
```

## 5. Metric Filter: SLO Breach Warnings

Track warnings emitted by telemetry for latency and loss-rate thresholds.

Loss-rate warning count:

```bash
aws logs put-metric-filter \
  --log-group-name "$LOG_GROUP" \
  --filter-name "crypto-monitor-loss-rate-warnings" \
  --filter-pattern '{ $.fields.message = "loss rate is above target threshold" }' \
  --metric-transformations metricName=LossRateWarnings,metricNamespace="$NAMESPACE",metricValue=1,defaultValue=0
```

Latency warning count:

```bash
aws logs put-metric-filter \
  --log-group-name "$LOG_GROUP" \
  --filter-name "crypto-monitor-latency-warnings" \
  --filter-pattern '{ $.fields.message = "p99 ingest-to-signal latency is above target threshold" }' \
  --metric-transformations metricName=P99LatencyWarnings,metricNamespace="$NAMESPACE",metricValue=1,defaultValue=0
```

## 6. Heartbeat Absence Alarm (No Data)

Use existing `service health heartbeat` log lines to build an uptime metric.

```bash
aws logs put-metric-filter \
  --log-group-name "$LOG_GROUP" \
  --filter-name "crypto-monitor-heartbeat" \
  --filter-pattern '{ $.fields.message = "service health heartbeat" }' \
  --metric-transformations metricName=HealthHeartbeat,metricNamespace="$NAMESPACE",metricValue=1,defaultValue=0
```

Alarm when heartbeat datapoints are missing:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "crypto-monitor-heartbeat-missing" \
  --metric-name HealthHeartbeat \
  --namespace "$NAMESPACE" \
  --statistic Sum \
  --period 60 \
  --evaluation-periods 5 \
  --threshold 1 \
  --comparison-operator LessThanThreshold \
  --treat-missing-data breaching \
  --alarm-actions "$ALARM_TOPIC_ARN"
```

## 7. Validation Queries

Check that JSON fields are present in log events:

```bash
aws logs tail "$LOG_GROUP" --since 10m --format short
```

Check alarm state:

```bash
aws cloudwatch describe-alarms --alarm-names \
  crypto-monitor-reconnect-storm \
  crypto-monitor-ws-idle-timeouts \
  crypto-monitor-s3-upload-failures \
  crypto-monitor-heartbeat-missing
```

## 8. Operational Tuning

- Increase reconnect thresholds if running many profiles in one instance.
- Keep heartbeat interval and alarm period aligned (default heartbeat is 30s).
- Use profile-specific log groups if you want per-symbol alarm isolation.
