#!/bin/sh
set -e

metrics_file="${1:-./metrics.json}"

if [ ! -f "$metrics_file" ]; then
  echo "❌ Metrics file not found: $metrics_file"
  exit 1
fi

echo "📊 Checking metrics in: $metrics_file"

fail=0

check_metric() {
  key="$1"
  threshold="$2"
  label="$3"
  mode="${4:-fail_if_below_threshold}"

  if [ -z "$threshold" ]; then
    echo "⚠️ No threshold for $label"
    return
  fi

  echo "🔍 Checking $label (mode = $mode, threshold = $threshold)..."

  values=$(jq -r --arg key "$key" '.[$key][0]?.values[]? | select(.[1] != "NaN") | .[1]' "$metrics_file")
  if [ -z "$values" ]; then
    echo "❌ Metric '$key' is missing or has no valid values. Stopping checks."
    exit 1
  fi

  sum=0
  count=0
  for value in $values; do
    val=$(echo "$value" | tr -d '"')
    sum=$(echo "$sum + $val" | bc -l)
    count=$((count + 1))
  done

  if [ "$count" -eq 0 ]; then
    echo "⚠️ No valid data points for $label."
    exit 1
  fi

  average=$(echo "$sum / $count" | bc -l)

  case "$mode" in
    fail_if_below_threshold)
      if [ "$(echo "$average < $threshold" | bc -l)" -eq 1 ]; then
        echo "❌ $label ($average) is below threshold $threshold"
        fail=1
        return
      fi
      ;;
    fail_if_above_threshold)
      if [ "$(echo "$average > $threshold" | bc -l)" -eq 1 ]; then
        echo "❌ $label ($average) is above threshold $threshold"
        fail=1
        return
      fi
      ;;
    *)
      echo "❌ Unknown mode: $mode"
      exit 1
      ;;
  esac

  echo "✅ $label passed (average = $average)"
}

check_metric "read_latency_ms" "$READ_LATENCY_MS_THRESHOLD" "Read Latency P95" fail_if_above_threshold
check_metric "write_latency_ms" "$WRITE_LATENCY_MS_THRESHOLD" "Write Latency P95" fail_if_above_threshold
check_metric "read_throughput" "$READ_THROUGHPUT_THRESHOLD" "Read Throughput" fail_if_below_threshold
check_metric "write_throughput" "$WRITE_THROUGHPUT_THRESHOLD" "Write Throughput" fail_if_below_threshold
check_metric "read_attempts" "$READ_ATTEMPTS_THRESHOLD" "Read Attempts" fail_if_above_threshold
check_metric "write_attempts" "$WRITE_ATTEMPTS_THRESHOLD" "Write Attempts" fail_if_above_threshold
check_metric "read_availability" "$READ_AVAILABILITY_THRESHOLD" "Read Availability" fail_if_below_threshold
check_metric "write_availability" "$WRITE_AVAILABILITY_THRESHOLD" "Write Availability" fail_if_below_threshold

if [ "$fail" -eq 1 ]; then
  echo "❗ Some metrics did not meet thresholds."
  exit 1
else
  echo "🎉 All metrics validated successfully."
fi
