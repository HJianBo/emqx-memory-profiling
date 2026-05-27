cat > /tmp/emqx_mem_watch.sh <<'EOF'
#!/usr/bin/env bash

EMQX=/geelyapp/emqx/bin/emqx
LOG=/tmp/emqx_mem_watch.log
INTERVAL=5

# RSS 单次增长超过 1GiB 时触发 emqx eval
RSS_JUMP_THRESHOLD_KB=$((1 * 1024 * 1024))

# RSS 相比脚本启动后的最低值高出 2GiB 时触发 emqx eval
RSS_ABOVE_MIN_THRESHOLD_KB=$((2 * 1024 * 1024))

# 高位期间，最短每 15 秒执行一次 emqx eval
EVAL_COOLDOWN_SEC=15

# 测试用：设为 1 时，每轮都尝试触发 emqx eval，仍受 EVAL_COOLDOWN_SEC 限制。
ALWAYS_EVAL=0

round=0
last_rss_kb=0
min_rss_kb=0
last_eval_epoch=0

top_res_to_kb() {
  awk -v res="$1" '
    BEGIN {
      res = tolower(res)
      gsub(/,/, ".", res)
      unit = substr(res, length(res), 1)
      value = res + 0
      if (unit == "g") {
        printf "%.0f\n", value * 1024 * 1024
      } else if (unit == "m") {
        printf "%.0f\n", value * 1024
      } else if (unit == "t") {
        printf "%.0f\n", value * 1024 * 1024 * 1024
      } else {
        printf "%.0f\n", value
      }
    }
  '
}

safe_emqx_snapshot() {
  timeout 10s "$EMQX" eval 'Mem = erlang:memory(), Allocated = catch recon_alloc:memory(allocated), Used = catch recon_alloc:memory(used), Fragmentation = catch recon_alloc:fragmentation(current), {erlang_memory, Mem, recon_alloc_allocated, Allocated, recon_alloc_used, Used, recon_alloc_fragmentation_current, Fragmentation}.' </dev/null 2>&1
}

while true; do
  NOW=$(date '+%F %T')
  NOW_EPOCH=$(date +%s)
  TOP_OUTPUT=$(top -b -n 1 -o +%MEM | head -12)
  BEAM_TOP_LINE=$(printf '%s\n' "$TOP_OUTPUT" | awk '$NF == "beam.smp" {print; exit}')
  PID=$(printf '%s\n' "$BEAM_TOP_LINE" | awk '{print $1}')
  TOP_RES=$(printf '%s\n' "$BEAM_TOP_LINE" | awk '{print $6}')

  {
    echo "================================================================"
    echo "TIME: $NOW"
    echo "PID: ${PID:-NA}"
    echo "ROUND: $round"
    echo "================================================================"

    echo
    echo "---- top memory ----"
    printf '%s\n' "$TOP_OUTPUT"

    if [ -n "$PID" ]; then
      rss_kb=$(top_res_to_kb "$TOP_RES")
      rss_kb=${rss_kb:-0}

      if [ "$min_rss_kb" -eq 0 ] || [ "$rss_kb" -lt "$min_rss_kb" ]; then
        min_rss_kb=$rss_kb
      fi

      rss_jump_kb=$((rss_kb - last_rss_kb))
      [ "$last_rss_kb" -eq 0 ] && rss_jump_kb=0

      rss_above_min_kb=$((rss_kb - min_rss_kb))
      eval_age_sec=$((NOW_EPOCH - last_eval_epoch))

      echo
      echo "---- rss trigger state ----"
      echo "TOP_RSS_KB: $rss_kb"
      echo "LAST_RSS_KB: $last_rss_kb"
      echo "MIN_RSS_KB: $min_rss_kb"
      echo "RSS_JUMP_KB: $rss_jump_kb"
      echo "RSS_ABOVE_MIN_KB: $rss_above_min_kb"

      trigger_reason=""

      if [ "$ALWAYS_EVAL" -eq 1 ]; then
        trigger_reason="always_eval"
      elif [ "$rss_jump_kb" -gt "$RSS_JUMP_THRESHOLD_KB" ]; then
        trigger_reason="rss_jump_gt_threshold"
      elif [ "$rss_above_min_kb" -gt "$RSS_ABOVE_MIN_THRESHOLD_KB" ]; then
        trigger_reason="rss_above_min_gt_threshold"
      fi

      if [ -n "$trigger_reason" ] && [ "$eval_age_sec" -ge "$EVAL_COOLDOWN_SEC" ]; then
        echo
        echo "---- emqx erlang_memory + recon_alloc snapshot ----"
        echo "TRIGGER_REASON: $trigger_reason"
        echo "EVAL_AGE_SEC: $eval_age_sec"
        safe_emqx_snapshot
        last_eval_epoch=$NOW_EPOCH
      elif [ -n "$trigger_reason" ]; then
        echo
        echo "---- emqx eval delayed ----"
        echo "TRIGGER_REASON: $trigger_reason"
        echo "EVAL_AGE_SEC: $eval_age_sec"
        echo "EVAL_COOLDOWN_SEC: $EVAL_COOLDOWN_SEC"
      else
        echo
        echo "---- emqx eval skipped ----"
        echo "TRIGGER_REASON: none"
        echo "EVAL_AGE_SEC: $eval_age_sec"
      fi

      last_rss_kb=$rss_kb
    else
      echo
      echo "beam.smp not found in top output"
    fi

    echo
  } >> "$LOG" 2>&1

  round=$((round + 1))
  sleep "$INTERVAL"
done
EOF

chmod +x /tmp/emqx_mem_watch.sh
