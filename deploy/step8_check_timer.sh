#!/bin/bash
echo "=== 当前 timer 配置 ==="
cat /etc/systemd/system/datawind-daily.timer
echo "=== timer 状态 ==="
systemctl list-timers datawind-daily.timer --no-pager | head -4
