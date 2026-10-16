#!/bin/bash
set -e
echo "== timer 恢复 UTC 语义(去掉 Timezone, 保持 UTC 10:30 每日全量) =="
cat > /etc/systemd/system/datawind-daily.timer <<'EOF'
[Unit]
Description=Daily trigger for datawind-daily (UTC 10:30)

[Timer]
OnCalendar=*-*-* 10:30:00
Persistent=true
Unit=datawind-daily.service

[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl restart datawind-daily.timer
systemctl list-timers datawind-daily.timer --no-pager | head -4
echo "TIMER_UTC_DONE"
