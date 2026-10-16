#!/bin/bash
set -e
VENV=/opt/datawind-venv/bin/python
APP=/opt/datawind-asset-center

echo "== 1. systemd timer 替代 cron(每日10:30 全量落库) =="
cat > /etc/systemd/system/datawind-daily.service <<'EOF'
[Unit]
Description=DataWind Asset Center - Daily Full Snapshot & Change Detect
After=network.target

[Service]
Type=oneshot
User=root
WorkingDirectory=/opt/datawind-asset-center
ExecStart=/opt/datawind-venv/bin/python /opt/datawind-asset-center/run_daily.py
Environment=PYTHONUNBUFFERED=1
EOF

cat > /etc/systemd/system/datawind-daily.timer <<'EOF'
[Unit]
Description=Daily trigger for datawind-daily (10:30)

[Timer]
OnCalendar=*-*-* 10:30:00
Persistent=true
Unit=datawind-daily.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable datawind-daily.timer
systemctl start datawind-daily.timer
systemctl list-timers datawind-daily.timer --no-pager | head -5

echo "== 2. 轮询进程日志验证(应看到首轮轮询完成+基线生成) =="
sleep 5
tail -n 15 $APP/logs/poll_5min.log 2>/dev/null || echo "poll log 尚未生成(等待首轮5分钟)"
ls -la $APP/datawind_poll_baseline.json 2>/dev/null || echo "基线尚未生成(等待首轮)"

echo "== 3. 汇总状态 =="
systemctl status datawind-poll --no-pager | head -5
systemctl status datawind-daily.timer --no-pager | head -5
echo "DAILY_TIMER_DONE"
