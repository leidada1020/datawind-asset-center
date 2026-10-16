#!/bin/bash
set -e
VENV=/opt/datawind-venv/bin/python
APP=/opt/datawind-asset-center
cd $APP

echo "== 1. 代码导入自检 =="
$VENV -c "
import sys; sys.path.insert(0, '$APP')
from datawind.config import load_config
from datawind.client import DataWindClient
from datawind import ddl
cfg = load_config()
c = DataWindClient(cfg)
print('import OK, app_id=%s' % cfg['app_id'])
"

echo "== 2. JWT + 轻量列表验证(只拉列表不落库) =="
$VENV -c "
import sys; sys.path.insert(0, '$APP')
from datawind.config import load_config
from datawind.client import DataWindClient
cfg = load_config()
c = DataWindClient(cfg)
ds = c.list_datasets()
print('list_datasets OK, count=%d' % len(ds))
"

echo "== 3. systemd unit 部署 =="
cat > /etc/systemd/system/datawind-poll.service <<'EOF'
[Unit]
Description=DataWind Asset Center - 5min Dataset Poller
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/datawind-asset-center
ExecStart=/opt/datawind-venv/bin/python /opt/datawind-asset-center/poll_5min.py --interval 300
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable datawind-poll
systemctl restart datawind-poll
sleep 8
systemctl status datawind-poll --no-pager | head -12

echo "== 4. cron 每日全量(10:30) =="
( crontab -l 2>/dev/null | grep -v datawind-asset-center || true
  echo "30 10 * * * cd /opt/datawind-asset-center && /opt/datawind-venv/bin/python run_daily.py >> /opt/datawind-asset-center/logs/run_daily.log 2>&1"
) | crontab -
crontab -l | grep datawind-asset-center

echo "== 5. 目录/权限检查 =="
mkdir -p $APP/logs
chmod 600 $APP/config/datawind_monitor_config.json
ls -la $APP/config $APP/logs
echo "DEPLOY_DONE"
