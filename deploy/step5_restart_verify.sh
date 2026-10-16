#!/bin/bash
set -e
APP=/opt/datawind-asset-center

echo "== 1. 拉取代码 =="
cd $APP && git pull

echo "== 2. 清理可能残留的锁 =="
rm -f $APP/poll_5min.lock

echo "== 3. 重启服务 =="
systemctl restart datawind-poll

echo "== 4. 等待 15s 确认常驻(不再 auto-restart) =="
sleep 15
systemctl status datawind-poll --no-pager | head -8
echo "---lock---"
ls -la $APP/poll_5min.lock 2>/dev/null && echo LOCK_OK || echo LOCK_MISSING
echo "---log tail---"
tail -n 3 $APP/logs/poll_5min.log
echo "RESTART_DONE"
