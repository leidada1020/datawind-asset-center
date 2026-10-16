#!/bin/bash
# 部署机: 拉取代码 + 应用层启动 poll_5min(纯应用层, 零系统配置)
set -e
APP=/opt/datawind-asset-center

echo "== 1. 拉取代码 =="
cd $APP && git pull

echo "== 2. 复制应用层脚本到应用目录(不在系统目录) =="
cp $APP/deploy/start.sh $APP/start.sh
cp $APP/deploy/stop.sh $APP/stop.sh
cp $APP/deploy/status.sh $APP/status.sh
chmod +x $APP/start.sh $APP/stop.sh $APP/status.sh

echo "== 3. 清理旧锁/旧PID(应用目录内) =="
rm -f $APP/poll_5min.lock $APP/poll_5min.pid

echo "== 4. 应用层启动 =="
cd $APP && ./start.sh

echo "== 5. 等待首轮(约40s) =="
sleep 40
./status.sh

echo "== 6. 确认无 systemd 依赖 =="
systemctl status datawind-poll --no-pager 2>&1 | head -2 || echo "确认: 无 systemd 服务(纯应用层)"
echo "APP_HOSTING_DONE"
