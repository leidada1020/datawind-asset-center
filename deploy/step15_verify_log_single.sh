#!/bin/bash
# 部署机: 拉取 + 重启(应用层) + 验证日志单写
set -e
APP=/opt/datawind-asset-center
echo "== 1. 拉取代码 =="
cd $APP && git pull
echo "== 2. 同步应用层脚本 =="
cp $APP/deploy/start.sh $APP/start.sh
chmod +x $APP/start.sh
echo "== 3. 应用层重启 =="
cd $APP && ./stop.sh || true
sleep 2
./start.sh
echo "== 4. 等待一轮轮询(约45s) =="
sleep 45
echo "== 5. 最近日志(启动行与轮询行应各只出现一次) =="
tail -n 4 $APP/logs/poll_5min.log
echo "== 6. 进程数(应为1) =="
ps aux | grep "$APP/poll_5min.py" | grep -v grep | wc -l
echo "LOG_SINGLE_DONE"
