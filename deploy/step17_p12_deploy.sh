#!/bin/bash
# 部署机: 拉取P1-2闭包代码 + 应用层重启
set -e
APP=/opt/datawind-asset-center
echo "== 1. 拉取代码 =="
cd $APP && git pull
echo "== 2. 同步应用层脚本 =="
cp $APP/deploy/start.sh $APP/start.sh && chmod +x $APP/start.sh
echo "== 3. 应用层重启 =="
cd $APP && ./stop.sh || true
sleep 2
./start.sh
sleep 3
echo "== 4. 确认运行 =="
./status.sh
echo "P12_DEPLOY_DONE"
