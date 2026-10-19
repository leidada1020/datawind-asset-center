#!/bin/bash
# 部署机: 拉取review2 change_id修复 + 重启
set -e
APP=/opt/datawind-asset-center
echo "== 1. 拉取代码 =="
cd $APP && git pull
echo "== 2. 应用层重启 =="
cd $APP && ./stop.sh || true
sleep 2
./start.sh
sleep 3
echo "== 3. 确认运行 =="
./status.sh
echo "REVIEW2_DEPLOY_DONE"
