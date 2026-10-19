#!/bin/bash
# 部署机: 安装sqlglot到venv + 拉取P1代码 + 应用层重启(零系统配置)
set -e
APP=/opt/datawind-asset-center
VENV=/opt/datawind-venv/bin/python

echo "== 1. venv 安装 sqlglot =="
/opt/datawind-venv/bin/pip install sqlglot 2>&1 | tail -2 || true
$VENV -c "import sqlglot; print('sqlglot', sqlglot.__version__)"

echo "== 2. 拉取代码 =="
cd $APP && git pull

echo "== 3. 同步应用层脚本 =="
cp $APP/deploy/start.sh $APP/start.sh
cp $APP/deploy/stop.sh $APP/stop.sh
cp $APP/deploy/status.sh $APP/status.sh
chmod +x $APP/start.sh $APP/stop.sh $APP/status.sh

echo "== 4. 应用层重启 =="
cd $APP && ./stop.sh || true
sleep 2
./start.sh

echo "== 5. 确认运行 =="
sleep 3
./status.sh
echo "P1_DEPLOY_DONE"
