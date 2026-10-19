#!/bin/bash
# 部署机: 拉取review修复代码 + 重启(零系统配置)
set -e
APP=/opt/datawind-asset-center
echo "== 1. 拉取代码 =="
cd $APP && git pull
echo "== 2. 应用层重启 =="
cd $APP && ./stop.sh || true
sleep 2
./start.sh
sleep 4
echo "== 3. 确认运行 =="
./status.sh
echo "== 4. config兼容性 =="
/opt/datawind-venv/bin/python -c "import sys; sys.path.insert(0,'/opt/datawind-asset-center'); from datawind.config import load_config; c=load_config(); print('config OK: doris_host=%s db=%s' % (c['doris_host'], c['doris_db']))"
echo "REVIEW_FIX_DEPLOY_DONE"
