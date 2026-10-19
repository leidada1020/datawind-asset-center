#!/bin/bash
# 部署机: P2 Web服务部署(纯应用层)
set -e
APP=/opt/datawind-asset-center
echo "== 1. venv 安装 fastapi/uvicorn =="
/opt/datawind-venv/bin/pip install fastapi uvicorn 2>&1 | tail -2
echo "== 2. 拉取代码 =="
cd $APP && git pull
echo "== 3. 启动 Web 服务 =="
chmod +x web/web_ctl.sh
cd $APP && ./web/web_ctl.sh start
sleep 3
echo "== 4. 本机验证 =="
curl -s -o /dev/null -w "GET / -> %{http_code}\n" http://127.0.0.1:8080/
curl -s -o /dev/null -w "GET /api/stats -> %{http_code}\n" http://127.0.0.1:8080/api/stats
curl -s http://127.0.0.1:8080/api/stats | head -c 300
echo ""
echo "== 5. 状态 =="
./web/web_ctl.sh status
echo "WEB_DEPLOY_DONE"
