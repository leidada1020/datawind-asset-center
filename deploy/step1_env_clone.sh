#!/bin/bash
set -e
echo "== 1. 创建 venv 并安装 pymysql =="
python3 -m venv /opt/datawind-venv
/opt/datawind-venv/bin/pip install --upgrade pip -q
/opt/datawind-venv/bin/pip install pymysql -q
/opt/datawind-venv/bin/python -c "import pymysql; print('pymysql', pymysql.__version__)"

echo "== 2. 创建部署目录并克隆代码 =="
mkdir -p /opt/datawind-asset-center
cd /opt/datawind-asset-center
if [ -d .git ]; then
  git pull
else
  git clone git@github.com:leidada1020/datawind-asset-center.git . || \
  git clone https://github.com/leidada1020/datawind-asset-center.git . || \
  echo "CLONE_FAILED: 部署机无法访问 GitHub 或仓库私有, 需人工处理"
fi
ls -la /opt/datawind-asset-center
