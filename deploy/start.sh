#!/bin/bash
# 应用层启动: nohup 常驻 poll_5min(5分钟轮询 + 每日UTC10:30全量调度), 零系统配置
# 用法: ./start.sh [--interval 300]
APP=/opt/datawind-asset-center
VENV=/opt/datawind-venv/bin/python
PID_FILE=$APP/poll_5min.pid
LOG_FILE=$APP/logs/poll_5min.log

cd $APP
mkdir -p logs

# 已运行则退出
if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
  echo "poll_5min 已在运行 (PID $(cat $PID_FILE))"
  exit 0
fi
rm -f $PID_FILE

# nohup 启动(常驻, 应用目录内管理, 不注册任何系统服务)
# 日志由 poll_5min.py 内部 FileHandler 写 logs/poll_5min.log, stdout 丢弃避免双写
nohup $VENV $APP/poll_5min.py "$@" > /dev/null 2>&1 &
echo $! > $PID_FILE
sleep 3
echo "poll_5min 已启动 (PID $(cat $PID_FILE)), 日志: $LOG_FILE"
echo "查看: ./status.sh | 停止: ./stop.sh"
