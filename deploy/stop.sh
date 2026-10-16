#!/bin/bash
# 应用层停止: 仅停 poll_5min 进程, 零系统配置
APP=/opt/datawind-asset-center
PID_FILE=$APP/poll_5min.pid

if [ -f "$PID_FILE" ]; then
  PID=$(cat $PID_FILE)
  if kill -0 $PID 2>/dev/null; then
    kill $PID
    # 等待进程退出(最多10s)
    for i in $(seq 1 10); do
      kill -0 $PID 2>/dev/null || break
      sleep 1
    done
    # 进程会通过 SIGTERM 处理器清理锁文件
    echo "poll_5min 已停止 (PID $PID)"
  else
    echo "进程已不在运行"
  fi
  rm -f $PID_FILE
  rm -f $APP/poll_5min.lock
else
  echo "未找到 PID 文件, 检查是否有残留进程..."
  pkill -f "$APP/poll_5min.py" 2>/dev/null && echo "已清理残留进程" || echo "无残留进程"
fi
