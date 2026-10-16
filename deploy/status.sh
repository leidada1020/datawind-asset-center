#!/bin/bash
# 应用层状态: 查看 poll_5min 进程与最近日志, 零系统配置
APP=/opt/datawind-asset-center
PID_FILE=$APP/poll_5min.pid

if [ -f "$PID_FILE" ] && kill -0 $(cat $PID_FILE) 2>/dev/null; then
  PID=$(cat $PID_FILE)
  echo "poll_5min: RUNNING (PID $PID)"
  echo "--- 进程信息 ---"
  ps -o pid,etime,rss,cmd -p $PID
  echo "--- 最近日志 ---"
  tail -n 6 $APP/logs/poll_5min.log
else
  echo "poll_5min: NOT RUNNING"
fi
echo "--- 每日全量标记 ---"
ls -la $APP/logs/daily_marks/ 2>/dev/null | tail -n 5 || echo "(暂无标记, 今日10:30后首次触发)"
