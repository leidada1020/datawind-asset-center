#!/bin/bash
# DataWind 资产中心 Web 服务管理脚本(纯应用层, 零系统配置)
# 用法: ./start_web.sh | ./stop_web.sh | ./status_web.sh
APP=/opt/datawind-asset-center
VENV=/opt/datawind-venv
LOG=$APP/logs/web.log
PID=$APP/web.pid
PORT=18081

start() {
  cd $APP || exit 1
  if [ -f $PID ] && kill -0 $(cat $PID) 2>/dev/null; then
    echo "web 已在运行 (PID $(cat $PID))"
    return 0
  fi
  nohup $VENV/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port $PORT >> $LOG 2>&1 &
  echo $! > $PID
  sleep 2
  if kill -0 $(cat $PID) 2>/dev/null; then
    echo "web 已启动 (PID $(cat $PID)), 端口 $PORT, 日志: $LOG"
  else
    echo "web 启动失败, 查看日志: $LOG"
    tail -20 $LOG
  fi
}

stop() {
  if [ ! -f $PID ]; then echo "web 未在运行"; return 0; fi
  kill $(cat $PID) 2>/dev/null && echo "web 已停止 (PID $(cat $PID))"
  rm -f $PID
}

status() {
  if [ -f $PID ] && kill -0 $(cat $PID) 2>/dev/null; then
    echo "web: RUNNING (PID $(cat $PID), port $PORT)"
    ps -p $(cat $PID) -o pid,etime,rss,cmd --no-headers 2>/dev/null
  else
    echo "web: STOPPED"
  fi
  echo "--- 最近日志 ---"
  tail -10 $LOG 2>/dev/null
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  restart) stop; sleep 1; start ;;
  *) echo "用法: $0 {start|stop|status|restart}"; exit 1 ;;
esac
