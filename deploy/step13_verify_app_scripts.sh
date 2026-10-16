#!/bin/bash
# 验证: 单进程 + stop/start 脚本可用性(应用层, 零系统配置)
APP=/opt/datawind-asset-center
echo "== 当前 poll_5min 进程数(应为1) =="
ps aux | grep "$APP/poll_5min.py" | grep -v grep | wc -l
ps aux | grep "$APP/poll_5min.py" | grep -v grep

echo "== 测试 stop.sh =="
cd $APP && ./stop.sh
sleep 2
echo "== stop 后进程数(应为0) =="
ps aux | grep "$APP/poll_5min.py" | grep -v grep | wc -l

echo "== 测试 start.sh 重启 =="
cd $APP && ./start.sh
sleep 2
echo "== 重启后进程数(应为1) =="
ps aux | grep "$APP/poll_5min.py" | grep -v grep | wc -l
ps aux | grep "$APP/poll_5min.py" | grep -v grep
echo "STOP_START_OK"
