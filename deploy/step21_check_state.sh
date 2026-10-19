#!/bin/bash
# 只读复核: 部署机轮询状态 + 每日调度标记 + 进程
echo "== 进程 =="
ps aux | grep "[p]oll_5min.py" | awk '{print $2, $3"%", $4"%", substr($0, index($0,$11))}'
echo "== 最近日志(20行) =="
tail -20 /opt/datawind-asset-center/logs/poll_5min.log
echo "== 每日调度标记 =="
ls -la /opt/datawind-asset-center/logs/daily_marks/ 2>/dev/null || echo "no daily marks yet"
echo "== 磁盘/内存 =="
df -h /opt | tail -1
free -m | head -2
echo "== run_daily最近一次子进程日志 =="
ls -la /opt/datawind-asset-center/logs/ | head -20
