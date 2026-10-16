#!/bin/bash
# 验证: 新进程(09:02启动) 已完成至少一轮轮询, 且未误触发每日全量(当前<10:30UTC)
APP=/opt/datawind-asset-center
echo "== 进程状态 =="
./status.sh 2>/dev/null || /opt/datawind-asset-center/status.sh
echo "== 最近日志 =="
tail -n 5 $APP/logs/poll_5min.log
echo "== 每日全量标记(当前未到10:30应无标记) =="
ls $APP/logs/daily_marks/ 2>/dev/null || echo "(无标记, 正确)"
echo "VERIFY_OK"
