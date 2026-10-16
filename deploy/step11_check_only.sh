#!/bin/bash
# 只读检查: datawind systemd 配置现状(不做任何修改)
echo "=== datawind-poll 服务状态 ==="
systemctl status datawind-poll --no-pager 2>&1 | head -4
echo "=== datawind-daily timer 状态 ==="
systemctl list-timers datawind-daily.timer --no-pager 2>&1 | head -3
echo "=== unit 文件是否存在 ==="
ls -la /etc/systemd/system/datawind-* 2>&1
echo "=== 轮询进程是否在跑 ==="
ps aux | grep poll_5min | grep -v grep || echo "无 poll_5min 进程"
echo "=== 检查完成(未做任何修改) ==="
