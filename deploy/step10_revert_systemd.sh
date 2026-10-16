#!/bin/bash
# 撤销本部署创建的全部 systemd 系统配置, 恢复 VM 原状
set -e
echo "== 1. 停止并禁用服务 =="
systemctl stop datawind-poll.service 2>/dev/null || true
systemctl disable datawind-poll.service 2>/dev/null || true
systemctl stop datawind-daily.timer 2>/dev/null || true
systemctl disable datawind-daily.timer 2>/dev/null || true
systemctl stop datawind-daily.service 2>/dev/null || true
systemctl disable datawind-daily.service 2>/dev/null || true

echo "== 2. 删除 unit 文件 =="
rm -f /etc/systemd/system/datawind-poll.service
rm -f /etc/systemd/system/datawind-daily.service
rm -f /etc/systemd/system/datawind-daily.timer
rm -f /etc/systemd/system/multi-user.target.wants/datawind-poll.service
rm -f /etc/systemd/system/timers.target.wants/datawind-daily.timer

echo "== 3. 重载 systemd =="
systemctl daemon-reload

echo "== 4. 确认已清除 =="
systemctl status datawind-poll --no-pager 2>&1 | head -2 || echo "datawind-poll 已不存在"
systemctl list-timers datawind-daily.timer --no-pager 2>&1 | head -2 || echo "datawind-daily.timer 已不存在"
ls /etc/systemd/system/datawind-* 2>/dev/null && echo "仍有残留!" || echo "systemd 配置已全部清除"
echo "SYSTEMD_REVERTED"
