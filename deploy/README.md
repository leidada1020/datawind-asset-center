# DataWind 资产中心 · 部署脚本（内网 Ubuntu VM）
# 目标主机: 192.168.2.76:21258 (root)  主机名 datacenter, Python 3.13.3
# 部署方式: systemd 托管 poll_5min 轮询 + systemd timer 每日 run_daily（部署机 UTC 时区, 每日 UTC 10:30 全量）

# ===== 1. 部署目录布局 =====
# /opt/datawind-asset-center/
#   ├── datawind/                  # 采集包
#   ├── run_daily.py               # 每日全量落库
#   ├── poll_5min.py               # 5分钟轮询
#   ├── config/datawind_monitor_config.json   # 真实凭证(不入Git, 部署时写入, 权限600)
#   ├── logs/                      # 轮询/日志
#   └── datawind_poll_baseline.json
#
# 代码通过 git clone 获取(脱敏), 真实凭证单独写入 config/
# Python 环境: /opt/datawind-venv (venv, 因 Ubuntu PEP668 禁止系统级 pip 安装)

# ===== 2. 部署执行顺序（脚本在 deploy/ 目录, 从本地经 ssh_exec_file.py 执行）=====
#   step1_env_clone.sh    venv + pymysql + git clone（首次）
#   step2_deploy.sh       导入自检 + systemd unit + 真实凭证（凭证经 ssh_put_config.py 单独传）
#   step3_timer_verify.sh systemd timer 每日全量（首次执行发现 cron 缺失, 改用 timer）
#   step4_refresh.sh      后续代码更新: git pull + 清理当日轮询伪变更 + 重启
#   step5_restart_verify.sh 重启后确认服务常驻（锁残留修复验证）
#   step6_reset_baseline.sh 重置基线(数据key类型变更时) + 清理误报 + 重启
#   step9_timer_utc.sh       timer 保持 UTC 语义（部署机 UTC, 每日 UTC 10:30 全量）
#
# 注意: 部署辅助脚本 ssh_run.py / ssh_exec_file.py / ssh_put_config.py 的
#       主机密码从环境变量 VMP_SSH_PWD 读取, 不入 Git

# ===== 3. 运维命令 =====
#   systemctl status datawind-poll        # 轮询进程状态
#   systemctl restart datawind-poll       # 重启轮询
#   journalctl -u datawind-poll -f        # 实时日志
#   tail -f /opt/datawind-asset-center/logs/poll_5min.log
#   systemctl list-timers datawind-daily.timer   # 每日任务
#   systemctl start datawind-daily        # 手动触发一次每日全量

# ===== 4. 验证 =====
#   轮询: tail -f logs/poll_5min.log, 应每5分钟 "轮询完成: 数据集=338, 变更=N"
#   每日: Doris 查 SELECT COUNT(*) FROM etl.ods_datawind_dataset_s_d; 确认新分区
#   变更: SELECT * FROM etl.dwd_datawind_dataset_change_i_d ORDER BY dt DESC;
