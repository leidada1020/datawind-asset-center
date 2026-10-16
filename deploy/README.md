# DataWind 资产中心 · 部署脚本（内网 Ubuntu VM）
# 目标主机: 192.168.2.76:21258 (root)  主机名 datacenter, Python 3.13.3
# 部署方式: systemd 托管 poll_5min 轮询 + cron 每日 run_daily

# ===== 1. 部署目录布局 =====
# /opt/datawind-asset-center/
#   ├── datawind/                  # 采集包
#   ├── run_daily.py               # 每日全量落库
#   ├── poll_5min.py               # 5分钟轮询
#   ├── config/datawind_monitor_config.json   # 真实凭证(不入Git, 部署时写入)
#   ├── logs/                      # 轮询/日志
#   └── datawind_poll_baseline.json
#
# 代码通过 git clone 获取(脱敏), 真实凭证单独写入 config/

# ===== 2. 依赖安装 =====
#   pip install pymysql
#   (paramiko 仅部署侧使用, 运行侧不需要)

# ===== 3. systemd 托管 poll_5min (5分钟轮询) =====
# 文件: /etc/systemd/system/datawind-poll.service  (见同目录 unit 文件)
#   systemctl daemon-reload
#   systemctl enable --now datawind-poll
#   状态: systemctl status datawind-poll / journalctl -u datawind-poll -f

# ===== 4. cron 每日全量 (每天 10:30, 与历史云端任务对齐) =====
# crontab -e:
#   30 10 * * * cd /opt/datawind-asset-center && /usr/bin/python3 run_daily.py >> logs/run_daily.log 2>&1

# ===== 5. 部署后验证 =====
#   systemctl status datawind-poll                 # 轮询进程 active
#   tail -f /opt/datawind-asset-center/logs/poll_5min.log
#   python3 run_daily.py --once (或看第二天 cron 日志)
#   Doris 侧: SELECT COUNT(*) FROM etl.ods_datawind_dataset_s_d; 确认新分区
