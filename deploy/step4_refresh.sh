#!/bin/bash
set -e
APP=/opt/datawind-asset-center
VENV=/opt/datawind-venv/bin/python

echo "== 1. 拉取最新代码 =="
cd $APP
git pull

echo "== 2. 清理轮询基线初始化噪音(当天338条'新增'实为首轮基线) =="
# 这些是 2026-10-16 08:34 首轮基线写入的伪变更, 由 remark='5分钟轮询检测' 且当日全量新增标记
$VENV - <<'PY'
import sys
sys.path.insert(0, '/opt/datawind-asset-center')
from datawind.config import load_config
from datawind.doris import Doris
cfg = load_config()
d = Doris(cfg); d.connect()
# 删除当日轮询产生的全部338条(首轮基线伪变更)
d.exec("DELETE FROM etl.dwd_datawind_dataset_change_i_d WHERE dt = CURDATE() AND remark = '5分钟轮询检测'")
print('cleaned, remain=%d' % d.count('etl.dwd_datawind_dataset_change_i_d'))
d.close()
PY

echo "== 3. 重启轮询服务(加载新逻辑) =="
systemctl restart datawind-poll
sleep 8
systemctl status datawind-poll --no-pager | head -8

echo "== 4. 确认服务运行中, 基线文件存在 =="
ls -la $APP/datawind_poll_baseline.json
tail -n 3 $APP/logs/poll_5min.log
echo "REFRESH_DONE"
