#!/bin/bash
set -e
APP=/opt/datawind-asset-center
VENV=/opt/datawind-venv/bin/python

echo "== 1. 拉取代码 =="
cd $APP && git pull

echo "== 2. 清理误报: 当日轮询伪变更(含首轮338 + key类型误报676) =="
$VENV - <<'PY'
import sys
sys.path.insert(0, '/opt/datawind-asset-center')
from datawind.config import load_config
from datawind.doris import Doris
cfg = load_config()
d = Doris(cfg); d.connect()
d.exec("DELETE FROM etl.dwd_datawind_dataset_change_i_d WHERE dt = CURDATE() AND remark = '5分钟轮询检测'")
print('change remain=%d' % d.count('etl.dwd_datawind_dataset_change_i_d'))
d.close()
PY

echo "== 3. 重置基线(旧基线key为int, 新代码统一str, 重新建基线) =="
rm -f $APP/datawind_poll_baseline.json
echo "baseline removed"

echo "== 4. 重启服务 =="
rm -f $APP/poll_5min.lock
systemctl restart datawind-poll
sleep 5
systemctl status datawind-poll --no-pager | head -6

echo "== 5. 等待首轮建基线(约40s) =="
sleep 40
tail -n 2 $APP/logs/poll_5min.log
echo "---change count---"
$VENV - <<'PY'
import sys
sys.path.insert(0, '/opt/datawind-asset-center')
from datawind.config import load_config
from datawind.doris import Doris
cfg = load_config()
d = Doris(cfg); d.connect()
print('change remain=%d' % d.count('etl.dwd_datawind_dataset_change_i_d'))
d.close()
PY
echo "RESET_DONE"
