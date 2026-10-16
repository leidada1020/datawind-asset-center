#!/bin/bash
# 只读: 检查部署机config是否含Doris段(修复config.py后兼容性检查)
echo "== config keys =="
python3 -c "import json; d=json.load(open('/opt/datawind-asset-center/config/datawind_monitor_config.json')); print(sorted(d.keys()))"
echo "== doris段存在性 =="
python3 -c "
import json
d=json.load(open('/opt/datawind-asset-center/config/datawind_monitor_config.json'))
need=['doris_host','doris_port','doris_user','doris_password','doris_db']
missing=[k for k in need if k not in d or not d.get(k)]
print('MISSING:', missing if missing else 'NONE - OK')
"
echo "== poll进程 =="
cat /opt/datawind-asset-center/poll_5min.pid 2>/dev/null || echo "no pid file"
ps aux | grep -c "[p]oll_5min.py" || true
