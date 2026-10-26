# -*- coding: utf-8 -*-
"""字段血缘全量重算（新别名解析逻辑）落库

用法(本地或部署机 venv):
  python deploy/recompute_lineage.py [--no-table] 
  - 读 ods_datawind_dataset_s_d 最新分区加工 SQL
  - 新 LineageParser 解析 -> write_lineage 幂等落库(删当日分区重插表级/字段级/解析统计)
  - 不动快照/变更/文件夹/闭包(闭包基于表级血缘, 本次未改表级逻辑)
"""
import sys, os, json, datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datawind.config import load_config
from datawind.doris import Doris
from datawind.lineage import LineageParser
from datawind.schema_provider import DorisSchemaProvider
from run_daily import write_lineage

cfg = load_config()
doris = Doris(cfg)
doris.connect()

cur = doris.exec("SELECT MAX(dt) FROM {db}.ods_datawind_dataset_s_d".format(db=doris.db), fetch=True)
max_dt = cur[0][0]
print("[info] latest dt =", max_dt)

rows = doris.exec(
    "SELECT dataset_id, sql_logic, source_tables FROM {db}.ods_datawind_dataset_s_d "
    "WHERE dt=%s AND sql_logic IS NOT NULL AND sql_logic != ''".format(db=doris.db),
    (max_dt,), fetch=True)
ds_map = {r[0]: (r[1], r[2]) for r in rows}
print("[info] datasets with sql =", len(ds_map))

etl_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
parser = LineageParser(schema_provider=DorisSchemaProvider(doris, ttl=300))
stat = write_lineage(doris, str(max_dt), etl_time, ds_map, parser)
print("[result]", json.dumps(stat, ensure_ascii=False))
doris.close()
