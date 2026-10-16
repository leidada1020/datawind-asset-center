# -*- coding: utf-8 -*-
"""
DataWind 资产中心 · P0 单入口：每日全量采集 + 落库 + 变更检测
=============================================================
流程:
  1. 换 JWT -> 拉全量数据集列表
  2. 拉文件夹树 -> 写 dim_datawind_folder_s_d 快照
  3. 并发抓每个数据集 info(文件夹/配置) + 加工逻辑(getDataModelInfo)
  4. 当日快照写 ods_datawind_dataset_s_d (dt 分区 + etl_time, 幂等覆盖)
  5. 与上一 dt 分区自比对 -> 变更写 dwd_datawind_dataset_change_i_d
  6. 打印汇总与校验

用法:
  python run_daily.py                     # 快照日期=今天
  python run_daily.py --date 2026-10-16   # 指定快照日期
  python run_daily.py --config ../config/datawind_monitor_config.json
依赖: pymysql; datawind 包同目录
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from datawind.config import load_config
from datawind.client import DataWindClient
from datawind import models
from datawind.doris import Doris
from datawind import ddl as ddl_mod

TBL_SNAPSHOT = "{db}.ods_datawind_dataset_s_d"
TBL_CHANGE = "{db}.dwd_datawind_dataset_change_i_d"
TBL_FOLDER = "{db}.dim_datawind_folder_s_d"
DDL_FOLDER = ddl_mod.DDL_FOLDER


def write_folder_snapshot(doris, stat_date, etl_time, tree_data):
    """文件夹树快照：平铺 + 归属命名空间"""
    rows = []
    for ns in ("public", "private"):

        def walk(items, path, parent_id):
            for it in items or []:
                fid = it.get("id")
                name = it.get("name") or ""
                rows.append((stat_date, fid, etl_time, parent_id, name,
                             "/".join(path + [name]), ns))
                walk(it.get("childList"), path + [name], fid)

        walk(tree_data.get(ns), [], None)
    if not rows:
        print("    [警告] 文件夹树为空（public/private 均无节点）")
        return 0
    sql = "INSERT INTO %s (dt, folder_id, etl_time, parent_id, folder_name, folder_path, namespace) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_FOLDER.format(db=doris.db)
    doris.execmany(sql, rows)
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=time.strftime("%Y-%m-%d"), help="快照日期, 默认今天")
    ap.add_argument("--config", default=None, help="配置文件路径")
    args = ap.parse_args()
    stat_date = args.date

    cfg = load_config(args.config)
    client = DataWindClient(cfg)
    doris = Doris(cfg)

    print("==> 连接 Doris ...")
    doris.connect()

    print("==> 拉取 DataWind 数据 ...")
    jwt = client.jwt()
    ds_list = client.list_datasets()
    current = {d["id"]: models.normalize_dataset(d) for d in ds_list}
    print("    共 %d 个数据集" % len(current))

    # 文件夹树
    tree_data = client.folder_tree_raw()
    folder_path_map = models.build_folder_path_map(tree_data)
    print("    文件夹树节点: %d 个" % len(folder_path_map))

    # 建表(幂等)
    doris.create_table(ddl_mod.DDL_SNAPSHOT.format(db=doris.db))
    doris.create_table(ddl_mod.DDL_CHANGE.format(db=doris.db))
    doris.create_table(DDL_FOLDER.format(db=doris.db))
    print("==> 表已就绪: %s / %s / %s" % (
        TBL_SNAPSHOT.format(db=doris.db), TBL_CHANGE.format(db=doris.db), TBL_FOLDER.format(db=doris.db)))

    # 文件夹树快照写入
    etl_time = time.strftime("%Y-%m-%d %H:%M:%S")
    fn = write_folder_snapshot(doris, stat_date, etl_time, tree_data)
    print("==> 文件夹树快照 %d 条 (dt=%s)" % (fn, stat_date))

    # 并发抓 info + 加工逻辑
    ids = list(current.keys())
    print("==> 并发抓取数据集详情/加工逻辑 (%d 个, 8 线程) ..." % len(ids))
    info_map = client.batch_fetch(ids, lambda i: models.normalize_info(client.dataset_info(i)))
    model_map = client.batch_fetch(ids, lambda i: client.model_logic(i))

    for i, d in current.items():
        inf = info_map.get(i) or {}
        d["folder_id"] = inf.get("folder_id")
        d["folder_path"] = folder_path_map.get(inf.get("folder_id")) or (
            "根目录" if not inf.get("folder_id") else "")
        d["connection_mode"] = inf.get("connection_mode")
        d["sync_mode"] = inf.get("sync_mode")
        d["partition_filter"] = inf.get("partition_filter")
        d["confidentiality"] = inf.get("confidentiality") or ""
        mdl = model_map.get(i)
        if mdl:
            d["sql_logic"], d["source_tables"], d["field_list"] = mdl
        else:
            d["sql_logic"], d["source_tables"], d["field_list"] = None, None, None

    # 取上一个快照日期
    rows = doris.exec("SELECT MAX(dt) FROM %s WHERE dt < %%s" % TBL_SNAPSHOT.format(db=doris.db),
                      (stat_date,), fetch=True)
    prev_date = rows[0][0] if rows and rows[0] else None
    print("    上一个快照日期: %s" % (prev_date or "(无, 本次为基线)"))

    # 写入当日快照（UNIQUE KEY(dt,dataset_id) 幂等，重跑同 key 覆盖）
    cols = ["dt", "etl_time", "dataset_id", "name", "owner_email", "type_code", "type_name",
            "status", "sync_type", "frequency", "schedule_time", "schedule_day", "ttl",
            "connection_mode", "sync_mode", "partition_filter", "confidentiality",
            "last_sync_time", "last_success_task_time", "rows_cnt", "elapsed_last_success",
            "upstream", "folder_id", "folder_path", "is_hive_table", "is_hive_partition",
            "hsql_used", "url", "ctime", "mtime", "sql_logic", "source_tables", "field_list"]
    ph = ",".join(["%s"] * len(cols))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (TBL_SNAPSHOT.format(db=doris.db), ",".join(cols), ph)
    rows_all = []
    for d in current.values():
        rows_all.append(tuple([stat_date, etl_time] + [d.get(c) for c in cols[2:]]))
    doris.execmany(sql, rows_all)
    print("==> 已写入当日快照 %d 行 (dt=%s)" % (len(rows_all), stat_date))

    # 对比上一分区 -> 变更日志
    changes = []
    if prev_date:
        rows = doris.exec(
            "SELECT dataset_id, name, type_code, status, sync_type, frequency, schedule_time, "
            "schedule_day, ttl, connection_mode, partition_filter, confidentiality, "
            "last_success_task_time, rows_cnt, upstream, folder_id, folder_path, mtime "
            "FROM %s WHERE dt = %%s" % TBL_SNAPSHOT.format(db=doris.db), (prev_date,), fetch=True)
        prev = {}
        for r in rows:
            prev[r[0]] = {"name": r[1], "type_code": r[2], "status": r[3], "sync_type": r[4],
                          "frequency": r[5], "schedule_time": r[6], "schedule_day": r[7],
                          "ttl": r[8], "connection_mode": r[9], "partition_filter": r[10],
                          "confidentiality": r[11], "last_success_task_time": r[12],
                          "rows_cnt": r[13], "upstream": r[14] or "", "folder_id": r[15],
                          "folder_path": r[16] or "", "mtime": r[17]}
        changes = models.diff_changes(current, prev)

    if changes:
        doris.exec("DELETE FROM %s WHERE dt = %%s" % TBL_CHANGE.format(db=doris.db), (stat_date,))
        ins = "INSERT INTO %s (dt, etl_time, change_id, dataset_id, dataset_name, change_type, change_type_name, changed_fields, diff_json, folder_path, remark) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_CHANGE.format(db=doris.db)
        ins_rows = []
        for cid, (ct, rid, rname, cfields, diff, fpath) in enumerate(changes, start=1):
            ins_rows.append((stat_date, etl_time, cid, rid, rname, ct,
                             models.CHANGE_NAMES.get(ct, ct), cfields, diff, fpath, None))
        doris.execmany(ins, ins_rows)
        print("==> 变更日志写入 %d 条" % len(changes))
    else:
        print("==> 与 %s 相比无变化(或首次基线, 无对比)" % (prev_date or "无"))

    # 校验
    cnt = doris.count(TBL_SNAPSHOT.format(db=doris.db), "dt=%s", (stat_date,))
    fcnt = doris.count(TBL_FOLDER.format(db=doris.db), "dt=%s", (stat_date,))
    lcnt = doris.count(TBL_CHANGE.format(db=doris.db))
    print("==> 校验: 当日快照=%d 行, 文件夹快照=%d 行, 变更日志累计=%d" % (cnt, fcnt, lcnt))

    print("---- 本次变化汇总 ----")
    if not changes:
        print("无变化")
    for ct, rid, rname, cfields, diff, fpath in changes:
        print("[%s] %s (%s) [%s]: %s" % (models.CHANGE_NAMES.get(ct, ct), rname, rid, fpath, diff[:120]))

    doris.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("【执行失败】%s" % e, file=sys.stderr)
        sys.exit(1)
