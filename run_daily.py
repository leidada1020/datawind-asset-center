# -*- coding: utf-8 -*-
"""
DataWind 资产中心 · 单入口：每日全量采集 + 落库 + 变更检测 + 血缘解析
=============================================================
流程:
  1. 换 JWT -> 拉全量数据集列表
  2. 拉文件夹树 -> 写 dim_datawind_folder_s_d 快照
  3. 并发抓每个数据集 info(文件夹/配置) + 加工逻辑(getDataModelInfo)
  4. 当日快照写 ods_datawind_dataset_s_d (dt 分区 + etl_time, 幂等覆盖)
  5. 与上一 dt 分区自比对 -> 变更写 dwd_datawind_dataset_change_i_d
  6. SQLGlot 解析加工 SQL -> 表级血缘 dwd_datawind_table_lineage_t_d + 字段级血缘 dwd_datawind_column_lineage_t_d + 解析统计 dwd_datawind_parse_stat_s_d
  7. 血缘 diff 并入变更(上游表变化 upstream_changed)
  8. 打印汇总与校验

用法:
  python run_daily.py                     # 快照日期=今天
  python run_daily.py --date 2026-10-16   # 指定快照日期
  python run_daily.py --config ../config/datawind_monitor_config.json
  python run_daily.py --no-lineage        # 跳过血缘解析(降级模式, 如 sqlglot 未装)
依赖: pymysql; datawind 包同目录; 血缘解析依赖 sqlglot(可选)
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
from datawind.lineage import LineageParser, normalize_column_edge

TBL_SNAPSHOT = "{db}.ods_datawind_dataset_s_d"
TBL_CHANGE = "{db}.dwd_datawind_dataset_change_i_d"
TBL_FOLDER = "{db}.dim_datawind_folder_s_d"
TBL_TBL_LINEAGE = "{db}.dwd_datawind_table_lineage_t_d"
TBL_COL_LINEAGE = "{db}.dwd_datawind_column_lineage_t_d"
TBL_PARSE_STAT = "{db}.dwd_datawind_parse_stat_s_d"
TBL_CLOSURE = "{db}.dwd_datawind_lineage_closure_t_d"
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


def write_lineage(doris, stat_date, etl_time, dataset_sql_map, parser):
    """血缘解析落库: 表级 + 字段级 + 解析统计

    dataset_sql_map: {dataset_id: (sql_logic, source_tables)}
    返回 stat 汇总 dict
    """
    t_rows, c_rows = [], []
    stat = {"total": 0, "parsed": 0, "pending": 0, "fail": 0,
            "high": 0, "medium": 0, "pend_conf": 0, "t_edges": 0, "c_edges": 0}
    for dsid, (sql, srcs) in dataset_sql_map.items():
        sql = (sql or "").strip()
        if not sql:
            stat["pending"] += 1
            continue
        stat["total"] += 1
        tables, cols, st = parser.parse_multi(sql, dsid)
        if st["parse_status"] == "fail":
            stat["fail"] += 1
            continue
        stat["parsed"] += 1
        # 表级边: SQLGlot 解析 + 接口 sourceTableList 合并
        table_set = set(tables)
        for s in (srcs or "").split(","):
            s = s.strip()
            if s:
                table_set.add(s)
        for t in sorted(table_set):
            t_rows.append((stat_date, dsid, t, "direct", 0, etl_time))
        # 字段级边
        for edge in cols:
            n = normalize_column_edge(edge, dsid)
            c_rows.append((stat_date, dsid, n["target_field"], n["src_table"],
                           n["src_field"], n["confidence"], etl_time))
            if n["confidence"] == "high":
                stat["high"] += 1
            elif n["confidence"] == "medium":
                stat["medium"] += 1
            else:
                stat["pend_conf"] += 1
        stat["t_edges"] += len(table_set)
        stat["c_edges"] += len(cols)

    # 落库(幂等: 先删当日分区再全量)
    if t_rows:
        doris.exec("DELETE FROM %s WHERE dt = %%s" % TBL_TBL_LINEAGE.format(db=doris.db), (stat_date,))
        sql_t = "INSERT INTO %s (dt, dataset_id, db_table, lineage_type, depth, etl_time) VALUES (%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_TBL_LINEAGE.format(db=doris.db)
        doris.execmany(sql_t, t_rows)
    if c_rows:
        doris.exec("DELETE FROM %s WHERE dt = %%s" % TBL_COL_LINEAGE.format(db=doris.db), (stat_date,))
        sql_c = "INSERT INTO %s (dt, dataset_id, target_field, src_table, src_field, confidence, etl_time) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_COL_LINEAGE.format(db=doris.db)
        doris.execmany(sql_c, c_rows)
    # 解析统计
    doris.exec("DELETE FROM %s WHERE dt = %%s" % TBL_PARSE_STAT.format(db=doris.db), (stat_date,))
    sql_p = "INSERT INTO %s (dt, etl_time, total_cnt, parsed_cnt, pending_cnt, fail_cnt, high_conf_cnt, medium_conf_cnt, pending_conf_cnt, table_edge_cnt, column_edge_cnt) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_PARSE_STAT.format(db=doris.db)
    doris.execmany(sql_p, [(stat_date, etl_time, stat["total"], stat["parsed"],
                            stat["pending"], stat["fail"], stat["high"],
                            stat["medium"], stat["pend_conf"],
                            stat["t_edges"], stat["c_edges"])])
    return stat


def build_closure(doris, stat_date, etl_time):
    """血缘闭包构建: 从当日 table_lineage 全量生成 hop=1 闭包行

    算法: 闭包 = 表级血缘(数据集->直接上游表) 展开为 up_db/up_table
    hop>1 需源表间依赖图(DataWind不提供, 预留; P3 ODS接入后可迭代扩展)
    返回闭包行数
    """
    rows = doris.exec(
        "SELECT dataset_id, db_table FROM %s WHERE dt = %%s" % TBL_TBL_LINEAGE.format(db=doris.db),
        (stat_date,), fetch=True)
    c_rows = []
    for dsid, db_table in rows:
        if "." in db_table:
            db, tb = db_table.rsplit(".", 1)
        else:
            db, tb = None, db_table
        c_rows.append((stat_date, dsid, db, tb, 1, db_table, etl_time))
    if not c_rows:
        return 0
    doris.exec("DELETE FROM %s WHERE dt = %%s" % TBL_CLOSURE.format(db=doris.db), (stat_date,))
    sql = "INSERT INTO %s (dt, dataset_id, up_db, up_table, hop_depth, lineage_path, etl_time) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_CLOSURE.format(db=doris.db)
    doris.execmany(sql, c_rows)
    return len(c_rows)


def lineage_diff(prev_dt, cur_dt, doris, dataset_names):
    """血缘 diff: 上游表变化 -> 变更列表

    对比上一 dt 与当日 dt 的表级血缘边(数据集维度, 两独立查询):
      upstream_changed = 上游表集合变化(新增/删除来源表), diff_json 记增减明细
    """
    def table_set(dt):
        rows = doris.exec(
            "SELECT dataset_id, db_table FROM %s WHERE dt = %%s" % TBL_TBL_LINEAGE.format(db=doris.db),
            (dt,), fetch=True)
        m = {}
        for dsid, t in rows:
            m.setdefault(dsid, set()).add(t)
        return m

    prev_m = table_set(prev_dt)
    cur_m = table_set(cur_dt)
    changes = []
    for dsid in sorted(set(prev_m) | set(cur_m)):
        p = prev_m.get(dsid, set())
        c = cur_m.get(dsid, set())
        if p != c:
            added = sorted(c - p)
            removed = sorted(p - c)
            changes.append((
                "upstream_changed", dsid, dataset_names.get(dsid, ""),
                "upstream", json.dumps({"added": added, "removed": removed},
                                       ensure_ascii=False),
                ""))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=time.strftime("%Y-%m-%d"), help="快照日期, 默认今天")
    ap.add_argument("--config", default=None, help="配置文件路径")
    ap.add_argument("--no-lineage", action="store_true", help="跳过血缘解析(降级模式)")
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
    doris.create_table(ddl_mod.DDL_TABLE_LINEAGE.format(db=doris.db))
    doris.create_table(ddl_mod.DDL_COLUMN_LINEAGE.format(db=doris.db))
    doris.create_table(ddl_mod.DDL_PARSE_STAT.format(db=doris.db))
    doris.create_table(ddl_mod.DDL_CLOSURE.format(db=doris.db))
    print("==> 表已就绪: 快照/变更/文件夹/表血缘/字段血缘/解析统计/闭包")

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

    # 血缘解析落库（可选；不阻塞快照主流程）
    lineage_stat = None
    if not args.no_lineage:
        try:
            from datawind.lineage import LineageParser
            parser = LineageParser()
            print("==> SQLGlot 血缘解析落库 ...")
            ds_sql_map = {i: (d.get("sql_logic"), d.get("source_tables"))
                          for i, d in current.items()}
            lineage_stat = write_lineage(doris, stat_date, etl_time, ds_sql_map, parser)
            print("    解析统计: 有SQL=%d 成功=%d 直连/无SQL=%d 失败=%d | "
                  "表边=%d 字段边=%d(high=%d medium=%d pending=%d)" % (
                      lineage_stat["total"], lineage_stat["parsed"],
                      lineage_stat["pending"], lineage_stat["fail"],
                      lineage_stat["t_edges"], lineage_stat["c_edges"],
                      lineage_stat["high"], lineage_stat["medium"],
                      lineage_stat["pend_conf"]))
            # 血缘 diff: 上游表变化并入变更
            if prev_date:
                names = {i: d.get("name", "") for i, d in current.items()}
                lc = lineage_diff(str(prev_date), stat_date, doris, names)
                if lc:
                    changes.extend(lc)
                    print("    血缘 diff: 上游表变化 %d 条" % len(lc))
            # 闭包表构建(影响分析: 上游全部来源 / 按表反查下游影响面)
            c_n = build_closure(doris, stat_date, etl_time)
            print("    闭包表: %d 条 (dt=%s)" % (c_n, stat_date))
        except Exception as e:
            print("    [警告] 血缘解析失败, 已跳过(不影响快照): %s" % e)

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
    if lineage_stat:
        tcnt = doris.count(TBL_TBL_LINEAGE.format(db=doris.db), "dt=%s", (stat_date,))
        ccnt = doris.count(TBL_COL_LINEAGE.format(db=doris.db), "dt=%s", (stat_date,))
        print("==> 血缘校验: 表级边=%d 条, 字段级边=%d 条 (dt=%s)" % (tcnt, ccnt, stat_date))
        clcnt = doris.count(TBL_CLOSURE.format(db=doris.db), "dt=%s", (stat_date,))
        print("==> 闭包校验: 闭包边=%d 条 (dt=%s)" % (clcnt, stat_date))

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
