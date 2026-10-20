# -*- coding: utf-8 -*-
"""DataWind 资产中心 · Web 服务（P2 页面化）

FastAPI 只读服务: 查 Doris 当日分区, 提供
  - 资产目录(搜索/筛选/分页)
  - 数据集详情(元数据+加工SQL+字段清单)
  - 表级+字段级血缘(数据集上游 / 表下游影响面)
  - 变更监控(按日/类型)
  - 文件夹树 / 统计

前端: 自包含 HTML+JS(内联 SVG 血缘图), 无外网 CDN 依赖(适配内网部署)。

启动(纯应用层, 不碰系统配置):
  uvicorn web.app:app --host 0.0.0.0 --port 8080
"""
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from datawind.config import load_config
from datawind.doris import Doris

BASE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(BASE, "static")

app = FastAPI(title="DataWind 资产中心", version="0.1.0")


def get_doris():
    cfg = load_config()
    d = Doris(cfg)
    d.connect()
    return d


def latest_dt(doris):
    rows = doris.exec("SELECT MAX(dt) FROM etl.ods_datawind_dataset_s_d", fetch=True)
    return rows[0][0] if rows and rows[0] else None


# ---------------- 仪表盘 / 统计 ----------------
@app.get("/api/stats")
def api_stats():
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        out = {"dt": str(dt)}
        for k, tbl in [
            ("datasets", "etl.ods_datawind_dataset_s_d"),
            ("folders", "etl.dim_datawind_folder_s_d"),
            ("table_edges", "etl.dwd_datawind_table_lineage_t_d"),
            ("column_edges", "etl.dwd_datawind_column_lineage_t_d"),
            ("closure_edges", "etl.dwd_datawind_lineage_closure_t_d"),
            ("doris_tables", "etl.ods_doris_table_s_d"),
            ("doris_columns", "etl.ods_doris_column_s_d"),
        ]:
            r = d.exec("SELECT COUNT(*) FROM %s WHERE dt=%%s" % tbl, (dt,), fetch=True)
            out[k] = r[0][0] if r else 0
        r = d.exec("SELECT parsed_cnt, fail_cnt, table_edge_cnt, column_edge_cnt FROM etl.dwd_datawind_parse_stat_s_d WHERE dt=%s", (dt,), fetch=True)
        if r:
            out["parsed"], out["fail"], out["t_edges_stat"], out["c_edges_stat"] = r[0]
        r = d.exec("SELECT COUNT(*) FROM etl.dwd_datawind_dataset_change_i_d WHERE dt=%s", (dt,), fetch=True)
        out["changes_today"] = r[0][0] if r else 0
        # 最近7天变更数
        rows = d.exec("SELECT dt, COUNT(*) FROM etl.dwd_datawind_dataset_change_i_d WHERE dt >= DATE_SUB(%s, INTERVAL 6 DAY) GROUP BY dt ORDER BY dt", (dt,), fetch=True)
        out["changes_7d"] = [{"dt": str(x[0]), "cnt": x[1]} for x in rows]
        return out
    finally:
        d.close()


# ---------------- 文件夹树 ----------------
@app.get("/api/folders")
def api_folders():
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        rows = d.exec("SELECT folder_id, parent_id, folder_name, folder_path, namespace FROM etl.dim_datawind_folder_s_d WHERE dt=%s ORDER BY folder_path", (dt,), fetch=True)
        return [{"folder_id": r[0], "parent_id": r[1], "name": r[2], "path": r[3], "ns": r[4]} for r in rows]
    finally:
        d.close()


@app.get("/api/folder_tree")
def api_folder_tree():
    """文件夹树(带数据集计数): 资产目录按 DataWind 数据集目录导航"""
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        rows = d.exec(
            "SELECT folder_id, parent_id, folder_name, folder_path "
            "FROM etl.dim_datawind_folder_s_d WHERE dt=%s ORDER BY folder_path", (str(dt),), fetch=True)
        cnts = {r[0] or "": r[1] for r in d.exec(
            "SELECT folder_path, COUNT(*) FROM etl.ods_datawind_dataset_s_d WHERE dt=%s GROUP BY folder_path",
            (str(dt),), fetch=True)}
        total = sum(cnts.values())
        items = []
        for r in rows:
            items.append({"id": r[0], "parent": r[1], "name": r[2],
                          "path": r[3] or "", "cnt": cnts.get(r[3] or "", 0)})
        return {"items": items, "total": total}
    finally:
        d.close()


# ---------------- 资产目录 ----------------
@app.get("/api/datasets")
def api_datasets(keyword: str = "", folder: str = "", type_code: str = "",
                 status: str = "", source: str = "", freq: str = "",
                 page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)):
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        where = ["dt = %s"]
        args = [dt]
        if keyword:
            where.append("(name LIKE %s OR dataset_id LIKE %s)")
            args += ["%%%s%%" % keyword, "%%%s%%" % keyword]
        if folder:
            where.append("folder_path LIKE %s")
            args.append("%%%s%%" % folder)
        if type_code:
            where.append("type_code = %s")
            args.append(type_code)
        if status:
            where.append("status = %s")
            args.append(status)
        if source:
            where.append("(data_source_type = %s OR cluster_name = %s)")
            args += [source, source]
        if freq:
            where.append("frequency = %s")
            args.append(freq)
        w = " AND ".join(where)
        cnt = d.exec("SELECT COUNT(*) FROM etl.ods_datawind_dataset_s_d WHERE %s" % w, args, fetch=True)[0][0]
        off = (page - 1) * size
        rows = d.exec(
            "SELECT dataset_id, name, folder_path, type_name, frequency, schedule_time, "
            "status, rows_cnt, last_success_task_time, mtime, source_tables, "
            "data_source_type, data_source_id, cluster_name, source_db "
            "FROM etl.ods_datawind_dataset_s_d WHERE %s ORDER BY dataset_id LIMIT %s OFFSET %s"
            % (w, size, off), args, fetch=True)
        items = []
        for r in rows:
            items.append({
                "dataset_id": r[0], "name": r[1], "folder_path": r[2] or "",
                "type_name": r[3] or "", "frequency": r[4] or "",
                "schedule_time": str(r[5]) if r[5] is not None else "",
                "status": r[6], "rows_cnt": r[7], "last_sync": str(r[8]) if r[8] else "",
                "mtime": str(r[9]) if r[9] else "",
                "source_tables": (r[10] or "")[:200],
                "data_source_type": r[11] or "", "data_source_id": r[12],
                "cluster_name": r[13] or "", "source_db": r[14] or "",
            })
        return {"total": cnt, "page": page, "size": size, "items": items}
    finally:
        d.close()


# ---------------- 数据集详情 ----------------
@app.get("/api/datasets/{dsid}")
def api_dataset_detail(dsid: int):
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        rows = d.exec("SELECT * FROM etl.ods_datawind_dataset_s_d WHERE dt=%s AND dataset_id=%s", (dt, dsid), fetch=True)
        if not rows:
            return JSONResponse({"error": "数据集不存在"}, status_code=404)
        cols = [c[0] for c in d.cur.description]
        info = dict(zip(cols, rows[0]))
        for k in ("ctime", "mtime", "last_sync_time", "last_success_task_time", "etl_time"):
            if info.get(k) is not None:
                info[k] = str(info[k])
        # 字段清单(解析字段列表JSON, 若为空则从字段血缘推导)
        field_list = info.get("field_list")
        fields = []
        if field_list:
            try:
                fields = json.loads(field_list) if isinstance(field_list, str) else field_list
            except Exception:
                fields = []
        info["fields"] = fields
        # 上游表(闭包)
        up = d.exec("SELECT up_db, up_table, hop_depth FROM etl.dwd_datawind_lineage_closure_t_d WHERE dt=%s AND dataset_id=%s ORDER BY up_db, up_table", (dt, dsid), fetch=True)
        info["upstream_tables"] = [{"db": r[0], "table": r[1], "hop": r[2]} for r in up]
        # 血缘树(树状分层): 数据集 <- hop1 直接上游 <- hop2 DS调度生产来源(ods_ds_lineage_s_d 补全)
        #   WHERE 不嵌套子查询: hop2 逐表查询
        tree_ups = []
        for r in up:
            up_db, up_tb = r[0], r[1]
            srcs = []
            if up_db and up_tb:
                src_rows = d.exec(
                    "SELECT src_db, src_table FROM etl.ods_ds_lineage_s_d "
                    "WHERE dt=%s AND dst_db=%s AND dst_table=%s ORDER BY src_db, src_table LIMIT 12",
                    (dt, up_db, up_tb), fetch=True)
                srcs = [{"db": x[0], "table": x[1]} for x in src_rows]
            tree_ups.append({"db": up_db, "table": up_tb, "hop": r[2], "srcs": srcs})
        info["lineage_tree"] = {"dataset_id": dsid, "name": info.get("name", ""),
                                "upstream": tree_ups}
        # 字段级血缘(该数据集 target_field -> src)
        # 仅展示最终查询结果字段: 过滤 pending/无来源/通配符*/纯数字索引(常量列)噪音;
        # 目标字段补齐业务列名(来自字段清单 name->business 映射, DataWind结果字段以业务列展示)
        biz_map = {}
        for f in fields:
            if isinstance(f, dict) and f.get("name"):
                b = f.get("business")
                biz_map[f["name"]] = (b[0] if isinstance(b, list) and b else (b or ""))
        fc = d.exec("SELECT target_field, src_table, src_field, confidence FROM etl.dwd_datawind_column_lineage_t_d WHERE dt=%s AND dataset_id=%s ORDER BY target_field", (dt, dsid), fetch=True)
        info["field_lineage"] = []
        for r in fc:
            t, st, sf, conf = r[0], r[1] or "", r[2] or "", r[3]
            if conf == "pending" or (not st and not sf):
                continue
            if t == "*" or (t and t.isdigit()):
                continue
            info["field_lineage"].append({"target": t, "business": biz_map.get(t, ""),
                                          "src_table": st, "src_field": sf, "conf": conf})
        # 变更历史(该数据集)
        ch = d.exec("SELECT dt, change_type, change_type_name, changed_fields, diff_json, remark FROM etl.dwd_datawind_dataset_change_i_d WHERE dataset_id=%s ORDER BY dt DESC, change_id DESC LIMIT 20", (dsid,), fetch=True)
        info["change_history"] = [{"dt": str(r[0]), "type": r[1], "type_name": r[2], "fields": r[3], "diff": r[4], "remark": r[5]} for r in ch]
        return info
    finally:
        d.close()


# ---------------- 表下游影响面 ----------------
@app.get("/api/tables/impact")
def api_table_impact(db: str = "", table: str = Query(..., min_length=1)):
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        where = ["dt = %s", "up_table LIKE %s"]
        args = [dt, "%%%s%%" % table]
        if db:
            where.append("up_db = %s")
            args.append(db)
        w = " AND ".join(where)
        rows = d.exec(
            "SELECT dataset_id, COUNT(*) AS c FROM etl.dwd_datawind_lineage_closure_t_d "
            "WHERE %s GROUP BY dataset_id ORDER BY c DESC" % w, args, fetch=True)
        out = []
        for r in rows:
            nm = d.exec("SELECT name, folder_path FROM etl.ods_datawind_dataset_s_d WHERE dt=%s AND dataset_id=%s", (dt, r[0]), fetch=True)
            out.append({"dataset_id": r[0], "ref_cnt": r[1],
                        "name": nm[0][0] if nm else "", "folder_path": nm[0][1] if nm else ""})
        return {"table": table, "db": db, "total": len(out), "items": out}
    finally:
        d.close()


# ---------------- 血缘图谱（上下游双向, P3） ----------------
# 边模型: 数据集(加工SQL) 引用/消费 上游表 -> 数据流向 表→数据集
#   down: 沿数据流向(表→数据集, 数据集暂无下游消费方)
#   up:   反向(数据集→上游表; 物理表无上游生产逻辑, 为空)
#   both: 双向BFS, 形成"相关表+数据集"血缘网络
@app.get("/api/lineage/graph")
def api_lineage_graph(node: str = Query(..., min_length=1),
                      direction: str = Query("both", pattern="^(up|down|both)$"),
                      depth: int = Query(2, ge=1, le=4),
                      max_nodes: int = Query(300, ge=20, le=800)):
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        dt_s = str(dt)

        # 1. 边集: 数据集 -> 上游表
        rows = d.exec("SELECT dataset_id, db_table FROM etl.dwd_datawind_table_lineage_t_d WHERE dt=%s", (dt_s,), fetch=True)
        up_map, down_map = {}, {}          # up: dsid->[table], down: table->[dsid]
        for dsid, db_table in rows:
            up_map.setdefault(dsid, set()).add(db_table)
            down_map.setdefault(db_table, set()).add(dsid)

        # 2. 定位起始节点（精确优先, 模糊兜底）
        start = None
        matched_note = ""
        if node.isdigit():
            dsid = int(node)
            if dsid in up_map:
                start = ("d", dsid)
        if start is None:
            # 表: 精确 db.table 或 仅表名(匹配首条)
            if "." in node:
                cand = [t for t in down_map if t == node]
            else:
                cand = [t for t in down_map if t.endswith("." + node) or t == node]
            if cand:
                start = ("t", cand[0])
        if start is None:
            # 模糊: 数据集名称 LIKE（取有血缘记录的首条）
            dss = d.exec(
                "SELECT dataset_id FROM etl.ods_datawind_dataset_s_d "
                "WHERE dt=%s AND name LIKE %s ORDER BY dataset_id LIMIT 5",
                (dt_s, "%%%s%%" % node), fetch=True)
            for r in dss:
                if r[0] in up_map:
                    start = ("d", r[0])
                    matched_note = "数据集名称模糊匹配"
                    break
        if start is None:
            # 模糊: 表名/库名 LIKE（取有血缘记录的首条）
            tabs = d.exec(
                "SELECT db_name, table_name FROM etl.ods_doris_table_s_d "
                "WHERE dt=%s AND (table_name LIKE %s OR db_name LIKE %s) "
                "ORDER BY db_name, table_name LIMIT 5",
                (dt_s, "%%%s%%" % node, "%%%s%%" % node), fetch=True)
            for r in tabs:
                t = "%s.%s" % (r[0], r[1])
                if t in down_map:
                    start = ("t", t)
                    matched_note = "表名模糊匹配"
                    break
        if start is None:
            return JSONResponse({"error": "未找到该表/数据集的血缘记录(支持数据集ID、db.table 全名、名称/表名模糊搜索)"}, status_code=404)

        # 3. 双向 BFS 收集节点与边
        nodes = {}   # id -> node dict
        edges = []   # {"from","to","type"}
        seen_e = set()

        def add_table(t):
            if t not in nodes:
                db, tb = t.rsplit(".", 1) if "." in t else ("", t)
                nodes[t] = {"id": "t:" + t, "kind": "table", "db": db, "table": tb,
                            "name": t, "layer": db}
            return "t:" + t

        def add_ds(dsid):
            nid = "d:%d" % dsid
            if nid not in nodes:
                nm = d.exec("SELECT name, folder_path FROM etl.ods_datawind_dataset_s_d WHERE dt=%s AND dataset_id=%s", (dt_s, dsid), fetch=True)
                nodes[nid] = {"id": nid, "kind": "dataset", "dataset_id": dsid,
                              "name": nm[0][0] if nm else ("数据集%d" % dsid),
                              "folder_path": nm[0][1] if nm else "", "db": "", "table": ""}
            return nid

        def add_edge(frm, to, etype):
            k = (frm, to)
            if k not in seen_e:
                seen_e.add(k)
                edges.append({"from": frm, "to": to, "type": etype})

        from collections import deque
        q = deque([(start, 0)])
        visited = {start}
        while q:
            (kind, key), hop = q.popleft()
            if hop >= depth:
                continue
            if kind == "t":
                src_id = "t:" + key
                # 表的下游 = 引用该表的数据集 (数据流向 表→数据集); 表的上游无生产逻辑, 不展开
                if direction in ("down", "both"):
                    for dsid in down_map.get(key, ()):
                        dst_id = add_ds(dsid)
                        add_edge(src_id, dst_id, "consume")
                        st = ("d", dsid)
                        if st not in visited:
                            visited.add(st); q.append((st, hop + 1))
            else:
                src_id = "d:%d" % key
                # 数据集的上游 = 它引用的上游表 (数据流向 表→数据集); 数据集暂无下游消费方
                if direction in ("up", "both"):
                    for t in up_map.get(key, ()):
                        dst_id = add_table(t)
                        add_edge(dst_id, src_id, "consume")
                        st = ("t", t)
                        if st not in visited:
                            visited.add(st); q.append((st, hop + 1))
            if len(nodes) >= max_nodes:
                break

        # 4. 数据集节点补充 layer 占位(表节点layer已含)
        return {"start": start, "direction": direction, "depth": depth, "dt": dt_s,
                "matched": matched_note,
                "nodes": list(nodes.values()), "edges": edges,
                "stats": {"tables": sum(1 for n in nodes.values() if n["kind"] == "table"),
                          "datasets": sum(1 for n in nodes.values() if n["kind"] == "dataset"),
                          "edges": len(edges)}}
    finally:
        d.close()


# ---------------- Doris 表资产（资产扩展） ----------------
@app.get("/api/doris/tables")
def api_doris_tables(keyword: str = "", db: str = "", layer: str = "",
                     type_code: str = "", page: int = Query(1, ge=1),
                     size: int = Query(20, ge=1, le=100)):
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        dt_s = str(dt)
        where = ["dt = %s"]
        args = [dt_s]
        if keyword:
            where.append("(table_name LIKE %s OR table_comment LIKE %s)")
            args += ["%%%s%%" % keyword, "%%%s%%" % keyword]
        if db:
            where.append("db_name = %s")
            args.append(db)
        if layer:
            where.append("layer = %s")
            args.append(layer)
        if type_code:
            where.append("table_type = %s")
            args.append(type_code)
        w = " AND ".join(where)
        cnt = d.exec("SELECT COUNT(*) FROM etl.ods_doris_table_s_d WHERE %s" % w, args, fetch=True)[0][0]
        off = (page - 1) * size
        rows = d.exec(
            "SELECT db_name, table_name, table_type, engine, rows_cnt, data_size, "
            "create_time, update_time, table_comment, layer "
            "FROM etl.ods_doris_table_s_d WHERE %s ORDER BY db_name, table_name LIMIT %s OFFSET %s"
            % (w, size, off), args, fetch=True)
        items = [{"db": r[0], "table": r[1], "type": r[2], "engine": r[3], "rows": r[4],
                  "size": r[5], "ctime": str(r[6]) if r[6] else "", "utime": str(r[7]) if r[7] else "",
                  "comment": (r[8] or "")[:200], "layer": r[9]} for r in rows]
        return {"total": cnt, "page": page, "size": size, "items": items}
    finally:
        d.close()


@app.get("/api/doris/layers")
def api_doris_layers():
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        rows = d.exec("SELECT layer, db_name, COUNT(*) FROM etl.ods_doris_table_s_d WHERE dt=%s GROUP BY layer, db_name ORDER BY layer, db_name", (str(dt),), fetch=True)
        return [{"layer": r[0], "db": r[1], "cnt": r[2]} for r in rows]
    finally:
        d.close()


# ---------------- 模糊搜索建议(血缘页/目录输入框自动补全) ----------------
@app.get("/api/search/suggest")
def api_search_suggest(kw: str = Query(..., min_length=1), limit: int = Query(8, ge=1, le=20)):
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        dt_s = str(dt)
        pat = "%%%s%%" % kw
        dss = d.exec(
            "SELECT dataset_id, name, folder_path FROM etl.ods_datawind_dataset_s_d "
            "WHERE dt=%s AND name LIKE %s ORDER BY dataset_id LIMIT %s",
            (dt_s, pat, limit), fetch=True)
        tabs = d.exec(
            "SELECT db_name, table_name, layer FROM etl.ods_doris_table_s_d "
            "WHERE dt=%s AND (table_name LIKE %s OR table_comment LIKE %s OR db_name LIKE %s) "
            "ORDER BY db_name, table_name LIMIT %s",
            (dt_s, pat, pat, pat, limit), fetch=True)
        return {"datasets": [{"id": r[0], "name": r[1], "folder": r[2] or ""} for r in dss],
                "tables": [{"db": r[0], "table": r[1], "layer": r[2]} for r in tabs]}
    finally:
        d.close()


# ---------------- 变更监控 ----------------
@app.get("/api/changes")
def api_changes(date: str = "", date_from: str = "", date_to: str = "",
                ctype: str = "", keyword: str = "", fields: str = "", source: str = "",
                page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=200)):
    d = get_doris()
    try:
        where = ["1=1"]
        args = []
        if date:
            where.append("dt = %s")
            args.append(date)
        if date_from:
            where.append("dt >= %s")
            args.append(date_from)
        if date_to:
            where.append("dt <= %s")
            args.append(date_to)
        if ctype:
            where.append("change_type = %s")
            args.append(ctype)
        if keyword:
            where.append("(dataset_name LIKE %s OR dataset_id LIKE %s)")
            args += ["%%%s%%" % keyword, "%%%s%%" % keyword]
        if fields:
            where.append("changed_fields LIKE %s")
            args.append("%%%s%%" % fields)
        if source == "daily":
            where.append("remark = %s")
            args.append("每日全量")
        elif source == "poll":
            where.append("remark LIKE %s")
            args.append("%%轮询%%")
        w = " AND ".join(where)
        cnt = d.exec("SELECT COUNT(*) FROM etl.dwd_datawind_dataset_change_i_d WHERE %s" % w, args, fetch=True)[0][0]
        off = (page - 1) * size
        rows = d.exec(
            "SELECT dt, change_id, dataset_id, dataset_name, change_type, change_type_name, "
            "changed_fields, diff_json, folder_path, remark, etl_time "
            "FROM etl.dwd_datawind_dataset_change_i_d WHERE %s ORDER BY dt DESC, change_id DESC LIMIT %s OFFSET %s"
            % (w, size, off), args, fetch=True)
        items = []
        for r in rows:
            diff = None
            try:
                diff = json.loads(r[7]) if r[7] else None
            except Exception:
                diff = {"raw": r[7]}
            items.append({
                "dt": str(r[0]), "change_id": r[1], "dataset_id": r[2], "name": r[3],
                "type": r[4], "type_name": r[5], "fields": r[6] or "",
                "diff": diff, "folder_path": r[8] or "", "remark": r[9] or "",
                "etl_time": str(r[10]) if r[10] else "",
            })
        return {"total": cnt, "page": page, "size": size, "items": items}
    finally:
        d.close()


# ---------------- 类型分布(目录筛选用) ----------------
@app.get("/api/types")
def api_types():
    d = get_doris()
    try:
        dt = latest_dt(d)
        if not dt:
            return JSONResponse({"error": "无快照数据"}, status_code=404)
        rows = d.exec("SELECT type_code, type_name, COUNT(*) FROM etl.ods_datawind_dataset_s_d WHERE dt=%s GROUP BY type_code, type_name ORDER BY COUNT(*) DESC", (dt,), fetch=True)
        return [{"code": r[0], "name": r[1], "cnt": r[2]} for r in rows]
    finally:
        d.close()


# ---------------- 静态页面 ----------------
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.get("/dataset/{dsid}")
def dataset_page(dsid: int):
    return FileResponse(os.path.join(STATIC, "dataset.html"))


@app.get("/lineage")
def lineage_page():
    return FileResponse(os.path.join(STATIC, "lineage.html"))


@app.get("/changes")
def changes_page():
    return FileResponse(os.path.join(STATIC, "changes.html"))
