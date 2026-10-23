# -*- coding: utf-8 -*-
"""数据模型：数据集归一化、文件夹树解析、变更检测（P0 地基）"""
import json
import time


def to_dt(s):
    """字符串 -> 'YYYY-MM-DD HH:MM:SS'；无效返回 None"""
    if not s:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(str(s), "%Y-%m-%d %H:%M:%S"))
    except ValueError:
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(str(s), "%Y-%m-%d"))
        except ValueError:
            return None


def normalize_dataset(ds):
    """dataSetList 单条记录 -> 落库字段字典（与 ods_datawind_dataset_s_d 列对应）"""
    ups = sorted({"%s.%s" % (u.get("dbName", ""), u.get("tableName", ""))
                  for u in (ds.get("upstreamInfo") or []) if u.get("tableName")})
    t = ds.get("type") or {}
    return {
        "dataset_id": ds.get("id"),
        "name": ds.get("name"),
        "owner_email": ds.get("owner"),
        "type_code": t.get("code"),
        "type_name": t.get("name", ""),
        "ctime": to_dt(ds.get("ctime")),
        "mtime": to_dt(ds.get("mtime")),
        "frequency": ds.get("frequency"),
        "schedule_time": str(ds.get("scheduleTime")) if ds.get("scheduleTime") is not None else None,
        "schedule_day": str(ds.get("scheduleDay")) if ds.get("scheduleDay") is not None else None,
        "last_sync_time": to_dt(ds.get("lastSyncTime")),
        "last_success_task_time": to_dt(ds.get("lastSuccessTaskTime")),
        "rows_cnt": ds.get("rowsForLastSuccessTaskTime"),
        "elapsed_last_success": ds.get("elapsedTimeForLastSuccessTaskTime"),
        "ttl": ds.get("ttl"),
        "status": ds.get("status"),
        "sync_type": ds.get("syncType"),
        "upstream": ", ".join(ups),
        "is_hive_table": 1 if ds.get("hiveTableUsed") else 0,
        "is_hive_partition": 1 if ds.get("hivePartitionSpecified") else 0,
        "hsql_used": 1 if ds.get("hsqlUsed") else 0,
        "url": ds.get("url"),
    }


def flatten_folder_tree(data):
    """文件夹树原始结构 -> 平铺列表 [{folder_id,parent_id,folder_name,folder_path}]

    支持 public/private 两个根命名空间；顶层节点 parent_id=None。
    """
    rows = []

    def walk(items, path, parent_id):
        for it in items or []:
            fid = it.get("id")
            name = it.get("name") or ""
            rows.append({
                "folder_id": fid,
                "parent_id": parent_id,
                "folder_name": name,
                "folder_path": "/".join(path + [name]),
            })
            walk(it.get("childList"), path + [name], fid)

    for ns in ("public", "private"):
        walk(data.get(ns), [], None)
    return rows


def build_folder_path_map(data):
    """文件夹树 -> {folder_id: 路径字符串}（数据集归属解析用）"""
    return {r["folder_id"]: r["folder_path"] for r in flatten_folder_tree(data)}


def normalize_info(info):
    """/dataset/{id}/info 提取落库所需字段"""
    bc = info.get("baseConf") or {}
    return {
        "folder_id": bc.get("parentId"),
        "connection_mode": bc.get("connectionMode"),
        "sync_mode": bc.get("syncMode"),
        "partition_filter": bc.get("partitionDefaultFilter"),
        "confidentiality": bc.get("confidentiality") or "",
    }


# 配置域字段（config_changed 检测）
CONFIG_FIELDS = ["name", "type_code", "status", "sync_type", "frequency", "schedule_time",
                 "schedule_day", "ttl", "connection_mode", "partition_filter", "confidentiality"]

CHANGE_NAMES = {"added": "新增", "deleted": "删除", "config_changed": "配置修改",
                "logic_changed": "加工逻辑变化", "source_changed": "数据源切换",
                "upstream_changed": "上游表变化", "folder_changed": "文件夹变更"}


def diff_changes(current, prev):
    """对比当前 vs 上一个快照，生成变更列表。

    current/prev: {dataset_id: 归一化字典}
    返回: [(change_type, dataset_id, dataset_name, changed_fields, diff_json, folder_path, mtime), ...]
    末位 mtime=数据集更新时间(变更发生时 DataWind 元数据值)
    """
    changes = []
    # 新增
    for i, d in current.items():
        if i not in prev:
            changes.append(("added", i, d["name"], "ALL",
                            json.dumps({"new": {"name": d["name"], "folder_path": d["folder_path"]}},
                                       ensure_ascii=False),
                            d["folder_path"], d.get("mtime") or ""))
    # 删除
    for i, old in prev.items():
        if i not in current:
            changes.append(("deleted", i, old.get("name") or "", "DELETED",
                            json.dumps({"old": {"name": old.get("name"), "folder_path": old.get("folder_path")}},
                                       ensure_ascii=False),
                            old.get("folder_path"), old.get("mtime") or ""))
    # 修改
    for i, d in current.items():
        old = prev.get(i)
        if not old:
            continue
        diff = {}
        cfg_changed = []
        for f in CONFIG_FIELDS:
            if old.get(f) != d.get(f):
                cfg_changed.append(f)
                diff[f] = {"old": old.get(f), "new": d.get(f)}
        if cfg_changed:
            changes.append(("config_changed", i, d["name"], ",".join(cfg_changed),
                            json.dumps(diff, ensure_ascii=False), d["folder_path"],
                            d.get("mtime") or ""))
        if old.get("folder_id") != d.get("folder_id"):
            changes.append(("folder_changed", i, d["name"], "folder_id,folder_path",
                            json.dumps({"folder_id": {"old": old.get("folder_id"), "new": d.get("folder_id")},
                                        "folder_path": {"old": old.get("folder_path"), "new": d.get("folder_path")}},
                                       ensure_ascii=False),
                            d["folder_path"], d.get("mtime") or ""))
        # 加工逻辑变化: 加工SQL全文对比(直连/无SQL数据集不产生该类型)
        old_sql = (old.get("sql_logic") or "").strip()
        new_sql = (d.get("sql_logic") or "").strip()
        if old_sql and new_sql and old_sql != new_sql:
            changes.append(("logic_changed", i, d["name"], "sql_logic",
                            json.dumps({"sql_logic": {"old": old_sql[:2000], "new": new_sql[:2000]}},
                                       ensure_ascii=False),
                            d["folder_path"], d.get("mtime") or ""))
        # 数据源切换: type/id/cluster/db 任一变化
        src_fields = ["data_source_type", "data_source_id", "cluster_name", "source_db"]
        src_diff = {}
        for f in src_fields:
            if (old.get(f) or "") != (d.get(f) or ""):
                src_diff[f] = {"old": old.get(f) or "", "new": d.get(f) or ""}
        if src_diff:
            changes.append(("source_changed", i, d["name"], ",".join(src_diff.keys()),
                            json.dumps(src_diff, ensure_ascii=False),
                            d["folder_path"], d.get("mtime") or ""))
        if (old.get("upstream") or "") != (d.get("upstream") or ""):
            changes.append(("upstream_changed", i, d["name"], "upstream",
                            json.dumps({"upstream": {"old": old.get("upstream"), "new": d.get("upstream")}},
                                       ensure_ascii=False),
                            d["folder_path"], d.get("mtime") or ""))
    return changes
