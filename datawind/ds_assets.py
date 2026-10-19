# -*- coding: utf-8 -*-
"""DolphinScheduler 调度资产采集模块（调度平台对接: 闭环一盘点 + 闭环二表间血缘）

流程:
  1. DSClient 遍历项目 -> 工作流 -> 工作流详情(任务节点 + taskParams.SQL)
  2. 任务节点名=数仓产出表名 -> 写 ods_ds_workflow_s_d 快照(任务级调度资产)
  3. SQLGlot 解析任务 SQL(INSERT目标 + SELECT来源) -> 表间血缘边 -> ods_ds_lineage_s_d
落库均幂等(先删当日分区再全量), dt datev2 分区 + etl_time, 沿用 _s_d 命名。

依赖: sqlglot(与血缘解析共用); 配置: cfg["ds"] = {enabled, base_url, token}
"""
import json
import logging
import time

try:
    import sqlglot
    from sqlglot import exp as sgl_exp
    HAS_SQLGLOT = True
    # 抑制 sqlglot 对 Doris 特有语法的 stderr warning 刷屏
    logging.getLogger("sqlglot").setLevel(logging.ERROR)
except Exception:
    HAS_SQLGLOT = False

from datawind.ds_client import DSClient


def _table_of(node):
    """取语句目标表: sqlglot 对 'INSERT INTO t (col1,...)' 把 this 解析为
    Schema(this=Table, expressions=[列]), 需解包取 Table"""
    if isinstance(node, sgl_exp.Schema):
        return node.this
    return node


def parse_task_sql(sql_text):
    """解析任务 SQL -> [(dst_db, dst_table, src_db, src_table), ...]（去重）

    兼容形态:
      use ods; CREATE TABLE IF NOT EXISTS ods.`t` (...); INSERT INTO ods.`t` (...) SELECT ... FROM 上游表
      CREATE TABLE x AS SELECT ... FROM ...
    方言依次尝试 doris -> mysql -> 默认; 跟踪 use 语句的库名补全裸表名;
    INSERT VALUES 无来源则跳过; 普通 CREATE(非 AS SELECT) 无来源跳过。
    """
    edges = []
    if not sql_text or not sql_text.strip() or not HAS_SQLGLOT:
        return edges
    stmts = None
    for dialect in ("doris", "mysql", None):
        try:
            stmts = sqlglot.parse(sql_text, dialect=dialect)
            break
        except Exception:
            stmts = None
    if not stmts:
        return edges
    use_db = ""
    for stmt in stmts:
        if not stmt:
            continue
        if isinstance(stmt, sgl_exp.Use):
            use_db = getattr(stmt.this, "name", "") or ""
            continue
        dst = None
        if isinstance(stmt, sgl_exp.Insert):
            dst = _table_of(stmt.this)
        elif isinstance(stmt, sgl_exp.Create):
            dst = _table_of(stmt.this)
            # 仅 CREATE TABLE AS SELECT 带来源; 普通建表语句无上游, 不产边
            if not isinstance(stmt.expression, sgl_exp.Select):
                dst = None
        if dst is None or not isinstance(dst, sgl_exp.Table):
            continue
        ddb = dst.db or ""
        dtb = dst.name or ""
        if not dtb:
            continue
        if not ddb and use_db:
            ddb = use_db
        seen = set()
        for t in stmt.find_all(sgl_exp.Table):
            sdb = t.db or ""
            stb = t.name or ""
            if not stb:
                continue
            if not sdb and use_db:
                sdb = use_db
            if (sdb, stb) == (ddb, dtb) or (sdb, stb) in seen:
                continue
            seen.add((sdb, stb))
            edges.append((ddb, dtb, sdb, stb))
    # 去重
    uniq = []
    for e in edges:
        if e not in uniq:
            uniq.append(e)
    return uniq


def _task_sql(task):
    """从任务节点提取 SQL 文本(taskParams 为 JSON 字符串或 dict)"""
    tp = task.get("taskParams") or ""
    if isinstance(tp, str):
        try:
            tp = json.loads(tp)
        except Exception:
            return ""
    if isinstance(tp, dict):
        return tp.get("sql") or ""
    return ""


def collect_ds_assets(doris, cfg, dt=None, etl_time=None):
    """DS 调度资产全量采集 + 表间血缘落库

    cfg["ds"]: {enabled, base_url, token}; enabled=false/缺失 -> 返回 None(跳过)
    返回统计 dict {projects, workflows, tasks, lineage_edges, sql_tasks}
    """
    ds_cfg = cfg.get("ds") or {}
    if not ds_cfg.get("enabled"):
        return None
    base_url = ds_cfg.get("base_url")
    token = ds_cfg.get("token")
    if not base_url or not token:
        raise RuntimeError("DS 配置缺失字段: base_url/token（配置文件 ds 段）")
    dt = dt or time.strftime("%Y-%m-%d")
    etl_time = etl_time or time.strftime("%Y-%m-%d %H:%M:%S")

    client = DSClient(base_url, token)
    wf_rows, lg_rows = [], []
    stat = {"projects": 0, "workflows": 0, "tasks": 0,
            "sql_tasks": 0, "lineage_edges": 0}

    projects = client.list_projects()
    stat["projects"] = len(projects)
    for p in projects:
        pcode, pname = p.get("code"), p.get("name")
        workflows = client.list_workflows(pcode)
        stat["workflows"] += len(workflows)
        for w in workflows:
            wcode, wname = w.get("code"), w.get("name")
            release_state = w.get("releaseState") or ""
            detail = client.get_workflow_detail(pcode, wcode)
            for t in detail.get("taskDefinitionList") or []:
                tcode = t.get("code")
                tname = t.get("name") or ""
                ttype = t.get("taskType") or ""
                tflag = t.get("flag") or ""
                sql_text = _task_sql(t) if ttype == "SQL" else ""
                wf_rows.append((dt, pcode, wcode, tcode, pname, wname, release_state,
                                tname, ttype, tflag, sql_text, etl_time))
                stat["tasks"] += 1
                if sql_text:
                    stat["sql_tasks"] += 1
                for ddb, dtb, sdb, stb in parse_task_sql(sql_text):
                    lg_rows.append((dt, pcode, pname, wcode, wname, tcode,
                                    tname, ddb, dtb, sdb, stb, etl_time))
                    stat["lineage_edges"] += 1

    # 幂等写入: 先删当日分区再全量
    if wf_rows:
        doris.exec("DELETE FROM %s WHERE dt = %%s"
                   % "{db}.ods_ds_workflow_s_d".format(db=doris.db), (dt,))
        sql_w = ("INSERT INTO {db}.ods_ds_workflow_s_d "
                 "(dt, project_code, workflow_code, task_code, project_name, workflow_name, "
                 "release_state, task_name, task_type, task_flag, sql_text, etl_time) "
                 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                 ).format(db=doris.db)
        doris.execmany(sql_w, wf_rows)
    if lg_rows:
        doris.exec("DELETE FROM %s WHERE dt = %%s"
                   % "{db}.ods_ds_lineage_s_d".format(db=doris.db), (dt,))
        sql_l = ("INSERT INTO {db}.ods_ds_lineage_s_d "
                 "(dt, project_code, project_name, workflow_code, workflow_name, task_code, "
                 "task_name, dst_db, dst_table, src_db, src_table, etl_time) "
                 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                 ).format(db=doris.db)
        doris.execmany(sql_l, lg_rows)
    return stat
