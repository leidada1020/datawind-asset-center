# -*- coding: utf-8 -*-
"""血缘解析模块（P1 核心）

基于 sqlglot 解析 DataWind 加工 SQL，产出表级 + 字段级血缘边：
  - 表级:   dataset_id -> 上游 db.table（含血缘类型）
  - 字段级: dataset_id -> {target_field <- src_db.src_table.src_field, confidence}
  - 置信度: high(直接列引用) / medium(函数/表达式/CTE推导) / pending(无法提取)
  - 方言:   按 default -> doris -> mysql 依次兜底（DataWind 加工 SQL 常见 Doris/MySQL 方言）
  - 解析失败: 记录 parse_status=pending, 不中断整体落库

用法:
  from datawind.lineage import LineageParser
  lp = LineageParser()
  tbl, col, stat = lp.parse(dataset_id, sql)
"""
import logging

try:
    import sqlglot
    from sqlglot import exp
    SQLGLOT_AVAILABLE = True
except ImportError:  # 部署环境未装 sqlglot 时降级（血缘解析失败不阻塞快照落库）
    sqlglot = None
    exp = None
    SQLGLOT_AVAILABLE = False

logger = logging.getLogger("lineage")

DIALECTS = [None, "doris", "mysql"]  # 依次兜底尝试


class LineageParser:
    def __init__(self):
        if not SQLGLOT_AVAILABLE:
            logger.warning("sqlglot 未安装, 血缘解析不可用(需 pip install sqlglot)")

    # ---------- 表级 ----------
    def _tables(self, ast):
        """AST -> 上游表集合 {db.table | table}（排除子查询别名表）"""
        tables = set()
        for t in ast.find_all(exp.Table):
            # 跳过 CTE/子查询别名（如 WITH x AS (...) 中的 x）
            if isinstance(t.parent, exp.Subquery):
                continue
            db, name = t.db or "", t.name or ""
            if not name:
                continue
            tables.add(("%s.%s" % (db, name)) if db else name)
        return tables

    # ---------- 字段级 ----------
    def _col_edges(self, ast):
        """AST -> [(target, src, confidence)] 字段血缘边

        对每个 SELECT 投影列:
          - 显式别名(Alias):  target=alias, 解析 this 的列引用
          - 无别名列引用:      target=列名, src=同列引用 (high)
          - 无别名表达式:      target=表达式文本, 提取其中列引用 (medium)
          - 无列引用(常量/聚合): target=表达式文本, src=None (pending)
        """
        edges = []
        for sel in ast.find_all(exp.Select):
            for proj in sel.expressions:
                if isinstance(proj, exp.Alias):
                    target = proj.alias or ""
                    src_expr = proj.this
                else:
                    # 无别名: 用列名/表达式文本作 target
                    if isinstance(proj, exp.Column):
                        src_expr = proj
                        target = proj.name or ""
                    else:
                        src_expr = proj
                        target = proj.sql() or ""
                if not target:
                    continue
                # 去重保序提取列引用
                src_cols = []
                seen = set()
                for c in src_expr.find_all(exp.Column):
                    tbl, name = c.table or "", c.name or ""
                    ref = ("%s.%s" % (tbl, name)) if tbl else name
                    if ref and ref not in seen:
                        seen.add(ref)
                        src_cols.append(ref)
                if len(src_cols) == 1 and isinstance(src_expr, exp.Column):
                    edges.append((target, src_cols[0], "high"))
                elif src_cols:
                    for c in src_cols:
                        edges.append((target, c, "medium"))
                else:
                    edges.append((target, None, "pending"))
        return edges

    # ---------- 主入口 ----------
    def parse(self, dataset_id, sql):
        """解析单条加工 SQL -> (table_edges, column_edges, stat)

        table_edges: [{"db_table": str, "lineage_type": "direct"}]
        column_edges: [{"target_field", "src_table", "src_field", "confidence"}]
        stat: {"parse_status": ok/pending/fail, "dialect", "table_cnt", "col_cnt",
               "conf_dist", "err"}
        """
        if not sql or not SQLGLOT_AVAILABLE:
            return [], [], {"parse_status": "pending" if not sql else "fail",
                            "dialect": None, "table_cnt": 0, "col_cnt": 0,
                            "conf_dist": {}, "err": "no-sql-or-no-sqlglot"}
        last_err = None
        for dia in DIALECTS:
            try:
                kw = {"read": dia} if dia else {}
                ast = sqlglot.parse_one(sql, **kw)
                tables = sorted(self._tables(ast))
                cols = self._col_edges(ast)
                conf_dist = {}
                for _, _, c in cols:
                    conf_dist[c] = conf_dist.get(c, 0) + 1
                stat = {"parse_status": "ok", "dialect": dia or "default",
                        "table_cnt": len(tables), "col_cnt": len(cols),
                        "conf_dist": conf_dist, "err": None}
                return tables, cols, stat
            except Exception as e:
                last_err = e
                continue
        return [], [], {"parse_status": "fail", "dialect": None,
                        "table_cnt": 0, "col_cnt": 0, "conf_dist": {},
                        "err": str(last_err or "")[:200]}

    def parse_multi(self, sql_parts, dataset_id):
        """多条 SQL(分号分隔/多节点)合并解析: 聚合并去重血缘边"""
        all_tables, all_cols, stats = [], [], []
        for one in sql_parts.split(";"):
            one = one.strip()
            if not one:
                continue
            tables, cols, stat = self.parse(dataset_id, one)
            all_tables.extend(tables)
            all_cols.extend(cols)
            stats.append(stat)
        # 汇总
        tbl_set = sorted(set(all_tables))
        col_set = []
        seen = set()
        for t, s, c in all_cols:
            key = (t, s, c)
            if key not in seen:
                seen.add(key)
                col_set.append((t, s, c))
        ok_n = sum(1 for s in stats if s["parse_status"] == "ok")
        fail_n = sum(1 for s in stats if s["parse_status"] == "fail")
        conf_dist = {}
        for _, _, c in col_set:
            conf_dist[c] = conf_dist.get(c, 0) + 1
        return (tbl_set, col_set,
                {"parse_status": "ok" if fail_n == 0 and ok_n else
                 ("fail" if fail_n > ok_n else "partial"),
                 "statements": len(stats), "ok_n": ok_n, "fail_n": fail_n,
                 "dialect": stats[0].get("dialect") if stats else None,
                 "table_cnt": len(tbl_set), "col_cnt": len(col_set),
                 "conf_dist": conf_dist})


def normalize_column_edge(edge, dataset_id):
    """字段血缘边 -> 落库行(拆 src_table 与 src_field)

    edge: (target, src_ref, confidence); src_ref 形如 "tbl.col" 或 "col"
    """
    target, src_ref, conf = edge
    src_table, src_field = "", ""
    if src_ref and "." in src_ref:
        src_table, src_field = src_ref.rsplit(".", 1)
    elif src_ref:
        src_field = src_ref
    return {"dataset_id": dataset_id, "target_field": target,
            "src_table": src_table, "src_field": src_field,
            "confidence": conf}
