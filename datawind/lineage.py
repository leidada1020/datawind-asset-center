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
import hashlib
import json
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
    def __init__(self, schema_provider=None):
        """schema_provider: 可选, 提供"表 -> 列集合"用于无前缀列消歧与 SELECT * 展开
        (如 datawind.schema_provider.DorisSchemaProvider); 为 None 时保持纯语法解析(schema-naive)
        """
        self.schema = schema_provider
        if not SQLGLOT_AVAILABLE:
            logger.warning("sqlglot 未安装, 血缘解析不可用(需 pip install sqlglot)")

    # ---------- 表级 ----------
    def _tables(self, ast):
        """AST -> 上游表集合 {db.table | table}

        过滤 CTE 名引用(如 FROM cte_name, 非真实上游表);
        子查询内真实表(如 FROM (SELECT ... FROM ods.t) x 中的 ods.t)正常保留;
        INSERT/CTAS: 排除目标表(下游), 只取 SELECT 来源
        """
        if isinstance(ast, exp.Insert):
            ast = ast.expression or ast  # 只解析 SELECT 部分
        tables = set()
        cte_names = {c.alias for c in ast.find_all(exp.CTE)}
        for t in ast.find_all(exp.Table):
            name = t.name or ""
            if not name or name in cte_names:
                continue
            db = t.db or ""
            tables.add(("%s.%s" % (db, name)) if db else name)
        return tables

    # ---------- 字段级 ----------
    def _from_ctx(self, sel, cte_names):
        """仅取当前 SELECT 直属 FROM/JOIN, 收集:
        alias_map:   真实表别名 -> 真实表(db.table 或 table)
        bare_tables: 无别名直接引用的真实表
        subq_map:    子查询别名 -> 子查询顶层 Select
        cte_table:   本层 FROM/JOIN 引用的 CTE 名集合(Table.name in cte_names)
        cte_alias:   本层 CTE 引用别名 -> CTE 名(如 {r: reg, h: his}, 供列前缀 r.col 定位 CTE)
        star_real:   SELECT * 场景下 FROM 唯一真实表(单表)或 None
        只取 sel.args 直属节点, 不穿透嵌套子查询(避免内层别名污染外层解析)
        """
        alias_map, bare_tables, subq_map, cte_table, cte_alias, star_real = {}, [], {}, set(), {}, None
        from_node = sel.args.get("from_")
        joins = sel.args.get("joins") or []
        sources = []
        if from_node is not None:
            sources.append(from_node)
        sources.extend(joins)
        for node_ in sources:
            src = node_.this
            if isinstance(src, exp.Subquery):
                inner = src.this
                if isinstance(inner, exp.Select):
                    subq_map[src.alias or ""] = inner
            elif isinstance(src, exp.Table):
                name = src.name or ""
                if not name:
                    continue
                if name in cte_names:
                    cte_table.add(name)
                    cte_alias[src.alias or name] = name
                    continue
                real = ("%s.%s" % (src.db, name)) if src.db else name
                if src.alias:
                    alias_map[src.alias] = real
                else:
                    bare_tables.append(real)
        # star 兜底: 仅当 FROM 为唯一真实表(无子查询/无CTE/无别名), SELECT * 才能归属该表
        if not subq_map and not cte_table and len(alias_map) == 0 and len(bare_tables) == 1:
            star_real = bare_tables[0]
        return alias_map, bare_tables, subq_map, cte_table, cte_alias, star_real

    def _star_of(self, sel):
        """sel 自身 FROM 唯一真实表(用于 SELECT * 子查询的兜底), 递归穿透 Star 链:
        select * from (select * from ods.x) a  -> ods.x
        """
        from_node = sel.args.get("from_")
        joins = sel.args.get("joins") or []
        if from_node is None or joins:
            return None
        src = from_node.this
        if isinstance(src, exp.Table):
            if src.alias or not src.name:
                return None
            return ("%s.%s" % (src.db, src.name)) if src.db else src.name
        if isinstance(src, exp.Subquery):
            inner = src.this
            if isinstance(inner, exp.Select):
                has_star = any(isinstance(p, exp.Star) for p in inner.expressions)
                if has_star:
                    return self._star_of(inner)
        return None

    def _cte_name_for_alias(self, sel, alias, cte_names):
        """从当前 SELECT 直属 FROM/JOIN 中找到 alias 对应 Table.name(即 CTE 名)"""
        from_node = sel.args.get("from_")
        joins = sel.args.get("joins") or []
        sources = ([from_node] if from_node is not None else []) + joins
        for node_ in sources:
            src = node_.this
            if isinstance(src, exp.Table) and src.alias == alias and src.name in cte_names:
                return src.name
        return None

    def _find_projection(self, sel, col_name, cte_defs, star_real):
        """在 SELECT 顶层投影中找 col_name 的定义:
        Returns: (expr, owner_sel, star_table)
          expr=None, star_table=表名  -> 投影为 SELECT * 且底层唯一表(该表兜底)
          expr=None, star_table=None  -> 未找到
        """
        for proj in sel.expressions:
            if isinstance(proj, exp.Alias) and proj.alias == col_name:
                return proj.this, sel, None
            if isinstance(proj, exp.Column) and proj.name == col_name:
                return proj, sel, None
        # SELECT * 兜底: 该子查询的 FROM 唯一真实表
        star = None
        for proj in sel.expressions:
            if isinstance(proj, exp.Star):
                star = True
                break
        if star and star_real:
            return None, sel, star_real
        return None, None, None

    def _resolve_src(self, src_expr, sel, cte_names, cte_defs, depth, memo):
        """返回 (真实表, 字段, 置信度, 转换路径)；路径按来源到目标排列。"""
        if depth > 8:
            return []
        alias_map, bare_tables, subq_map, cte_table, cte_alias, star_real = self._from_ctx(sel, cte_names)
        cols = list(src_expr.find_all(exp.Column))
        resolved, seen = [], set()

        def add_refs(refs):
            for item in refs:
                table, field = item[0], item[1]
                path = item[3] if len(item) > 3 else []
                key = (table, field, tuple(path))
                if table and key not in seen:
                    seen.add(key)
                    resolved.append((table, field, path))

        def resolve_projection(inner, name, star_table):
            expr2, owner2, star2 = self._find_projection(inner, name, cte_defs, star_table)
            if expr2 is not None:
                return self._resolve_src(expr2, owner2, cte_names, cte_defs, depth + 1, memo)
            if star2:
                return [(star2, name, "high", [])]
            return []

        for col in cols:
            name, qualifier = col.name or "", col.table or ""
            if not name:
                continue
            refs = []
            if qualifier in alias_map:
                refs = [(alias_map[qualifier], name, "high", [])]
            elif qualifier in subq_map:
                refs = resolve_projection(subq_map[qualifier], name, self._star_of(subq_map[qualifier]))
            elif qualifier in cte_alias:
                cte_name = cte_alias[qualifier]
                if cte_name in cte_defs:
                    refs = resolve_projection(cte_defs[cte_name], name, self._star_of(cte_defs[cte_name]))
            elif qualifier:
                # 未知前缀不猜来源。
                refs = []
            elif len(alias_map) == 0 and len(bare_tables) == 1 and not subq_map and not cte_table:
                refs = [(bare_tables[0], name, "high", [])]
            else:
                # 先按当前 SELECT 中的子查询投影解析，避免把不同作用域混在一起。
                for inner in subq_map.values():
                    refs = resolve_projection(inner, name, self._star_of(inner))
                    if refs:
                        break
                if not refs:
                    # CTE 投影作为兜底；保留表达式的全部来源字段。
                    for cte_name in dict.fromkeys(cte_alias.values()):
                        inner = cte_defs.get(cte_name)
                        if inner is None:
                            continue
                        refs = resolve_projection(inner, name, self._star_of(inner))
                        if refs:
                            break
                if not refs and self.schema is not None:
                    candidates = list(alias_map.values()) + bare_tables
                    hits = self.schema.tables_with_column(name, candidates) if candidates else []
                    if len(hits) == 1:
                        refs = [(hits[0], name, "high", [])]
            add_refs(refs)

        if not resolved and not cols:
            if len(alias_map) == 0 and len(bare_tables) == 1 and not subq_map and not cte_table:
                resolved.append((bare_tables[0], "", []))
            elif star_real and not subq_map and not cte_table:
                resolved.append((star_real, "", []))

        is_direct = isinstance(src_expr, exp.Column) and len(cols) == 1
        conf = "high" if is_direct else "medium"
        expression = src_expr.sql() or ""
        return [(table, field, conf, path + ([expression] if expression else []))
                for table, field, path in resolved]

    def _star_edges(self, sel, cte_names):
        """顶层 SELECT * 展开: 用 schema 将 * 展开为 FROM 各真实表的列

        -> [(target_col, "db.table.col", conf)]
        单表: 全部列直接归属(high);
        多表: 列名在所有来源表中唯一归属某表则归属(high), 同名列跳过(避免假阳性);
        含子查询/CTE 的 FROM 不展开(保持原跳过行为)
        """
        alias_map, bare_tables, subq_map, cte_table, cte_alias, _ = self._from_ctx(sel, cte_names)
        if subq_map or cte_table:
            return []
        tables = list(alias_map.values()) + bare_tables
        if not tables:
            return []
        cols_map = {}
        for t in tables:
            cset = self.schema.columns_of(t) if self.schema else None
            if cset:
                cols_map[t] = cset
        if not cols_map:
            return []
        edges, seen_col = [], set()
        if len(tables) == 1:
            t = tables[0]
            for col in sorted(cols_map[t]):
                key = col.lower()
                if key in seen_col:
                    continue
                seen_col.add(key)
                edges.append((col, "%s.%s" % (t, col), "high",
                              json.dumps([["SELECT *"]], ensure_ascii=False)))
        else:
            for t, cset in cols_map.items():
                for col in sorted(cset):
                    key = col.lower()
                    if key in seen_col:
                        continue
                    owners = [tt for tt in cols_map
                              if any((c or "").lower() == key for c in cols_map[tt])]
                    if len(owners) == 1:
                        seen_col.add(key)
                        edges.append((col, "%s.%s" % (owners[0], col), "high",
                                      json.dumps([["SELECT *"]], ensure_ascii=False)))
        return edges

    def _col_edges(self, ast, cte_names):
        """AST -> (target, src, confidence, transform_path_json) 字段血缘边

        只取最外层顶层 SELECT 的投影列作为目标字段; 来源经 _resolve_src 递归穿透
        子查询/CTE/聚合, 真实表直连 high, 表达式/聚合/穿透 medium;
        无法归属的裸列名不再产出(消除"来源表为空"噪音, 根治结果列来自子查询时血缘归零)
        """
        edges = []
        if isinstance(ast, exp.Select):
            top_selects = [ast]
        else:
            top_selects = list(ast.find_all(exp.Select))[:1] or []
        # CTE 定义映射(整棵 AST, 供递归穿透)
        cte_defs = {}
        for c in ast.find_all(exp.CTE):
            if isinstance(c.this, exp.Select):
                cte_defs[c.alias] = c.this
        memo = {}
        for sel in top_selects:
            # 顶层 SELECT *: schema 可用时按 FROM 真实表展开(单表全列/多表列名唯一消歧)
            if self.schema is not None and any(isinstance(p, exp.Star) for p in sel.expressions):
                edges.extend(self._star_edges(sel, cte_names))
            for proj in sel.expressions:
                if isinstance(proj, exp.Star):
                    continue
                if isinstance(proj, exp.Alias):
                    target = proj.alias or ""
                    src_expr = proj.this
                else:
                    if isinstance(proj, exp.Column):
                        src_expr = proj
                        target = proj.name or ""
                    else:
                        src_expr = proj
                        target = proj.sql() or ""
                if not target:
                    continue
                resolved = self._resolve_src(src_expr, sel, cte_names, cte_defs, 0, memo)
                seen = set()
                for t, f, conf, path in resolved:
                    key = (t, f, conf, tuple(path))
                    if key in seen:
                        continue
                    seen.add(key)
                    ref = ("%s.%s" % (t, f)) if f else t
                    edges.append((target, ref, conf, json.dumps([path], ensure_ascii=False)))
        # 解析层去重保序(同 target/src/conf 只保留一条)
        seen_e = set()
        cols_dedup = []
        for e in edges:
            if e not in seen_e:
                seen_e.add(e)
                cols_dedup.append(e)
        return cols_dedup

    # ---------- 主入口 ----------
    def parse(self, dataset_id, sql):
        """解析单条加工 SQL -> (table_edges, column_edges, stat)

        table_edges: [{"db_table": str, "lineage_type": "direct"}]
        column_edges: (target_field, source_ref, confidence, transform_path_json)
        stat: {"parse_status": ok/pending/fail, "error_type": no-sql/no-sqlglot/parse-fail/None,
               "dialect", "table_cnt", "col_cnt", "conf_dist", "target_table", "err"}
        target_table: INSERT/CTAS 的目标表(下游), 供 DS 任务血缘等场景使用; SELECT 场景为空
        """
        if not sql or not SQLGLOT_AVAILABLE:
            if not sql:
                return [], [], {"parse_status": "pending", "error_type": "no-sql",
                                "dialect": None, "table_cnt": 0, "col_cnt": 0,
                                "conf_dist": {}, "target_table": "", "err": "empty-sql"}
            return [], [], {"parse_status": "fail", "error_type": "no-sqlglot",
                            "dialect": None, "table_cnt": 0, "col_cnt": 0,
                            "conf_dist": {}, "target_table": "", "err": "no-sqlglot"}
        last_err = None
        for dia in DIALECTS:
            try:
                kw = {"read": dia} if dia else {}
                ast = sqlglot.parse_one(sql, **kw)
                # INSERT/CTAS 目标表(下游): 不并入上游 tables, 单独记录
                target_table = ""
                if isinstance(ast, exp.Insert):
                    try:
                        tgt = ast.this
                        if isinstance(tgt, exp.Table) and (tgt.name or ""):
                            target_table = ("%s.%s" % (tgt.db, tgt.name)) if tgt.db else tgt.name
                    except Exception:
                        target_table = ""
                tables = sorted(self._tables(ast))
                # CTE 名从整棵 AST 根收集(嵌套 WITH 也能覆盖), 供各层 Select 过滤
                cte_names = {c.alias for c in ast.find_all(exp.CTE)}
                cols = self._col_edges(ast, cte_names)
                # 解析层去重保序(同字段边及同处理路径只保留一条)
                seen_e = set()
                cols_dedup = []
                for e in cols:
                    if e not in seen_e:
                        seen_e.add(e)
                        cols_dedup.append(e)
                cols = cols_dedup
                conf_dist = {}
                for _, _, c, _ in cols:
                    conf_dist[c] = conf_dist.get(c, 0) + 1
                stat = {"parse_status": "ok", "error_type": None,
                        "dialect": dia or "default",
                        "table_cnt": len(tables), "col_cnt": len(cols),
                        "conf_dist": conf_dist, "target_table": target_table,
                        "err": None}
                return tables, cols, stat
            except Exception as e:
                last_err = e
                continue
        return [], [], {"parse_status": "fail", "error_type": "parse-fail",
                        "dialect": None, "table_cnt": 0, "col_cnt": 0, "conf_dist": {},
                        "target_table": "", "err": str(last_err or "")[:200]}

    def parse_multi(self, sql_parts, dataset_id):
        """多条 SQL(分号分隔/多节点)合并解析: 聚合并去重血缘边"""
        sql_parts = sql_parts or ""
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
        path_map = {}
        for t, s, c, paths_json in all_cols:
            key = (t, s, c)
            paths = json.loads(paths_json) if paths_json else []
            merged = path_map.setdefault(key, [])
            for path in paths:
                if path not in merged:
                    merged.append(path)
        for (t, s, c), paths in path_map.items():
            col_set.append((t, s, c, json.dumps(paths, ensure_ascii=False)))
        ok_n = sum(1 for s in stats if s["parse_status"] == "ok")
        fail_n = sum(1 for s in stats if s["parse_status"] == "fail")
        conf_dist = {}
        for _, _, c, _ in col_set:
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

    edge: (target, src_ref, confidence, transform_path_json); src_ref 形如 "tbl.col" 或 "col"
    """
    target, src_ref, conf, transform_path = edge
    src_table, src_field = "", ""
    if src_ref and "." in src_ref:
        src_table, src_field = src_ref.rsplit(".", 1)
    elif src_ref:
        src_field = src_ref
    return {"dataset_id": dataset_id, "target_field": target,
            "src_table": src_table, "src_field": src_field,
            "confidence": conf, "transform_path": transform_path,
            "transform_id": hashlib.sha256((transform_path or "[]").encode("utf-8")).hexdigest()}
