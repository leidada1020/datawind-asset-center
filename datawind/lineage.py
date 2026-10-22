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
        """AST -> 上游表集合 {db.table | table}

        过滤 CTE 名引用(如 FROM cte_name, 非真实上游表);
        子查询内真实表(如 FROM (SELECT ... FROM ods.t) x 中的 ods.t)正常保留
        """
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
                    if src.alias:
                        cte_alias[src.alias] = name
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
        """递归解析投影表达式 src_expr 的真实来源边列表 [(table, field, conf)]

        规则:
          - 带表前缀列: 真实表直连(high); 子查询/CTE 列引用 -> 递归进定义
          - 无前缀列: 唯一真实表直连; 否则按 FROM 顺序递归子查询/CTE 投影; SELECT * 用底层唯一表兜底
          - 无法归属(多表同名列且无定义): 丢弃, 不产裸列名噪音边
          - 无列引用(常量/COUNT(*)): 唯一真实表/唯一*表 -> (表, '', medium); 否则不产
          - 置信度: 最终表达式为直接列引用 high, 表达式/聚合/穿透后 medium
        """
        if depth > 8:
            return []
        alias_map, bare_tables, subq_map, cte_table, cte_alias, star_real = self._from_ctx(sel, cte_names)
        cols = list(src_expr.find_all(exp.Column))
        resolved = []
        seen = set()
        for c in cols:
            name, tbl_alias = c.name or "", c.table or ""
            if not name:
                continue
            if tbl_alias:
                if tbl_alias in alias_map:
                    ref = (alias_map[tbl_alias], name)
                elif tbl_alias in subq_map:
                    inner = subq_map[tbl_alias]
                    expr2, owner2, star2 = self._find_projection(inner, name, cte_defs, star_real)
                    if expr2 is not None:
                        refs = self._resolve_src(expr2, owner2, cte_names, cte_defs,
                                                 depth + 1, memo)
                        if refs:
                            ref = (refs[0][0], refs[0][1])
                        else:
                            continue
                    elif star2:
                        ref = (star2, name)
                    else:
                        continue
                elif tbl_alias in cte_alias:
                    # 别名实际是 CTE 引用(如 FROM reg r), 由别名映射定位 CTE 定义
                    tname = cte_alias.get(tbl_alias)
                    if tname and tname in cte_defs:
                        inner = cte_defs[tname]
                        expr2, owner2, star2 = self._find_projection(inner, name, cte_defs, self._star_of(inner))
                        if expr2 is not None:
                            refs = self._resolve_src(expr2, owner2, cte_names, cte_defs,
                                                     depth + 1, memo)
                            if refs:
                                ref = (refs[0][0], refs[0][1])
                            else:
                                continue
                        elif star2:
                            ref = (star2, name)
                        else:
                            continue
                    else:
                        continue
                else:
                    continue  # 未知前缀, 丢弃
            else:
                # 无前缀
                if len(alias_map) == 0 and len(bare_tables) == 1 and not subq_map and not cte_table:
                    ref = (bare_tables[0], name)
                else:
                    # 优先在子查询投影中找定义(按 FROM 顺序), 再 CTE
                    found = False
                    for alias, inner in subq_map.items():
                        expr2, owner2, star2 = self._find_projection(inner, name, cte_defs,
                                                                     self._star_of(inner))
                        if expr2 is not None:
                            refs = self._resolve_src(expr2, owner2, cte_names, cte_defs,
                                                     depth + 1, memo)
                            if refs:
                                ref = (refs[0][0], refs[0][1])
                            else:
                                continue
                            found = True
                            break
                        elif star2:
                            ref = (star2, name)
                            found = True
                            break
                    if not found:
                        for tname, inner in cte_defs.items():
                            expr2, owner2, star2 = self._find_projection(inner, name, cte_defs,
                                                                         self._star_of(inner))
                            if expr2 is not None:
                                refs = self._resolve_src(expr2, owner2, cte_names, cte_defs,
                                                         depth + 1, memo)
                                if refs:
                                    ref = (refs[0][0], refs[0][1])
                                else:
                                    continue
                                found = True
                                break
                            elif star2:
                                ref = (star2, name)
                                found = True
                                break
                    if not found:
                        continue
            key = (ref[0], ref[1])
            if key in seen or not ref[0]:
                continue
            seen.add(key)
            resolved.append(ref)
        if not resolved and not cols:
            # 聚合/常量(COUNT(*)等): 归属唯一真实表或唯一*表
            if len(alias_map) == 0 and len(bare_tables) == 1 and not subq_map and not cte_table:
                resolved.append((bare_tables[0], ""))
            elif star_real and not subq_map and not cte_table:
                resolved.append((star_real, ""))
        # 置信度: src_expr 为纯 Column -> high; 否则 medium
        is_direct = isinstance(src_expr, exp.Column) and len(cols) == 1
        conf = "high" if is_direct else "medium"
        out = []
        for t, f in resolved:
            out.append((t, f, conf))
        return out

    def _col_edges(self, ast, cte_names):
        """AST -> [(target, src, confidence)] 字段血缘边(仅最终查询结果字段)

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
                for t, f, conf in resolved:
                    key = (t, f, conf)
                    if key in seen:
                        continue
                    seen.add(key)
                    ref = ("%s.%s" % (t, f)) if f else t
                    edges.append((target, ref, conf))
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
                # CTE 名从整棵 AST 根收集(嵌套 WITH 也能覆盖), 供各层 Select 过滤
                cte_names = {c.alias for c in ast.find_all(exp.CTE)}
                cols = self._col_edges(ast, cte_names)
                # 解析层去重保序(同 target/src/conf 只保留一条, 防嵌套层级重复产出)
                seen_e = set()
                cols_dedup = []
                for e in cols:
                    if e not in seen_e:
                        seen_e.add(e)
                        cols_dedup.append(e)
                cols = cols_dedup
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
