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
import base64
import hashlib
import json
import logging

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.lineage import lineage as sqlglot_column_lineage
    from sqlglot.optimizer import build_scope
    from sqlglot.optimizer.scope import find_all_in_scope
    SQLGLOT_AVAILABLE = True
except ImportError:  # 部署环境未装 sqlglot 时降级（血缘解析失败不阻塞快照落库）
    sqlglot = None
    exp = None
    sqlglot_column_lineage = None
    build_scope = None
    find_all_in_scope = None
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
                conf = item[2] if len(item) > 2 else "medium"
                path = item[3] if len(item) > 3 else []
                key = (table, field, conf, tuple(path))
                if table and key not in seen:
                    seen.add(key)
                    resolved.append((table, field, conf, path))

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
            refs, ambiguous = [], False
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
                candidate_groups = []
                for inner in subq_map.values():
                    group = resolve_projection(inner, name, self._star_of(inner))
                    if group:
                        candidate_groups.append(group)
                # CTE 投影也参与唯一性判断，避免按遍历顺序猜同名字段来源。
                for cte_name in dict.fromkeys(cte_alias.values()):
                    inner = cte_defs.get(cte_name)
                    if inner is not None:
                        group = resolve_projection(inner, name, self._star_of(inner))
                        if group:
                            candidate_groups.append(group)
                signatures = {
                    tuple(sorted((r[0], r[1], r[2]) for r in group))
                    for group in candidate_groups
                }
                if len(signatures) == 1 and candidate_groups:
                    refs = [r for group in candidate_groups for r in group]
                elif len(signatures) > 1:
                    ambiguous = True
                if not refs and not ambiguous and self.schema is not None:
                    candidates = list(alias_map.values()) + bare_tables
                    hits = self.schema.tables_with_column(name, candidates) if candidates else []
                    if len(hits) == 1:
                        refs = [(hits[0], name, "high", [])]
            add_refs(refs)

        has_star = any(isinstance(node, exp.Star) for node in src_expr.walk())
        if not resolved and not cols and has_star:
            if len(alias_map) == 0 and len(bare_tables) == 1 and not subq_map and not cte_table:
                resolved.append((bare_tables[0], "", "medium", []))
            elif star_real and not subq_map and not cte_table:
                resolved.append((star_real, "", "medium", []))

        is_direct = isinstance(src_expr, exp.Column) and len(cols) == 1
        conf = "high" if is_direct else "medium"
        expression = src_expr.sql() or ""
        return [(table, field,
                 "high" if conf == "high" and inner_conf == "high" else "medium",
                 path + ([expression] if expression else []))
                for table, field, inner_conf, path in resolved]

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

    def _col_edges(self, ast, cte_names, dialect=None):
        """Build field edges from SQLGlot's scope-aware lineage graph."""
        edges, unresolved = [], []
        query = ast.expression if isinstance(ast, (exp.Insert, exp.Create)) else ast

        def set_branches(node):
            if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
                return set_branches(node.this) + set_branches(node.expression)
            return [node] if isinstance(node, exp.Select) else []

        top_selects = set_branches(query)
        if not top_selects:
            return [], []

        # Keep schema-backed star expansion for queries whose output columns
        # cannot be inferred without table metadata.
        for sel in top_selects:
            if self.schema is not None and any(isinstance(p, exp.Star) for p in sel.expressions):
                before_star = len(edges)
                edges.extend(self._star_edges(sel, cte_names))
                if len(edges) == before_star:
                    unresolved.append("*")
            elif any(isinstance(p, exp.Star) for p in sel.expressions):
                unresolved.append("*")

        targets, target_projection_idxs = [], []
        for proj_idx, proj in enumerate(top_selects[0].expressions):
            if isinstance(proj, exp.Star) or not list(proj.find_all(exp.Column)):
                continue
            target = proj.alias_or_name or proj.sql()
            if target and target != "*":
                targets.append(target)
                target_projection_idxs.append(proj_idx)

        cte_defs = {
            cte.alias: cte.this
            for cte in ast.find_all(exp.CTE)
            if isinstance(cte.this, exp.Select)
        }

        try:
            root_scope = build_scope(query)
        except Exception as e:
            logger.debug("Unable to build SQL scope for column lineage: %s", e)
            root_scope = None
        def add_leaf_paths(node, ancestors, target):
            if node.downstream:
                for child in node.downstream:
                    add_leaf_paths(child, ancestors + [node], target)
                return

            predicates_by_step = []
            using_dependencies = []
            for step in ancestors:
                source_select = step.source
                if not isinstance(source_select, exp.Select):
                    continue
                predicates = []
                where = source_select.args.get("where")
                if where is not None:
                    predicates.append(("where", "WHERE条件", where.this))
                for join_index, join in enumerate(source_select.args.get("joins") or []):
                    on = join.args.get("on")
                    if on is not None:
                        predicates.append(("join", "JOIN ON条件", on))
                    using = join.args.get("using") or []
                    if using:
                        predicates.append(("join", "JOIN USING口径", join))
                        # USING columns belong to both sides of the join. Resolve
                        # the right side by its alias; resolve the left side
                        # against preceding sources with the schema-aware resolver.
                        right = join.this
                        right_alias = right.alias or right.name if isinstance(right, exp.Table) else ""
                        prior_sources = ([source_select.args.get("from_")] +
                                         (source_select.args.get("joins") or [])[:join_index])
                        for using_col in using:
                            using_name = using_col.name if hasattr(using_col, "name") else str(using_col)
                            if not using_name:
                                continue
                            if right_alias:
                                using_dependencies.append((source_select,
                                    exp.column(using_name, table=right_alias), "join_using"))
                            for prior in prior_sources:
                                if prior is None:
                                    continue
                                prior_src = prior.this
                                if isinstance(prior_src, exp.Table):
                                    prior_alias = prior_src.alias or prior_src.name
                                    using_dependencies.append((source_select,
                                        exp.column(using_name, table=prior_alias), "join_using"))
                having = source_select.args.get("having")
                if having is not None:
                    predicates.append(("having", "HAVING条件", having.this))
                qualify = source_select.args.get("qualify")
                if qualify is not None:
                    predicates.append(("qualify", "QUALIFY条件", qualify.this))
                group = source_select.args.get("group")
                if group is not None:
                    predicates.append(("group_by", "GROUP BY口径", group))
                unique_predicates = []
                seen_predicates = set()
                for kind, label, expression in predicates:
                    expression_sql = expression.sql(dialect=dialect)
                    key = (kind, expression_sql)
                    if key not in seen_predicates:
                        seen_predicates.add(key)
                        unique_predicates.append((kind, label, expression,
                                                  expression_sql))
                predicates_by_step.append((source_select, unique_predicates))

            # Predicates change the row set or aggregation grain, so they are
            # first-class dependencies of every output on this lineage path.
            for source_select, predicates in predicates_by_step:
                for kind, label, expression, expression_sql in predicates:
                    if kind == "join" and label == "JOIN USING口径":
                        continue
                    columns = (find_all_in_scope(expression, exp.Column)
                               if find_all_in_scope else expression.find_all(exp.Column))
                    for column in columns:
                        refs = self._resolve_src(column, source_select, cte_names,
                                                 cte_defs, 0, {})
                        if not refs:
                            unresolved.append(target)
                        for table, field, _, source_path in refs:
                            if not field:
                                continue
                            src_ref = "%s.%s" % (table, field)
                            dependency = "[口径依赖:%s] %s" % (kind, expression_sql)
                            path = list(source_path) + [dependency]
                            edges.append((target, src_ref, "medium",
                                          json.dumps([path], ensure_ascii=False)))
            for source_select, column, kind in using_dependencies:
                refs = self._resolve_src(column, source_select, cte_names,
                                         cte_defs, 0, {})
                for table, field, _, source_path in refs:
                    if field:
                        dependency = "[口径依赖:%s] JOIN USING (%s)" % (kind, column.name)
                        edges.append((target, "%s.%s" % (table, field), "medium",
                                      json.dumps([list(source_path) + [dependency]],
                                                 ensure_ascii=False)))

            if not isinstance(node.expression, exp.Table):
                unresolved.append(target)
                return
            if node.name == "*":
                unresolved.append(target)
                return
            try:
                field = sqlglot.parse_one(node.name, read=dialect).name
            except Exception:
                field = node.name.rsplit(".", 1)[-1].strip('`"')
            table_expr = node.expression
            table = ".".join(
                part for part in
                (table_expr.catalog, table_expr.db, table_expr.name) if part
            )
            if not table or not field:
                return

            path, is_direct = [], True
            for step in reversed(ancestors):
                if (isinstance(query, (exp.Union, exp.Intersect, exp.Except))
                        and step.name in ("UNION", "INTERSECT", "EXCEPT")):
                    continue
                for predicate_select, predicates in predicates_by_step:
                    if predicate_select is step.source:
                        for _, label, _, expression_sql in predicates:
                            context = "%s: %s" % (label, expression_sql)
                            if context not in path:
                                path.append(context)
                expression = step.expression
                expression_sql = expression.sql(dialect=dialect)
                if not path or path[-1] != expression_sql:
                    path.append(expression_sql)
                if isinstance(expression, exp.Alias):
                    expression = expression.this
                is_direct = is_direct and isinstance(expression, exp.Column)
            confidence = "high" if is_direct else "medium"
            src_ref = "%s.%s" % (table, field)
            edges.append((target, src_ref, confidence,
                          json.dumps([path], ensure_ascii=False)))

        def add_legacy_edges(target, projection_idx):
            fully_resolved = True
            known_sources = {edge[1] for edge in edges if edge[0] == target}
            for sel in top_selects:
                if projection_idx >= len(sel.expressions):
                    fully_resolved = False
                    continue
                proj = sel.expressions[projection_idx]
                src_expr = proj.this if isinstance(proj, exp.Alias) else proj
                source_columns = list(src_expr.find_all(exp.Column))
                for column in source_columns:
                    if not self._resolve_src(column, sel, cte_names, cte_defs, 0, {}):
                        fully_resolved = False
                for table, field, confidence, path in self._resolve_src(
                        src_expr, sel, cte_names, cte_defs, 0, {}):
                    src_ref = "%s.%s" % (table, field) if field else table
                    if src_ref in known_sources:
                        continue
                    known_sources.add(src_ref)
                    edges.append((target, src_ref, confidence,
                                  json.dumps([path], ensure_ascii=False)))
            return fully_resolved

        for target_idx, target in enumerate(targets):
            count_before = len(edges)
            if root_scope is not None and sqlglot_column_lineage is not None:
                try:
                    target_expr = exp.column(target)
                    target_expr.this.meta["case_sensitive"] = True
                    lineage_root = sqlglot_column_lineage(
                        target_expr, query, schema=None, dialect=dialect,
                        scope=root_scope, trim_selects=False, copy=False,
                    )
                    add_leaf_paths(lineage_root, [], target)
                except Exception as e:
                    logger.debug("Column lineage failed for %s: %s", target, e)

            # The original resolver can use Doris column snapshots to repair
            # unresolved or ambiguous leaves from the SQLGlot graph.
            if len(edges) == count_before or target in unresolved:
                projection_idx = target_projection_idxs[target_idx]
                if add_legacy_edges(target, projection_idx):
                    unresolved[:] = [field for field in unresolved if field != target]
                elif target not in unresolved:
                    unresolved.append(target)

        deduped, seen = [], set()
        for edge in edges:
            if edge not in seen:
                seen.add(edge)
                deduped.append(edge)
        deduped.sort(key=lambda edge: (edge[0], edge[1], edge[2], edge[3]))
        stage_label = None
        for comment in _collect_node_meta_comments(ast):
            comment = comment.strip()
            marker = "DATAWIND_NODE_META:"
            if not comment.startswith(marker):
                continue
            token = comment[len(marker):].strip()
            try:
                token += "=" * (-len(token) % 4)
                node_meta = json.loads(base64.urlsafe_b64decode(token).decode("utf-8"))
                order = node_meta.get("order")
                name = node_meta.get("name") or ""
                node_type = node_meta.get("type") or ""
                node_id = node_meta.get("id") or ""
                label = "DataWind节点%s" % (order or "")
                if name:
                    label += ":" + str(name)
                if node_type:
                    label += " / " + str(node_type)
                if node_id:
                    label += " (#%s)" % node_id
                stage_label = label
            except (ValueError, TypeError, UnicodeDecodeError):
                logger.debug("Ignoring malformed DataWind node metadata")
            break
        if stage_label:
            staged_edges = []
            for target, source, confidence, paths_json in deduped:
                try:
                    paths = json.loads(paths_json) if paths_json else []
                except (TypeError, ValueError):
                    paths = []
                staged_paths = []
                for path in paths:
                    path = list(path) if isinstance(path, list) else [str(path)]
                    if not path or path[0] != stage_label:
                        path.insert(0, stage_label)
                    staged_paths.append(path)
                staged_edges.append((target, source, confidence,
                                     json.dumps(staged_paths, ensure_ascii=False)))
            deduped = staged_edges
        return deduped, sorted(set(unresolved))

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
                cols, unresolved_fields = self._col_edges(ast, cte_names, dia)
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
                stat = {"parse_status": "partial" if unresolved_fields else "ok", "error_type": None,
                        "dialect": dia or "default",
                        "table_cnt": len(tables), "col_cnt": len(cols),
                        "conf_dist": conf_dist, "target_table": target_table,
                        "unresolved_fields": unresolved_fields,
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
        try:
            statements = sqlglot.parse(sql_parts, read="doris") if SQLGLOT_AVAILABLE else [sql_parts]
        except Exception:
            # Preserve the original text as one statement so parse() reports the syntax error.
            statements = [sql_parts]
        active_node_comment = None
        for statement in statements:
            node_comments = [c.strip() for c in _collect_node_meta_comments(statement)]
            if node_comments:
                active_node_comment = node_comments[-1]
            one = statement.sql(dialect="doris") if hasattr(statement, "sql") else str(statement)
            if active_node_comment and not node_comments:
                one = "/* %s */\n%s" % (active_node_comment, one)
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
        ok_n = sum(1 for s in stats if s["parse_status"] in ("ok", "partial"))
        fail_n = sum(1 for s in stats if s["parse_status"] == "fail")
        partial_n = sum(1 for s in stats if s["parse_status"] == "partial")
        unresolved_fields = sorted({f for s in stats for f in s.get("unresolved_fields", [])})
        conf_dist = {}
        for _, _, c, _ in col_set:
            conf_dist[c] = conf_dist.get(c, 0) + 1
        return (tbl_set, col_set,
                {"parse_status": "partial" if partial_n or fail_n else
                 ("ok" if ok_n else "fail"),
                 "statements": len(stats), "ok_n": ok_n, "fail_n": fail_n,
                 "partial_n": partial_n, "unresolved_fields": unresolved_fields,
                 "dialect": stats[0].get("dialect") if stats else None,
                 "table_cnt": len(tbl_set), "col_cnt": len(col_set),
                 "conf_dist": conf_dist})


def _collect_node_meta_comments(node):
    """Collect DATAWIND_NODE_META comments across the whole AST.

    SQLGlot may attach a leading comment to the statement, the WITH clause or
    another inner node depending on the original SQL shape, so walk all nodes
    instead of only reading statement-level comments. ``node`` can be a plain
    string when sqlglot.parse fails (syntax-error fallback keeps the raw text).
    """
    found = []
    candidates = list(getattr(node, "comments", None) or [])
    if hasattr(node, "walk"):
        for child in node.walk():
            candidates.extend(getattr(child, "comments", None) or [])
    for c in candidates:
        c = str(c).strip()
        if c.startswith("DATAWIND_NODE_META:"):
            found.append(c)
    return found


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
