# -*- coding: utf-8 -*-
"""Doris 表结构提供者（schema-aware 列消歧的数据源）

从每日 Doris 字段资产快照 etl.ods_doris_column_s_d（run_daily 采集 information_schema.columns）
**全量加载** "上游表 -> 列名集合" 到内存（TTL 刷新），供 LineageParser 做无前缀列消歧与 SELECT * 展开。

设计：
  - 全量快照: 一次查询全业务库列(约 4.7 万行), 内存约几 MB; 按 ttl 刷新(生产 poll 5min 用 300s, run_daily 用 300s)
  - 表名兼容 db.table / 裸表名(裸表名按传入库兜底)
  - 快照不存在的表 -> 返回 None, 解析器退回 schema-naive 分支(不降级现有结果)
  - 并发安全: threading.RLock 保护快照重建
用法：
  provider = DorisSchemaProvider(doris, ttl=300)
  parser = LineageParser(schema_provider=provider)
"""
import logging
import threading
import time

logger = logging.getLogger("lineage.schema")

_DB = "etl"
_TBL = "%s.ods_doris_column_s_d" % _DB


class DorisSchemaProvider:
    def __init__(self, doris, ttl=300):
        self.doris = doris
        self.ttl = ttl
        self._all = {}        # "db.table" -> set(column)
        self._t = 0.0
        self._lock = threading.RLock()
        self._dt = None

    # ---------- 内部 ----------
    def _load_all(self):
        """全量加载最新 dt 的列信息: {db.table -> set(column)}"""
        with self._lock:
            now = time.time()
            if self._all and (now - self._t) < self.ttl:
                return
            if self._dt is None:
                rows = self.doris.exec(
                    "SELECT MAX(dt) FROM %s" % _TBL, fetch=True) or []
                if rows and rows[0] and rows[0][0]:
                    self._dt = rows[0][0]
            if not self._dt:
                self._all, self._t = {}, now
                return
            rows = self.doris.exec(
                "SELECT db_name, table_name, column_name FROM %s WHERE dt = %%s"
                % _TBL, (self._dt,), fetch=True) or []
            all_map = {}
            for db, tb, col in rows:
                if not db or not tb:
                    continue
                all_map.setdefault("%s.%s" % (db, tb), set()).add(col or "")
            self._all = all_map
            self._t = now
            logger.info("schema 全量加载 %d 张表 (%s)", len(all_map), self._dt)

    @staticmethod
    def _split(db_table):
        if "." in db_table:
            db, tb = db_table.rsplit(".", 1)
            return db, tb
        return _DB, db_table

    # ---------- 对外 ----------
    def columns_of(self, db_table):
        """db_table: 'db.table' 或 'table'(按 DEFAULT_DB 兜底) -> set(column) | None(表不在快照)"""
        if not db_table:
            return None
        db, tb = self._split(db_table)
        key = "%s.%s" % (db, tb)
        self._load_all()
        return self._all.get(key)

    def tables_with_column(self, col, candidates):
        """在候选表列表中, 找包含 col 的表集合(用于无前缀列消歧) -> [db.table, ...]"""
        col_l = (col or "").lower()
        if not col_l:
            return []
        self._load_all()
        hits = []
        for db_table in candidates:
            db, tb = self._split(db_table)
            cols = self._all.get("%s.%s" % (db, tb))
            if cols and any((c or "").lower() == col_l for c in cols):
                hits.append(db_table)
        return hits
