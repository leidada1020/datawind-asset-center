# -*- coding: utf-8 -*-
"""Doris 连接与写入封装（MySQL 协议, 断线自动重连）

依赖: pymysql
约束（企业规范/实测经验）:
  - replication_num >= 2
  - 表名以字母开头
  - 字段 COMMENT 不含引号
"""
import sys
import threading
import time

import pymysql

RETRY_CODES = (2003, 2006, 2013)  # 连接失败/连接丢失/查询期间丢失


class Doris:
    def __init__(self, cfg, db=None):
        self.host = cfg["doris_host"]
        self.port = int(cfg.get("doris_port", 9030))
        self.user = cfg["doris_user"]
        self.password = cfg["doris_password"]
        self.db = db or cfg.get("doris_db", "etl")
        self.conn = None
        self.cur = None
        # 连接非线程安全: FastAPI 多线程并发复用同一连接时必须串行化
        self._lock = threading.RLock()

    # ---------- 连接 ----------
    def connect(self, retries=5, gap=20):
        last = None
        for i in range(retries):
            try:
                self.conn = pymysql.connect(
                    host=self.host, port=self.port, user=self.user,
                    password=self.password, charset="utf8mb4",
                    connect_timeout=20, read_timeout=180, write_timeout=180,
                    autocommit=True)
                self.cur = self.conn.cursor()
                return self
            except Exception as e:
                last = e
                print("    Doris 连接失败(第%d次): %s, %ds后重试..." % (i + 1, e, gap), file=sys.stderr)
                time.sleep(gap)
        raise last

    def close(self):
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
        except Exception:
            pass

    def _reconnect(self):
        with self._lock:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = pymysql.connect(
                host=self.host, port=self.port, user=self.user,
                password=self.password, charset="utf8mb4",
                connect_timeout=20, read_timeout=180, write_timeout=180,
                autocommit=True)
            self.cur = self.conn.cursor()

    # ---------- 执行 ----------
    def exec(self, sql, args=None, fetch=False):
        """执行单条 SQL；连接抖动自动重连（最多 4 次尝试）; 线程安全(RLock)"""
        with self._lock:
            for attempt in range(4):
                try:
                    self.cur.execute(sql, args)
                    rows = self.cur.fetchall() if fetch else None
                    return rows
                except pymysql.err.OperationalError as e:
                    code = e.args[0] if e.args else 0
                    if code in RETRY_CODES and attempt < 3:
                        print("    DB连接异常(%s), 重连重试(%d/4)..." % (e, attempt + 1), file=sys.stderr)
                        self._reconnect()
                        continue
                    raise

    def execmany(self, sql, args_list):
        with self._lock:
            for attempt in range(4):
                try:
                    self.cur.executemany(sql, args_list)
                    return
                except pymysql.err.OperationalError as e:
                    code = e.args[0] if e.args else 0
                    if code in RETRY_CODES and attempt < 3:
                        print("    DB连接异常(%s), 重连重试(%d/4)..." % (e, attempt + 1), file=sys.stderr)
                        self._reconnect()
                        continue
                    raise

    def count(self, table, where=None, args=None):
        sql = "SELECT COUNT(*) FROM %s" % table
        if where:
            sql += " WHERE " + where
        rows = self.exec(sql, args, fetch=True)
        return rows[0][0] if rows and rows[0] else 0

    # ---------- 建表 ----------
    def create_table(self, ddl):
        """建表(幂等): 目标表已存在则跳过 CREATE, 避免只读账号无 CREATE 权限时启动即报错"""
        import re
        m = re.search(r"CREATE TABLE IF NOT EXISTS\s+`?(\w+)`?\.`?(\w+)`?", ddl, re.IGNORECASE)
        if m:
            db_, tbl = m.group(1), m.group(2)
            rows = self.exec(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s", (db_, tbl), fetch=True)
            if rows and rows[0] and rows[0][0] > 0:
                return
        self.exec(ddl)

    def table_exists(self, table):
        """表是否存在（information_schema 查询，不走 SHOW 语句避免转义问题）"""
        rows = self.exec(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s", (self.db, table.split(".")[-1]), fetch=True)
        return bool(rows and rows[0] and rows[0][0] > 0)

    def drop_table(self, table):
        self.exec("DROP TABLE IF EXISTS %s" % table)
