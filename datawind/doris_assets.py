# -*- coding: utf-8 -*-
"""Doris 表资产采集模块（资产扩展: Doris 物理表/字段元数据）

从 information_schema.tables / columns 采集全业务库（排除系统库）,
产出落库行供每日全量写 ods_doris_table_s_d / ods_doris_column_s_d:

  collect_tables(doris)  -> list[tuple]  (dt, db, table, type, engine, rows, size, avg, ctime, utime, comment, layer, etl_time)
  collect_columns(doris) -> list[tuple]  (dt, db, table, column, ordinal, col_type, data_type, nullable, default, comment, etl_time)

分层标签 layer: 库名命中已知分层集合(mv/ods/dwd/dim/dws/ads/tmp/fin)按库名,
                否则归 other(如 app/bak/etl/shield/stg/test 等)。
系统库排除: information_schema/mysql/__internal_schema/internal/sys
"""
import time

# 系统库（Doris 内部/元数据库, 非业务资产）
SYSTEM_DBS = {"information_schema", "mysql", "__internal_schema", "internal", "sys"}

# 已知数仓分层库名（按库名前缀识别 layer）
LAYER_DBS = {"mv", "ods", "dwd", "dim", "dws", "ads", "tmp", "fin"}


def _layer_of(db):
    """库名 -> 分层标签"""
    if db in LAYER_DBS:
        return db
    if db == "fin_test":
        return "fin"
    return "other"


def collect_tables(doris, dt=None, etl_time=None):
    """采集全业务库表资产 -> 落库行列表

    一次查全集群 information_schema.tables, 排除系统库; 返回 [] 表示无数据
    """
    dt = dt or time.strftime("%Y-%m-%d")
    etl_time = etl_time or time.strftime("%Y-%m-%d %H:%M:%S")
    dbs = [r[0] for r in doris.exec("SHOW DATABASES", fetch=True)
           if r and r[0] and r[0] not in SYSTEM_DBS]
    if not dbs:
        return []
    marks = ",".join(["%s"] * len(dbs))
    sql = (
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_ROWS, "
        "DATA_LENGTH, AVG_ROW_LENGTH, CREATE_TIME, UPDATE_TIME, TABLE_COMMENT "
        "FROM information_schema.tables "
        "WHERE TABLE_SCHEMA IN (%s)" % marks
    )
    rows = doris.exec(sql, dbs, fetch=True) or []
    out = []
    for r in rows:
        db = r[0]
        out.append((dt, db, r[1], r[2], r[3], r[4], r[5], r[6],
                    r[7], r[8], (r[9] or "")[:1000], _layer_of(db), etl_time))
    return out


def collect_columns(doris, dt=None, etl_time=None):
    """采集全业务库字段资产 -> 落库行列表"""
    dt = dt or time.strftime("%Y-%m-%d")
    etl_time = etl_time or time.strftime("%Y-%m-%d %H:%M:%S")
    dbs = [r[0] for r in doris.exec("SHOW DATABASES", fetch=True)
           if r and r[0] and r[0] not in SYSTEM_DBS]
    if not dbs:
        return []
    marks = ",".join(["%s"] * len(dbs))
    sql = (
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "
        "COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT "
        "FROM information_schema.columns "
        "WHERE TABLE_SCHEMA IN (%s)" % marks
    )
    rows = doris.exec(sql, dbs, fetch=True) or []
    out = []
    for r in rows:
        out.append((dt, r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                    r[7], (r[8] or "")[:1000], etl_time))
    return out


def write_doris_assets(doris, stat_date, etl_time, t_rows, c_rows):
    """Doris 资产落库（幂等: 删当日分区再全量）返回 (表数, 字段数)"""
    if t_rows:
        doris.exec("DELETE FROM %s WHERE dt = %%s" % "{db}.ods_doris_table_s_d".format(db=doris.db), (stat_date,))
        sql = ("INSERT INTO %s (dt, db_name, table_name, table_type, engine, rows_cnt, "
               "data_size, avg_row_len, create_time, update_time, table_comment, layer, etl_time) "
               "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
               % "{db}.ods_doris_table_s_d".format(db=doris.db))
        doris.execmany(sql, t_rows)
    if c_rows:
        doris.exec("DELETE FROM %s WHERE dt = %%s" % "{db}.ods_doris_column_s_d".format(db=doris.db), (stat_date,))
        sql = ("INSERT INTO %s (dt, db_name, table_name, column_name, ordinal_position, "
               "column_type, data_type, is_nullable, column_default, column_comment, etl_time) "
               "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
               % "{db}.ods_doris_column_s_d".format(db=doris.db))
        doris.execmany(sql, c_rows)
    return len(t_rows), len(c_rows)
