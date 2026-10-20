# -*- coding: utf-8 -*-
"""
DataWind 资产中心 · P0 轮询进程：5 分钟列表 diff -> 变更落库
============================================================
设计（对应 v0.3「准实时监控设计」章节）:
  - 高频 5 分钟: 轮询 dataSetList, 比对 id/name/mtime/status
  - 变更即落库: added/deleted/mtime_changed 写 dwd_datawind_dataset_change_i_d
    (mtime_changed = 元数据被修改的信号, 具体字段级差异由每日全量校正为 config_changed 等)
  - 指数退避 + 熔断: 连续 5 次 API 失败 -> 降级 30 分钟再试, 恢复后回 5 分钟
  - 单例互斥: 锁文件, 同一时刻仅一个实例
  - 基线: datawind_poll_baseline.json (id -> {name, mtime, status})

用法:
  python poll_5min.py                # 5 分钟间隔, 前台常驻
  python poll_5min.py --interval 300 --once   # 跑一次即退(调试)
  python poll_5min.py --config ../config/datawind_monitor_config.json
依赖: pymysql; datawind 包同目录
"""
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from logging.handlers import TimedRotatingFileHandler

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from datawind.config import load_config
from datawind.client import DataWindClient
from datawind import ddl as ddl_mod
from datawind.doris import Doris

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_FILE = os.path.join(BASE_DIR, "poll_5min.lock")
BASELINE_FILE = os.path.join(BASE_DIR, "datawind_poll_baseline.json")
LOG_FILE = os.path.join(BASE_DIR, "logs", "poll_5min.log")
DAILY_MARK_DIR = os.path.join(BASE_DIR, "logs", "daily_marks")
RUN_DAILY = os.path.join(BASE_DIR, "run_daily.py")

TBL_CHANGE = "{db}.dwd_datawind_dataset_change_i_d"
TBL_SNAPSHOT = "{db}.ods_datawind_dataset_s_d"
TBL_TBL_LINEAGE = "{db}.dwd_datawind_table_lineage_t_d"
TBL_COL_LINEAGE = "{db}.dwd_datawind_column_lineage_t_d"
TBL_CLOSURE = "{db}.dwd_datawind_lineage_closure_t_d"

# 熔断参数
MAX_CONSEC_FAIL = 5      # 连续失败 N 次进入熔断
COOLDOWN_SEC = 1800      # 熔断休息 30 分钟

# 业务时区: 赞比亚 = UTC+2 (CAT, 全年无夏令时)。所有"日期/触发点"按赞比亚本地时间计算。
TZ_OFFSET_SEC = 2 * 3600

# 每日全量调度（应用内自触发, 不依赖 cron/systemd）
DAILY_HOUR = 0          # 每日本地(UTC+2) 00:01 触发 run_daily
DAILY_MIN = 1


def _local_now():
    """赞比亚本地时间(UTC+2)"""
    return time.gmtime(time.time() + TZ_OFFSET_SEC)


def _today_local():
    """赞比亚本地日期(业务日)"""
    return time.strftime("%Y-%m-%d", _local_now())


def setup_logger():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    logger = logging.getLogger("poll5min")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=30, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    # stdout 非终端(如 nohup 重定向到日志文件)时不挂 StreamHandler, 避免日志双写
    if sys.stdout.isatty():
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


def _pid_alive(pid):
    """探测 PID 是否存活(跨平台; Windows 用 tasklist 探测, Linux 用 kill 0)"""
    if os.name == "nt":
        try:
            import subprocess
            r = subprocess.run(["tasklist", "/FI", "PID eq %d" % pid],
                               capture_output=True, text=True, timeout=10)
            return str(pid) in r.stdout
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def acquire_lock():
    """单例互斥: 锁文件记录 PID; 若文件存在但 PID 已死(残留锁)则自动抢占"""
    global _lock_fd
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r", encoding="utf-8") as f:
                old_pid = int(f.read().strip() or 0)
            if old_pid and _pid_alive(old_pid):
                return False  # 另一个实例存活
            # 残留锁: 旧进程已退出, 抢占
            os.remove(LOCK_FILE)
        except (ValueError, OSError):
            pass
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        _lock_fd = fd
        return True
    except FileExistsError:
        return False


_lock_fd = None


def release_lock():
    global _lock_fd
    try:
        if _lock_fd:
            os.close(_lock_fd)
            _lock_fd = None
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, "r", encoding="utf-8") as f:
                pid = int(f.read().strip() or 0)
            if pid == os.getpid():
                os.remove(LOCK_FILE)
    except (ValueError, OSError):
        pass


def _signal_handler(signum, frame):
    """SIGTERM/SIGINT: 清理锁后退出(避免 systemd 重启时残留锁)"""
    release_lock()
    sys.exit(0)


def load_baseline():
    if not os.path.exists(BASELINE_FILE):
        return {}
    with open(BASELINE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_baseline(baseline):
    tmp = BASELINE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(baseline, f, ensure_ascii=False, indent=1)
    os.replace(tmp, BASELINE_FILE)


def build_current(ds_list):
    """列表 -> {id(str): {name, mtime, status}}（仅保留 diff 所需字段）

    key 统一为 str: 基线从 JSON 加载后 key 是字符串, 避免 int/str 不一致导致全量误报
    """
    cur = {}
    for ds in ds_list:
        cur[str(ds.get("id"))] = {
            "name": ds.get("name"),
            "mtime": str(ds.get("mtime")) if ds.get("mtime") else "",
            "status": ds.get("status"),
        }
    return cur


def next_change_id(doris):
    """当日最大 change_id + 1（与 dt 构成唯一键, 幂等）"""
    rows = doris.exec("SELECT COALESCE(MAX(change_id), 0) + 1 FROM %s WHERE dt = %%s"
                      % TBL_CHANGE.format(db=doris.db), (_today_local(),), fetch=True)
    return rows[0][0] if rows and rows[0] else 1


def write_changes(doris, changes, logger):
    if not changes:
        return
    dt = _today_local()
    etl_time = time.strftime("%Y-%m-%d %H:%M:%S", _local_now())
    cid = next_change_id(doris)
    names = {"added": "新增", "deleted": "删除",
             "logic_changed": "加工逻辑变化", "source_changed": "数据源切换"}
    ins = ("INSERT INTO %s (dt, etl_time, change_id, dataset_id, dataset_name, change_type, "
           "change_type_name, changed_fields, diff_json, folder_path, remark) "
           "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
           % TBL_CHANGE.format(db=doris.db))
    rows = []
    for ct, rid, rname, diff in changes:
        rows.append((dt, etl_time, cid, rid, rname, ct, names.get(ct, ct),
                     ",".join(sorted(diff.keys())), json.dumps(diff, ensure_ascii=False),
                     None, "5分钟轮询检测"))
        cid += 1
    doris.execmany(ins, rows)
    logger.info("变更落库 %d 条: %s", len(rows), ", ".join(
        "[%s]%s" % (names.get(c, c), r) for c, _, r, _ in changes))


def sync_lineage_incremental(client, doris, sync_map, logger):
    """轮询增量血缘同步（P1-3）: 对新增/mtime变化的数据集重新解析血缘

    语义（与每日全量一致）:
      - 先 DELETE 当日分区该数据集的血缘行(表级/字段级/闭包), 再 INSERT 最新解析结果
        (UNIQUE KEY 幂等但只覆盖同key; 若上游表集合收缩, 旧key会残留, 必须整集删除重插)
      - 顺手将最新加工SQL/来源表/字段清单 UPDATE 进当日快照(详情页"加工SQL"准实时,
        当日分区未建则 UPDATE 0 行, 由每日全量兜底)
      - 失败仅记日志, 不阻塞轮询主流程; 每日全量仍兜底全量重建
    返回成功同步的数据集数
    """
    from datawind.lineage import LineageParser, normalize_column_edge
    if not sync_map:
        return 0
    parser = LineageParser()
    dt = _today_local()
    etl_time = time.strftime("%Y-%m-%d %H:%M:%S", _local_now())
    synced = 0
    for dsid, meta in sync_map.items():
        try:
            dsid_int = int(dsid)
        except (ValueError, TypeError):
            logger.warning("血缘增量同步[%s] 非数字数据集ID, 跳过", dsid)
            continue
        try:
            try:
                sql, srcs, field_list, ds_src = client.model_logic(dsid_int)
            except Exception as e:
                logger.warning("血缘增量同步[%s] 拉取加工SQL失败: %s", dsid, e)
                continue
            # 实质变更检测: 对比当日快照 -> 加工逻辑变化/数据源切换(仅mtime变化不记录)
            try:
                old = doris.exec(
                    "SELECT sql_logic, data_source_type, data_source_id, cluster_name, source_db "
                    "FROM %s WHERE dt=%%s AND dataset_id=%%s"
                    % TBL_SNAPSHOT.format(db=doris.db), (dt, dsid_int), fetch=True)
                old_row = old[0] if old else None
                extra_changes = []
                if old_row is not None:
                    old_sql = (old_row[0] or "").strip()
                    new_sql = (sql or "").strip()
                    if old_sql and new_sql and old_sql != new_sql:
                        extra_changes.append(("logic_changed", dsid_int, (meta or {}).get("name") or "",
                                              {"sql_logic": {"old": old_sql[:2000], "new": new_sql[:2000]}}))
                    src_old = [old_row[1] or "", old_row[2] or "", old_row[3] or "", old_row[4] or ""]
                    src_new = [(ds_src or {}).get("type") or "", (ds_src or {}).get("id") or "",
                               (ds_src or {}).get("cluster") or "", (ds_src or {}).get("db") or ""]
                    if src_old != src_new:
                        extra_changes.append(("source_changed", dsid_int, (meta or {}).get("name") or "",
                                              {"data_source": {"old": src_old, "new": src_new}}))
                if extra_changes:
                    write_changes(doris, extra_changes, logger)
                    logger.info("实质变更[%s]: %s", dsid,
                                ", ".join(c[0] for c in extra_changes))
            except Exception as e:
                logger.warning("血缘增量同步[%s] 实质变更检测失败: %s", dsid, e)
            if not sql:
                continue
            tables, cols, st = parser.parse_multi(sql, dsid_int)
        except Exception as e:
            logger.warning("血缘增量同步[%s] 解析异常: %s", dsid, e)
            continue
        if st["parse_status"] == "fail":
            logger.warning("血缘增量同步[%s] 解析失败: %s", dsid, st.get("err"))
            continue
        table_set = set(tables)
        for s in (srcs or "").split(","):
            s = s.strip()
            if s:
                table_set.add(s)
        t_rows, c_rows, cl_rows = [], [], []
        for t in sorted(table_set):
            db, tb = t.rsplit(".", 1) if "." in t else (None, t)
            t_rows.append((dt, dsid_int, t, "direct", 0, etl_time))
            cl_rows.append((dt, dsid_int, db, tb, 1, t, etl_time))
        for edge in cols:
            n = normalize_column_edge(edge, dsid_int)
            c_rows.append((dt, dsid_int, n["target_field"], n["src_table"],
                           n["src_field"], n["confidence"], etl_time))
        # 整集删除重插: 避免上游表集合收缩时旧 key 残留
        try:
            for tbl in (TBL_TBL_LINEAGE, TBL_COL_LINEAGE, TBL_CLOSURE):
                doris.exec("DELETE FROM %s WHERE dt = %%s AND dataset_id = %%s"
                           % tbl.format(db=doris.db), (dt, dsid_int))
            if t_rows:
                doris.execmany(
                    "INSERT INTO %s (dt, dataset_id, db_table, lineage_type, depth, etl_time) "
                    "VALUES (%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_TBL_LINEAGE.format(db=doris.db), t_rows)
            if c_rows:
                doris.execmany(
                    "INSERT INTO %s (dt, dataset_id, target_field, src_table, src_field, confidence, etl_time) "
                    "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_COL_LINEAGE.format(db=doris.db), c_rows)
            if cl_rows:
                doris.execmany(
                    "INSERT INTO %s (dt, dataset_id, up_db, up_table, hop_depth, lineage_path, etl_time) "
                    "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % TBL_CLOSURE.format(db=doris.db), cl_rows)
            synced += 1
            logger.info("血缘增量同步[%s] 完成: 表边%d 字段边%d 闭包%d",
                        dsid, len(t_rows), len(c_rows), len(cl_rows))
        except Exception as e:
            logger.error("血缘增量同步[%s] 落库失败: %s (该数据集血缘待下次轮询/每日全量纠正)", dsid, e)
            continue
        # 快照准实时: 更新当日分区加工SQL/来源表/字段清单/数据源(详情页加工SQL 5分钟内刷新)
        try:
            ds_t, ds_id, ds_cl, ds_db = (ds_src or {}).get("type"), (ds_src or {}).get("id"), \
                (ds_src or {}).get("cluster"), (ds_src or {}).get("db")
            doris.exec(
                "UPDATE %s SET sql_logic=%%s, source_tables=%%s, field_list=%%s, "
                "data_source_type=%%s, data_source_id=%%s, cluster_name=%%s, source_db=%%s, "
                "mtime=%%s, etl_time=%%s WHERE dt=%%s AND dataset_id=%%s"
                % TBL_SNAPSHOT.format(db=doris.db),
                (sql or "", srcs or "", field_list or "",
                 ds_t or "", ds_id, ds_cl or "", ds_db or "",
                 (meta or {}).get("mtime") or "", etl_time, dt, dsid_int))
            logger.info("快照准实时[%s] 更新加工SQL/来源表/字段清单/数据源", dsid)
        except Exception as e:
            logger.warning("快照准实时[%s] 更新失败(当日分区可能未建, 每日全量兜底): %s", dsid, e)
    logger.info("血缘增量同步汇总: %d/%d 数据集 (dt=%s)", synced, len(sync_map), dt)
    return synced


def _daily_mark_file(today):
    return os.path.join(DAILY_MARK_DIR, today)


def _daily_done(today):
    return os.path.exists(_daily_mark_file(today))


def _mark_daily_done(today):
    os.makedirs(DAILY_MARK_DIR, exist_ok=True)
    with open(_daily_mark_file(today), "w", encoding="utf-8") as f:
        f.write(time.strftime("%Y-%m-%d %H:%M:%S", _local_now()))


def run_daily_if_due(logger, force=False):
    """应用内每日调度: 本地(UTC+2) 00:01 后触发 run_daily.py 全量落库(当日仅一次)

    - 标记文件防重复; 进程启动时若已过点且当日未跑, 自动补跑(catch-up)
    - 子进程独立运行, 失败不阻塞轮询主循环
    """
    today = _today_local()
    if not force and _daily_done(today):
        return False
    if not force:
        now = _local_now()
        if (now.tm_hour, now.tm_min) < (DAILY_HOUR, DAILY_MIN):
            return False
    logger.info("触发每日全量落库(run_daily.py, date=%s)...", today)
    try:
        proc = subprocess.run(
            [sys.executable, RUN_DAILY, "--date", today],
            capture_output=True, text=True, timeout=1800)
        tail = (proc.stdout or "")[-800:]
        if proc.returncode == 0:
            _mark_daily_done(today)
            logger.info("每日全量完成(exit=0): %s", tail.replace("\n", " ")[-400:])
        else:
            logger.error("每日全量失败(exit=%s): %s",
                         proc.returncode, (tail + (proc.stderr or ""))[-400:])
    except Exception as e:
        logger.error("每日全量异常: %s", e)
    return True


def poll_once(client, doris, baseline, logger):
    ds_list = client.list_datasets()
    current = build_current(ds_list)

    # 首次运行(基线为空): 仅建立基线, 不写变更(避免 338 条全量"新增"噪音)
    if not baseline:
        save_baseline(current)
        logger.info("首次轮询: 建立基线 %d 个数据集(不写入变更)", len(current))
        return 0

    changes = []
    # 新增
    for i, d in current.items():
        if i not in baseline:
            changes.append(("added", i, d["name"], {"new": {"name": d["name"], "mtime": d["mtime"]}}))
    # 删除
    for i, old in baseline.items():
        if i not in current:
            changes.append(("deleted", i, old.get("name") or "", {"old": {"name": old.get("name")}}))
    # 元数据修改(mtime 变化): 不直接记变更, 仅作为"可能逻辑/数据源变化"的信号
    #   由 sync_lineage_incremental 拉取加工逻辑后对比当日快照, 只记实质变化
    changed_meta = []
    for i, d in current.items():
        old = baseline.get(i)
        if old and old.get("mtime") != d["mtime"]:
            changed_meta.append((i, d["name"], d["mtime"]))
    # 仅写新增/删除(实质变更); mtime 信号变更由增量同步里判断后写入
    write_changes(doris, [c for c in changes if c[0] in ("added", "deleted")], logger)
    save_baseline(current)
    # 增量血缘同步: 新增/元数据修改的数据集重新解析(重接口按需, 不阻塞主流程),
    #   并顺手将最新加工SQL/来源表/字段清单更新进当日快照(详情页准实时);
    #   同时对比当日快照检测 加工逻辑变化(logic_changed)/数据源切换(source_changed) 写入变更
    sync_map = {}
    for ct, rid, rname, diff in changes:
        if ct == "added":
            sync_map[rid] = {"mtime": (diff.get("new") or {}).get("mtime", ""), "name": rname}
    for rid, rname, mt in changed_meta:
        sync_map[rid] = {"mtime": mt, "name": rname}
    if sync_map:
        sync_lineage_incremental(client, doris, sync_map, logger)
    logger.info("轮询完成: 数据集=%d, 新增/删除=%d, 待核查mtime=%d (基线已更新)",
                len(current), len(changes), len(changed_meta))
    return len(changes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=300, help="轮询间隔秒数, 默认 300(5分钟)")
    ap.add_argument("--once", action="store_true", help="只跑一次即退出(调试)")
    ap.add_argument("--config", default=None, help="配置文件路径")
    args = ap.parse_args()

    if not acquire_lock():
        print("已有 poll_5min 实例在运行, 本实例退出 (锁: %s)" % LOCK_FILE)
        sys.exit(0)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    logger = setup_logger()
    logger.info("poll_5min 启动: interval=%ds, once=%s", args.interval, args.once)

    try:
        cfg = load_config(args.config)
        client = DataWindClient(cfg)
        doris = Doris(cfg)
        doris.connect()
        # 幂等建表(变更表; 快照表每日脚本建, 轮询只写变更表; 血缘三表供增量同步)
        doris.create_table(ddl_mod.DDL_CHANGE.format(db=doris.db))
        doris.create_table(ddl_mod.DDL_TABLE_LINEAGE.format(db=doris.db))
        doris.create_table(ddl_mod.DDL_COLUMN_LINEAGE.format(db=doris.db))
        doris.create_table(ddl_mod.DDL_CLOSURE.format(db=doris.db))

        baseline = load_baseline()
        consec_fail = 0
        # 启动时 catch-up: 已过每日触发点且当日未跑 -> 立即补跑
        run_daily_if_due(logger)
        while True:
            t0 = time.time()
            try:
                poll_once(client, doris, baseline, logger)
                consec_fail = 0  # 成功复位
                # 更新内存基线(文件已由 poll_once 保存, 重新加载保持内存一致)
                baseline = load_baseline()
                sleep = args.interval
            except Exception as e:
                consec_fail += 1
                logger.error("轮询失败(%d/%d): %s", consec_fail, MAX_CONSEC_FAIL, e)
                if consec_fail >= MAX_CONSEC_FAIL:
                    sleep = COOLDOWN_SEC
                    logger.warning("连续失败 %d 次, 进入熔断: %ds 后重试", consec_fail, sleep)
                else:
                    sleep = min(args.interval * (2 ** (consec_fail - 1)), COOLDOWN_SEC)
                    logger.warning("指数退避: %ds 后重试", sleep)

            if args.once:
                break
            # 每日调度检查(本地 UTC+2 00:01 后触发一次全量)
            run_daily_if_due(logger)
            # 补偿运行耗时, 保持稳定间隔
            elapsed = time.time() - t0
            time.sleep(max(1, sleep - elapsed))
    finally:
        release_lock()
        logger.info("poll_5min 退出")


if __name__ == "__main__":
    main()
