# -*- coding: utf-8 -*-
"""
DS 调度资产增量轮询：5 分钟轻量比对 -> 只重解析变更的工作流
============================================================
设计（对应调度闭环二「Doris 表间血缘」的准实时升级）:
  - 每日全量(run_daily)采集全部 DS 资产(项目/工作流/任务/表间血缘), 当日分区为权威基线
  - 高频 5 分钟: 轻量拉各项目 process-definition 列表(带 version/updateTime/releaseState),
    与基线 ds_baseline.json 比对 -> 仅对变更(新增/修改/删除)的工作流
    拉详情重解析 SQL -> 当日分区内增量更新 ods_ds_workflow_s_d / ods_ds_lineage_s_d
  - 无变更时零详情接口开销; 与 poll_5min 同频, 锁/退避/熔断/日志模式一致
  - 变更信号: 工作流级 version(保存即+1) + updateTime 双信号

用法:
  python ds_poll.py                # 5 分钟间隔, 前台常驻
  python ds_poll.py --interval 300 --once   # 跑一次即退(调试)
依赖: pymysql, sqlglot; datawind 包同目录; 配置 cfg["ds"] = {enabled, base_url, token}
"""
import argparse
import json
import logging
import os
import signal
import sys
import time
from logging.handlers import TimedRotatingFileHandler

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from datawind.config import load_config
from datawind.ds_client import DSClient
from datawind.doris import Doris
from datawind import ds_assets as ds_mod

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_FILE = os.path.join(BASE_DIR, "ds_poll.lock")
BASELINE_FILE = os.path.join(BASE_DIR, "ds_baseline.json")
LOG_FILE = os.path.join(BASE_DIR, "logs", "ds_poll.log")

TBL_WF = "{db}.ods_ds_workflow_s_d"
TBL_LG = "{db}.ods_ds_lineage_s_d"

# 熔断参数
MAX_CONSEC_FAIL = 5      # 连续失败 N 次进入熔断
COOLDOWN_SEC = 1800      # 熔断休息 30 分钟


def setup_logger():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    logger = logging.getLogger("dspoll")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=30, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    if sys.stdout.isatty():
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


def _pid_alive(pid):
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


_lock_fd = None


def acquire_lock():
    global _lock_fd
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r", encoding="utf-8") as f:
                old_pid = int(f.read().strip() or 0)
            if old_pid and _pid_alive(old_pid):
                return False
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


def collect_workflow_rows(pcode, pname, w, detail):
    """工作流详情 -> 任务快照行 + 血缘行(与 ds_assets.collect_ds_assets 行格式一致)"""
    wcode, wname = w.get("code"), w.get("name")
    release_state = w.get("releaseState") or ""
    wf_rows, lg_rows = [], []
    for t in detail.get("taskDefinitionList") or []:
        tcode = t.get("code")
        tname = t.get("name") or ""
        ttype = t.get("taskType") or ""
        tflag = t.get("flag") or ""
        sql_text = ds_mod._task_sql(t) if ttype == "SQL" else ""
        wf_rows.append((pcode, wcode, tcode, pname, wname, release_state,
                        tname, ttype, tflag, sql_text))
        for ddb, dtb, sdb, stb in ds_mod.parse_task_sql(sql_text):
            lg_rows.append((pcode, pname, wcode, wname, tcode, tname,
                            ddb, dtb, sdb, stb))
    return wf_rows, lg_rows


def apply_workflow_change(doris, logger, dt, etl_time, pcode, pname, w, detail):
    """当日分区内增量更新一个工作流(删该工作流行 -> 重插任务快照 + 血缘边)"""
    wcode = w.get("code")
    wf_rows, lg_rows = collect_workflow_rows(pcode, pname, w, detail)
    # 1. 任务快照
    doris.exec("DELETE FROM %s WHERE dt = %%s AND project_code = %%s AND workflow_code = %%s"
               % TBL_WF.format(db=doris.db), (dt, pcode, wcode))
    if wf_rows:
        rows = [(dt,) + r + (etl_time,) for r in wf_rows]
        sql_w = ("INSERT INTO {db}.ods_ds_workflow_s_d "
                 "(dt, project_code, workflow_code, task_code, project_name, workflow_name, "
                 "release_state, task_name, task_type, task_flag, sql_text, etl_time) "
                 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(db=doris.db)
        doris.execmany(sql_w, rows)
    # 2. 表间血缘
    doris.exec("DELETE FROM %s WHERE dt = %%s AND workflow_code = %%s"
               % TBL_LG.format(db=doris.db), (dt, wcode))
    if lg_rows:
        rows = [(dt,) + r + (etl_time,) for r in lg_rows]
        sql_l = ("INSERT INTO {db}.ods_ds_lineage_s_d "
                 "(dt, project_code, project_name, workflow_code, workflow_name, task_code, "
                 "task_name, dst_db, dst_table, src_db, src_table, etl_time) "
                 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(db=doris.db)
        doris.execmany(sql_l, rows)
    logger.info("DS增量[%s] %s/%s: 任务%d条 血缘边%d条", wcode, pname,
                w.get("name") or "", len(wf_rows), len(lg_rows))


def poll_once(client, doris, baseline, logger):
    """一轮: 拉工作流列表 -> diff 基线 -> 增量落库变更工作流; 返回变更数"""
    dt = time.strftime("%Y-%m-%d")
    etl_time = time.strftime("%Y-%m-%d %H:%M:%S")
    changed = []
    seen = set()
    current = {}
    projects = client.list_projects()
    for p in projects:
        pcode, pname = p.get("code"), p.get("name")
        for w in client.list_workflows(pcode):
            wcode = w.get("code")
            key = str(wcode)
            seen.add(key)
            rec = {"project_code": pcode, "project_name": pname,
                   "workflow_name": w.get("name") or "",
                   "version": w.get("version"), "updateTime": w.get("updateTime"),
                   "releaseState": w.get("releaseState") or ""}
            current[key] = rec
            old = baseline.get(key)
            if not old:
                changed.append(("added", pcode, pname, w))
            elif (old.get("version") != rec["version"]
                    or (old.get("updateTime") or "") != (rec["updateTime"] or "")
                    or (old.get("releaseState") or "") != rec["releaseState"]
                    or (old.get("workflow_name") or "") != rec["workflow_name"]):
                changed.append(("modified", pcode, pname, w))

    # 删除: 基线有但当前列表无
    for key, old in baseline.items():
        if key not in seen:
            changed.append(("deleted", old.get("project_code"), old.get("project_name"), key))
            logger.info("DS增量: 检测到工作流删除 code=%s name=%s", key, old.get("workflow_name"))

    applied = 0
    for ct, pcode, pname, extra in changed:
        if ct == "deleted":
            wcode = int(extra)   # extra=key(str)
            # 删当日分区该工作流数据 + 从基线移除
            try:
                doris.exec("DELETE FROM %s WHERE dt = %%s AND workflow_code = %%s"
                           % TBL_LG.format(db=doris.db), (dt, wcode))
                doris.exec("DELETE FROM %s WHERE dt = %%s AND project_code = %%s AND workflow_code = %%s"
                           % TBL_WF.format(db=doris.db), (dt, pcode, wcode))
                baseline.pop(extra, None)
                applied += 1
                logger.info("DS增量: 删除工作流数据 code=%s", extra)
            except Exception as e:
                logger.error("DS增量[删除%s] 落库失败: %s", extra, e)
            continue
        w = extra   # added/modified: extra=workflow dict
        try:
            detail = client.get_workflow_detail(pcode, w.get("code"))
            apply_workflow_change(doris, logger, dt, etl_time, pcode, pname, w, detail)
            # 更新基线
            key = str(w.get("code"))
            baseline[key] = {"project_code": pcode, "project_name": pname,
                             "workflow_name": w.get("name") or "",
                             "version": w.get("version"), "updateTime": w.get("updateTime"),
                             "releaseState": w.get("releaseState") or ""}
            applied += 1
        except Exception as e:
            logger.error("DS增量[%s/%s] 处理失败: %s (保留基线, 下轮重试)",
                         pname, w.get("name") or "", e)

    save_baseline(baseline)
    if changed:
        logger.info("DS轮询完成: 工作流=%d, 变更=%d (%s), 已应用%d (基线已更新)",
                    len(seen), len(changed),
                    ", ".join("%s:%s" % (c, b.get("workflow_name") or "") for c, _, _, b in changed if c != "deleted") or "",
                    applied)
    else:
        logger.info("DS轮询完成: 工作流=%d, 无变更", len(seen))
    return len(changed)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=300, help="轮询间隔秒数, 默认 300(5分钟)")
    ap.add_argument("--once", action="store_true", help="只跑一次即退出(调试)")
    ap.add_argument("--config", default=None, help="配置文件路径")
    args = ap.parse_args()

    if not acquire_lock():
        print("已有 ds_poll 实例在运行, 本实例退出 (锁: %s)" % LOCK_FILE)
        sys.exit(0)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    logger = setup_logger()
    logger.info("ds_poll 启动: interval=%ds, once=%s", args.interval, args.once)

    try:
        cfg = load_config(args.config)
        ds_cfg = cfg.get("ds") or {}
        if not ds_cfg.get("enabled") or not ds_cfg.get("base_url") or not ds_cfg.get("token"):
            logger.error("DS 配置缺失/未启用(配置文件 ds 段), 退出")
            sys.exit(1)
        client = DSClient(ds_cfg["base_url"], ds_cfg["token"])
        doris = Doris(cfg)
        doris.connect()

        baseline = load_baseline()
        consec_fail = 0
        while True:
            t0 = time.time()
            try:
                poll_once(client, doris, baseline, logger)
                consec_fail = 0
                baseline = load_baseline()
                sleep = args.interval
            except Exception as e:
                consec_fail += 1
                logger.error("DS轮询失败(%d/%d): %s", consec_fail, MAX_CONSEC_FAIL, e)
                if consec_fail >= MAX_CONSEC_FAIL:
                    sleep = COOLDOWN_SEC
                    logger.warning("连续失败 %d 次, 进入熔断: %ds 后重试", consec_fail, sleep)
                else:
                    sleep = min(args.interval * (2 ** (consec_fail - 1)), COOLDOWN_SEC)
                    logger.warning("指数退避: %ds 后重试", sleep)

            if args.once:
                break
            elapsed = time.time() - t0
            time.sleep(max(1, sleep - elapsed))
    finally:
        release_lock()
        logger.info("ds_poll 退出")


if __name__ == "__main__":
    main()
