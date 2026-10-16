# -*- coding: utf-8 -*-
"""配置加载：datawind_monitor_config.json 或环境变量"""
import json
import os

CONFIG_DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "..", "config", "datawind_monitor_config.json")


def load_config(path=None):
    """加载 DataWind + Doris 配置。
    优先级: 环境变量 > 配置文件。
    不打印凭证明文（仅校验存在）。
    """
    cfg_path = path or CONFIG_DEFAULT_PATH
    if not os.path.exists(cfg_path):
        raise FileNotFoundError("配置文件不存在: %s（可从新聊天目录 datawind_monitor_config.json 复制）" % cfg_path)
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    env_map = {
        "api_base": "DATAWIND_API_BASE",
        "region": "DATAWIND_REGION",
        "client_id": "DATAWIND_CLIENT_ID",
        "client_secret": "DATAWIND_CLIENT_SECRET",
        "proxy_user": "DATAWIND_PROXY_USER",
        "expire": "DATAWIND_EXPIRE",
        "app_id": "DATAWIND_APP_ID",
        # Doris
        "doris_host": "DORIS_HOST",
        "doris_port": "DORIS_PORT",
        "doris_user": "DORIS_USER",
        "doris_password": "DORIS_PASSWORD",
        "doris_db": "DORIS_DB",
    }
    for key, env in env_map.items():
        val = os.environ.get(env)
        if val is not None:
            if key in ("expire", "app_id", "doris_port"):
                cfg[key] = int(val)
            else:
                cfg[key] = val

    required = ["api_base", "region", "client_id", "client_secret", "app_id"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise RuntimeError("配置缺失字段: %s" % ", ".join(missing))

    # Doris 连接信息必须来自配置文件(host/port/user/password/db)，缺失即报错
    # （不硬编码默认值: 连接口令属于敏感信息, 不入代码库; 模板见 config/*.example.json）
    doris_required = ["doris_host", "doris_port", "doris_user", "doris_password", "doris_db"]
    doris_missing = [k for k in doris_required if not cfg.get(k)]
    if doris_missing:
        raise RuntimeError("Doris 配置缺失字段: %s（请在配置文件补齐, 参考 example 模板）" % ", ".join(doris_missing))
    cfg["doris_port"] = int(cfg["doris_port"])
    return cfg
