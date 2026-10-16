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

    # Doris 默认值（config 文件可不含 Doris 段，沿用既有连接）
    cfg.setdefault("doris_host", "203.123.86.71")
    cfg.setdefault("doris_port", 9030)
    cfg.setdefault("doris_user", "pl")
    cfg.setdefault("doris_password", "m!s6uDY+!wHepi@WIWs+")
    cfg.setdefault("doris_db", "etl")
    return cfg
