# -*- coding: utf-8 -*-
"""
datawind 包：DataWind 数据集资产采集层（P0 地基）
=================================================
职责：
  - client  : DataWind OpenAPI 只读客户端（JWT 换发 + 列表/文件夹树/详情/加工逻辑/血缘）
  - models  : 数据集归一化、文件夹树解析、变更检测
  - doris   : Doris 连接与写入（MySQL 协议, 断线重连）

设计原则：
  - 不新增第三方依赖（仅 pymysql, 复用 urllib）
  - 凭证从 config JSON 或环境变量读取，不硬编码
  - 表命名沿用企业规范：层前缀_业务域_对象_粒度_后缀(_s_d 快照/_i_d 增量)
"""
from .config import load_config
from .client import DataWindClient
from . import models, doris

__all__ = ["load_config", "DataWindClient", "models", "doris"]
__version__ = "0.1.0"
