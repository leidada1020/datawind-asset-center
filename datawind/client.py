# -*- coding: utf-8 -*-
"""DataWind OpenAPI 只读客户端（P0 地基）

封装已验证可用的只读接口：
  - exchange_jwt     : 凭证换 JWT（1h 有效，带缓存）
  - list_datasets    : dataSetList（列表 + 同步信息）
  - folder_tree      : dataSetFolderTreeV2（文件夹树 -> id->路径）
  - dataset_info     : /dataset/{id}/info（文件夹/配置字段）
  - model_logic      : getDataModelInfo（加工 SQL + 来源表 + 字段清单）
  - dim_met          : dimMet（维度/度量元数据，预留 P1 血缘用）
  - lineage          : lineage 血缘接口（预留 P1）
仅 urllib，无第三方依赖。
"""
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


class DataWindClient:
    def __init__(self, cfg, jwt=None, timeout=180):
        self.cfg = cfg
        self.base = cfg["api_base"].rstrip("/")
        self.region = cfg["region"]
        self.app_id = cfg["app_id"]
        self.timeout = timeout
        self._jwt = jwt

    # ---------- 基础 ----------
    def _url(self, path):
        return self.base + path

    def _headers(self, with_auth=True):
        h = {"x-vedi-region": self.region,
             "x-request-id": os.urandom(8).hex(),
             "request-id": os.urandom(8).hex()}
        if with_auth:
            h["Authorization"] = "Bearer " + self.jwt()
        return h

    def http_json(self, path, method="GET", body=None, timeout=None, with_auth=True):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self._url(path), data=data, method=method)
        req.add_header("Content-Type", "application/json; charset=utf-8")
        for k, v in self._headers(with_auth).items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def jwt(self, force=False):
        """JWT 缓存复用（1h 有效, 到期前 300s 自动刷新）

        长驻进程(轮询)必须自动续期: 否则 1h 后全部请求 401, 连续失败触发熔断
        """
        now = time.time()
        if self._jwt and not force:
            expire_at = getattr(self, "_jwt_expire_at", 0)
            if expire_at - 300 > now:  # 剩余 >5min 直接复用
                return self._jwt
        body = {"metadata": {
            "clientId": self.cfg["client_id"],
            "clientSecret": self.cfg["client_secret"],
            "proxyUser": self.cfg.get("proxy_user", ""),
            "expire": self.cfg.get("expire", 3600),
        }}
        r = self.http_json("/api/v3/openapi/jwtToken", "POST", body,
                           timeout=60, with_auth=False)
        if r.get("code") != "aeolus/ok":
            raise RuntimeError("换取JWT失败: %s %s" % (r.get("code"), r.get("msg")))
        self._jwt = r["data"]["jwtToken"]
        self._jwt_expire_at = now + int(self.cfg.get("expire", 3600))
        return self._jwt

    # ---------- 数据集 ----------
    def list_datasets(self, need_sync_info=True):
        """全量数据集列表（元数据 + 同步信息）"""
        r = self.http_json("/api/v3/open/metadata/dataSetList", "POST",
                           {"appId": self.app_id, "needSyncInfo": need_sync_info},
                           timeout=300)
        if r.get("code") not in (None, "aeolus/ok"):
            raise RuntimeError("拉取数据集列表失败: %s %s" % (r.get("code"), r.get("msg")))
        return r.get("data") or []

    def list_datasets_light(self):
        """轻量列表（仅 id/name/mtime/type，供 5 分钟轮询 diff；与列表接口同源，字段裁剪到最小）"""
        return self.list_datasets(need_sync_info=False)

    # ---------- 文件夹树 ----------
    def folder_tree(self):
        """文件夹树 -> {folder_id: 路径字符串}"""
        r = self.http_json("/api/v4/open/app/%d/dataSetFolderTreeV2" % self.app_id,
                           timeout=120)
        data = r.get("data") or {}
        nodes = {}

        def walk(items, path):
            for it in items or []:
                fid = it.get("id")
                name = it.get("name")
                nodes[fid] = "/".join(path + [name])
                walk(it.get("childList"), path + [name])

        walk(data.get("public"), [])
        walk(data.get("private"), [])
        return nodes

    def folder_tree_raw(self):
        """文件夹树原始结构（含父子关系，供 dim_datawind_folder_s_d 落库）"""
        r = self.http_json("/api/v4/open/app/%d/dataSetFolderTreeV2" % self.app_id,
                           timeout=120)
        return r.get("data") or {}

    # ---------- 单数据集详情 ----------
    def dataset_info(self, dsid):
        """单数据集 info：baseConf 含 parentId/connectionMode 等"""
        r = self.http_json("/api/v4/open/dataset/%d/info" % dsid, timeout=120)
        return r.get("data") or {}

    def model_logic(self, dsid):
        """加工逻辑: (sql, source_tables, field_list, data_source)
        data_source: 首节点数据源 {type, id, cluster, db}（区分多 Doris/多源）"""
        r = self.http_json(
            "/api/v3/open/openDataFactory/getDataModelInfo?dataSetId=%d" % dsid,
            timeout=120)
        d = r.get("data") or {}
        nc = d.get("nodeConf") or []
        if not isinstance(nc, list):
            nc = [nc]
        sql_parts, srcs, fields = [], [], []
        data_source = None
        for node in nc:
            if not isinstance(node, dict):
                continue
            if data_source is None:
                ds_type = node.get("dataSourceType") or ""
                ds_id = node.get("dataSourceId")
                if ds_type:
                    data_source = {"type": ds_type,
                                   "id": ds_id,
                                   "cluster": node.get("clusterName") or "",
                                   "db": node.get("displayDbName") or node.get("dbName") or ""}
            q = node.get("query")
            if q:
                sql_parts.append(str(q))
            for st in node.get("sourceTableList") or []:
                db = (st.get("dbName") or "").strip()
                tb = (st.get("tableName") or "").strip()
                if db and tb:
                    srcs.append(db + "." + tb)
            for f in node.get("fields") or []:
                if isinstance(f, dict):
                    fields.append({"name": f.get("name"),
                                   "business": f.get("dsColumnNames"),
                                   "type": f.get("prepType") or f.get("type")})
        return ("\n;\n".join(sql_parts),
                ", ".join(sorted(set(srcs))),
                json.dumps(fields, ensure_ascii=False),
                data_source)

    # ---------- 并发批量 ----------
    def batch_fetch(self, ids, fetch_fn, max_workers=8):
        """并发抓取，单项失败返回 None/{}（不中断整体）"""
        out = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(fetch_fn, i): i for i in ids}
            for fu in as_completed(futs):
                i = futs[fu]
                try:
                    out[i] = fu.result()
                except Exception:
                    out[i] = None
        return out

    # ---------- 预留：血缘（P1） ----------
    def lineage(self, dsid):
        """血缘接口（预留 P1，实测可用性后再启用）"""
        raise NotImplementedError("血缘接口在 P1 阶段启用")
