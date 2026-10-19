# -*- coding: utf-8 -*-
"""DolphinScheduler API 客户端（调度资产采集）

认证: 请求头 token=<安全中心·令牌管理创建的API令牌>（DS 3.x, 令牌有有效期需定期更新）
接口（已实测通过 2026-10-19）:
  GET /projects                               项目列表(9个, 按数仓分层命名)
  GET /projects/{code}/process-definition     工作流定义列表
  GET /projects/{code}/process-definition/{code}  工作流详情(任务节点+taskParams.SQL)
  GET /projects/{code}/process-instances      工作流实例(状态/时间, 健康度监控预留)
响应统一 {code:0, msg, data, failed, success}; code!=0 抛 RuntimeError
"""
import json
import time
import urllib.parse
import urllib.request


class DSClient:
    def __init__(self, base_url, token, timeout=20, retries=3):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.retries = retries

    def _get(self, path, params=None):
        """GET 请求: 带 token 头, code!=0 抛错; 网络异常有限重试"""
        url = self.base_url + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        last = None
        for i in range(self.retries):
            try:
                req = urllib.request.Request(url, headers={"token": self.token})
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    data = json.loads(r.read().decode("utf-8"))
                if data.get("code") != 0:
                    raise RuntimeError("DS API code=%s msg=%s path=%s"
                                       % (data.get("code"), data.get("msg"), path))
                return data.get("data") or {}
            except Exception as e:
                last = e
                time.sleep(2 * (i + 1))
        raise last

    def _paged(self, path, page_size):
        """通用翻页: 返回 totalList 全量"""
        out, page = [], 1
        while True:
            d = self._get(path, {"pageNo": page, "pageSize": page_size})
            items = d.get("totalList") or []
            out.extend(items)
            total_page = d.get("totalPage") or 1
            if not items or page >= total_page:
                break
            page += 1
        return out

    def list_projects(self, page_size=100):
        """项目列表: [{id, code, name, userName, defCount, instRunningCount, ...}]"""
        return self._paged("/projects", page_size)

    def list_workflows(self, project_code, page_size=100):
        """工作流定义列表: [{code, name, releaseState, scheduleReleaseState, ...}]"""
        return self._paged("/projects/%s/process-definition" % project_code, page_size)

    def get_workflow_detail(self, project_code, workflow_code):
        """工作流详情: {name, taskDefinitionList:[{code,name,taskType,flag,taskParams,...}]}"""
        return self._get("/projects/%s/process-definition/%s"
                         % (project_code, workflow_code))

    def list_instances(self, project_code, page_no=1, page_size=20):
        """工作流实例列表(健康度监控): [{id, name, state, startTime, endTime, host, ...}]"""
        return self._get("/projects/%s/process-instances" % project_code,
                         {"pageNo": page_no, "pageSize": page_size})
