#!/bin/bash
# 部署机: Web服务全端点冒烟验证
B=http://127.0.0.1:8080
echo "== 页面 =="
for p in / /lineage /changes /dataset/10621761; do
  code=$(curl -s -o /dev/null -w "%{http_code}" $B$p)
  echo "$p -> $code"
done
echo "== API =="
for p in /api/stats /api/folders /api/types /api/datasets?size=2 /api/datasets/10621761 /api/datasets/10649581 "/api/tables/impact?table=ods_zamloan_orderinfo&size=3" /api/changes?size=3; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$B$p")
  echo "$p -> $code"
done
echo "== 详情字段血缘抽样 =="
curl -s "$B/api/datasets/10649581" | python3 -c "import json,sys; d=json.load(sys.stdin); print('name:', d['name']); print('field_lineage:', len(d.get('field_lineage') or [])); print('upstream_tables:', len(d.get('upstream_tables') or []))"
echo "SMOKE_DONE"
