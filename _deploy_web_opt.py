# -*- coding: utf-8 -*-
"""部署优化版 app.py 到 VM 并重启 web"""
import time
import paramiko

HOST, PORT, USER, PWD = "192.168.2.76", 21258, "root", "BiuTech321.@"
LOCAL_APP = r"C:\work_flie\作业运维\datawind-asset-center\web\app.py"
REMOTE_APP = "/opt/datawind-asset-center/web/app.py"
LOCAL_DORIS = r"C:\work_flie\作业运维\datawind-asset-center\datawind\doris.py"
REMOTE_DORIS = "/opt/datawind-asset-center/datawind/doris.py"

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, port=PORT, username=USER, password=PWD,
            banner_timeout=30, auth_timeout=30, timeout=30)

# 1. 备份
cli.exec_command("cp %s %s.bak_%s" % (REMOTE_APP, REMOTE_APP, time.strftime("%Y%m%d%H%M%S")), timeout=60)
cli.exec_command("cp %s %s.bak_%s" % (REMOTE_DORIS, REMOTE_DORIS, time.strftime("%Y%m%d%H%M%S")), timeout=60)
time.sleep(1)

# 3. 上传
sftp = cli.open_sftp()
sftp.put(LOCAL_APP, REMOTE_APP)
sftp.put(LOCAL_DORIS, REMOTE_DORIS)
for f in ["index.html", "changes.html", "dataset.html", "lineage.html"]:
    sftp.put(r"C:\work_flie\作业运维\datawind-asset-center\web\static\%s" % f,
             "/opt/datawind-asset-center/web/static/%s" % f)
for lp, rp in [(r"C:\work_flie\作业运维\datawind-asset-center\datawind\models.py",
                "/opt/datawind-asset-center/datawind/models.py"),
               (r"C:\work_flie\作业运维\datawind-asset-center\datawind\ddl.py",
                "/opt/datawind-asset-center/datawind/ddl.py"),
               (r"C:\work_flie\作业运维\datawind-asset-center\run_daily.py",
                "/opt/datawind-asset-center/run_daily.py"),
               (r"C:\work_flie\作业运维\datawind-asset-center\poll_5min.py",
                "/opt/datawind-asset-center/poll_5min.py")]:
    sftp.put(lp, rp)
sftp.close()
print("uploaded app.py + doris.py + models.py + ddl.py + run_daily.py + poll_5min.py + static(3 html)")

# 3. 重启 web
def run(cmd, t=120):
    _, out, err = cli.exec_command(cmd, timeout=t)
    return out.read().decode(), err.read().decode()

run("pkill -f '[u]vicorn app:app'", 30)
time.sleep(2)
start = '#!/bin/bash\ncd /opt/datawind-asset-center/web\nexec /opt/datawind-venv/bin/uvicorn app:app --host 0.0.0.0 --port 18081 --workers 1 >> /opt/datawind-asset-center/logs/web.log 2>&1\n'
run("cat > /tmp/start_web.sh <<'EOF'\n%sEOF\nchmod +x /tmp/start_web.sh" % start, 30)
time.sleep(1)
run("setsid nohup /tmp/start_web.sh < /dev/null > /dev/null 2>&1 &", 30)
time.sleep(8)
o, e = run("ss -ltnp | grep 18081 || netstat -ltnp 2>/dev/null | grep 18081", 30)
print("listen:", o.strip() or e.strip() or "NONE")
o, e = run("tail -5 /opt/datawind-asset-center/logs/web.log", 30)
print("log tail:", o[-600:])
cli.close()
