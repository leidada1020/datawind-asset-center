# -*- coding: utf-8 -*-
"""传输真实凭证到部署机 config/（不打印内容）

用法: python ssh_put_config.py <本地config路径>
凭证从环境变量读取(不入库): VMP_SSH_HOST/VMP_SSH_PORT/VMP_SSH_USER/VMP_SSH_PWD
"""
import os
import sys
import paramiko

HOST = os.environ.get("VMP_SSH_HOST", "192.168.2.76")
PORT = int(os.environ.get("VMP_SSH_PORT", "21258"))
USER = os.environ.get("VMP_SSH_USER", "root")
PWD = os.environ.get("VMP_SSH_PWD", "")
REMOTE = "/opt/datawind-asset-center/config/datawind_monitor_config.json"


def main():
    local = sys.argv[1]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, port=PORT, username=USER, password=PWD, timeout=20)
    sftp = ssh.open_sftp()
    sftp.put(local, REMOTE)
    sftp.close()
    # 权限收紧 + 校验大小
    ssh.exec_command("chmod 600 %s && ls -l %s" % (REMOTE, REMOTE))
    stdin, stdout, stderr = ssh.exec_command("wc -c < %s" % REMOTE)
    size = stdout.read().decode().strip()
    print("config transferred: %s bytes -> %s" % (size, REMOTE))
    ssh.close()


if __name__ == "__main__":
    main()
