# -*- coding: utf-8 -*-
"""SSH 执行本地脚本文件内容（部署机 bash 执行，避免引号转义问题）

用法: python ssh_exec_file.py <本地脚本路径>
将脚本内容上传为 /tmp/deploy_script.sh 并 bash 执行。
凭证从环境变量读取(不入库): VMP_SSH_HOST/VMP_SSH_PORT/VMP_SSH_USER/VMP_SSH_PWD
"""
import os
import sys
import paramiko

HOST = os.environ.get("VMP_SSH_HOST", "192.168.2.76")
PORT = int(os.environ.get("VMP_SSH_PORT", "21258"))
USER = os.environ.get("VMP_SSH_USER", "root")
PWD = os.environ.get("VMP_SSH_PWD", "")


def main():
    script_path = sys.argv[1]
    with open(script_path, "r", encoding="utf-8") as f:
        content = f.read()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, port=PORT, username=USER, password=PWD, timeout=20)

    sftp = ssh.open_sftp()
    with sftp.open("/tmp/deploy_script.sh", "w") as f:
        f.write(content)
    sftp.close()

    chan = ssh.get_transport().open_session()
    chan.settimeout(300)
    chan.exec_command("bash /tmp/deploy_script.sh")
    out = b""
    err = b""
    while True:
        try:
            if chan.recv_ready():
                out += chan.recv(65536)
            if chan.recv_stderr_ready():
                err += chan.recv_stderr(65536)
            if chan.exit_status_ready() and not chan.recv_ready() and not chan.recv_stderr_ready():
                break
        except Exception:
            break
        if chan.exit_status_ready():
            # 最后再清一次缓冲区
            while chan.recv_ready():
                out += chan.recv(65536)
            while chan.recv_stderr_ready():
                err += chan.recv_stderr(65536)
            break
    print(out.decode("utf-8", "replace"))
    if err.strip():
        print("[stderr]\n" + err.decode("utf-8", "replace"))
    print("[exit=%s]" % chan.recv_exit_status())
    ssh.close()


if __name__ == "__main__":
    main()
