# -*- coding: utf-8 -*-
"""SSH 远程执行辅助：在部署机上执行命令并打印结果
用法: python ssh_run.py <cmd>
凭证从环境变量读取(不入库): VMP_SSH_HOST/VMP_SSH_PORT/VMP_SSH_USER/VMP_SSH_PWD
"""
import os
import sys
import paramiko

HOST = os.environ.get("VMP_SSH_HOST", "192.168.2.76")
PORT = int(os.environ.get("VMP_SSH_PORT", "21258"))
USER = os.environ.get("VMP_SSH_USER", "root")
PWD = os.environ.get("VMP_SSH_PWD", "")


def run(ssh, cmd, timeout=120):
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    return (out + "\n" + err).strip()


def main():
    # 用法: python ssh_run.py <cmd>  或  python ssh_run.py cmd1 -- cmds以空格拼接为一条
    cmd = " ".join(sys.argv[1:])
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, port=PORT, username=USER, password=PWD, timeout=20)
    print("$ %s" % cmd)
    print(run(ssh, cmd))
    ssh.close()


if __name__ == "__main__":
    main()
