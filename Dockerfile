# datawind-asset-center 本地 Docker 镜像
# 运行形态与 VM 一致: WORKDIR=/app/web, uvicorn app:app, 监听 0.0.0.0:18081
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY datawind ./datawind
COPY config ./config
COPY web ./web
COPY run_daily.py poll_5min.py ds_poll.py ./

WORKDIR /app/web
EXPOSE 18081

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "18081", "--workers", "1"]
