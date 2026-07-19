FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖 + 下载核心
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sL "https://github.com/v2fly/v2ray-core/releases/latest/download/v2ray-linux-64.zip" -o /tmp/core.zip \
    && unzip -o /tmp/core.zip -d /usr/local/bin/ v2ray \
    && chmod +x /usr/local/bin/v2ray 

# 下载 geosite.dat / geoip.dat 数据文件
RUN curl -sL "https://github.com/v2fly/domain-list-community/releases/latest/download/dlc.dat" -o /usr/local/bin/geosite.dat \
    && curl -sL "https://github.com/v2fly/geoip/releases/latest/download/geoip.dat" -o /usr/local/bin/geoip.dat

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY . .

# 创建数据目录
RUN mkdir -p /app/data

EXPOSE 8080

CMD ["python", "-m", "app.main"]
