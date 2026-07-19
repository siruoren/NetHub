FROM python:3.11-slim

# GitHub 下载镜像前缀（国内构建可设为 https://ghgo.xyz/ 等）
ARG GH_MIRROR="https://gh-proxy.com/"

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*


# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# 下载核心（固定版本 + 超时重试）
# v2ray-core v5.23.0
RUN curl -fsSL --connect-timeout 30 --max-time 180 --retry 3 --retry-delay 5 \
    "${GH_MIRROR}https://github.com/v2fly/v2ray-core/releases/download/v5.23.0/v2ray-linux-64.zip" \
    -o /tmp/core.zip \
    && unzip -o /tmp/core.zip -d /usr/local/bin/ v2ray \
    && chmod +x /usr/local/bin/v2ray \
    && rm -f /tmp/core.zip

# Xray-core v25.7.16
RUN curl -fsSL --connect-timeout 30 --max-time 180 --retry 3 --retry-delay 5 \
    "${GH_MIRROR}https://github.com/XTLS/Xray-core/releases/download/v25.7.16/Xray-linux-64.zip" \
    -o /tmp/xray.zip \
    && unzip -o /tmp/xray.zip -d /usr/local/bin/ xray \
    && chmod +x /usr/local/bin/xray \
    && rm -f /tmp/xray.zip

# 下载 geosite.dat / geoip.dat 数据文件
RUN curl -fsSL --connect-timeout 30 --max-time 120 --retry 3 --retry-delay 5 \
    "${GH_MIRROR}https://github.com/v2fly/domain-list-community/releases/download/20250718043914/dlc.dat" \
    -o /usr/local/bin/geosite.dat \
    && curl -fsSL --connect-timeout 30 --max-time 120 --retry 3 --retry-delay 5 \
    "${GH_MIRROR}https://github.com/v2fly/geoip/releases/download/202507170706/geoip.dat" \
    -o /usr/local/bin/geoip.dat


# 复制代码
COPY . .

# 创建数据目录
RUN mkdir -p /app/data

EXPOSE 8080

CMD ["python", "-m", "app.main"]
