# deploy/Dockerfile — AQT_v1 容器映像
#
# 建置：
#   docker build -f deploy/Dockerfile -t aqt_v1:latest .
#
# 執行（單機）：
#   docker run -d \
#     --name aqt_v1_bot \
#     --restart unless-stopped \
#     --env-file .env \
#     -v $(pwd)/data:/app/data \
#     -v $(pwd)/logs:/app/logs \
#     aqt_v1:latest
#
# 注意：
#   - .env 檔案包含 API 金鑰，切勿 COPY 進映像（使用 --env-file 掛載）
#   - data/ 與 logs/ 目錄請掛載為 volume，確保容器重啟後資料不遺失

# ── 基礎映像：使用 slim 減小體積 ─────────────────────────────
FROM python:3.11-slim

# ── 時區設定（④ 確保日誌顯示時間一致）──────────────────────
ENV TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# ── 建立非 root 使用者（安全強化）──────────────────────────
RUN groupadd -r aqt && useradd -r -g aqt -d /app aqt

# ── 工作目錄 ─────────────────────────────────────────────────
WORKDIR /app

# ── 安裝依賴（先複製 requirements 利用 layer cache）─────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 複製程式碼（排除 .env、*.db、logs/）────────────────────
COPY --chown=aqt:aqt . .

# ── 確保資料目錄存在並可寫入 ────────────────────────────────
RUN mkdir -p /app/data /app/logs /app/runtime \
    && chown -R aqt:aqt /app/data /app/logs /app/runtime

# ── 切換到非 root ────────────────────────────────────────────
USER aqt

# ── DB 與日誌路徑（可被 docker-compose.yml 的 environment 覆蓋）
ENV DB_PATH=/app/data/trading_bot.db
ENV LOG_DIR=/app/logs
ENV BOT_VERSION=grid

# ── 健康檢查：確認進程存在（每 30 秒）──────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD pgrep -f "run_bot.py" > /dev/null || exit 1

# ── ③ 優雅關閉：Docker stop 會送 SIGTERM，stopsignal 確認 ──
STOPSIGNAL SIGTERM

# ── 啟動指令 ─────────────────────────────────────────────────
CMD ["python", "run_bot.py"]
