# ── Dockerfile ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

# 1) Set working dir
WORKDIR /app

# 2) Copy & install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3) Copy the rest of your code
COPY . .

# 4) Prepare the Kùzu data directory
ENV KUZU_PATH=/app/kuzudb_data
RUN mkdir -p ${KUZU_PATH}
VOLUME ${KUZU_PATH}

# 5) Default command
CMD ["python", "server.py"]