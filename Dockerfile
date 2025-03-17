# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# タイムゾーンを設定
ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 必要なパッケージをインストール
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Pythonの依存パッケージをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY . .

# プロジェクトをインストール可能なパッケージとして設定
RUN pip install -e .

# Streamlitのデフォルトポート
EXPOSE 8501

# コンテナ起動時のコマンド
CMD ["streamlit", "run", "src/app.py"]