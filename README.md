# MLB投手パフォーマンスダッシュボード

MLBの投手パフォーマンスを視覚化するStreamlitベースのダッシュボードアプリケーションです。球種ごとの使用割合、球速、空振り率、各種指標（FIP、WHIP、ERA、K/9など）を分析し、視覚的に表示します。

![ダッシュボードサンプル](docs/dashboard_sample.png)

## 機能

- **データ取得**: Statcastから最新の投手データを取得（pybaseballライブラリ使用）
- **データ保存**: SQLiteデータベースでデータを管理
- **パフォーマンス指標**: ERA、FIP、WHIP、K/9、BB/9、HR/9などの基本指標
- **球種分析**: 球種ごとの使用割合、球速、回転数、変化量の可視化
- **スイング指標**: SwStr%、CSW%、O-Swing%、Z-Contact%など
- **時系列分析**: 各指標の推移を可視化
- **検索機能**: 投手名、チーム、期間による検索

## 必要条件

- Python 3.8以上
- Dockerがインストールされていること（Dockerを使用する場合）

## セットアップ

### Dockerを使用する場合

1. リポジトリをクローン
```bash
git clone https://github.com/yourusername/mlb-pitcher-dashboard.git
cd mlb-pitcher-dashboard
```

2. Dockerコンテナをビルドして起動
```bash
docker-compose up -d
```

3. ブラウザでアクセス: `http://localhost:8501`

### ローカル環境にインストールする場合

1. リポジトリをクローン
```bash
git clone https://github.com/yourusername/mlb-pitcher-dashboard.git
cd mlb-pitcher-dashboard
```

2. 仮想環境を作成してアクティベート
```bash
python -m venv venv
source venv/bin/activate  # Windowsの場合: venv\Scripts\activate
```

3. 依存パッケージをインストール
```bash
pip install -r requirements.txt
pip install -e .
```

4. アプリケーションを起動
```bash
streamlit run src/app.py
```

5. ブラウザでアクセス: `http://localhost:8501`

## データ取得方法

アプリケーションが初期起動時、またはデータ更新ボタンをクリックした際に、Statcastから最新のMLB投手データを取得します。

```bash
# 手動でデータを更新する場合
python -m src.data_acquisition.update_data
```

## プロジェクト構造

```
mlb_pitcher_dashboard/
├── data/                  # データベースファイル
├── docs/                  # ドキュメントとイメージ
├── src/                   # ソースコード
│   ├── data_acquisition/  # データ取得モジュール
│   ├── data_storage/      # データストレージモジュール
│   ├── data_analysis/     # データ分析モジュール
│   ├── visualization/     # 可視化モジュール
│   └── app.py             # メインアプリケーション
├── tests/                 # テストコード
├── Dockerfile             # Dockerビルド設定
├── docker-compose.yml     # Dockerコンポーズ設定
├── requirements.txt       # 依存パッケージリスト
├── setup.py               # パッケージ設定
└── README.md              # このファイル
```

## 開発方法

このプロジェクトはテスト駆動開発（TDD）アプローチを採用しています。

### テストの実行

```bash
# すべてのテストを実行
pytest

# 特定のテストを実行
pytest tests/data_acquisition/test_statcast_client.py

# カバレッジレポートを生成
pytest --cov=src --cov-report=html
```

### コードスタイルとフォーマット

```bash
# コードフォーマット
black src tests

# インポートの整理
isort src tests

# コード品質チェック
pylint src tests
```

## CI/CD

このプロジェクトはGitHub Actionsを使用して継続的インテグレーションを実行しています:

- プッシュとプルリクエスト時に自動テスト実行
- コードスタイルとフォーマットチェック
- Dockerイメージのビルドテスト

## 貢献方法

貢献をお待ちしています！詳細は[CONTRIBUTING.md](CONTRIBUTING.md)をご覧ください。

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。詳細は[LICENSE](LICENSE)ファイルをご覧ください。

## 謝辞

- [pybaseball](https://github.com/jldbc/pybaseball): MLBデータ取得のためのPythonライブラリ
- [Streamlit](https://streamlit.io/): インタラクティブなデータアプリケーション構築のためのフレームワーク
- [Statcast](https://baseballsavant.mlb.com/statcast_search): MLBのアドバンスト統計データを提供