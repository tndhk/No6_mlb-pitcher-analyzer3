# src/data_acquisition/update_script.sh
#!/bin/bash
# MLB投手データ更新スクリプト

# スクリプトのディレクトリを取得
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# プロジェクトのルートディレクトリ
ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

# ログディレクトリ
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

# タイムスタンプ
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/update_$TIMESTAMP.log"

# ヘルプメッセージ
function show_help {
    echo "使用方法: $0 [オプション]"
    echo "MLBの投手データを更新するスクリプト"
    echo ""
    echo "オプション:"
    echo "  -t, --teams TEAMS     更新するチームを指定（カンマ区切り、例: 'NYY,LAD,BOS'）"
    echo "  -y, --years YEARS     取得する年数を指定（デフォルト: 3）"
    echo "  -f, --force           既存のデータがあっても強制的に更新"
    echo "  -h, --help            このヘルプメッセージを表示"
    echo ""
    echo "例:"
    echo "  $0 --teams NYY,LAD,BOS --years 2"
    echo "  $0 --force"
}

# デフォルト値
TEAMS=""
YEARS=3
FORCE_UPDATE=false

# 引数の解析
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--teams)
            TEAMS="$2"
            shift 2
            ;;
        -y|--years)
            YEARS="$2"
            shift 2
            ;;
        -f|--force)
            FORCE_UPDATE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "エラー: 不明なオプション: $1"
            show_help
            exit 1
            ;;
    esac
done

echo "MLB投手データ更新を開始します..."
echo "ログファイル: $LOG_FILE"

# Pythonスクリプトの実行
cd "$ROOT_DIR"

# 仮想環境が存在するか確認
if [ -d "venv" ]; then
    echo "仮想環境を有効化します..."
    source venv/bin/activate
fi

# チームリストの処理
TEAM_ARGS=""
if [ -n "$TEAMS" ]; then
    # カンマ区切りをスペース区切りに変換
    TEAMS_SPACE=$(echo "$TEAMS" | tr ',' ' ')
    TEAM_ARGS="--teams $TEAMS_SPACE"
fi

# 強制更新オプション
FORCE_ARGS=""
if [ "$FORCE_UPDATE" = true ]; then
    FORCE_ARGS="--force-update"
fi

echo "投手データ更新コマンドを実行します..."
echo "python -m src.data_acquisition.update_data $TEAM_ARGS --years $YEARS $FORCE_ARGS"

# 実行
python -m src.data_acquisition.update_data $TEAM_ARGS --years $YEARS $FORCE_ARGS 2>&1 | tee "$LOG_FILE"

# 実行結果の確認
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "データ更新に成功しました！"
    exit 0
else
    echo "データ更新中にエラーが発生しました。ログを確認してください: $LOG_FILE"
    exit 1
fi