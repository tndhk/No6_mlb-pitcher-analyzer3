#!/bin/bash
# run_tests.sh - 様々なテストを実行するスクリプト

# カラー表示のための設定
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# ヘルプメッセージ
function show_help {
    echo "使用方法: $0 [オプション]"
    echo "テストを実行するスクリプト"
    echo ""
    echo "オプション:"
    echo "  -u, --unit         ユニットテストを実行"
    echo "  -i, --integration  結合テストを実行"
    echo "  -a, --all          すべてのテストを実行"
    echo "  -c, --coverage     カバレッジレポートを生成"
    echo "  -v, --verbose      詳細な出力"
    echo "  -h, --help         このヘルプメッセージを表示"
    echo ""
    echo "例:"
    echo "  $0 --unit                   # ユニットテストを実行"
    echo "  $0 --integration            # 結合テストを実行"
    echo "  $0 --all --coverage         # すべてのテストを実行し、カバレッジレポートを生成"
}

# デフォルト値
RUN_UNIT=false
RUN_INTEGRATION=false
RUN_COVERAGE=false
VERBOSE=false

# 引数がない場合はヘルプを表示
if [ $# -eq 0 ]; then
    show_help
    exit 1
fi

# 引数の解析
while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--unit)
            RUN_UNIT=true
            shift
            ;;
        -i|--integration)
            RUN_INTEGRATION=true
            shift
            ;;
        -a|--all)
            RUN_UNIT=true
            RUN_INTEGRATION=true
            shift
            ;;
        -c|--coverage)
            RUN_COVERAGE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
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

# テスト用の一時ディレクトリを作成
mkdir -p tests/data

# コマンドの構築
PYTEST_CMD="pytest"

# 詳細出力オプションの追加
if [ "$VERBOSE" = true ]; then
    PYTEST_CMD="$PYTEST_CMD -v"
fi

# カバレッジオプションの追加
if [ "$RUN_COVERAGE" = true ]; then
    PYTEST_CMD="$PYTEST_CMD --cov=src --cov-report=html --cov-report=term"
fi

# ユニットテストの実行
if [ "$RUN_UNIT" = true ]; then
    echo -e "${YELLOW}ユニットテストを実行しています...${NC}"
    if [ "$RUN_INTEGRATION" = true ]; then
        # 結合テストも実行する場合は、unit_testディレクトリのみをターゲットにする
        UNIT_RESULT=$(eval "$PYTEST_CMD tests/data_acquisition tests/data_storage tests/data_analysis tests/visualization -m 'not integration' -v")
    else
        # 結合テストを除外
        UNIT_RESULT=$(eval "$PYTEST_CMD -m 'not integration' -v")
    fi
    UNIT_EXIT_CODE=$?
    
    if [ $UNIT_EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}ユニットテストが成功しました${NC}"
    else
        echo -e "${RED}ユニットテストが失敗しました${NC}"
        echo "$UNIT_RESULT"
    fi
fi

# 結合テストの実行
if [ "$RUN_INTEGRATION" = true ]; then
    echo -e "${YELLOW}結合テストを実行しています...${NC}"
    INTEGRATION_RESULT=$(eval "$PYTEST_CMD -m integration -v")
    INTEGRATION_EXIT_CODE=$?
    
    if [ $INTEGRATION_EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}結合テストが成功しました${NC}"
    else
        echo -e "${RED}結合テストが失敗しました${NC}"
        echo "$INTEGRATION_RESULT"
    fi
fi

# 実行結果の確認
if [ "$RUN_UNIT" = true ] && [ "$RUN_INTEGRATION" = true ]; then
    if [ $UNIT_EXIT_CODE -eq 0 ] && [ $INTEGRATION_EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}すべてのテストが成功しました${NC}"
        exit 0
    else
        echo -e "${RED}一部のテストが失敗しました${NC}"
        exit 1
    fi
elif [ "$RUN_UNIT" = true ]; then
    if [ $UNIT_EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}すべてのユニットテストが成功しました${NC}"
        exit 0
    else
        echo -e "${RED}一部のユニットテストが失敗しました${NC}"
        exit 1
    fi
elif [ "$RUN_INTEGRATION" = true ]; then
    if [ $INTEGRATION_EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}すべての結合テストが成功しました${NC}"
        exit 0
    else
        echo -e "${RED}一部の結合テストが失敗しました${NC}"
        exit 1
    fi
fi