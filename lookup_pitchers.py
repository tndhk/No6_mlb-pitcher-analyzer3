#!/usr/bin/env python
# lookup_pitchers.py
# MLB投手のIDを検索してCSVに保存するスクリプト

import os
import sys
import pandas as pd
import argparse
import csv
from pybaseball import playerid_lookup

def lookup_pitcher_id(first_name, last_name):
    """
    選手の名前からMLB IDを検索
    
    Args:
        first_name: ファーストネーム
        last_name: ラストネーム
        
    Returns:
        tuple: (mlb_id, name, team) またはエラーの場合None
    """
    try:
        # playerid_lookupを使用してIDを検索
        results = playerid_lookup(last_name, first_name)
        
        if results.empty:
            print(f"選手が見つかりませんでした: {first_name} {last_name}")
            return None
            
        # 結果の最初の行を使用（同姓同名の場合は最初のものを使用）
        player_info = results.iloc[0]
        mlb_id = player_info.get('key_mlbam')
        
        # フルネームを取得
        name = f"{player_info.get('name_first')} {player_info.get('name_last')}"
        
        # チームはAPIから取得できないので空にする
        team = ""
        
        return (mlb_id, name, team)
        
    except Exception as e:
        print(f"エラー: {str(e)}")
        return None

def lookup_multiple_pitchers(input_file=None):
    """
    複数の投手を一括検索し、CSVに保存
    
    Args:
        input_file: 入力CSVファイルのパス（オプション）
    """
    pitcher_data = []
    
    if input_file:
        # 入力ファイルから名前を読み込み
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    if len(row) >= 2:
                        first_name, last_name = row[0], row[1]
                        result = lookup_pitcher_id(first_name, last_name)
                        if result:
                            pitcher_data.append(result)
        except Exception as e:
            print(f"入力ファイルの読み込みエラー: {str(e)}")
            return
    else:
        # 対話モードで名前を入力
        print("投手の名前を入力してください。終了するには'q'を入力してください。")
        while True:
            name_input = input("投手名（名 姓、例: 'Shohei Ohtani'）: ")
            if name_input.lower() == 'q':
                break
                
            name_parts = name_input.strip().split()
            if len(name_parts) < 2:
                print("名前と姓を入力してください")
                continue
                
            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])  # 複数の名字に対応
            
            result = lookup_pitcher_id(first_name, last_name)
            if result:
                pitcher_data.append(result)
    
    # 結果をDataFrameに変換
    df = pd.DataFrame(pitcher_data, columns=['mlb_id', 'name', 'team'])
    
    # CSVファイルに保存
    output_file = "pitchers.csv"
    
    # 既存のCSVファイルが存在する場合は追加モードで保存
    if os.path.exists(output_file):
        # 既存のデータを読み込み
        existing_df = pd.read_csv(output_file)
        
        # 新しいデータを追加（重複を防ぐため、mlb_idで重複チェック）
        combined_df = pd.concat([existing_df, df])
        combined_df = combined_df.drop_duplicates(subset=['mlb_id'])
        
        # 保存
        combined_df.to_csv(output_file, index=False)
        print(f"{len(df)} 人の投手を追加しました。合計: {len(combined_df)} 人")
    else:
        # 新規作成
        df.to_csv(output_file, index=False)
        print(f"{len(df)} 人の投手情報を {output_file} に保存しました")

def main():
    parser = argparse.ArgumentParser(description="MLB投手のIDを検索してCSVに保存するスクリプト")
    parser.add_argument('-i', '--input', help='入力CSVファイルパス (オプション)')
    parser.add_argument('-n', '--name', help='検索する投手の名前 (例: "Shohei Ohtani")')
    
    args = parser.parse_args()
    
    if args.name:
        # 単一の投手を検索
        name_parts = args.name.strip().split()
        if len(name_parts) < 2:
            print("名前と姓を入力してください")
            return
            
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])
        
        result = lookup_pitcher_id(first_name, last_name)
        if result:
            mlb_id, name, _ = result
            print(f"ID: {mlb_id}, 名前: {name}")
            
            # CSVに保存するか確認
            save = input("この投手をCSVに保存しますか？ (y/n): ")
            if save.lower() == 'y':
                df = pd.DataFrame([result], columns=['mlb_id', 'name', 'team'])
                
                # 既存のCSVファイルがあるか確認
                output_file = "pitchers.csv"
                if os.path.exists(output_file):
                    existing_df = pd.read_csv(output_file)
                    combined_df = pd.concat([existing_df, df])
                    combined_df = combined_df.drop_duplicates(subset=['mlb_id'])
                    combined_df.to_csv(output_file, index=False)
                    print(f"投手を {output_file} に追加しました")
                else:
                    df.to_csv(output_file, index=False)
                    print(f"投手情報を {output_file} に保存しました")
    else:
        # 複数の投手を検索
        lookup_multiple_pitchers(args.input)

if __name__ == "__main__":
    main()