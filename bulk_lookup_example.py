#!/usr/bin/env python
# bulk_lookup_example.py
# 複数の投手をまとめて検索し、CSVに追加する例

import csv
import pandas as pd
from pybaseball import playerid_lookup

# 検索したい投手のリスト（ファーストネーム、ラストネーム）
pitchers_to_lookup = [
    # 例として著名な投手をいくつか挙げています
    ("Shohei", "Ohtani"),
    ("Clayton", "Kershaw"),
    ("Max", "Scherzer"),
    ("Jacob", "deGrom"),
    ("Gerrit", "Cole"),
    ("Yu", "Darvish"),
    ("Kodai", "Senga"),
    ("Yoshinobu", "Yamamoto")
]

# 結果を格納するリスト
pitcher_data = []

# 各投手を検索
for first_name, last_name in pitchers_to_lookup:
    try:
        print(f"検索中: {first_name} {last_name}")
        results = playerid_lookup(last_name, first_name)
        
        if not results.empty:
            # 最初の結果を使用
            player_info = results.iloc[0]
            mlb_id = player_info.get('key_mlbam')
            name = f"{player_info.get('name_first')} {player_info.get('name_last')}"
            
            # チームはAPIから直接取得できないので空欄にしておく
            # 後で手動で追加する必要があります
            team = ""
            
            pitcher_data.append((mlb_id, name, team))
            print(f"見つかりました: ID={mlb_id}, 名前={name}")
        else:
            print(f"選手が見つかりませんでした: {first_name} {last_name}")
    
    except Exception as e:
        print(f"エラー ({first_name} {last_name}): {str(e)}")

# 結果をDataFrameに変換
df = pd.DataFrame(pitcher_data, columns=['mlb_id', 'name', 'team'])

# 出力ファイル名
output_file = "new_pitchers.csv"

# CSVファイルに保存
df.to_csv(output_file, index=False)
print(f"{len(df)} 人の投手情報を {output_file} に保存しました")

# 既存のpitchers.csvに追加したい場合は以下のコードを使用
try:
    existing_file = "pitchers.csv"
    
    # 既存のファイルがあるか確認
    try:
        existing_df = pd.read_csv(existing_file)
        print(f"既存のファイル {existing_file} から {len(existing_df)} 人の投手を読み込みました")
        
        # 新しいデータを追加（重複を防ぐため、mlb_idで重複チェック）
        combined_df = pd.concat([existing_df, df])
        combined_df = combined_df.drop_duplicates(subset=['mlb_id'])
        
        # 保存
        combined_df.to_csv(existing_file, index=False)
        print(f"{len(df)} 人の投手を追加しました。合計: {len(combined_df)} 人")
    except FileNotFoundError:
        print(f"既存のファイル {existing_file} が見つかりませんでした。新規ファイルを作成します。")
        df.to_csv(existing_file, index=False)
        print(f"{len(df)} 人の投手情報を {existing_file} に保存しました")
except Exception as e:
    print(f"既存ファイルへの追加中にエラーが発生しました: {str(e)}")