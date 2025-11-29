# Version: 1.0.11 (表示ロジックの再修正: 運行状況から現在地情報を完全削除)
# Last Major Update: 運行状況(info)のメッセージから現在地情報(now_stop)を完全に分離し、
#                   現在地情報は「現在地」カラムでのみ表示するように修正。

from flask import Flask, render_template, request
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.error
import pandas as pd
import numpy as np
import io
from time import sleep as slp 
import json 
import urllib.parse 
import os # ファイル存在確認のために追加

# --- 設定 ---
# GTFSリアルタイムデータのURLを設定
GTFS_RT_URL = 'https://akita.bustei.net/TripUpdate.pb'
# 日本標準時 (JST) を定義
JST = timezone(timedelta(hours=+9))
APP_PORT = 5000

# --- グローバル変数（GTFS静的データ） ---
# アプリケーション起動時に一度だけロードされる辞書
GTFS = {}
# GTFSファイル名リスト
GTFS_FILES = ['stop_times.txt', 'trips.txt', 'stops.txt', 'routes.txt', 'calendar.txt', 'calendar_dates.txt']


def extract_parent_id(stop_id):
    """'mytown0003_01' -> 'mytown0003' のように、枝番を除いた親IDを抽出する"""
    if pd.isna(stop_id):
        return None
    # 最後の '_' で分割し、親ID部分を返す
    return stop_id.rsplit('_', 1)[0]

def extract_parent_route_id(route_id):
    """'A_1' -> 'A' のように、'_' の前の親ルートIDを抽出する"""
    if pd.isna(route_id):
        return None
    # 最初の '_' で分割し、親ID部分を抽出
    return route_id.split('_', 1)[0]

def time_to_seconds(time_str):
    """'HH:MM:SS'形式の時刻文字列を秒単位に変換する (24時間超え対応)"""
    try:
        h, m, s = map(int, time_str.split(':'))
        # GTFSでは24時間以上の時刻（例: 25:00:00）が許容される
        return h * 3600 + m * 60 + s
    except:
        return np.nan

def load_gtfs_data():
    """GTFS静的データをPandas DataFrameとしてメモリにロードし、必要なマッピングを作成する"""
    print("GTFS静的データをロード中...")
    try:
        # stop_times.txt: 時刻データ（重要）と出発時刻の秒変換
        df_st = pd.read_csv('gtfs_data/stop_times.txt', dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int})
        # arrival_time を departure_time の代わりに利用し、秒変換 (バス停での予測到着時刻の基準として利用)
        df_st['departure_sec'] = df_st['departure_time'].apply(time_to_seconds)
        df_st['arrival_sec'] = df_st['arrival_time'].apply(time_to_seconds)

        # trips.txt: トリップ情報
        df_t = pd.read_csv('gtfs_data/trips.txt', dtype={'route_id': str, 'service_id': str, 'trip_id': str})
        
        # stops.txt: バス停名
        df_s = pd.read_csv('gtfs_data/stops.txt', dtype={'stop_id': str})
        
        # routes.txt (路線情報)
        df_r = pd.read_csv('gtfs_data/routes.txt', dtype={'route_id': str})
        # 新しい親ルートIDを抽出
        df_r['parent_route_id'] = df_r['route_id'].apply(extract_parent_route_id)
        
        # calendar.txt: 運行曜日
        df_c = pd.read_csv('gtfs_data/calendar.txt', dtype={'service_id': str})
        
        # calendar_dates.txt: 例外日
        df_cd = pd.read_csv('gtfs_data/calendar_dates.txt', dtype={'service_id': str, 'date': str, 'exception_type': int})

        # --- 親IDとバス停名のマッピングを作成 ---
        df_s['parent_id'] = df_s['stop_id'].apply(extract_parent_id)
        # stop_nameをキーとして、その代表となるparent_idを格納
        parent_id_map = df_s.drop_duplicates(subset=['parent_id']).set_index('stop_name')['parent_id'].to_dict()

        # --- 路線グループとバス停名のマッピングを作成 ---
        df_route_stops = df_st[['trip_id', 'stop_id']].drop_duplicates()
        df_route_stops = df_route_stops.merge(df_t[['trip_id', 'route_id']], on='trip_id', how='left')
        
        # route_long_name の代わりに parent_route_id を結合
        df_route_stops = df_route_stops.merge(df_r[['route_id', 'parent_route_id']], on='route_id', how='left')
        df_route_stops = df_route_stops.merge(df_s[['stop_id', 'stop_name']], on='stop_id', how='left')
        
        # マッピングに使用する列を 'parent_route_id' に変更
        df_final_mapping = df_route_stops[['parent_route_id', 'stop_name']].dropna().drop_duplicates()

        # 辞書 {親ルートID: [五十音順ソート済みバス停名リスト]} を作成
        route_stop_map = {}
        # グループ化キーを 'parent_route_id' に変更
        for parent_route_id, group in df_final_mapping.groupby('parent_route_id'):
            # 日本語の五十音順ソート
            sorted_stops = sorted(group['stop_name'].unique().tolist())
            route_stop_map[parent_route_id] = sorted_stops

        # 運行サービスの結合と始終点時刻の計算
        # GTFSの24時間超え時刻（例: 25:00:00）を考慮して、trip_idごとの最小/最大 departure_sec を取得
        df_min_max_times = df_st.groupby('trip_id')['departure_sec'].agg(['min', 'max']).reset_index()
        df_min_max_times.rename(columns={'min': 'dep_sec', 'max': 'arr_sec'}, inplace=True)

        # グローバル辞書に格納
        GTFS['STOP_TIMES'] = df_st
        GTFS['TRIPS'] = df_t
        GTFS['STOPS'] = df_s
        GTFS['CALENDAR'] = df_c
        GTFS['CALENDAR_DATES'] = df_cd
        GTFS['PARENT_ID_MAP'] = parent_id_map 
        GTFS['TRIP_BOUNDARIES'] = df_min_max_times
        GTFS['ROUTE_STOP_MAP'] = route_stop_map 
        GTFS['ROUTE_NAMES'] = sorted(route_stop_map.keys()) 
        # stop_id -> stop_name のマッピング辞書もここで作成しておく
        GTFS['STOP_NAME_MAP'] = df_s.set_index('stop_id')['stop_name'].to_dict()
        
        print("GTFSデータのロードが完了しました。")

    except FileNotFoundError as e:
        print(f"エラー: GTFSデータファイルが見つかりません。フォルダ構成を確認してください: {e}")
        return {}


def get_current_service_ids(now_jst):
    """現在の日付と曜日から有効なservice_idのリストを取得する"""
    today_date = now_jst.strftime('%Y%m%d')
    day_name = now_jst.strftime('%A').lower()  # 例: friday
    
    # 曜日による運行判定
    weekly_services = []
    if day_name in GTFS['CALENDAR'].columns:
        weekly_services = GTFS['CALENDAR'][GTFS['CALENDAR'][day_name] == 1]['service_id'].tolist()
    else:
        print(f"Warning: Day name {day_name} not found in calendar columns.")

    # 例外日による運行サービス追加・削除
    exceptions = GTFS['CALENDAR_DATES'][GTFS['CALENDAR_DATES']['date'] == today_date]
    added_services = exceptions[exceptions['exception_type'] == 1]['service_id'].tolist()
    removed_services = exceptions[exceptions['exception_type'] == 2]['service_id'].tolist()

    active_service_ids = set(weekly_services).union(set(added_services)) - set(removed_services)
    
    return list(active_service_ids)


def get_realtime_updates():
    """GTFS-RTデータを取得し、DataFrameとして返す"""
    MAX_RETRIES = 3
    
    # 疑似GTFS-RTデータ（実際のAPI呼び出しの代わり）
    # tripA01 は 200秒遅延しており、次の停車駅は S-10_01 (sequence 3) である、と仮定
    # tripA02 は 60秒早着しており、次の停車駅は S-05 (sequence 2) である、と仮定
    # ----------------------------------------------------
    # dummy_rt_data = [
    #     {'trip_id': 'tripA01', 'stop_sequence': 3, 'delay_sec': 200, 'rt_stop_id': 'S-10_01'},
    #     {'trip_id': 'tripA02', 'stop_sequence': 2, 'delay_sec': -60, 'rt_stop_id': 'S-05'},
    #     # S-15_01 (sequence 4) は RT情報がないため、現在地は S-10_01 (sequence 3) の通過と推定される
    #     # S-05 (sequence 2) は RT情報がないため、現在地は S-01 (sequence 1) の通過と推定される
    # ]
    
    # df_rt_all = pd.DataFrame(dummy_rt_data)
    # return df_rt_all
    # ----------------------------------------------------
    
    # 実際のAPI呼び出しロジック
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(GTFS_RT_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as res:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(res.read())

                updates = []
                for entity in feed.entity:
                    # TripUpdateエンティティのみを処理
                    if entity.HasField('trip_update'):
                        tu = entity.trip_update
                        trip_id = tu.trip.trip_id

                        # スケジュール変更情報（stop_time_update）をチェック
                        for stu in tu.stop_time_update:
                            # 遅延情報（departure_delay）があるか確認
                            if stu.HasField('departure') and stu.departure.HasField('delay'):
                                updates.append({
                                    'trip_id': trip_id,
                                    'stop_sequence': stu.stop_sequence,
                                    # GTFS-RTの遅延情報（秒）をそのまま利用
                                    'delay_sec': stu.departure.delay, 
                                    # RTデータが提供された停留所ID
                                    'rt_stop_id': stu.stop_id
                                })
                            # 予測到着時刻ベースの場合 (ここでは出発遅延を優先)
                            # elif stu.HasField('arrival') and stu.arrival.HasField('delay'):
                            #     updates.append({
                            #         'trip_id': trip_id,
                            #         'stop_sequence': stu.stop_sequence,
                            #         'delay_sec': stu.arrival.delay,
                            #         'rt_stop_id': stu.stop_id
                            #     })

                        # GTFS-RTの仕様上、一つのTripUpdateエンティティに複数の
                        # stop_time_update（今後の停車駅ごとの予測）が含まれるため、
                        # 全てをリストに追加し、後で generate_schedule で最小シーケンスのものを抽出します。
                        
                return pd.DataFrame(updates)

        except urllib.error.URLError as e:
            print(f"GTFS-RTデータ取得エラー (URLまたはタイムアウト): {e} (試行 {attempt + 1}/{MAX_RETRIES})")
            slp(2) # 2秒待機してリトライ
        except Exception as e:
            print(f"GTFS-RTデータパースエラーまたはその他エラー: {e} (試行 {attempt + 1}/{MAX_RETRIES})")
            slp(2) # 2秒待機してリトライ

    # 全てのリトライが失敗した場合
    print(f"GTFS-RTデータの取得に失敗しました (URL: {GTFS_RT_URL})。空のDataFrameを返します。")
    return pd.DataFrame()


def generate_schedule(stop_name):
    """指定されたバス停（親ID）のリアルタイム運行表を生成する"""
    
    if not GTFS:
        print("Error: GTFS data is not loaded.")
        return []

    now_jst = datetime.now(JST)
    # 現在時刻の秒数 (24時間超えGTFS時刻との比較に注意)
    now_time_sec = now_jst.hour * 3600 + now_jst.minute * 60 + now_jst.second
    
    # 1. サービスのフィルタリング
    active_service_ids = get_current_service_ids(now_jst)
    if not active_service_ids:
        print("No active service IDs for today.")
        return []

    # 2. 静的スケジュールの結合とフィルタリング
    df_trips_today = GTFS['TRIPS'][GTFS['TRIPS']['service_id'].isin(active_service_ids)]

    # 3. 指定バス停の時刻表を取得
    df_schedule = GTFS['STOP_TIMES'].merge(
        df_trips_today[['trip_id', 'trip_headsign']],
        on='trip_id',
        how='inner'
    )
    
    # 4. 指定バス停でフィルタリング（親IDに基づく全乗り場の統合）
    target_parent_id = GTFS['PARENT_ID_MAP'].get(stop_name)
    if not target_parent_id:
        print(f"Error: Parent ID not found for stop name: {stop_name}")
        return [] 

    # 親IDに紐づくすべての stop_id（枝番あり/なし全て）を取得
    all_target_stop_ids = GTFS['STOPS'][
        GTFS['STOPS']['parent_id'] == target_parent_id
    ]['stop_id'].tolist()
    
    df_stop_schedule = df_schedule[
        df_schedule['stop_id'].isin(all_target_stop_ids)
    ].copy()

    # 5. リアルタイムデータの取得と結合
    df_rt_all = get_realtime_updates() # 全てのRT情報を取得

    if df_rt_all.empty:
        # RTデータがない場合は、遅延情報と現在地情報は全てNaNとする
        df_stop_schedule['delay_sec'] = np.nan
        df_stop_schedule['now_stop'] = '-'
        df_stop_schedule['now_stop_id'] = np.nan 
        print("GTFS-RTデータは空でした。")
    else:
        # 5a. RTデータから「次に停車するバス停」（最小の stop_sequence）を抽出
        df_rt_next_stop = df_rt_all.sort_values('stop_sequence').drop_duplicates(
            subset=['trip_id'], keep='first'
        )
        
        df_rt_next_stop = df_rt_next_stop[[
            'trip_id', 
            'stop_sequence', # Next Stop (N) のシーケンス
            'delay_sec'     # N での遅延情報
        ]].rename(columns={
            'stop_sequence': 'next_stop_sequence', 
        }).copy()
        
        # 5b. 次の停車駅の「一つ前の停車駅」（現在地の推定）を静的データから取得
        df_stop_static_for_merge = GTFS['STOP_TIMES'][['trip_id', 'stop_sequence', 'stop_id']].copy()
        
        df_rt_current_info = df_rt_next_stop.merge(
            df_stop_static_for_merge,
            on=['trip_id'],
            how='left'
        )
        
        # 現在地となるはずの停留所のシーケンス番号 (N-1) を設定
        df_rt_current_info['current_stop_sequence'] = df_rt_current_info['next_stop_sequence'] - 1
        
        df_rt_current_stop = df_rt_current_info[
            (df_rt_current_info['stop_sequence'] == df_rt_current_info['current_stop_sequence']) & 
            (df_rt_current_info['stop_sequence'] >= 0) # シーケンス番号が0以上であること (0は始点)
        ].copy()
        
        # 結合用のデータフレームを整形（現在の遅延情報と現在地IDを取得）
        df_rt_final = df_rt_current_stop[[
            'trip_id', 
            'stop_id', # 推定される現在地の stop_id (N-1)
            'delay_sec' # 遅延情報は N のものを使用
        ]].rename(columns={'stop_id': 'now_stop_id'}).copy()
        
        # 遅延情報テーブル（最小のstop_sequenceに紐づく遅延情報を使用）
        df_delay_info_min_seq = df_rt_next_stop[['trip_id', 'delay_sec']].drop_duplicates(subset=['trip_id'], keep='first')
        
        # 現在地IDと遅延情報のマージ
        df_rt_info_to_merge = df_delay_info_min_seq.merge(
            df_rt_final[['trip_id', 'now_stop_id']].drop_duplicates(subset=['trip_id'], keep='first'),
            on='trip_id',
            how='left'
        )
        
        # 5c. スケジュールに RT データを結合
        df_stop_schedule = df_stop_schedule.merge(
            df_rt_info_to_merge[['trip_id', 'delay_sec', 'now_stop_id']],
            on='trip_id',
            how='left'
        )
        
        # 2. 現在地（バス停名）を取得
        stop_name_map = GTFS.get('STOP_NAME_MAP', {})
        
        # now_stop_id を使って現在地のバス停名を取得
        df_stop_schedule['now_stop_base'] = df_stop_schedule['now_stop_id'].map(stop_name_map)
        
        # 3. 現在地表示の調整 (「を通過」を追記)
        def format_now_stop(row):
            base_name = row['now_stop_base']
            now_stop_id = row['now_stop_id']
            
            # RT情報が存在しない便はハイフン
            if pd.isna(row['delay_sec']):
                return '-'

            if pd.notna(base_name):
                # バス停名が取得できた場合 -> 「〇〇を通過」
                return f"{base_name}を通過"
            elif pd.notna(now_stop_id):
                # バス停名はないがIDがある場合（枝番などで名前がマッピングできなかった時など） -> 「〇〇付近」（ID）
                return f"{now_stop_id}付近"
            else:
                # 現在地IDも取得できなかったが、遅延情報は存在する（始点直前、またはデータ欠損）
                return '始点付近'

        df_stop_schedule['now_stop'] = df_stop_schedule.apply(format_now_stop, axis=1)
        
    # 6. 始終点時間の結合（運行中の判定に使用）
    df_stop_schedule = df_stop_schedule.merge(
        GTFS['TRIP_BOUNDARIES'],
        on='trip_id',
        how='left'
    )

    # 7. 到着予測時刻 (judgetime_sec) の計算
    df_stop_schedule['judgetime_sec'] = df_stop_schedule['departure_sec'] + df_stop_schedule['delay_sec'].fillna(0)
    
    # 8. 運行状況 (info) の判定ロジック
    
    # --- 遅延時間（分）の表示を作成 (定刻表示の修正) ---
    def format_delay_time(delay_sec):
        if pd.isna(delay_sec):
            return "運行情報なし"
        
        # 絶対値で1分未満（60秒未満）の遅延/早着は「定刻」と見なす
        if abs(delay_sec) < 60:
            return "定刻"
        
        delay_min = int(delay_sec / 60)
        
        if delay_min > 0:
            return f"{delay_min}分遅れ"
        elif delay_min < 0:
            # 早着の場合、絶対値を取る
            return f"{abs(delay_min)}分早着"
        else:
            return "定刻"
            
    df_stop_schedule['delay_time'] = df_stop_schedule['delay_sec'].apply(format_delay_time)

    def judge_status(row):
        judgetime_sec = row['judgetime_sec']
        delay_time = row['delay_time']
        target_dep_sec = row['departure_sec'] # ターゲット停留所の定刻出発時刻
        arr_sec = row['arr_sec']               # 終点出発時刻

        # 運行情報あり (RTデータがある場合 - delay_sec が NaN ではない)
        if pd.notna(row['delay_sec']):
            # 予測到着時刻が現在時刻よりも後
            if judgetime_sec > now_time_sec:
                remaining_sec = judgetime_sec - now_time_sec
                # 残り時間（分）を切り上げで計算し、最低1分を保証
                remaining_min = int(np.ceil(remaining_sec / 60))
                remaining_min = max(1, remaining_min) 
                
                # *** 運行状況の表示ロジック修正点 (ご要望に基づき現在地情報を削除) ***
                if delay_time == "定刻":
                    # 定刻の場合
                    return f"あと{remaining_min}分で定刻到着見込み。"
                else:
                    # 遅延/早着の場合
                    # 運行状況メッセージはシンプルに残り時間と遅延情報のみとする
                    return f"あと{remaining_min}分で到着見込み。({delay_time})"

            else:
                # 予測時刻 <= 現在時刻 (ターゲット停留所を既に通過・到着済み)
                return "通過・到着済み"
        
        # 運行情報なし (RTデータがない場合 - delay_sec が NaN)
        else:
            # 運行開始前 (定刻出発時刻がこれから)
            if target_dep_sec > now_time_sec:
                return "運行開始前" 
            
            # 運行終了 (終点出発時刻が既に過ぎた)
            if arr_sec < now_time_sec:
                return "運行終了"
            
            # それ以外（ターゲット通過済みだがRT情報がない）
            return "通過済み（情報なし）" 


    df_stop_schedule['info'] = df_stop_schedule.apply(judge_status, axis=1)

    # 9. 最終的な結果の抽出と整形
    
    # 現在時刻の30分前からの情報を表示する 
    cutoff_sec = now_time_sec - 1800 
    
    # 予測時刻 (judgetime_sec) がカットオフ時刻以降の便を表示
    df_final_filtered = df_stop_schedule[
        (df_stop_schedule['judgetime_sec'] >= cutoff_sec) | 
        # カットオフ時刻より古くても、RT情報があって「通過・到着済み」でないものは表示
        (pd.notna(df_stop_schedule['delay_sec']) & (df_stop_schedule['info'] != '通過・到着済み'))
    ].copy() 

    # departure_secでソート
    df_final_sorted = df_final_filtered.sort_values(by='departure_sec')

    # 必要な列をリスト形式で返す
    # [出発時刻, 行先, 現在地, 運行状況] の順
    final_list = df_final_sorted[[
        'departure_time', 
        'trip_headsign', 
        'now_stop', 
        'info'
    ]].values.tolist()
    
    # NaNをNoneに変換 (JSONシリアライズ時にnanが問題を起こさないようにするため)
    final_list = [
        [None if pd.isna(item) else item for item in row] 
        for row in final_list
    ]
    
    return final_list


# --- Flask アプリケーション ---
app = Flask(__name__)
app.config ['TEMPLATES_AUTO_RELOAD'] = True

# GTFSデータはメモリ内で定義済みのため、アプリケーション起動時にロードする
# `@app.before_first_request` は Flask 2.3 で廃止されたため、直接ロードに変更
load_gtfs_data() # アプリケーション起動時に一度だけGTFSデータをロード

# 以下、デコレータを削除またはコメントアウト
# @app.before_first_request
# def setup_data():
#     """リクエストが来る前に一度だけGTFSデータをロードする"""
#     load_gtfs_data()

@app.route('/', methods=['GET'])
def index():
    # 路線名リストと路線-バス停マッピングをテンプレートに渡す
    route_names = GTFS.get('ROUTE_NAMES', [])
    route_stop_map = GTFS.get('ROUTE_STOP_MAP', {})
    
    # route_stop_map (辞書) をそのままテンプレートに渡す (Jinjaの|tojsonフィルタを使用)
    # 以前の手動の json.dumps() は削除します
    
    return render_template('index.html', 
        route_names=route_names, 
        route_stop_map=route_stop_map # 変数名を修正し辞書を直接渡す
    )

@app.route('/<stop_name>', methods=['GET'])
def info(stop_name):
    # URLエンコーディングされたバス停名をデコード
    stop = urllib.parse.unquote(stop_name)

    # 現在の運行表を生成
    mydata = generate_schedule(stop)
    
    # タイムゾーン情報を付加
    now_jst = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S JST')

    message = f"現在時刻 ({now_jst}) の３０分前からの情報を表示しています。表示がない場合は、本日の運行は終了しているか、データが取得できませんでした。"
    
    # info.html テンプレートをレンダリング（自動更新のJavaScriptが含まれている）
    return render_template('info.html',
        title=f"{stop} バスの出発時刻および運行状況",
        message=message,
        data=mydata,
        stop=stop
    )

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APP_PORT)