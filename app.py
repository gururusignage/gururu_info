# Version: 1.1 (GTFS-RTパース実装 & 当日スケジュールキャッシュによる最適化)
# Last Major Update: GTFS-RTのパースロジックを実装。当日運行スケジュールをキャッシュし、リクエストごとの負荷を低減。

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
import os 
# requests は標準ライブラリではないため、urllibを維持します。

# --- 設定 ---
# GTFSリアルタイムデータのURLを設定
GTFS_RT_URL = 'https://akita.bustei.net/TripUpdate.pb'
# 日本標準時 (JST) を定義
JST = timezone(timedelta(hours=+9))
APP_PORT = 5000

# --- グローバル変数（GTFS静的データ） ---
GTFS = {}
# 当日の運行スケジュール (リクエストごとに再計算しないためのキャッシュ)
TODAY_SCHEDULE = None 
# GTFSファイル名リスト
GTFS_FILES = ['stop_times.txt', 'trips.txt', 'stops.txt', 'routes.txt', 'calendar.txt', 'calendar_dates.txt']


def extract_parent_id(stop_id):
    """'mytown0003_01' -> 'mytown0003' のように、枝番を除いた親IDを抽出する"""
    if pd.isna(stop_id):
        return None
    return stop_id.rsplit('_', 1)[0]

def extract_parent_route_id(route_id):
    """'A_1' -> 'A' のように、'_' の前の親ルートIDを抽出する"""
    if pd.isna(route_id):
        return None
    return route_id.split('_', 1)[0]

def time_to_seconds(time_str):
    """'HH:MM:SS'形式の時刻文字列を秒単位に変換する (24時間超え対応)"""
    try:
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except:
        return np.nan

def load_gtfs_data():
    """GTFS静的データをPandas DataFrameとしてメモリにロードし、必要なマッピングを作成する"""
    print("GTFS静的データをロード中...")
    global GTFS
    try:
        # stop_times.txt: 時刻データ（重要）と出発時刻の秒変換
        df_st = pd.read_csv('gtfs_data/stop_times.txt', dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int})
        df_st['departure_sec'] = df_st['departure_time'].apply(time_to_seconds)
        df_st['arrival_sec'] = df_st['arrival_time'].apply(time_to_seconds)

        # trips.txt: トリップ情報
        df_t = pd.read_csv('gtfs_data/trips.txt', dtype={'route_id': str, 'service_id': str, 'trip_id': str})
        
        # stops.txt: バス停名
        df_s = pd.read_csv('gtfs_data/stops.txt', dtype={'stop_id': str})
        
        # routes.txt (路線情報)
        df_r = pd.read_csv('gtfs_data/routes.txt', dtype={'route_id': str})
        df_r['parent_route_id'] = df_r['route_id'].apply(extract_parent_route_id)
        
        # calendar.txt: 運行曜日
        df_c = pd.read_csv('gtfs_data/calendar.txt', dtype={'service_id': str})
        
        # calendar_dates.txt: 例外日
        df_cd = pd.read_csv('gtfs_data/calendar_dates.txt', dtype={'service_id': str, 'date': str, 'exception_type': int})

        # --- 親IDとバス停名のマッピングを作成 ---
        df_s['parent_id'] = df_s['stop_id'].apply(extract_parent_id)
        parent_id_map = df_s.drop_duplicates(subset=['parent_id']).set_index('stop_name')['parent_id'].to_dict()

        # --- 路線グループとバス停名のマッピングを作成 ---
        df_route_stops = df_st[['trip_id', 'stop_id']].drop_duplicates()
        df_route_stops = df_route_stops.merge(df_t[['trip_id', 'route_id']], on='trip_id', how='left')
        df_route_stops = df_route_stops.merge(df_r[['route_id', 'parent_route_id']], on='route_id', how='left')
        df_route_stops = df_route_stops.merge(df_s[['stop_id', 'stop_name']], on='stop_id', how='left')
        df_final_mapping = df_route_stops[['parent_route_id', 'stop_name']].dropna().drop_duplicates()

        route_stop_map = {}
        for parent_route_id, group in df_final_mapping.groupby('parent_route_id'):
            sorted_stops = sorted(group['stop_name'].unique().tolist())
            route_stop_map[parent_route_id] = sorted_stops

        # 運行サービスの結合と始終点時刻の計算
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
        GTFS['STOP_NAME_MAP'] = df_s.set_index('stop_id')['stop_name'].to_dict()
        
        print("GTFSデータのロードが完了しました。")
        # データのロード完了後、当日のスケジュールをキャッシュ
        cache_todays_schedule() 

    except FileNotFoundError as e:
        print(f"エラー: GTFSデータファイルが見つかりません。フォルダ構成を確認してください: {e}")
        return {}


def get_current_service_ids(now_jst):
    """現在の日付と曜日から有効なservice_idのリストを取得する"""
    # ... (変更なし)
    today_date = now_jst.strftime('%Y%m%d')
    day_name = now_jst.strftime('%A').lower()  
    
    weekly_services = []
    if day_name in GTFS['CALENDAR'].columns:
        weekly_services = GTFS['CALENDAR'][GTFS['CALENDAR'][day_name] == 1]['service_id'].tolist()
    else:
        print(f"Warning: Day name {day_name} not found in calendar columns.")

    exceptions = GTFS['CALENDAR_DATES'][GTFS['CALENDAR_DATES']['date'] == today_date]
    added_services = exceptions[exceptions['exception_type'] == 1]['service_id'].tolist()
    removed_services = exceptions[exceptions['exception_type'] == 2]['service_id'].tolist()

    active_service_ids = set(weekly_services).union(set(added_services)) - set(removed_services)
    
    return list(active_service_ids)


def cache_todays_schedule():
    """当日の運行サービスに基づき、全バス停の静的スケジュールを事前に結合し、グローバル変数にキャッシュする"""
    global TODAY_SCHEDULE
    if not GTFS:
        print("Error: GTFS data is not loaded for caching.")
        return

    now_jst = datetime.now(JST)
    active_service_ids = get_current_service_ids(now_jst)

    if not active_service_ids:
        print("No active service IDs for today. Caching empty schedule.")
        TODAY_SCHEDULE = pd.DataFrame()
        return

    # 1. 今日のトリップを取得
    df_trips_today = GTFS['TRIPS'][GTFS['TRIPS']['service_id'].isin(active_service_ids)]

    # 2. 静的スケジュールの結合と始終点時間の結合
    df_schedule = GTFS['STOP_TIMES'].merge(
        df_trips_today[['trip_id', 'trip_headsign']],
        on='trip_id',
        how='inner'
    ).merge(
        GTFS['TRIP_BOUNDARIES'],
        on='trip_id',
        how='left'
    )
    
    # 3. 親バス停IDを追加
    df_schedule['parent_id'] = df_schedule['stop_id'].apply(extract_parent_id)
    
    TODAY_SCHEDULE = df_schedule
    print(f"本日 ({now_jst.strftime('%Y%m%d')}) の静的スケジュールをキャッシュしました。")


def get_realtime_updates():
    """GTFS-RTデータを取得し、DataFrameとして返す (パースロジック実装済み)"""
    MAX_RETRIES = 3
    
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(GTFS_RT_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as res:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(res.read())

                updates = []
                for entity in feed.entity:
                    if entity.HasField('trip_update'):
                        tu = entity.trip_update
                        trip_id = tu.trip.trip_id
                        
                        # stop_time_update のリストから直近の情報を取得
                        for stu in tu.stop_time_update:
                            delay = 0
                            # 遅延情報を取得（到着または出発のいずれか早い方があれば）
                            if stu.HasField('departure') and stu.departure.HasField('delay'):
                                delay = stu.departure.delay
                            elif stu.HasField('arrival') and stu.arrival.HasField('delay'):
                                delay = stu.arrival.delay
                            
                            # 少なくとも遅延情報か、次の停車駅の情報がある場合のみ記録
                            if delay != 0 or stu.HasField('stop_id'):
                                updates.append({
                                    'trip_id': trip_id,
                                    'stop_sequence': stu.stop_sequence,
                                    'delay_sec': delay,
                                    'rt_stop_id': stu.stop_id
                                })
                
                # データフレーム化
                if updates:
                    return pd.DataFrame(updates)
                else:
                    return pd.DataFrame(columns=['trip_id', 'stop_sequence', 'delay_sec', 'rt_stop_id'])

        except Exception as e:
            print(f"GTFS-RT取得エラー (Attempt {attempt+1}): {e}")
            slp(1) # リトライ前に待機

    print("GTFS-RTデータの取得に失敗しました。空のDataFrameを返します。")
    return pd.DataFrame()


def generate_schedule(stop_name):
    """指定されたバス停（親ID）のリアルタイム運行表を生成する"""
    
    global TODAY_SCHEDULE # キャッシュされたスケジュールを使用
    if TODAY_SCHEDULE is None or TODAY_SCHEDULE.empty:
        # スケジュールがキャッシュされていない、または空の場合は再生成を試みる
        cache_todays_schedule()
        if TODAY_SCHEDULE is None or TODAY_SCHEDULE.empty:
            print("Error: Today's schedule is empty.")
            return []

    now_jst = datetime.now(JST)
    now_time_sec = now_jst.hour * 3600 + now_jst.minute * 60 + now_jst.second
    
    # 1. 指定バス停でフィルタリング（キャッシュされたスケジュールを使用）
    target_parent_id = GTFS['PARENT_ID_MAP'].get(stop_name)
    if not target_parent_id:
        print(f"Error: Parent ID not found for stop name: {stop_name}")
        return [] 
    
    # 親IDに紐づくすべての便を取得
    df_stop_schedule = TODAY_SCHEDULE[
        TODAY_SCHEDULE['parent_id'] == target_parent_id
    ].copy()

    # 2. リアルタイムデータの取得と結合 (ここから以前のロジックを維持しつつ、DataFrameをdf_stop_scheduleに限定)
    df_rt_all = get_realtime_updates() 

    if df_rt_all.empty:
        df_stop_schedule['delay_sec'] = np.nan
        df_stop_schedule['now_stop'] = '-'
    else:
        # RTデータが存在するトリップIDに限定して処理
        rt_trip_ids = df_rt_all['trip_id'].unique()
        df_stop_schedule_with_rt = df_stop_schedule[df_stop_schedule['trip_id'].isin(rt_trip_ids)].copy()
        
        # 2a. RTデータから「次に停車するバス停」（最小の stop_sequence）と遅延情報を抽出
        # 運行状況は最小の stop_sequence の遅延情報を使用するのが一般的
        df_rt_next_stop = df_rt_all.sort_values('stop_sequence').drop_duplicates(
            subset=['trip_id'], keep='first'
        )
        
        df_rt_next_stop = df_rt_next_stop[[
            'trip_id', 
            'stop_sequence', 
            'delay_sec'
        ]].rename(columns={
            'stop_sequence': 'next_stop_sequence', 
        }).copy()
        
        # 2b. 次の停車駅の一つ前の停車駅（現在地の推定）を静的データから取得
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
            (df_rt_current_info['stop_sequence'] >= 0) 
        ].copy()
        
        # 現在地IDと遅延情報の取得
        df_rt_final = df_rt_current_stop[[
            'trip_id', 
            'stop_id', 
            'delay_sec' 
        ]].rename(columns={'stop_id': 'now_stop_id'}).drop_duplicates(subset=['trip_id'], keep='first').copy()
        
        # 遅延情報テーブル（最小のstop_sequenceに紐づく遅延情報を使用）
        df_delay_info_min_seq = df_rt_next_stop[['trip_id', 'delay_sec']].drop_duplicates(subset=['trip_id'], keep='first')
        
        # 現在地IDと遅延情報のマージ
        df_rt_info_to_merge = df_delay_info_min_seq.merge(
            df_rt_final[['trip_id', 'now_stop_id']].drop_duplicates(subset=['trip_id'], keep='first'),
            on='trip_id',
            how='left'
        )
        
        # 2c. スケジュールに RT データを結合
        df_stop_schedule = df_stop_schedule.merge(
            df_rt_info_to_merge[['trip_id', 'delay_sec', 'now_stop_id']],
            on='trip_id',
            how='left'
        )
        
        # 現在地（バス停名）を取得
        stop_name_map = GTFS.get('STOP_NAME_MAP', {})
        df_stop_schedule['now_stop_base'] = df_stop_schedule['now_stop_id'].map(stop_name_map)
        
        # 現在地表示の調整
        def format_now_stop(row):
            base_name = row['now_stop_base']
            now_stop_id = row['now_stop_id']
            
            if pd.isna(row['delay_sec']):
                return '-'

            if pd.notna(base_name):
                return f"{base_name}を通過"
            elif pd.notna(now_stop_id):
                return f"{now_stop_id}付近"
            else:
                return '始点付近'

        df_stop_schedule['now_stop'] = df_stop_schedule.apply(format_now_stop, axis=1)
        
    # 3. 到着予測時刻 (judgetime_sec) の計算
    df_stop_schedule['judgetime_sec'] = df_stop_schedule['departure_sec'] + df_stop_schedule['delay_sec'].fillna(0)
    
    # 4. 遅延時間（分）の表示を作成
    def format_delay_time(delay_sec):
        if pd.isna(delay_sec):
            return "運行情報なし"
        
        if abs(delay_sec) < 60:
            return "定刻"
        
        delay_min = int(delay_sec / 60)
        
        if delay_min > 0:
            return f"{delay_min}分遅れ"
        elif delay_min < 0:
            return f"{abs(delay_min)}分早着"
        else:
            return "定刻"
            
    df_stop_schedule['delay_time'] = df_stop_schedule['delay_sec'].apply(format_delay_time)

    # 5. 運行状況 (info) の判定ロジック
    def judge_status(row):
        judgetime_sec = row['judgetime_sec']
        delay_time = row['delay_time']
        target_dep_sec = row['departure_sec'] 
        arr_sec = row['arr_sec']               

        if pd.notna(row['delay_sec']):
            if judgetime_sec > now_time_sec:
                remaining_sec = judgetime_sec - now_time_sec
                remaining_min = int(np.ceil(remaining_sec / 60))
                remaining_min = max(1, remaining_min) 
                
                if delay_time == "定刻":
                    return f"あと{remaining_min}分で定刻到着見込み。"
                else:
                    return f"あと{remaining_min}分で到着見込み。({delay_time})"
            else:
                return "通過・到着済み"
        
        else:
            if target_dep_sec > now_time_sec:
                return "運行開始前" 
            
            if arr_sec < now_time_sec:
                return "運行終了"
            
            return "通過済み（情報なし）" 


    df_stop_schedule['info'] = df_stop_schedule.apply(judge_status, axis=1)

    # 6. 最終的な結果の抽出と整形
    cutoff_sec = now_time_sec - 1800 
    
    df_final_filtered = df_stop_schedule[
        (df_stop_schedule['judgetime_sec'] >= cutoff_sec) | 
        (pd.notna(df_stop_schedule['delay_sec']) & (df_stop_schedule['info'] != '通過・到着済み'))
    ].copy() 

    df_final_sorted = df_final_filtered.sort_values(by='departure_sec')

    # [出発時刻, 行先, 現在地, 運行状況] の順
    final_list = df_final_sorted[[
        'departure_time', 
        'trip_headsign', 
        'now_stop', 
        'info'
    ]].values.tolist()
    
    final_list = [
        [None if pd.isna(item) else item for item in row] 
        for row in final_list
    ]
    
    return final_list


# --- Flask アプリケーション ---
app = Flask(__name__)
app.config ['TEMPLATES_AUTO_RELOAD'] = True

# GTFSデータはメモリ内で定義済みのため、アプリケーション起動時にロードする
# Flask 2.3で@app.before_first_requestが廃止されたため、直接ロード
load_gtfs_data() 

# ... (index, info ルートは変更なし)

@app.route('/', methods=['GET'])
def index():
    route_names = GTFS.get('ROUTE_NAMES', [])
    route_stop_map = GTFS.get('ROUTE_STOP_MAP', {})
    
    return render_template('index.html', 
        route_names=route_names, 
        route_stop_map=route_stop_map 
    )

@app.route('/<stop_name>', methods=['GET'])
def info(stop_name):
    stop = urllib.parse.unquote(stop_name)

    mydata = generate_schedule(stop)
    
    now_jst = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S JST')

    message = f"現在時刻 ({now_jst}) の３０分前からの情報を表示しています。表示がない場合は、本日の運行は終了しているか、データが取得できませんでした。"
    
    return render_template('info.html',
        title=f"{stop} バスの出発時刻および運行状況",
        message=message,
        data=mydata,
        stop=stop
    )

if __name__ == '__main__':
    # 実行前に本日のスケジュールを再チェックする（念のため）
    if TODAY_SCHEDULE is None:
        cache_todays_schedule()
        
    app.run(debug=True, host='0.0.0.0', port=APP_PORT)