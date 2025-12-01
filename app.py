# Version: 1.0.14 (requestsライブラリへの完全移行、安定版)
# Last Major Update: GTFS-RTの取得をrequestsに完全移行し、接続の安定性を大幅に向上。
#                   favicon.icoルートの処理、インデントエラーの修正を統合。

from flask import Flask, render_template, request, abort # abortを追加
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import io
from time import sleep as slp 
import json 
import urllib.parse 
import os 
import requests # ★urllib.request, urllib.error の代わりに使用

# --- 設定 ---
GTFS_RT_URL = 'https://akita.bustei.net/TripUpdate.pb'
JST = timezone(timedelta(hours=+9))
APP_PORT = 5000
TIMEOUT_SECONDS = 15 # GTFS-RTのタイムアウト時間 (接続+読み込み)

# --- グローバル変数（GTFS静的データ） ---
GTFS = {}
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
        df_st = pd.read_csv('gtfs_data/stop_times.txt', dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int})
        df_st['departure_sec'] = df_st['departure_time'].apply(time_to_seconds)
        df_st['arrival_sec'] = df_st['arrival_time'].apply(time_to_seconds)

        df_t = pd.read_csv('gtfs_data/trips.txt', dtype={'route_id': str, 'service_id': str, 'trip_id': str})
        df_s = pd.read_csv('gtfs_data/stops.txt', dtype={'stop_id': str})
        df_r = pd.read_csv('gtfs_data/routes.txt', dtype={'route_id': str})
        df_r['parent_route_id'] = df_r['route_id'].apply(extract_parent_route_id)
        df_c = pd.read_csv('gtfs_data/calendar.txt', dtype={'service_id': str})
        df_cd = pd.read_csv('gtfs_data/calendar_dates.txt', dtype={'service_id': str, 'date': str, 'exception_type': int})

        # --- マッピング作成 ---
        df_s['parent_id'] = df_s['stop_id'].apply(extract_parent_id)
        parent_id_map = df_s.drop_duplicates(subset=['parent_id']).set_index('stop_name')['parent_id'].to_dict()

        df_route_stops = df_st[['trip_id', 'stop_id']].drop_duplicates()
        df_route_stops = df_route_stops.merge(df_t[['trip_id', 'route_id']], on='trip_id', how='left')
        df_route_stops = df_route_stops.merge(df_r[['route_id', 'parent_route_id']], on='route_id', how='left')
        df_route_stops = df_route_stops.merge(df_s[['stop_id', 'stop_name']], on='stop_id', how='left')
        df_final_mapping = df_route_stops[['parent_route_id', 'stop_name']].dropna().drop_duplicates()

        route_stop_map = {}
        for parent_route_id, group in df_final_mapping.groupby('parent_route_id'):
            sorted_stops = sorted(group['stop_name'].unique().tolist())
            route_stop_map[parent_route_id] = sorted_stops

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

    except FileNotFoundError as e:
        print(f"エラー: GTFSデータファイルが見つかりません。フォルダ構成を確認してください: {e}")
        return {}


def get_current_service_ids(now_jst):
    """現在の日付と曜日から有効なservice_idのリストを取得する"""
    if 'CALENDAR' not in GTFS: return []
    today_date = now_jst.strftime('%Y%m%d')
    day_name = now_jst.strftime('%A').lower()  
    
    weekly_services = []
    if day_name in GTFS['CALENDAR'].columns:
        weekly_services = GTFS['CALENDAR'][GTFS['CALENDAR'][day_name] == 1]['service_id'].tolist()

    exceptions = GTFS['CALENDAR_DATES'][GTFS['CALENDAR_DATES']['date'] == today_date]
    added_services = exceptions[exceptions['exception_type'] == 1]['service_id'].tolist()
    removed_services = exceptions[exceptions['exception_type'] == 2]['service_id'].tolist()

    active_service_ids = set(weekly_services).union(set(added_services)) - set(removed_services)
    
    return list(active_service_ids)


def get_realtime_updates():
    """GTFS-RTデータをrequestsで取得し、DataFrameとして返す"""
    MAX_RETRIES = 3
    
    for attempt in range(MAX_RETRIES):
        try:
            # ★requestsでアクセス（urllibからの変更点）
            res = requests.get(
                GTFS_RT_URL, 
                headers={'User-Agent': 'Mozilla/5.0'}, 
                timeout=TIMEOUT_SECONDS # 設定したタイムアウトを適用
            )
            res.raise_for_status() # HTTPエラー (4xx, 5xx) を例外として処理

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(res.content) # .content でバイナリデータを取得

            updates = []
            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    tu = entity.trip_update
                    trip_id = tu.trip.trip_id
                    
                    for stu in tu.stop_time_update:
                        delay = 0
                        if stu.HasField('departure') and stu.departure.HasField('delay'):
                            delay = stu.departure.delay
                        elif stu.HasField('arrival') and stu.arrival.HasField('delay'):
                            delay = stu.arrival.delay
                        
                        if delay != 0 or stu.HasField('stop_id'):
                            updates.append({
                                'trip_id': trip_id,
                                'stop_sequence': stu.stop_sequence,
                                'delay_sec': delay,
                                'rt_stop_id': stu.stop_id
                            })
            
            # インデント修正済み
            if updates:
                return pd.DataFrame(updates)
            else:
                return pd.DataFrame(columns=['trip_id', 'stop_sequence', 'delay_sec', 'rt_stop_id'])

        except requests.exceptions.Timeout:
            print(f"GTFS-RT取得エラー (Attempt {attempt+1}): [requests ERROR] 理由: 接続または読み込みタイムアウト ({TIMEOUT_SECONDS}秒)")
            slp(1)
        except requests.exceptions.RequestException as e:
            # 接続、HTTPエラー (4xx, 5xx) など
            print(f"GTFS-RT取得エラー (Attempt {attempt+1}): [requests ERROR] 詳細: {e}")
            slp(1)
        except Exception as e:
            print(f"GTFS-RT取得エラー (Attempt {attempt+1}): [OTHER ERROR] 詳細: {e}")
            slp(1)
            
    print("GTFS-RTデータの取得に失敗しました。空のDataFrameを返します。")
    return pd.DataFrame()


def generate_schedule(stop_name):
    """指定されたバス停（親ID）のリアルタイム運行表を生成する"""
    
    if not GTFS: return []

    now_jst = datetime.now(JST)
    now_time_sec = now_jst.hour * 3600 + now_jst.minute * 60 + now_jst.second
    
    active_service_ids = get_current_service_ids(now_jst)
    if not active_service_ids: return []

    df_trips_today = GTFS['TRIPS'][GTFS['TRIPS']['service_id'].isin(active_service_ids)]

    df_schedule = GTFS['STOP_TIMES'].merge(
        df_trips_today[['trip_id', 'trip_headsign']],
        on='trip_id',
        how='inner'
    )
    
    target_parent_id = GTFS['PARENT_ID_MAP'].get(stop_name)
    if not target_parent_id: return [] 

    all_target_stop_ids = GTFS['STOPS'][
        GTFS['STOPS']['parent_id'] == target_parent_id
    ]['stop_id'].tolist()
    
    df_stop_schedule = df_schedule[
        df_schedule['stop_id'].isin(all_target_stop_ids)
    ].copy()

    df_rt_all = get_realtime_updates() 

    if df_rt_all.empty:
        df_stop_schedule['delay_sec'] = np.nan
        df_stop_schedule['now_stop'] = '-'
        df_stop_schedule['now_stop_id'] = np.nan 
    else:
        df_rt_next_stop = df_rt_all.sort_values('stop_sequence').drop_duplicates(
            subset=['trip_id'], keep='first'
        )
        
        df_rt_next_stop = df_rt_next_stop[[
            'trip_id', 
            'stop_sequence', 
            'delay_sec'
        ]].rename(columns={'stop_sequence': 'next_stop_sequence'}).copy()
        
        df_stop_static_for_merge = GTFS['STOP_TIMES'][['trip_id', 'stop_sequence', 'stop_id']].copy()
        
        df_rt_current_info = df_rt_next_stop.merge(
            df_stop_static_for_merge,
            on=['trip_id'],
            how='left'
        )
        
        df_rt_current_info['current_stop_sequence'] = df_rt_current_info['next_stop_sequence'] - 1
        
        df_rt_current_stop = df_rt_current_info[
            (df_rt_current_info['stop_sequence'] == df_rt_current_info['current_stop_sequence']) & 
            (df_rt_current_info['stop_sequence'] >= 0) 
        ].copy()
        
        df_rt_final = df_rt_current_stop[[
            'trip_id', 
            'stop_id', 
            'delay_sec' 
        ]].rename(columns={'stop_id': 'now_stop_id'}).copy()
        
        df_delay_info_min_seq = df_rt_next_stop[['trip_id', 'delay_sec']].drop_duplicates(subset=['trip_id'], keep='first')
        
        df_rt_info_to_merge = df_delay_info_min_seq.merge(
            df_rt_final[['trip_id', 'now_stop_id']].drop_duplicates(subset=['trip_id'], keep='first'),
            on='trip_id',
            how='left'
        )
        
        df_stop_schedule = df_stop_schedule.merge(
            df_rt_info_to_merge[['trip_id', 'delay_sec', 'now_stop_id']],
            on='trip_id',
            how='left'
        )
        
        stop_name_map = GTFS.get('STOP_NAME_MAP', {})
        df_stop_schedule['now_stop_base'] = df_stop_schedule['now_stop_id'].map(stop_name_map)
        
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
        
    df_stop_schedule = df_stop_schedule.merge(
        GTFS['TRIP_BOUNDARIES'],
        on='trip_id',
        how='left'
    )

    df_stop_schedule['judgetime_sec'] = df_stop_schedule['departure_sec'] + df_stop_schedule['delay_sec'].fillna(0)
    
    def format_delay_time(delay_sec):
        if pd.isna(delay_sec): return "運行情報なし"
        if abs(delay_sec) < 60: return "定刻"
        
        delay_min = int(delay_sec / 60)
        
        if delay_min > 0: return f"{delay_min}分遅れ"
        elif delay_min < 0: return f"{abs(delay_min)}分早着"
        else: return "定刻"
            
    df_stop_schedule['delay_time'] = df_stop_schedule['delay_sec'].apply(format_delay_time)

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

    cutoff_sec = now_time_sec - 1800 
    
    df_final_filtered = df_stop_schedule[
        (df_stop_schedule['judgetime_sec'] >= cutoff_sec) | 
        (pd.notna(df_stop_schedule['delay_sec']) & (df_stop_schedule['info'] != '通過・到着済み'))
    ].copy() 

    df_final_sorted = df_final_filtered.sort_values(by='departure_sec')

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
load_gtfs_data() 

# ★修正箇所: favicon.ico のリクエストを処理してエラーを防ぐ
@app.route('/favicon.ico')
def favicon():
    # 404を返して処理を終了させることで、メインのロジックに流れるのを防ぐ
    return abort(404)

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

@app.route('/rt_status', methods=['GET'])
def rt_status():
    df_rt = get_realtime_updates()
    
    if df_rt.empty:
        rt_data_html = "<p class='text-red-600 font-semibold text-xl'>リアルタイムデータは取得できませんでした。GTFS_RT_URLが正しいか、データが利用可能か確認してください。</p>"
        rt_info = "データなし"
    else:
        rt_data_html = df_rt.to_html(
            classes='table-auto w-full text-sm text-left text-gray-700', 
            index=False,
            justify='left'
        )
        rt_info = f"取得したデータの件数: {len(df_rt)}件"

    now_jst = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S JST')
    
    return render_template('rt_status.html',
        title="GTFSリアルタイムデータ診断",
        data_table=rt_data_html,
        timestamp=now_jst,
        rt_info=rt_info
    )

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APP_PORT)