# Version: 1.0.13 (GTFS Static Data Load Robustness/Error Reporting)
# Last Major Update: GTFS静的データの読み込みにencoding='utf-8'を追加し、
#                    読み込みエラーが発生した場合、その詳細をindex.htmlに表示するように修正。
from flask import Flask, render_template, request, Response, url_for
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.error
import pandas as pd
import numpy as np
import io
from time import sleep as slp 
import json 
import urllib.parse 
import csv

# --- 設定 ---
# GTFSリアルタイムデータのURLを設定
GTFS_RT_URL = 'https://akita.bustei.net/TripUpdate.pb'
# 日本標準時 (JST) を定義
JST = timezone(timedelta(hours=+9))
APP_PORT = 5000

# --- グローバル変数（GTFS静的データ） ---
# アプリケーション起動時に一度だけロードされる辞書
GTFS = {}

# --- ヘルパー関数 ---

def extract_parent_id(stop_id):
    """'mytown0003_01' -> 'mytown0003' のように、枝番を除いた親IDを抽出する"""
    if pd.isna(stop_id):
        return None
    # 最後の '_' で分割し、親ID部分を返す
    return stop_id.rsplit('_', 1)[0]

def format_gtfs_sec(total_seconds):
    """24時間を超える秒数 (GTFS形式) を HH:MM:SS 形式に整形する"""
    if pd.isna(total_seconds):
        return "N/A"
    total_seconds = int(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def format_delay_time(delay_sec):
    """遅延秒数を「定刻」「+X分」「-X分」形式に整形する"""
    if pd.isna(delay_sec):
        return "情報なし"
    
    delay_min = round(delay_sec / 60)
    
    if abs(delay_min) < 1:
        return "定刻"
    elif delay_min > 0:
        return f"+{delay_min}分遅延"
    else:
        return f"{-delay_min}分早着"

def unix_to_gtfs_sec(rt_unix_time, static_dep_sec):
    """UnixタイムスタンプをGTFSの24時間超え対応の秒数に変換する"""
    # rt_unix_timeが0（未設定）の場合はNaNを返す
    if rt_unix_time == 0:
        return np.nan
    
    # Unix時刻をJSTのdatetimeオブジェクトに変換
    rt_dt_jst = datetime.fromtimestamp(rt_unix_time, JST)
    
    # 変換後の時刻を秒に変換 (24時間未満)
    rt_sec_24 = rt_dt_jst.hour * 3600 + rt_dt_jst.minute * 60 + rt_dt_jst.second
    
    DAY_SECONDS = 86400
    
    # 静的定刻時間 (departure_sec) が24時間を超えている場合、rt_sec_24も24時間を加算して調整
    if static_dep_sec >= DAY_SECONDS and rt_sec_24 < 4 * 3600: # 始発前4時までの時刻は翌日扱い
        return rt_sec_24 + DAY_SECONDS
    else:
        return rt_sec_24


# --- データロード ---

def load_gtfs_data():
    """アプリケーション起動時にGTFS静的データをロードし、GTFSグローバル辞書に格納する"""
    
    # エラーメッセージをリセット
    if 'ERROR' in GTFS:
        del GTFS['ERROR']
        
    try:
        # stop_times.txt: 時刻データ（重要）と出発時刻の秒変換
        # CSV読み込み時に encoding='utf-8' を明示的に指定
        df_st = pd.read_csv('gtfs_data/stop_times.txt', dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int}, encoding='utf-8')
        df_st['departure_sec'] = df_st['departure_time'].apply(lambda x: int(x.split(':')[0]) * 3600 + int(x.split(':')[1]) * 60 + int(x.split(':')[2]))
        
        # trips.txt: 旅程IDと路線ID・行先
        df_tr = pd.read_csv('gtfs_data/trips.txt', dtype={'route_id': str, 'trip_id': str}, encoding='utf-8')
        df_tr = df_tr[['route_id', 'trip_id', 'direction_id', 'trip_headsign']]
        
        # routes.txt: 路線IDと路線名
        df_ro = pd.read_csv('gtfs_data/routes.txt', dtype={'route_id': str}, encoding='utf-8')
        df_ro = df_ro[['route_id', 'route_short_name', 'route_long_name']]
        
        # stops.txt: 停留所IDと停留所名
        df_s = pd.read_csv('gtfs_data/stops.txt', dtype={'stop_id': str}, encoding='utf-8')
        df_s = df_s[['stop_id', 'stop_name', 'parent_station']]
        
        # stop_times, trips, routesを結合
        df_base = df_st.merge(df_tr, on='trip_id', how='left').merge(df_ro, on='route_id', how='left')

        # stops.txtを加工し、親IDを追加
        df_s['parent_id'] = df_s['stop_id'].apply(extract_parent_id)

        # 全停留所の親IDリストと停留所名リスト
        parent_id_list = df_s['parent_id'].dropna().unique().tolist()
        stop_name_list = df_s['stop_name'].dropna().unique().tolist()

        # 路線-バス停マッピングを作成
        route_stop_map = {}
        df_route_stops = df_base.merge(df_s[['stop_id', 'stop_name']], on='stop_id', how='left').dropna(subset=['route_short_name', 'stop_name'])
        for route_name in df_route_stops['route_short_name'].unique():
            stops = df_route_stops[df_route_stops['route_short_name'] == route_name]['stop_name'].unique().tolist()
            route_stop_map[route_name] = sorted(stops)
        
        # グローバル辞書にデータを格納
        GTFS['DF_BASE'] = df_base
        GTFS['DF_STOPS'] = df_s
        GTFS['PARENT_ID_LIST'] = parent_id_list
        GTFS['STOP_NAME_LIST'] = stop_name_list
        GTFS['ROUTE_NAMES'] = sorted(df_ro['route_short_name'].unique().tolist())
        GTFS['ROUTE_STOP_MAP'] = route_stop_map
        
        print("GTFS Static Data Loaded Successfully.")
        
    except Exception as e:
        # エラーが発生した場合、グローバル辞書にエラーメッセージを格納
        print(f"Error loading GTFS static data: {e}")
        GTFS['ERROR'] = f"GTFSファイルの読み込みに失敗しました。詳細: {e}"
        # データロード失敗時もアプリケーションを起動させるが、データは空のまま

# --- リアルタイムデータ処理 (変更なし) ---
def get_realtime_updates(gtfs_rt_url):
    """GTFSリアルタイムデータを取得・解析し、リスト形式で返す"""
    updates = []
    try:
        # タイムアウトを短縮し、データが取得できなかった場合も続行
        response = urllib.request.urlopen(gtfs_rt_url, timeout=5)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.read())

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip_id = entity.trip_update.trip.trip_id
                
                for stu in entity.trip_update.stop_time_update:
                    delay_sec = getattr(stu.departure, 'delay', getattr(stu.arrival, 'delay', 0))
                    rt_time = getattr(stu.departure, 'time', getattr(stu.arrival, 'time', 0)) 
                    
                    updates.append({
                        'trip_id': trip_id,
                        'stop_sequence': stu.stop_sequence,
                        'delay_sec': delay_sec,
                        'rt_time': rt_time, 
                        'rt_stop_id': stu.stop_id 
                    })
        return updates

    except urllib.error.URLError as e:
        print(f"Error fetching GTFS-RT data: {e.reason}")
        return updates
    except Exception as e:
        print(f"Error parsing GTFS-RT data: {e}")
        return updates


# --- スケジュール生成ロジック (変更なし) ---
def generate_schedule(target_stop_name):
    # ... (省略: 処理ロジックに変更なし) ...
    # 処理ロジックは以前に提供した最新版を使用

    df_base = GTFS.get('DF_BASE')
    df_stops = GTFS.get('DF_STOPS')
    
    if df_base is None or df_stops is None:
        return []

    # 1. ターゲット停留所IDの特定
    target_stops_df = df_stops[df_stops['stop_name'] == target_stop_name]
    
    if target_stops_df.empty:
        return []
    
    target_parent_id = target_stops_df.iloc[0]['parent_station']
    if pd.isna(target_parent_id):
        target_stop_ids = target_stops_df['stop_id'].tolist()
    else:
        target_stop_ids = df_stops[df_stops['parent_station'] == target_parent_id]['stop_id'].tolist()


    # 2. 静的運行表の抽出と、運行中の便の絞り込み
    df_stop_schedule = df_base[df_base['stop_id'].isin(target_stop_ids)].copy()
    
    now_jst = datetime.now(JST)
    current_time_sec = now_jst.hour * 3600 + now_jst.minute * 60 + now_jst.second

    # 3. GTFSリアルタイムデータの取得とデータフレーム化
    rt_updates = get_realtime_updates(GTFS_RT_URL)
    if not rt_updates:
        df_rt = pd.DataFrame(columns=['trip_id', 'stop_sequence', 'delay_sec', 'rt_time', 'rt_stop_id'])
    else:
        df_rt = pd.DataFrame(rt_updates)
        df_rt['trip_id'] = df_rt['trip_id'].astype(str)

    # 4. RTデータと静的運行表の結合準備
    if df_rt.empty:
        df_stop_schedule['delay_sec'] = np.nan
        df_stop_schedule['rt_time'] = np.nan
        df_stop_schedule['rt_stop_id'] = np.nan
        df_stop_schedule['now_stop_id'] = np.nan
        df_stop_schedule['now_stop_name'] = "RT情報なし"
    else:
        df_stop_schedule = df_stop_schedule.merge(
            df_rt[['trip_id', 'stop_sequence', 'delay_sec', 'rt_time', 'rt_stop_id']],
            on=['trip_id', 'stop_sequence'],
            how='left'
        )
        
        df_rt_next_stop = df_stop_schedule[
            (df_stop_schedule['delay_sec'].notna() | df_stop_schedule['rt_time'].notna())
        ].copy()

        df_rt_info_to_merge = df_rt_next_stop.loc[df_rt_next_stop.groupby('trip_id')['stop_sequence'].idxmin()].copy()
        
        df_rt_info_to_merge['now_stop_id'] = df_rt_info_to_merge['rt_stop_id']
        df_rt_info_to_merge = df_rt_info_to_merge.merge(
            df_stops[['stop_id', 'stop_name']],
            left_on='now_stop_id',
            right_on='stop_id',
            how='left'
        ).rename(columns={'stop_name': 'now_stop_name'})
        
        df_stop_schedule = df_base[df_base['stop_id'].isin(target_stop_ids)].copy()
        df_stop_schedule = df_stop_schedule.merge(
            df_rt_info_to_merge[['trip_id', 'delay_sec', 'rt_time', 'now_stop_id', 'now_stop_name']],
            on='trip_id',
            how='left'
        )
        
        df_stop_schedule['now_stop_name'] = df_stop_schedule['now_stop_name'].fillna("RT情報なし")
        df_stop_schedule['delay_sec'] = df_stop_schedule['delay_sec'].fillna(np.nan)
        df_stop_schedule['rt_time'] = df_stop_schedule['rt_time'].fillna(np.nan)


    # 5. 到着予測時刻 (judgetime_sec) の計算
    df_stop_schedule['T_predicted_gtfs_sec'] = df_stop_schedule.apply(
        lambda row: unix_to_gtfs_sec(row['rt_time'], row['departure_sec']), 
        axis=1
    )

    df_stop_schedule['judgetime_sec'] = df_stop_schedule['T_predicted_gtfs_sec'].combine_first(
        df_stop_schedule['departure_sec'] + df_stop_schedule['delay_sec'].fillna(0)
    )
    df_stop_schedule['final_delay_sec'] = df_stop_schedule['judgetime_sec'] - df_stop_schedule['departure_sec']

    # 6. 時刻を整形
    df_stop_schedule['departure_time'] = df_stop_schedule['departure_sec'].apply(format_gtfs_sec)
    df_stop_schedule['predict_time'] = df_stop_schedule['judgetime_sec'].apply(format_gtfs_sec)
    
    # 7. 運行状況 (info) の判定ロジック
    
    df_stop_schedule['delay_time'] = df_stop_schedule['final_delay_sec'].apply(format_delay_time)

    def judge_status(row):
        if row['departure_sec'] < current_time_sec - 3600:
             return "運行終了", "通過・到着済み"
        
        if pd.notna(row['final_delay_sec']):
            if row['judgetime_sec'] < current_time_sec - 60:
                return "運行終了", "通過・到着済み"
            else:
                delay_status = row['delay_time']
                return "運行中", f"到着見込み ({delay_status})"

        else:
            if row['departure_sec'] < current_time_sec - 60:
                return "運行終了", "通過・到着済み"
            else:
                return "運行前", "運行開始前 (定刻運行予定)"


    df_stop_schedule[['status', 'info']] = df_stop_schedule.apply(
        lambda row: pd.Series(judge_status(row)), 
        axis=1
    )
    
    def format_current_location_status(row):
        if row['status'] == '運行中' and row['now_stop_name'] != "RT情報なし":
            return f"現在地: {row['now_stop_name']}"
        elif row['status'] == '運行中':
             return "現在地: 不明 (RT情報あり)"
        else:
            return ""

    df_stop_schedule['current_location_status'] = df_stop_schedule.apply(format_current_location_status, axis=1)

    # 8. 最終リストの整形とソート
    df_stop_schedule = df_stop_schedule.sort_values(by='judgetime_sec', ascending=True)

    final_list = df_stop_schedule[[
        'route_short_name', 
        'trip_headsign', 
        'departure_time', 
        'predict_time', 
        'status', 
        'info', 
        'current_location_status',
        'final_delay_sec' 
    ]].values.tolist()
    
    return final_list


# --- Flask アプリケーション ---

app = Flask(__name__)
app.config ['TEMPLATES_AUTO_RELOAD'] = True

load_gtfs_data()

@app.route('/', methods=['GET'])
def index():
    # 路線名リストと路線-バス停マッピングを取得
    route_names = GTFS.get('ROUTE_NAMES', [])
    route_stop_map = GTFS.get('ROUTE_STOP_MAP', {})
    # エラーメッセージを取得 (ない場合はNone)
    error_message = GTFS.get('ERROR', None) 
    
    return render_template('index.html', 
        route_names=route_names, 
        route_stop_map=route_stop_map,
        error_message=error_message # <-- エラーメッセージを渡す
    )

@app.route('/<stop_name>', methods=['GET'])
def info(stop_name):
    # URLエンコーディングされたバス停名をデコード
    stop = urllib.parse.unquote(stop_name)

    # 現在の運行表を生成
    mydata = generate_schedule(stop)
    
    # タイムゾーン情報を付加
    now_jst = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S JST')
    
    return render_template('info.html', 
        title=stop + "の運行状況", 
        stop_name=stop,              
        stop_url=stop_name,         
        now_jst=now_jst, 
        bus_schedule=mydata
    )

@app.route('/download/<stop_name>', methods=['GET'])
def download_schedule_csv(stop_name):
    # URLエンコーディングされたバス停名をデコード
    stop = urllib.parse.unquote(stop_name)

    df_list = generate_schedule(stop) 

    cols = [
        'route_short_name', 
        'trip_headsign', 
        'departure_time', 
        'predict_time', 
        'status_short', 
        'info', 
        'current_location_status',
        'final_delay_sec'
    ]
    df = pd.DataFrame(df_list, columns=cols)
    
    df_output = df[[
        'route_short_name', 
        'trip_headsign', 
        'departure_time', 
        'predict_time', 
        'info', 
        'current_location_status', 
        'final_delay_sec' 
    ]].copy()

    df_output.columns = [
        '路線名', 
        '行先', 
        '静的出発時刻', 
        '予測出発時刻', 
        '運行状況（詳細）', 
        '現在地情報',
        '最終遅延秒 (RTデータ確認用)'
    ]

    # DataFrameをCSV形式の文字列に変換 (UTF-8のBOM付きでExcel対応)
    csv_data = df_output.to_csv(index=False, encoding='utf-8-sig')

    response = Response(
        response=csv_data,
        mimetype='text/csv'
    )
    
    filename = f"{stop}_{datetime.now(JST).strftime('%Y%m%d_%H%M%S')}_schedule.csv"
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'

    return response
