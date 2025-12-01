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
        df_schedule['stop_id'].isin