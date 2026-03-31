import requests
import pandas as pd
import os
import time

API_KEY = "v2:9e548aad37d5ebac0103d89291d37c5710ce7c1685d7ae396c77b54ffb5090a7:bL2QAonN4QGEA58Lr3U0GyumcmF7mh9m"
START_DATE = "2026-01-01"
END_DATE = "2026-03-01"
SAVE_PATH = f"datasets/rainfall_with_locations_{START_DATE}_to_{END_DATE}.csv"

URL = "https://api-open.data.gov.sg/v2/real-time/api/rainfall"

os.makedirs('datasets', exist_ok=True)

date_range = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
total_days = len(date_range)

print(f"开始抓取降雨数据（含位置信息）：{START_DATE} 至 {END_DATE}，共 {total_days} 天")

headers = {'x-api-key': API_KEY}

for i, current_date in enumerate(date_range):
    date_str = current_date.strftime('%Y-%m-%d')
    params = {'date': date_str}

    try:
        response = requests.get(URL, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        json_response = response.json()

        if json_response.get('code') == 0:
            res_data = json_response.get('data', {})
            stations = res_data.get('stations', [])
            readings_list = res_data.get('readings', [])

            if stations and readings_list:
                station_map = {}
                for s in stations:
                    s_id = s.get('id')
                    loc = s.get('location')
                    if s_id and loc:
                        station_map[s_id] = {
                            'station_name': s.get('name'),
                            'latitude': loc.get('latitude'),
                            'longitude': loc.get('longitude')
                        }

                processed_records = []
                for entry in readings_list:
                    timestamp = entry.get('timestamp')
                    data_points = entry.get('data', [])

                    for dp in data_points:
                        sid = dp.get('stationId')
                        if sid in station_map:
                            record = {
                                'timestamp': timestamp,
                                'station_id': sid,
                                'rainfall_value': dp.get('value'),
                                **station_map[sid]  # 展开站点名称和经纬度
                            }
                            processed_records.append(record)

                if processed_records:
                    df_day = pd.DataFrame(processed_records)
                    file_exists = os.path.isfile(SAVE_PATH)
                    df_day.to_csv(SAVE_PATH, mode='a', index=False, header=not file_exists, encoding='utf-8')
                    print(f"[Success] [{i + 1}/{total_days}] {date_str} | 抓取到 {len(processed_records)} 条记录")
                else:
                    print(f"[Warning] [{i + 1}/{total_days}] {date_str} | 该日期无有效读数数据")
            else:
                print(f"[Warning] [{i + 1}/{total_days}] {date_str} | API 返回为空")
        else:
            print(f"[Error] [{i + 1}/{total_days}] {date_str} | API 返回错误码: {json_response.get('code')}")

    except Exception as e:
        print(f"[Error] [{i + 1}/{total_days}] {date_str} | 网络或解析错误: {e}")

    time.sleep(2)

print(f"数据集已保存至: {SAVE_PATH}")