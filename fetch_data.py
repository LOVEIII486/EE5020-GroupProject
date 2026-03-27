import requests
import pandas as pd
import os
import time

API_KEY = "v2:9e548aad37d5ebac0103d89291d37c5710ce7c1685d7ae396c77b54ffb5090a7:bL2QAonN4QGEA58Lr3U0GyumcmF7mh9m"

START_TIME = "2026-01-01"
END_TIME = "2026-03-27"

SAVE_PATH = f"datasets/carpark_data_typeC_{START_TIME}_to_{END_TIME}.csv"
PL57_SAVE_PATH = f"datasets/PL57_data_typeC_{START_TIME}_to_{END_TIME}.csv"

time_steps = pd.date_range(start=START_TIME, end=END_TIME, freq='h')
total_hours = len(time_steps)
success_count = 0

print(f"开始抓取 {START_TIME} 至 {END_TIME} 的数据，共 {total_hours} 个小时")

headers = {'x-api-key': API_KEY}

for i, ts in enumerate(time_steps):
    date_str = ts.strftime('%Y-%m-%d')
    hour_str = ts.strftime('%H')
    minute = "01" if hour_str == "00" else "00"
    url = f'https://api.data.gov.sg/v1/transport/carpark-availability?date_time={date_str}T{hour_str}%3A{minute}%3A00'

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        if 'items' in json_data and json_data['items'] and 'carpark_data' in json_data['items'][0]:
            df = pd.DataFrame(json_data['items'][0]['carpark_data'])

            for col in ("total_lots", "lot_type", "lots_available"):
                df[col] = df["carpark_info"].apply(lambda x: x[0][col])

            # 所有停车场的数据
            df = df[df['lot_type'] == 'C'].copy()
            df['data_snapshot_time'] = f"{date_str} {hour_str}:{minute}"
            df.drop(columns=["carpark_info"], inplace=True)

            # 单独保存PL57数据
            df_pl57 = df[df['carpark_number'] == 'PL57'].copy()
            if not df_pl57.empty:
                pl57_exists = os.path.isfile(PL57_SAVE_PATH)
                df_pl57.to_csv(PL57_SAVE_PATH, mode='a', index=False, header=not pl57_exists, encoding='utf-8')

            file_exists = os.path.isfile(SAVE_PATH)
            df.to_csv(SAVE_PATH, mode='a', index=False, header=not file_exists, encoding='utf-8')

            success_count += 1
            print(f"[Info] [{i + 1}/{total_hours}] {date_str} {hour_str}:{minute} | 成功保存 {len(df)} 条数据")
        else:
            print(f"[Warning] [{i + 1}/{total_hours}] | API 返回空")

    except Exception as e:
        print(f"[Error] [{i + 1}/{total_hours}] | 错误: {e}")

    time.sleep(5.1)

print(f"所有停车场数据已保存至: {SAVE_PATH}")
print(f"PL57数据已保存至: {PL57_SAVE_PATH}")