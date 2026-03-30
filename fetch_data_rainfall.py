import requests
import pandas as pd
import os
import time

API_KEY = "v2:9e548aad37d5ebac0103d89291d37c5710ce7c1685d7ae396c77b54ffb5090a7:bL2QAonN4QGEA58Lr3U0GyumcmF7mh9m"
START_TIME = "2026-01-01"
END_TIME = "2026-03-27"

SAVE_PATH = f"datasets/rainfall_all_stations_{START_TIME}_to_{END_TIME}.csv"

time_steps = pd.date_range(start=START_TIME, end=END_TIME, freq='h')
total_hours = len(time_steps)
success_count = 0

print(f"开始抓取降雨数据：{START_TIME} 至 {END_TIME}，共 {total_hours} 个点")

headers = {'x-api-key': API_KEY}

for i, ts in enumerate(time_steps):
    date_str = ts.strftime('%Y-%m-%d')
    hour_str = ts.strftime('%H')
    minute = "01" if hour_str == "00" else "00"
    url = f'https://api.data.gov.sg/v1/environment/rainfall?date_time={date_str}T{hour_str}%3A{minute}%3A00'

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        if 'items' in json_data and json_data['items'] and 'readings' in json_data['items'][0]:
            readings = json_data['items'][0]['readings']
            df = pd.DataFrame(readings)

            if not df.empty:
                df['data_snapshot_time'] = f"{date_str} {hour_str}:{minute}"

                all_exists = os.path.isfile(SAVE_PATH)
                df.to_csv(SAVE_PATH, mode='a', index=False, header=not all_exists, encoding='utf-8')

                success_count += 1
                print(f"[Info] [{i + 1}/{total_hours}] {date_str} {hour_str}:{minute}")
            else:
                print(f"[Warning] [{i + 1}/{total_hours}] | readings 列表为空")
        else:
            print(f"[Warning] [{i + 1}/{total_hours}] | API 未返回 items 数据")

    except Exception as e:
        print(f"[Error] [{i + 1}/{total_hours}] | 错误: {e}")

    time.sleep(2.0)

print(f"\n抓取完成！成功率: {success_count}/{total_hours}")
print(f"全站点数据: {SAVE_PATH}")