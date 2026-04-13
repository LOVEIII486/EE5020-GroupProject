import requests
import pandas as pd
import os
import time

API_KEY = "v2:9e548aad37d5ebac0103d89291d37c5710ce7c1685d7ae396c77b54ffb5090a7:bL2QAonN4QGEA58Lr3U0GyumcmF7mh9m"

START_TIME = "2026-01-01"
END_TIME = "2026-03-27"

SAVE_PATH = f"datasets/air_temp_{START_TIME}_to_{END_TIME}.csv"

time_steps = pd.date_range(start=START_TIME, end=END_TIME, freq='h')
total_hours = len(time_steps)
success_count = 0

print(f"开始抓取 {START_TIME} 至 {END_TIME} 的气温数据，共 {total_hours} 个小时")

headers = {'x-api-key': API_KEY}

for i, ts in enumerate(time_steps):
    date_str = ts.strftime('%Y-%m-%d')
    hour_str = ts.strftime('%H')
    date_time_param = f"{date_str}T{hour_str}:00:00"
    url = f'https://api-open.data.gov.sg/v2/real-time/api/air-temperature?date={date_time_param}'

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        if 'code' in json_data and json_data['code'] == 0 and 'data' in json_data and 'readings' in json_data['data']:
            readings = json_data['data']['readings']

            all_readings = []
            for record in readings:
                timestamp = record['timestamp']
                for item in record['data']:
                    all_readings.append({
                        'timestamp': timestamp,
                        'station_id': item['stationId'],
                        'value': item['value']
                    })

            if all_readings:
                df = pd.DataFrame(all_readings)

                file_exists = os.path.isfile(SAVE_PATH)
                df.to_csv(SAVE_PATH, mode='a', index=False, header=not file_exists, encoding='utf-8')

                success_count += 1
                print(f"[Info] [{i + 1}/{total_hours}] {date_time_param} | 成功保存 {len(df)} 条气温数据")
            else:
                print(f"[Warning] [{i + 1}/{total_hours}] | {date_time_param} 返回无读数")
        else:
            print(f"[Warning] [{i + 1}/{total_hours}] | API 返回异常或数据不存在")

    except Exception as e:
        print(f"[Error] [{i + 1}/{total_hours}] | 错误: {e}")

    time.sleep(3)

print(f"所有气温数据已保存至: {SAVE_PATH}")