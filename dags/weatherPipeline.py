import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta




baseUrl = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst'

KST = pytz.timezone("Asia/Seoul")
now = datetime.now(KST).replace(microsecond=0, second=0, minute=0)
base_date = now.strftime("%Y%m%d")
base_time = now.strftime("%H%M")

params = {
        'serviceKey': 'RhXURHVaUAqX9AS4dKYbnvOnegy8sGL1hWqwmUYZbv4QdBuStJWpVTcXUdquDSPp/vsHR1ItrM3JqEr92xP4jw==',
        'dataType': 'JSON',
        'base_date': base_date,
        'base_time': base_time,
        'nx': 86,
        'ny': 106
    }

response = requests.get(baseUrl, params)

response_json = response.json()


response_json['response']['body']['items']['item']

df = pd.DataFrame(response_json['response']['body']['items']['item'])

df_pivot = df.pivot(index='baseTime', columns='category', values='obsrValue')

df_left = df[['baseDate', 'baseTime', 'nx', 'ny']].drop_duplicates()

pd.merge(df_left, df_pivot, on='baseTime', how='left')

