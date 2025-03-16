import json
import pytz
from datetime import datetime
import logging
import os
import requests
from dotenv import load_dotenv



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


LOCATIONS = [(63,125), (53,124), (66,120), (86,86)] # 서울 장지동 쿠팡, 인천 연안동 쿠팡17센터, 이천 마장면 쿠팡, 대구 현풍 쿠팡

KST = pytz.timezone("Asia/Seoul")
now = datetime.now(KST).replace(microsecond=0, second=0, minute=0)
base_date = now.strftime("%Y%m%d")
base_time = now.strftime("%H%M")

def fetch_weather_data(ti):
    try:
        logger.info("Step 1: fetching data from multiple weather API endpoints")
        load_dotenv('/opt/airflow/.env')  # take environment variables from .env.

        all_items=[]
        # write code here
        for nx, ny in LOCATIONS:
            params = {
                    'serviceKey': 'RhXURHVaUAqX9AS4dKYbnvOnegy8sGL1hWqwmUYZbv4QdBuStJWpVTcXUdquDSPp/vsHR1ItrM3JqEr92xP4jw==',
                    'dataType': 'JSON',
                    'base_date': base_date,
                    'base_time': base_time,
                    'nx': nx,
                    'ny': ny
                    }
            response = requests.get('http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst', params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            items = data['response']['body']['items']['item']
            for item in items:
                all_items.append((
                    json.dumps(item),
                    datetime.strptime(item["baseDate"], "%Y%m%d").date(),
                    item["baseTime"],
                    nx,
                    ny
                ))
        logger.info(f"Fetched {len(all_items)} records from {len(LOCATIONS)} locations")
        # push data to Xcom for next task
        ti.xcom_push(key='weather_data', value=all_items)
        return all_items # optional, for logging/debugging
    except Exception as e:
        logger.error(f'Fetch failed : {str(e)}')
        raise

