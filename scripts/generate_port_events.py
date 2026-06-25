import pandas as pd
import random
from datetime import timedelta, datetime
from pathlib import Path
from ikidatagen import IkiDataGenerator

cust_n = 2000
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
output_path = DATA_DIR / "raw_vessel_calls.csv"

PH_PORTS = [
    {'code': 'MICT',  'name': 'Manila International Container Terminal',
        'lat': 14.5820, 'lon': 120.9650},
    {'code': 'MNLS',  'name': 'Manila South Harbor',
        'lat': 14.5690, 'lon': 120.9640},
    {'code': 'MNLN',  'name': 'Manila North Harbor',
        'lat': 14.5980, 'lon': 120.9630},
    {'code': 'BCT',   'name': 'Batangas Container Terminal',
        'lat': 13.7565, 'lon': 121.0583},
    {'code': 'SBIC',  'name': 'Subic Bay International Terminal',
        'lat': 14.7969, 'lon': 120.2705},
    {'code': 'CIP',   'name': 'Cebu International Port',
        'lat': 10.2939, 'lon': 123.8988},
    {'code': 'MACTC', 'name': 'Mactan-Cebu International Container Terminal',
        'lat': 10.3103, 'lon': 123.9494},
    {'code': 'DIP',
        'name': 'Davao International Port (Sasa Wharf)',                'lat':  7.0708, 'lon': 125.6127},
    {'code': 'GDXP',  'name': 'General Santos Port (Makar Wharf)',
     'lat':  6.1060, 'lon': 125.1720},
    {'code': 'ILO',   'name': 'Iloilo Port',
        'lat': 10.6969, 'lon': 122.5644},
    {'code': 'CGY',   'name': 'Cagayan de Oro Port (Macabalan)',
     'lat':  8.4800, 'lon': 124.6500},
    {'code': 'ILG',   'name': 'Port of Iligan',
        'lat':  8.2280, 'lon': 124.2420},
    {'code': 'ZAM',   'name': 'Zamboanga Port',
        'lat':  6.9100, 'lon': 122.0700},
    {'code': 'OZM',   'name': 'Port of Ozamiz',
        'lat':  8.1500, 'lon': 123.8500},
    {'code': 'NASPT', 'name': 'Port of Nasipit',
        'lat':  8.9667, 'lon': 125.3167},
    {'code': 'SRGP',  'name': 'Port of Surigao',
        'lat':  9.7833, 'lon': 125.5000},
    {'code': 'BGO',   'name': 'Port of Bacolod (Bredco Port)',
     'lat': 10.6833, 'lon': 122.9500},
    {'code': 'TAC',   'name': 'Port of Tacloban',
        'lat': 11.2500, 'lon': 125.0000},
    {'code': 'DGT',   'name': 'Port of Dumaguete',
        'lat':  9.3066, 'lon': 123.3075},
    {'code': 'CBY',   'name': 'Port of Calbayog',
        'lat': 12.0667, 'lon': 124.6000},
    {'code': 'ORM',   'name': 'Port of Ormoc',
        'lat': 11.0064, 'lon': 124.6075},
    {'code': 'PPT',   'name': 'Puerto Princesa Port',
        'lat':  9.7333, 'lon': 118.7167},
    {'code': 'LGP',   'name': 'Legazpi Port',
        'lat': 13.1400, 'lon': 123.7300},
    {'code': 'BGV',   'name': 'Port of Bulan',
        'lat': 12.6700, 'lon': 123.8700},
    {'code': 'BOHL',  'name': 'Port of Tagbilaran',
        'lat':  9.6550, 'lon': 123.8540},
    {'code': 'CDN',
        'name': 'Port of Cebu-Bohol Ferry (Calapan)',                   'lat': 13.4100, 'lon': 121.1800},
    {'code': 'JRD',   'name': 'Port of Jordan (Guimaras)',
     'lat': 10.6000, 'lon': 122.5900},
    {'code': 'BABK',  'name': 'Port of Babak (Samal)',
     'lat':  7.0800, 'lon': 125.7200},
    {'code': 'NASP',  'name': 'Port of San Fernando (La Union)',
     'lat': 16.6167, 'lon': 120.3167},
    {'code': 'MDO',   'name': 'Port of Calapan (Oriental Mindoro)',
     'lat': 13.4100, 'lon': 121.1800},
    {'code': 'MAT', 'name': 'Port of Matnog',
        'lat': 12.5870, 'lon': 124.0870},
    {'code': 'ALL', 'name': 'Port of Allen',
        'lat': 12.5040, 'lon': 124.2840},
    {'code': 'MSB', 'name': 'Port of Masbate',
        'lat': 12.3690, 'lon': 123.6240},
    {'code': 'CTX', 'name': 'Port of Catbalogan',
        'lat': 11.7750, 'lon': 124.8860},
    {'code': 'RXS', 'name': 'Port of Roxas',
        'lat': 12.5830, 'lon': 121.5170},
    {'code': 'CRN', 'name': 'Port of Coron',
        'lat': 11.9980, 'lon': 120.2040},
    {'code': 'BPT', 'name': "Port of Brookes Point",
        'lat':  8.7810, 'lon': 117.8410},
    {'code': 'COT', 'name': 'Port of Cotabato',
        'lat':  7.2230, 'lon': 124.2460},
    {'code': 'POL', 'name': 'Port of Polloc',
        'lat':  7.3530, 'lon': 124.2690},
    {'code': 'JOL', 'name': 'Port of Jolo',
        'lat':  6.0530, 'lon': 121.0020},
    {'code': 'LAM', 'name': 'Port of Lamitan',
        'lat':  6.6540, 'lon': 122.1390},
    {'code': 'LCN', 'name': 'Port of Lucena',
        'lat': 13.9380, 'lon': 121.6200},
]

VESSEL_POOL = [f'IMO{i:07d}' for i in range(9000001, 9000201)]

schema = [
    {'label': 'vessel_imo', 'key_label': 'custom_list',
     'options': {'values': VESSEL_POOL}},
    {'label': 'port_code', 'key_label': 'lambda',
     'options': {'func': lambda: random.choice(PH_PORTS)['code']}},
    {'label': 'congestion_delay', 'key_label': 'lambda',
     'options': {'func': lambda: random.choices([0, random.randint(12, 72)], weights=[70, 30])[0]}},
    {"label": "arrival_ts", "key_label": "datetime", "options": {
        "from_date": '1/1/2026', "to_date": '06/01/2026', "date_format": "iso"
    }},
    {'label': 'berth_ts', 'key_label': 'lambda',
     'options': {'func': lambda row: (datetime.fromisoformat(row['arrival_ts']) + timedelta(hours=row['congestion_delay']))}},
    {'label': 'departure_ts', 'key_label': 'lambda',
     'options': {'func': lambda row: row['berth_ts'] + timedelta(hours=random.randint(6, 96))}},
    {'label': 'dwell_hours', 'key_label': 'lambda',
     'options': {'func': lambda row: int((row['departure_ts'] - datetime.fromisoformat(row['arrival_ts'])).total_seconds() // 3600)}},
    {'label': 'anchorage_wait_h', 'key_label': 'lambda',
     'options': {'func': lambda row: row['congestion_delay']}},
    {'label': 'cargo_type', 'key_label': 'custom_list',
     'options': {'values': ['FCL', 'LCL', 'RORO', 'BULK']}},
    {"label": "teu_count", "key_label": "number",
        "options": {"min": 10, "max": 2000}},
]

payload = IkiDataGenerator(schema).many(cust_n).data
df = pd.DataFrame(payload)
df = df.drop(columns=['congestion_delay'])
df.to_csv(output_path, index=False)
print(f"Generated {cust_n:,} accounts rows → {output_path}")
