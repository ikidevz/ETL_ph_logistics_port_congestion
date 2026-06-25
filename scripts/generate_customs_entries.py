import pandas as pd
import random
import uuid
from pathlib import Path
from datetime import timedelta
from ikidatagen import IkiDataGenerator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
output_path = DATA_DIR / "raw_customs_entries.csv"
input_path = DATA_DIR / "raw_shipments.csv"

ENTRY_TYPES = ['FORMAL', 'INFORMAL', 'WAREHOUSING', 'CONSUMPTION', 'TRANSIT', 'TRANSSHIPMENT', 'TEMPORARY_IMPORTATION', 'TEMPORARY_EXPORTATION', 'EXPORT_DECLARATION', 'REEXPORTATION', 'REIMPORTATION', 'DUTY_DRAWBACK',
               'FREEPORT_ENTRY', 'FREEZONE_ENTRY', 'COURIER_ENTRY', 'POSTAL_ENTRY', 'PROJECT_CARGO_ENTRY', 'GOVERNMENT_IMPORTATION', 'DIPLOMATIC_ENTRY', 'RELIEF_GOODS_ENTRY', 'BONDED_TRANSFER', 'SPECIAL_PERMIT_ENTRY']
PAYMENT_MODES = ['CASH', 'SURETY_BOND', 'DEFERRED', 'BANK_TRANSFER', 'MANAGER_CHECK', 'LETTER_OF_CREDIT', 'TELEGRAPHIC_TRANSFER', 'DOCUMENTARY_COLLECTION',
                 'E_PAYMENT', 'ONLINE_BANKING', 'CUSTOMS_ACCOUNT', 'ADVANCE_DEPOSIT', 'ESCROW_ACCOUNT', 'CORPORATE_CREDIT', 'GUARANTEE_BOND', 'TRUST_RECEIPT']


ships_df = pd.read_csv(input_path, parse_dates=['eta', 'ata'])
rows = []
for _, i in ships_df.iterrows():
    lodge_dt = i['ata'] - timedelta(days=random.randint(0, 3))
    assess_dt = lodge_dt + timedelta(days=random.randint(1, 5))
    release_dt = assess_dt + timedelta(hours=random.randint(4, 72))
    schema = [
        {'label': 'entry_number', 'key_label': 'lambda',
         'options': {'func': lambda: f'CE{uuid.uuid4().hex[:12].upper()}'}},
        {'label': 'shipment_id', 'key_label': 'lambda',
         'options': {'func': lambda: i['shipment_id']}},
        {'label': 'bl_number', 'key_label': 'lambda',
         'options': {'func': lambda: i['bl_number']}},
        {'label': 'commodity_code', 'key_label': 'lambda',
         'options': {'func': lambda: i['commodity_code']}},
        {'label': 'entry_type', 'key_label': 'custom_list',
         'options': {'values': ENTRY_TYPES}},
        {'label': 'declared_value_usd', 'key_label': 'lambda',
         'options': {'func': lambda: round((i['declared_value_usd'] + i['freight_usd'] + i['insurance_usd']), 2)}},
        {'label': 'tariff_rate_pct', 'key_label': 'custom_list',
         'options': {'values': [0, 3, 5, 7, 10, 15]}},
        {'label': 'duties_usd', 'key_label': 'lambda',
         'options': {'func': lambda row: round(row['declared_value_usd'] * row['tariff_rate_pct'] / 100, 2)}},
        {'label': 'vat_usd', 'key_label': 'lambda',
         'options': {'func': lambda row: round((row['declared_value_usd'] * row['duties_usd']) * 0.12, 2)}},
        {'label': 'total_tax_usd', 'key_label': 'lambda',
         'options': {'func': lambda row: round(row['duties_usd'] + (row['declared_value_usd'] * row['duties_usd']) * 0.12, 2)}},
        {'label': 'lodge_date', 'key_label': 'lambda',
         'options': {'func': lambda: lodge_dt.date()}},
        {'label': 'assessment_date', 'key_label': 'lambda',
         'options': {'func': lambda: assess_dt.date()}},
        {'label': 'release_date', 'key_label': 'lambda',
         'options': {'func': lambda: release_dt.date()}},
        {'label': 'processing_days', 'key_label': 'lambda',
         'options': {'func': lambda: (release_dt.date() - lodge_dt.date()).days}},
        {'label': 'payment_mode', 'key_label': 'custom_list',
         'options': {'values': PAYMENT_MODES}},
        {'label': 'port_code', 'key_label': 'lambda',
         'options': {'func': lambda: i['dest_port']}},

    ]
    payload = IkiDataGenerator(schema).one()
    rows.append(payload)

df = pd.DataFrame(rows)
df.to_csv(output_path, index=False)
print(f'Generated {len(df)} customs entries → {output_path}')
