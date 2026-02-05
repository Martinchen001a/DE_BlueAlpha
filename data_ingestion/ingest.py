import pandas as pd
import json
from sqlalchemy import create_engine
import os
import re


DB_URL = "postgresql://postgres:password@localhost:5433/postgres"
engine = create_engine(DB_URL)


def python_pre_clean(df, source_name):
    df.columns = [
        re.sub(r'[\s-]+', '_', c.strip().lower())
        for c in df.columns
    ]

    str_cols = df.select_dtypes(include=['object']).columns
    date_cols = [c for c in df.columns if 'date' in c]

    for col in str_cols:
        if col in date_cols:
            continue

        df[col] = df[col].astype(str).str.lower()

        if df[col].str.contains('&', na=False).any():
            df[col] = df[col].str.replace(r'\s+', '', regex=True)
        else:
            df[col] = df[col].str.replace(r'[\s-]+', '_', regex=True)

    for col in date_cols:
        df[col] = df[col].astype(str).str.replace('_', '-').str.strip()
        df[col] = pd.to_datetime(
            df[col], dayfirst=True, format='mixed', errors='coerce').dt.strftime('%Y-%m-%d')
    return df


def ingest_data():
    current_script_path = os.path.abspath(__file__)
    data_dir = os.path.join(os.path.dirname(current_script_path), '..', 'data')

    # Ingest crm data
    crm_path = os.path.join(data_dir, 'crm_revenue.csv')
    if os.path.exists(crm_path):
        df_crm = pd.read_csv(
            crm_path, on_bad_lines='warn', engine='python')
        df_crm = python_pre_clean(df_crm, "CRM")
        df_crm.to_sql('stg_crm_revenue', con=engine,
                      if_exists='replace', index=False)
        print(f"Ingested CRM data: {len(df_crm)} rows")

    # Ingest facebook data
    fb_path = os.path.join(data_dir, 'facebook_export.csv')
    if os.path.exists(fb_path):
        df_fb = pd.read_csv(fb_path, on_bad_lines='warn', engine='python')
        df_fb = python_pre_clean(df_fb, "Facebook")
        df_fb.to_sql('stg_facebook_ads', con=engine,
                     if_exists='replace', index=False)
        print(f"Ingested Facebook data: {len(df_fb)} rows")

    # Ingest google data(Json)
    google_path = os.path.join(data_dir, 'google_ads_api.json')
    if os.path.exists(google_path):
        with open(google_path, 'r') as f:
            google_data = json.load(f)

        # Transform data from Json to Dataframe
        df_google = pd.json_normalize(
            google_data['campaigns'],
            record_path=['daily_metrics'],
            meta=['campaign_name', 'campaign_id', 'campaign_type']
        )
        df_google = python_pre_clean(df_google, "Google")
        df_google.to_sql('stg_google_ads', con=engine,
                         if_exists='replace', index=False)
        print(f"Ingested Google Ads data: {len(df_google)} rows")


if __name__ == "__main__":
    try:
        ingest_data()
        print("\n Data Ingestion Done!")
    except Exception as e:
        print(f"\n Data Ingestion Failed: {e}")
