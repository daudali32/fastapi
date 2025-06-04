from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import requests
import pytz
from datetime import datetime

app = FastAPI()

@app.get("/btc-summary/")
def btc_summary(
    report_type: str = Query(..., enum=["weekday", "hour", "date"]),
    interval: str = Query(..., description="Kline interval (e.g., 1h, 1d, etc.)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to fetch (default: 100, max: 1000)")
):
    # Build API URL
    url = (
        f"http://cvh.plagx.com/api/klines"
        f"?interval={interval}&symbol=BTCUSDT"
        f"&TRADING_API_CODE_TEAM=atb_team"
        f"&limit={limit}"
    )
    response = requests.get(url)

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch data from external API")

    data = response.json()
    df = pd.DataFrame(data)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data returned from API")

    # Convert time
    pakistan_tz = pytz.timezone('Asia/Karachi')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True).dt.tz_convert(pakistan_tz)

    # Ensure numeric columns
    cols = ['open', 'close', 'high', 'low', 'volume', 'number_of_trades']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

    # Add % change
    df['avg_pct_change'] = (df['close'] / df['open']) * 100 - 100

    # Prepare output
    if report_type == "weekday":
        df['day'] = df['close_time'].dt.strftime('%A')
        group_field = 'day'
    elif report_type == "date":
        df['date'] = df['close_time'].dt.date
        group_field = 'date'
    elif report_type == "hour":
        if interval != "1h":
            raise HTTPException(status_code=400, detail="Hourly summary requires interval=1h")
        df['hour'] = df['close_time'].dt.hour
        group_field = 'hour'
    else:
        raise HTTPException(status_code=400, detail="Invalid report type")

    # Grouping
    summary = df.groupby(group_field).agg(
        max_high=('high', 'max'),
        min_low=('low', 'min'),
        avg_high=('high', 'mean'),
        avg_low=('low', 'mean'),
        total_volume=('volume', 'sum'),
        avg_volume=('volume', 'mean'),
        avg_open_close_diff=('avg_pct_change', 'mean'),
        count=(group_field, 'size')
    ).reset_index()

    # Round for readability
    round_cols = ['max_high', 'min_low', 'avg_high', 'avg_low', 'total_volume', 'avg_volume', 'avg_open_close_diff']
    summary[round_cols] = summary[round_cols].round(2)

    # Sort by avg_open_close_diff
    summary = summary.sort_values(by='avg_open_close_diff', ascending=False)

    return summary.to_dict(orient='records')
