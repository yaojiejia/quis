import os
from typing import Tuple, Optional
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def _slack_client() -> Optional[WebClient]:
    token = '' # set your token here or use ENV
    if not token:
        return None
    return WebClient(token=token)

def send_slack(text: str, channel: Optional[str] = None) -> Tuple[bool, str]:
    client = _slack_client()
    if client is None:
        return False, "Slack token not set"
    ch = '' # set your channel here 
    text = text + " current_company_name has more than 2% Please Check Dashboard"
    try:
        resp = client.chat_postMessage(channel=ch, text=text)
        return True, resp.get("ts", "sent")
    except SlackApiError as e:
        return False, e.response.get("error", "unknown_error")

def find_bad_urls(df: pd.DataFrame) -> pd.DataFrame:
    if "url" not in df.columns:
        return pd.DataFrame(columns=df.columns)
    s = df["url"].astype(str).str.lower()
    return df[~s.str.contains("linkedin", na=False)]

def summarize_quality(total: int, bad: int, threshold_pct: float):
    rate = 0.0 if total == 0 else (bad / total) * 100.0
    should_alert = rate >= threshold_pct if total > 0 else False
    msg = f"URL quality: {bad}/{total} rows ({rate:.2f}%) missing 'linkedin' in url; threshold {threshold_pct:.2f}%"
    return should_alert, msg, rate

def company_nulls_summary(total: int, nulls: int, threshold_pct: float = 40.0):
    rate = 0.0 if total == 0 else (nulls / total) * 100.0
    should_alert = rate > threshold_pct if total > 0 else False
    msg = f"Top companies nulls: {nulls}/{total} rows ({rate:.2f}%) blank current_company_name; threshold {threshold_pct:.2f}%"
    return should_alert, msg, rate

if __name__ == "__main__":
    ok, msg = send_slack("Testing Alerter")
    print("Result:", ok, msg)