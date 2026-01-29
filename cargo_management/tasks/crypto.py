# cargo_management/tasks/crypto.py

import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
import psycopg2
import psycopg2.extras
import frappe

DATABASE_URL = "postgresql://neondb_owner:npg_mM4J2iAtzYuk@ep-still-bar-add0xr42-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
CMC_API_KEY   = "dd143d9f-c387-4629-8b1d-0c00119b5c17"
CMC_URL       = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
FIAT          = "USD"

UPSERT = """
INSERT INTO crypto_snapshots (
  cmc_id, symbol, name, slug,
  fiat, ts,
  price, market_cap, fully_diluted_mcap, market_cap_dominance_pct, volume_24h, volume_change_24h_pct,
  pct_change_1h, pct_change_24h, pct_change_7d, pct_change_30d, pct_change_60d, pct_change_90d,
  cmc_rank, num_market_pairs, circulating_supply, total_supply, max_supply, infinite_supply,
  date_added, tags, status_timestamp, payload_last_updated, ingested_at
) VALUES (
  %(cmc_id)s, %(symbol)s, %(name)s, %(slug)s,
  %(fiat)s, %(ts)s,
  %(price)s, %(market_cap)s, %(fully_diluted_mcap)s, %(market_cap_dominance_pct)s, %(volume_24h)s, %(volume_change_24h_pct)s,
  %(pct_change_1h)s, %(pct_change_24h)s, %(pct_change_7d)s, %(pct_change_30d)s, %(pct_change_60d)s, %(pct_change_90d)s,
  %(cmc_rank)s, %(num_market_pairs)s, %(circulating_supply)s, %(total_supply)s, %(max_supply)s, %(infinite_supply)s,
  %(date_added)s, %(tags)s, %(status_timestamp)s, %(payload_last_updated)s, now()
)
ON CONFLICT (cmc_id, fiat, ts) DO NOTHING;
"""

# --- logging helpers ---
def _log(msg: str, title: str = "crypto"):
    try:
        frappe.log_error(message=msg, title=title)
    except Exception:
        print(f"[{title}] {msg}", file=sys.stdout)

def _log_exc(err_title: str, err: Exception):
    tb = traceback.format_exc()
    _log(f"{err_title}\n{str(err)}\n\n{tb}", title="crypto:ERROR")

# --- utils ---
def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def _safe_num(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def _row_from_item(item: Dict[str, Any], status_ts: Optional[datetime], fiat: str) -> Optional[Dict[str, Any]]:
    q = (item.get("quote") or {}).get(fiat) or {}
    payload_last_updated = _parse_iso(q.get("last_updated")) or status_ts or datetime.now(timezone.utc)
    ts = payload_last_updated
    return {
        "cmc_id": item.get("id"),
        "symbol": item.get("symbol"),
        "name": item.get("name"),
        "slug": item.get("slug"),
        "fiat": fiat,
        "ts": ts,
        "price": _safe_num(q.get("price")),
        "market_cap": _safe_num(q.get("market_cap")),
        "fully_diluted_mcap": _safe_num(q.get("fully_diluted_market_cap")),
        "market_cap_dominance_pct": _safe_num(q.get("market_cap_dominance")),
        "volume_24h": _safe_num(q.get("volume_24h")),
        "volume_change_24h_pct": _safe_num(q.get("volume_change_24h")),
        "pct_change_1h": _safe_num(q.get("percent_change_1h")),
        "pct_change_24h": _safe_num(q.get("percent_change_24h")),
        "pct_change_7d": _safe_num(q.get("percent_change_7d")),
        "pct_change_30d": _safe_num(q.get("percent_change_30d")),
        "pct_change_60d": _safe_num(q.get("percent_change_60d")),
        "pct_change_90d": _safe_num(q.get("percent_change_90d")),
        "cmc_rank": item.get("cmc_rank"),
        "num_market_pairs": item.get("num_market_pairs"),
        "circulating_supply": _safe_num(item.get("circulating_supply")),
        "total_supply": _safe_num(item.get("total_supply")),
        "max_supply": _safe_num(item.get("max_supply")),
        "infinite_supply": bool(item.get("infinite_supply")) if item.get("infinite_supply") is not None else None,
        "date_added": _parse_iso(item.get("date_added")),
        "tags": item.get("tags") or [],
        "status_timestamp": status_ts,
        "payload_last_updated": payload_last_updated,
    }

def pull_and_store():
    """Fetch latest listings from CMC (defaults) and upsert into Postgres (USD only)."""
    try:
        headers = {"Accept": "application/json", "X-CMC_PRO_API_KEY": CMC_API_KEY}

        _log(f"Calling CoinMarketCap API (no params) at {CMC_URL} | fiat={FIAT}")

        # no params – rely on CMC defaults
        resp = requests.get(CMC_URL, headers=headers, timeout=60)
        if resp.status_code != 200:
            # _log(f"CMC non-200 response: {resp.status_code} {resp.text[:500]}")
            resp.raise_for_status()

        try:
            payload = resp.json()
        except Exception:
            _log(f"Failed to decode JSON. Body (first 500): {resp.text[:500]}")
            raise

        status = payload.get("status") or {}
        status_ts = _parse_iso(status.get("timestamp"))
        data = payload.get("data") or []
        if not data:
            _log("CMC returned no data")
            return

        rows: List[Dict[str, Any]] = []
        for item in data:
            row = _row_from_item(item, status_ts, FIAT)
            if row and row.get("cmc_id") and row.get("symbol") and row.get("name"):
                rows.append(row)

        if not rows:
            _log("No parsable rows")
            return

        # _log(f"Parsed {len(rows)} rows; connecting to Postgres…")

        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=1000)
            conn.commit()

        # _log(f"Upserted {len(rows)} snapshot rows into crypto_snapshots.")
    except Exception as e:
        _log_exc("pull_and_store failed", e)