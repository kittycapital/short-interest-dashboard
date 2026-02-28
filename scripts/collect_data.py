"""
FINRA Short Interest + yfinance Price Data Collector
=====================================================
GitHub Actions에서 월 2~3회 실행하여 데이터를 수집합니다.

사용법:
  pip install requests yfinance pandas
  python collect_data.py

환경변수 필요:
  FINRA_CLIENT_ID - FINRA API Client ID
  FINRA_CLIENT_SECRET - FINRA API Client Secret

FINRA API 등록: https://developer.finra.org
"""

import os
import json
import time
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ============================================
# CONFIG
# ============================================
# 레포 루트 기준 경로 (scripts/ 안에서 실행해도 루트에서 실행해도 동작)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"

# 페이지 1: 시장 개요 ETF
MARKET_ETFS = ['SPY', 'QQQ', 'IWM']

# 페이지 1: 섹터 ETF
SECTOR_ETFS = ['XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLC', 'XLP', 'XLU', 'XLY', 'XLB', 'XLRE']

# 페이지 1: 레버리지/인버스 ETF
LEVERAGE_ETFS = ['TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'TNA', 'TZA', 'UVXY', 'SVXY']

# 페이지 1: 채권/안전자산 ETF
BOND_ETFS = ['TLT', 'HYG', 'LQD']

# 페이지 2: 서학개미 인기 종목
KOREAN_FAVORITES = [
    # 빅테크
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA',
    # 반도체
    'AMD', 'AVGO', 'QCOM', 'MU', 'INTC', 'ARM', 'SMCI',
    # AI/소프트웨어
    'PLTR', 'SNOW', 'CRM', 'NOW', 'SHOP',
    # 핀테크/금융
    'COIN', 'SOFI', 'SQ', 'PYPL', 'V', 'MA',
    # 바이오/헬스
    'LLY', 'NVO', 'MRNA', 'PFE',
    # 에너지/산업
    'XOM', 'CVX', 'LMT', 'BA',
    # 기타 인기
    'NFLX', 'DIS', 'NIO', 'RIVN', 'RBLX', 'MARA',
]

# 모든 추적 종목 (중복 제거)
ALL_TRACKED = list(set(
    MARKET_ETFS + SECTOR_ETFS + LEVERAGE_ETFS + BOND_ETFS + KOREAN_FAVORITES
))


# ============================================
# FINRA API
# ============================================
class FINRAClient:
    """FINRA API Client for Short Interest data"""

    TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
    # Consolidated Short Interest는 equity 그룹에 있음
    API_BASE = "https://api.finra.org/data/group/equity/name"

    def __init__(self):
        self.client_id = os.environ.get('FINRA_CLIENT_ID', '')
        self.client_secret = os.environ.get('FINRA_CLIENT_SECRET', '')
        self.token = None
        self.token_expiry = None
        self.dataset_name = 'consolidatedShortInterest'
        self.field_map = {}

    def authenticate(self):
        """OAuth 2.0 인증"""
        if not self.client_id or not self.client_secret:
            print("⚠️  FINRA credentials not found. Using sample data.")
            return False

        try:
            resp = requests.post(
                self.TOKEN_URL,
                auth=(self.client_id, self.client_secret),
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            resp.raise_for_status()
            data = resp.json()
            self.token = data['access_token']
            self.token_expiry = datetime.now() + timedelta(seconds=data.get('expires_in', 1800))
            print("✅ FINRA API authenticated")
            self._check_metadata()
            return True
        except Exception as e:
            print(f"❌ FINRA auth failed: {e}")
            return False

    def _check_metadata(self):
        """API 메타데이터 확인 - 필드명 디버깅용"""
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/json'
        }
        # 여러 가능한 엔드포인트 시도
        endpoints = [
            ("equity", "consolidatedShortInterest"),
            ("otcMarket", "consolidatedShortInterest"),
            ("otcMarket", "EquityShortInterest"),
        ]
        for group, name in endpoints:
            url = f"https://api.finra.org/data/group/{group}/name/{name}"
            try:
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    meta = resp.json()
                    fields = [f.get('name') for f in meta.get('fields', [])]
                    print(f"✅ Found dataset: {group}/{name}")
                    print(f"   Fields: {fields[:10]}...")
                    self.API_BASE = f"https://api.finra.org/data/group/{group}/name"
                    self.dataset_name = name
                    self.field_map = {f.get('name'): f for f in meta.get('fields', [])}
                    return
                else:
                    print(f"   ⏭️  {group}/{name}: {resp.status_code}")
            except Exception as e:
                print(f"   ⏭️  {group}/{name}: {e}")
        print("⚠️  Could not find short interest dataset metadata")

    def get_short_interest(self, symbol=None, limit=5000):
        """
        Consolidated Short Interest 데이터 조회
        symbol=None이면 전체 종목 (페이지 3용)
        """
        if not self.token:
            return None

        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        payload = {
            "limit": limit,
            "offset": 0,
            "sortFields": ["-settlementDate"],
        }

        if symbol:
            payload["compareFilters"] = [{
                "compareType": "EQUAL",
                "fieldName": "symbolCode",
                "fieldValue": symbol
            }]

        url = f"{self.API_BASE}/{getattr(self, 'dataset_name', 'consolidatedShortInterest')}"

        try:
            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                print(f"❌ FINRA API error for {symbol}: {resp.status_code} - {resp.text[:300]}")
                return None
            return resp.json()
        except Exception as e:
            print(f"❌ FINRA API error for {symbol}: {e}")
            return None

    def get_all_latest_short_interest(self):
        """최신 settlement date의 전체 종목 숏 인터레스트 조회 (페이지 3용)"""
        if not self.token:
            return None

        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # 먼저 최신 날짜 확인
        payload = {
            "limit": 1,
            "sortFields": ["-settlementDate"],
        }

        try:
            resp = requests.post(
                f"{self.API_BASE}/{getattr(self, 'dataset_name', 'consolidatedShortInterest')}",
                headers=headers,
                json=payload
            )
            if resp.status_code != 200:
                print(f"❌ Latest SI fetch failed: {resp.status_code} - {resp.text[:300]}")
                return None
            data = resp.json()
            if not data:
                return None

            latest_date = data[0].get('settlementDate')
            print(f"📅 Latest settlement date: {latest_date}")

            # 전체 데이터 조회 (페이지네이션)
            all_data = []
            offset = 0
            batch_size = 5000

            while True:
                payload = {
                    "limit": batch_size,
                    "offset": offset,
                    "compareFilters": [{
                        "compareType": "EQUAL",
                        "fieldName": "settlementDate",
                        "fieldValue": latest_date
                    }],
                    "sortFields": ["-currentShortPositionQuantity"]
                }

                resp = requests.post(
                    f"{self.API_BASE}/{getattr(self, 'dataset_name', 'consolidatedShortInterest')}",
                    headers=headers,
                    json=payload
                )
                batch = resp.json()

                if not batch:
                    break

                all_data.extend(batch)
                print(f"  Fetched {len(all_data)} records...")

                if len(batch) < batch_size:
                    break

                offset += batch_size
                time.sleep(0.5)  # Rate limit 존중

            return all_data

        except Exception as e:
            print(f"❌ Error fetching all SI data: {e}")
            return None


# ============================================
# yfinance PRICE DATA
# ============================================
def fetch_price_data(symbols, period='5y'):
    """yfinance로 가격 히스토리 수집"""
    print(f"\n📈 Fetching price data for {len(symbols)} symbols...")
    all_prices = {}

    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)

            if hist.empty:
                print(f"  ⚠️  No price data for {symbol}")
                continue

            prices = []
            for date, row in hist.iterrows():
                prices.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'close': round(row['Close'], 2),
                    'volume': int(row['Volume']),
                })

            all_prices[symbol] = prices
            print(f"  ✅ {symbol}: {len(prices)} days")

        except Exception as e:
            print(f"  ❌ {symbol}: {e}")

        time.sleep(0.3)  # Rate limit

    return all_prices


# ============================================
# DATA PROCESSING & SAVE
# ============================================
def process_finra_data(raw_data, symbol):
    """FINRA 원시 데이터를 정리된 형식으로 변환"""
    if not raw_data:
        return []

    records = []
    for item in raw_data:
        records.append({
            'date': item.get('settlementDate', ''),
            'short_interest': item.get('currentShortPositionQuantity', 0),
            'prev_short_interest': item.get('previousShortPositionQuantity', 0),
            'change_pct': item.get('changePct', 0),
            'avg_daily_volume': item.get('averageDailyVolumeQuantity', 0),
            'days_to_cover': item.get('daysToCoverQuantity', 0),
        })

    return sorted(records, key=lambda x: x['date'])


def save_json(data, filepath):
    """JSON 파일 저장"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 Saved: {filepath}")


def build_market_overview(finra_client, price_data):
    """페이지 1: 시장 개요 데이터 빌드"""
    print("\n🏗️  Building market overview data...")

    overview = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'market_etfs': {},
        'sector_etfs': {},
        'leverage_etfs': {},
        'bond_etfs': {},
    }

    # 시장 ETF
    for symbol in MARKET_ETFS:
        si_data = finra_client.get_short_interest(symbol) if finra_client.token else None
        overview['market_etfs'][symbol] = {
            'price': price_data.get(symbol, []),
            'short_interest': process_finra_data(si_data, symbol),
        }
        time.sleep(0.3)

    # 섹터 ETF
    for symbol in SECTOR_ETFS:
        si_data = finra_client.get_short_interest(symbol) if finra_client.token else None
        overview['sector_etfs'][symbol] = {
            'price': price_data.get(symbol, []),
            'short_interest': process_finra_data(si_data, symbol),
        }
        time.sleep(0.3)

    # 레버리지 ETF
    for symbol in LEVERAGE_ETFS:
        si_data = finra_client.get_short_interest(symbol) if finra_client.token else None
        overview['leverage_etfs'][symbol] = {
            'price': price_data.get(symbol, []),
            'short_interest': process_finra_data(si_data, symbol),
        }
        time.sleep(0.3)

    # 채권 ETF
    for symbol in BOND_ETFS:
        si_data = finra_client.get_short_interest(symbol) if finra_client.token else None
        overview['bond_etfs'][symbol] = {
            'price': price_data.get(symbol, []),
            'short_interest': process_finra_data(si_data, symbol),
        }
        time.sleep(0.3)

    save_json(overview, DATA_DIR / 'market_overview.json')
    return overview


def build_korean_favorites(finra_client, price_data):
    """페이지 2: 서학개미 인기 종목 데이터 빌드"""
    print("\n🏗️  Building Korean favorites data...")

    favorites = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'stocks': {},
    }

    for symbol in KOREAN_FAVORITES:
        si_data = finra_client.get_short_interest(symbol) if finra_client.token else None
        favorites['stocks'][symbol] = {
            'price': price_data.get(symbol, []),
            'short_interest': process_finra_data(si_data, symbol),
        }
        time.sleep(0.3)

    save_json(favorites, DATA_DIR / 'korean_favorites.json')
    return favorites


def build_all_stocks_snapshot(finra_client):
    """페이지 3: 전체 종목 최신 스냅샷 (Top 20 + 검색용)"""
    print("\n🏗️  Building all stocks snapshot...")

    all_data = finra_client.get_all_latest_short_interest() if finra_client.token else None

    if not all_data:
        print("  ⚠️  No full data available, saving empty snapshot")
        snapshot = {'updated_at': datetime.now().strftime('%Y-%m-%d'), 'settlement_date': '', 'stocks': []}
        save_json(snapshot, DATA_DIR / 'all_stocks.json')
        return snapshot

    snapshot = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'settlement_date': all_data[0].get('settlementDate', '') if all_data else '',
        'stocks': [],
    }

    for item in all_data:
        si = item.get('currentShortPositionQuantity', 0)
        prev_si = item.get('previousShortPositionQuantity', 0)
        avg_vol = item.get('averageDailyVolumeQuantity', 0)

        snapshot['stocks'].append({
            'ticker': item.get('symbolCode', ''),
            'name': item.get('issueName', ''),
            'short_interest': si,
            'prev_short_interest': prev_si,
            'change_pct': round(((si - prev_si) / prev_si * 100) if prev_si > 0 else 0, 2),
            'avg_daily_volume': avg_vol,
            'days_to_cover': round(si / avg_vol, 2) if avg_vol > 0 else 0,
        })

    # 숏 인터레스트 기준 정렬
    snapshot['stocks'].sort(key=lambda x: x['short_interest'], reverse=True)

    save_json(snapshot, DATA_DIR / 'all_stocks.json')
    print(f"  📊 Total stocks in snapshot: {len(snapshot['stocks'])}")
    return snapshot


# ============================================
# MAIN
# ============================================
def main():
    print("=" * 60)
    print("🚀 FINRA Short Interest Data Collector")
    print(f"📅 Run time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Initialize
    DATA_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(exist_ok=True)

    # FINRA Authentication
    finra = FINRAClient()
    finra.authenticate()

    # Fetch price data for all tracked symbols
    price_data = fetch_price_data(ALL_TRACKED)

    # Build page data
    build_market_overview(finra, price_data)
    build_korean_favorites(finra, price_data)
    build_all_stocks_snapshot(finra)

    # Save individual history files (for search chart in page 3)
    print("\n🏗️  Saving individual history files...")
    for symbol in ALL_TRACKED:
        if symbol in price_data:
            si_data = finra.get_short_interest(symbol) if finra.token else None
            history = {
                'ticker': symbol,
                'price': price_data.get(symbol, []),
                'short_interest': process_finra_data(si_data, symbol),
            }
            save_json(history, HISTORY_DIR / f'{symbol}.json')
            time.sleep(0.2)

    print("\n" + "=" * 60)
    print("✅ Data collection complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
