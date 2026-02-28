"""
FINRA Short Interest + yfinance Price Data Collector
=====================================================
GitHub Actions에서 자동 실행하여 데이터를 수집합니다.

데이터 소스:
  1. FINRA Reg SHO Daily Short Sale Volume (일별 숏 거래량) - 확실히 작동
  2. FINRA Consolidated Short Interest (반월 숏 잔량) - 엔드포인트 자동 탐지
  3. yfinance 가격 데이터

환경변수:
  FINRA_CLIENT_ID - FINRA API Client ID
  FINRA_CLIENT_SECRET - FINRA API Client Secret
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
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"

# 페이지 1: 시장 개요 ETF
MARKET_ETFS = ['SPY', 'QQQ', 'IWM']
SECTOR_ETFS = ['XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLC', 'XLP', 'XLU', 'XLY', 'XLB', 'XLRE']
LEVERAGE_ETFS = ['TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'TNA', 'TZA', 'UVXY', 'SVXY']
BOND_ETFS = ['TLT', 'HYG', 'LQD']

# 페이지 2: 서학개미 인기 종목
KOREAN_FAVORITES = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA',
    'AMD', 'AVGO', 'QCOM', 'MU', 'INTC', 'ARM', 'SMCI',
    'PLTR', 'SNOW', 'CRM', 'NOW', 'SHOP',
    'COIN', 'SOFI', 'SQ', 'PYPL', 'V', 'MA',
    'LLY', 'NVO', 'MRNA', 'PFE',
    'XOM', 'CVX', 'LMT', 'BA',
    'NFLX', 'DIS', 'NIO', 'RIVN', 'RBLX', 'MARA',
]

ALL_TRACKED = list(set(
    MARKET_ETFS + SECTOR_ETFS + LEVERAGE_ETFS + BOND_ETFS + KOREAN_FAVORITES
))


# ============================================
# FINRA API CLIENT
# ============================================
class FINRAClient:
    TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
    
    # 시도할 엔드포인트 조합들 (순서대로 시도)
    SHORT_INTEREST_ENDPOINTS = [
        ("otcMarket", "consolidatedShortInterest"),
        ("equity", "consolidatedShortInterest"),
        ("otcMarket", "equityShortInterestStandardized"),
        ("otcMarket", "EquityShortInterest"),
    ]
    
    # Reg SHO Daily Short Sale Volume - 확실히 작동하는 엔드포인트
    REG_SHO_GROUP = "otcMarket"
    REG_SHO_DATASET = "regShoDaily"

    def __init__(self):
        self.client_id = os.environ.get('FINRA_CLIENT_ID', '')
        self.client_secret = os.environ.get('FINRA_CLIENT_SECRET', '')
        self.token = None
        self.si_endpoint = None  # 발견된 short interest 엔드포인트
        self.si_symbol_field = None  # 심볼 필드명
        self.si_fields = {}

    def authenticate(self):
        if not self.client_id or not self.client_secret:
            print("⚠️  FINRA credentials not found.")
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
            print("✅ FINRA API authenticated")
            self._discover_endpoints()
            return True
        except Exception as e:
            print(f"❌ FINRA auth failed: {e}")
            return False

    def _get_headers(self):
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def _discover_endpoints(self):
        """사용 가능한 엔드포인트 자동 탐지"""
        print("\n🔍 Discovering FINRA API endpoints...")
        
        # 1) Reg SHO Daily - 메타데이터 확인
        meta_url = f"https://api.finra.org/metadata/group/{self.REG_SHO_GROUP}/name/{self.REG_SHO_DATASET}"
        try:
            resp = requests.get(meta_url, headers=self._get_headers())
            if resp.status_code == 200:
                meta = resp.json()
                fields = [f['name'] for f in meta.get('fields', [])]
                print(f"  ✅ Reg SHO Daily: {self.REG_SHO_GROUP}/{self.REG_SHO_DATASET}")
                print(f"     Fields: {fields}")
            else:
                print(f"  ⚠️  Reg SHO metadata: {resp.status_code}")
        except Exception as e:
            print(f"  ⚠️  Reg SHO metadata error: {e}")

        # 2) Short Interest 엔드포인트 탐색
        for group, name in self.SHORT_INTEREST_ENDPOINTS:
            meta_url = f"https://api.finra.org/metadata/group/{group}/name/{name}"
            try:
                resp = requests.get(meta_url, headers=self._get_headers())
                if resp.status_code == 200:
                    meta = resp.json()
                    fields = [f['name'] for f in meta.get('fields', [])]
                    print(f"  ✅ Short Interest found: {group}/{name}")
                    print(f"     Fields: {fields}")
                    self.si_endpoint = f"https://api.finra.org/data/group/{group}/name/{name}"
                    self.si_fields = {f['name']: f for f in meta.get('fields', [])}
                    # 심볼 필드 자동 감지
                    for possible in ['symbolCode', 'issueSymbolIdentifier', 'symbol']:
                        if possible in self.si_fields:
                            self.si_symbol_field = possible
                            break
                    print(f"     Symbol field: {self.si_symbol_field}")
                    return
                else:
                    print(f"  ⏭️  {group}/{name}: {resp.status_code}")
            except Exception as e:
                print(f"  ⏭️  {group}/{name}: {e}")
        
        # 3) GET datasets 로 전체 목록 조회 시도
        print("  🔍 Trying GET datasets list...")
        try:
            resp = requests.get(
                "https://api.finra.org/data/group/otcMarket",
                headers=self._get_headers()
            )
            if resp.status_code == 200:
                datasets = resp.json()
                print(f"  📋 Available otcMarket datasets: {datasets}")
        except:
            pass

        try:
            resp = requests.get(
                "https://api.finra.org/data/group/equity",
                headers=self._get_headers()
            )
            if resp.status_code == 200:
                datasets = resp.json()
                print(f"  📋 Available equity datasets: {datasets}")
        except:
            pass

        print("  ⚠️  No Short Interest endpoint found - will use Reg SHO Daily only")

    def get_reg_sho_daily(self, symbol, limit=5000):
        """
        Reg SHO Daily Short Sale Volume 조회
        일별 숏 거래량/전체 거래량 데이터
        """
        if not self.token:
            return None

        url = f"https://api.finra.org/data/group/{self.REG_SHO_GROUP}/name/{self.REG_SHO_DATASET}"
        payload = {
            "limit": limit,
            "compareFilters": [{
                "compareType": "EQUAL",
                "fieldName": "securitiesInformationProcessorSymbolIdentifier",
                "fieldValue": symbol
            }]
        }

        try:
            resp = requests.post(url, headers=self._get_headers(), json=payload)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"  ❌ regShoDaily for {symbol}: {resp.status_code} - {resp.text[:200]}")
                return None
        except Exception as e:
            print(f"  ❌ regShoDaily for {symbol}: {e}")
            return None

    def get_short_interest(self, symbol, limit=5000):
        """Consolidated Short Interest 조회 (엔드포인트가 발견된 경우만)"""
        if not self.token or not self.si_endpoint:
            return None

        payload = {
            "limit": limit,
            "offset": 0,
            "compareFilters": [{
                "compareType": "EQUAL",
                "fieldName": self.si_symbol_field,
                "fieldValue": symbol
            }]
        }

        try:
            resp = requests.post(self.si_endpoint, headers=self._get_headers(), json=payload)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"  ❌ SI for {symbol}: {resp.status_code}")
                return None
        except Exception as e:
            print(f"  ❌ SI for {symbol}: {e}")
            return None

    def get_all_latest_reg_sho(self):
        """최신 거래일의 전체 Reg SHO 데이터"""
        if not self.token:
            return None

        url = f"https://api.finra.org/data/group/{self.REG_SHO_GROUP}/name/{self.REG_SHO_DATASET}"
        
        try:
            # sortFields 없이 소량 가져와서 최신 날짜 파악
            resp = requests.post(url, headers=self._get_headers(), json={"limit": 100})
            if resp.status_code != 200:
                print(f"  ❌ Latest date fetch: {resp.status_code} - {resp.text[:200]}")
                return None
            
            sample = resp.json()
            if not sample:
                return None
            
            # 클라이언트에서 최신 날짜 찾기
            dates = [r.get('tradeReportDate', '') for r in sample if r.get('tradeReportDate')]
            if not dates:
                return None
            latest_date = max(dates)
            print(f"  📅 Latest Reg SHO date: {latest_date}")

            # 해당 날짜 전체 데이터 조회 (파티션 키 지정했으니 페이지네이션 가능)
            all_data = []
            offset = 0
            batch_size = 5000

            while True:
                payload = {
                    "limit": batch_size,
                    "offset": offset,
                    "compareFilters": [{
                        "compareType": "EQUAL",
                        "fieldName": "tradeReportDate",
                        "fieldValue": latest_date
                    }]
                }
                resp = requests.post(url, headers=self._get_headers(), json=payload)
                if resp.status_code != 200:
                    print(f"  ❌ Batch fetch: {resp.status_code}")
                    break
                batch = resp.json()
                if not batch:
                    break
                all_data.extend(batch)
                print(f"    Fetched {len(all_data)} records...")
                if len(batch) < batch_size:
                    break
                offset += batch_size
                time.sleep(0.5)

            return all_data

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return None


# ============================================
# yfinance PRICE DATA
# ============================================
def fetch_price_data(symbols, period='5y'):
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
        time.sleep(0.3)
    return all_prices


# ============================================
# DATA PROCESSING
# ============================================
def process_reg_sho(raw_data):
    """Reg SHO Daily → 정리된 형식"""
    if not raw_data:
        return []
    records = []
    for item in raw_data:
        short_vol = item.get('totalShortTradeQuantity', 0) or 0
        total_vol = item.get('totalVolumeQuantity', 0) or 0
        records.append({
            'date': item.get('tradeReportDate', ''),
            'short_volume': short_vol,
            'total_volume': total_vol,
            'short_ratio': round(short_vol / total_vol * 100, 2) if total_vol > 0 else 0,
        })
    return sorted(records, key=lambda x: x['date'])


def process_short_interest(raw_data):
    """Consolidated Short Interest → 정리된 형식"""
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
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 Saved: {filepath}")


# ============================================
# PAGE BUILDERS
# ============================================
def build_symbol_data(finra, symbol, price_data):
    """개별 종목 데이터 수집"""
    result = {
        'price': price_data.get(symbol, []),
        'short_volume': [],
        'short_interest': [],
    }
    
    # Reg SHO Daily (확실히 작동)
    reg_sho = finra.get_reg_sho_daily(symbol) if finra.token else None
    result['short_volume'] = process_reg_sho(reg_sho)
    
    # Consolidated Short Interest (가능한 경우만)
    if finra.si_endpoint:
        si_data = finra.get_short_interest(symbol)
        result['short_interest'] = process_short_interest(si_data)
    
    time.sleep(0.3)
    return result


def build_market_overview(finra, price_data):
    print("\n🏗️  Building market overview data...")
    overview = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'market_etfs': {},
        'sector_etfs': {},
        'leverage_etfs': {},
        'bond_etfs': {},
    }

    for label, symbols, key in [
        ("Market ETFs", MARKET_ETFS, 'market_etfs'),
        ("Sector ETFs", SECTOR_ETFS, 'sector_etfs'),
        ("Leverage ETFs", LEVERAGE_ETFS, 'leverage_etfs'),
        ("Bond ETFs", BOND_ETFS, 'bond_etfs'),
    ]:
        print(f"  📊 {label}...")
        for symbol in symbols:
            overview[key][symbol] = build_symbol_data(finra, symbol, price_data)

    save_json(overview, DATA_DIR / 'market_overview.json')
    return overview


def build_korean_favorites(finra, price_data):
    print("\n🏗️  Building Korean favorites data...")
    favorites = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'stocks': {},
    }
    for symbol in KOREAN_FAVORITES:
        favorites['stocks'][symbol] = build_symbol_data(finra, symbol, price_data)

    save_json(favorites, DATA_DIR / 'korean_favorites.json')
    return favorites


def build_all_stocks_snapshot(finra):
    print("\n🏗️  Building all stocks snapshot...")
    all_data = finra.get_all_latest_reg_sho() if finra.token else None

    if not all_data:
        print("  ⚠️  No data, saving empty snapshot")
        snapshot = {'updated_at': datetime.now().strftime('%Y-%m-%d'), 'trade_date': '', 'stocks': []}
        save_json(snapshot, DATA_DIR / 'all_stocks.json')
        return snapshot

    snapshot = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'trade_date': all_data[0].get('tradeReportDate', ''),
        'stocks': [],
    }

    for item in all_data:
        short_vol = item.get('totalShortTradeQuantity', 0) or 0
        total_vol = item.get('totalVolumeQuantity', 0) or 0
        snapshot['stocks'].append({
            'ticker': item.get('securitiesInformationProcessorSymbolIdentifier', ''),
            'short_volume': short_vol,
            'total_volume': total_vol,
            'short_ratio': round(short_vol / total_vol * 100, 2) if total_vol > 0 else 0,
        })

    snapshot['stocks'].sort(key=lambda x: x['short_volume'], reverse=True)
    save_json(snapshot, DATA_DIR / 'all_stocks.json')
    print(f"  📊 Total stocks: {len(snapshot['stocks'])}")
    return snapshot


# ============================================
# MAIN
# ============================================
def main():
    print("=" * 60)
    print("🚀 FINRA Short Interest Data Collector")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    DATA_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(exist_ok=True)

    # FINRA
    finra = FINRAClient()
    finra.authenticate()

    # Price data
    price_data = fetch_price_data(ALL_TRACKED)

    # Build pages
    build_market_overview(finra, price_data)
    build_korean_favorites(finra, price_data)
    build_all_stocks_snapshot(finra)

    # Individual history files
    print("\n🏗️  Saving individual history files...")
    for symbol in ALL_TRACKED:
        if symbol in price_data:
            data = build_symbol_data(finra, symbol, price_data)
            data['ticker'] = symbol
            save_json(data, HISTORY_DIR / f'{symbol}.json')

    print("\n" + "=" * 60)
    print("✅ Complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
