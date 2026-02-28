"""
FINRA Short Volume + yfinance Price Data Collector
=====================================================
GitHub Actions에서 자동 실행하여 데이터를 수집합니다.

전략 (벌크 우선):
  1. FINRA regShoDaily 벌크 데이터를 날짜별로 가져옴
     (개별 심볼 쿼리는 파티션 키 문제로 빈 결과 → 날짜 기준 벌크가 확실함)
  2. 심볼별로 인덱싱하여 히스토리 구축
  3. 기존 히스토리 파일에 새 데이터 누적 (incremental)

데이터 소스:
  - FINRA Reg SHO Daily Short Sale Volume (일별 숏 거래량)
  - yfinance 가격 데이터 (5년)

환경변수:
  FINRA_CLIENT_ID - FINRA API Client ID
  FINRA_CLIENT_SECRET - FINRA API Client Secret
"""

import os
import json
import time
import requests
import yfinance as yf
from datetime import datetime
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

# 첫 실행 시 가져올 최근 거래일 수 (약 1개월)
INITIAL_DAYS = 20
# 매 실행 시 가져올 최근 날짜 수
INCREMENTAL_DAYS = 3


# ============================================
# FINRA API CLIENT
# ============================================
class FINRAClient:
    TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
    REG_SHO_GROUP = "otcMarket"
    REG_SHO_DATASET = "regShoDaily"

    def __init__(self):
        self.client_id = os.environ.get('FINRA_CLIENT_ID', '')
        self.client_secret = os.environ.get('FINRA_CLIENT_SECRET', '')
        self.token = None

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
            self.token = resp.json()['access_token']
            print("✅ FINRA API authenticated")
            return True
        except Exception as e:
            print(f"❌ FINRA auth failed: {e}")
            return False

    def _headers(self):
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def _api_url(self):
        return f"https://api.finra.org/data/group/{self.REG_SHO_GROUP}/name/{self.REG_SHO_DATASET}"

    def discover_recent_dates(self, n=5):
        """
        최근 N개 거래일 발견
        1) 소량 fetch로 최신 날짜 확인
        2) 비즈니스 데이 역산으로 후보 날짜 생성
        3) 각 후보 날짜에 데이터 존재 여부 확인
        """
        if not self.token:
            return []

        url = self._api_url()

        # Step 1: 최신 날짜 발견
        try:
            resp = requests.post(url, headers=self._headers(), json={"limit": 100})
            if resp.status_code != 200:
                print(f"  ❌ Date discovery: {resp.status_code}")
                return []
            sample = resp.json()
            if not sample:
                return []
            dates_found = set(r.get('tradeReportDate', '') for r in sample if r.get('tradeReportDate'))
            if not dates_found:
                return []
            latest = max(dates_found)
            print(f"  📅 Latest date from sample: {latest}")
        except Exception as e:
            print(f"  ❌ Date discovery error: {e}")
            return []

        # Step 2: 비즈니스 데이 역산으로 후보 생성
        from datetime import timedelta
        dt = datetime.strptime(latest, '%Y-%m-%d')
        candidates = [latest]
        check_dt = dt
        # n*2 후보를 만들어서 (공휴일 감안)
        while len(candidates) < n * 2 and len(candidates) < 200:
            check_dt -= timedelta(days=1)
            if check_dt.weekday() < 5:  # 월~금
                candidates.append(check_dt.strftime('%Y-%m-%d'))

        # Step 3: 각 후보 날짜 검증 (limit=1로 빠르게)
        valid_dates = []
        for dt_str in candidates:
            if len(valid_dates) >= n:
                break
            if dt_str in dates_found:
                valid_dates.append(dt_str)
                continue
            try:
                resp = requests.post(url, headers=self._headers(), json={
                    "limit": 1,
                    "compareFilters": [{
                        "compareType": "EQUAL",
                        "fieldName": "tradeReportDate",
                        "fieldValue": dt_str
                    }]
                })
                if resp.status_code == 200 and resp.json():
                    valid_dates.append(dt_str)
                time.sleep(0.1)
            except:
                pass

        valid_dates.sort(reverse=True)
        print(f"  📅 Valid dates ({len(valid_dates)}): {valid_dates[:10]}{'...' if len(valid_dates)>10 else ''}")
        return valid_dates

    def fetch_bulk_by_date(self, trade_date):
        """특정 날짜의 전체 Reg SHO 데이터 (파티션 키 = tradeReportDate)"""
        if not self.token:
            return []

        url = self._api_url()
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
                    "fieldValue": trade_date
                }]
            }
            try:
                resp = requests.post(url, headers=self._headers(), json=payload)
                if resp.status_code != 200:
                    print(f"    ❌ Batch {trade_date} offset={offset}: {resp.status_code}")
                    break
                batch = resp.json()
                if not batch:
                    break
                all_data.extend(batch)
                if len(batch) < batch_size:
                    break
                offset += batch_size
                time.sleep(0.3)
            except Exception as e:
                print(f"    ❌ Batch error: {e}")
                break

        return all_data

    def fetch_multi_date_bulk(self, dates):
        """
        여러 날짜의 벌크 데이터를 가져와서 심볼별로 인덱싱
        Returns: {symbol: [{date, short_volume, total_volume, short_ratio}, ...]}
        """
        symbol_index = {}

        for i, dt in enumerate(dates):
            print(f"  📊 [{i+1}/{len(dates)}] Fetching {dt}...")
            raw = self.fetch_bulk_by_date(dt)
            count = 0

            for item in raw:
                sym = item.get('securitiesInformationProcessorSymbolIdentifier', '')
                if not sym:
                    continue
                short_vol = item.get('totalShortTradeQuantity', 0) or 0
                total_vol = item.get('totalVolumeQuantity', 0) or 0
                record = {
                    'date': dt,
                    'short_volume': short_vol,
                    'total_volume': total_vol,
                    'short_ratio': round(short_vol / total_vol * 100, 2) if total_vol > 0 else 0,
                }
                if sym not in symbol_index:
                    symbol_index[sym] = []
                symbol_index[sym].append(record)
                count += 1

            print(f"    → {count} symbols")
            time.sleep(0.5)

        # 각 심볼의 레코드를 날짜순 정렬
        for sym in symbol_index:
            symbol_index[sym].sort(key=lambda x: x['date'])

        print(f"  ✅ Indexed {len(symbol_index)} unique symbols across {len(dates)} dates")
        return symbol_index


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
# HISTORY ACCUMULATION
# ============================================
def load_existing_history(symbol):
    """기존 히스토리 파일 로드"""
    filepath = HISTORY_DIR / f'{symbol}.json'
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return None


def merge_short_volume(existing_sv, new_sv):
    """기존 short_volume에 새 데이터 병합 (중복 날짜 → 새 데이터로 덮어씀)"""
    date_map = {}
    for r in (existing_sv or []):
        date_map[r['date']] = r
    for r in (new_sv or []):
        date_map[r['date']] = r
    return sorted(date_map.values(), key=lambda x: x['date'])


# ============================================
# DATA SAVING
# ============================================
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 Saved: {filepath}")


# ============================================
# PAGE BUILDERS
# ============================================
def build_symbol_data(symbol, price_data, symbol_sv_index):
    """개별 종목 데이터 조합 (기존 히스토리 + 새 벌크 데이터 병합)"""
    existing = load_existing_history(symbol)
    existing_sv = existing.get('short_volume', []) if existing else []
    new_sv = symbol_sv_index.get(symbol, [])
    merged_sv = merge_short_volume(existing_sv, new_sv)

    return {
        'price': price_data.get(symbol, []),
        'short_volume': merged_sv,
    }


def build_market_overview(price_data, symbol_sv_index):
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
            overview[key][symbol] = build_symbol_data(symbol, price_data, symbol_sv_index)

    save_json(overview, DATA_DIR / 'market_overview.json')
    return overview


def build_korean_favorites(price_data, symbol_sv_index):
    print("\n🏗️  Building Korean favorites data...")
    favorites = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'stocks': {},
    }
    for symbol in KOREAN_FAVORITES:
        favorites['stocks'][symbol] = build_symbol_data(symbol, price_data, symbol_sv_index)

    save_json(favorites, DATA_DIR / 'korean_favorites.json')
    return favorites


def build_all_stocks_snapshot(symbol_sv_index, latest_date):
    """최신 날짜의 전체 종목 스냅샷"""
    print("\n🏗️  Building all stocks snapshot...")

    stocks = []
    for sym, records in symbol_sv_index.items():
        latest = [r for r in records if r['date'] == latest_date]
        if latest:
            r = latest[0]
            stocks.append({
                'ticker': sym,
                'short_volume': r['short_volume'],
                'total_volume': r['total_volume'],
                'short_ratio': r['short_ratio'],
            })

    stocks.sort(key=lambda x: x['short_volume'], reverse=True)

    snapshot = {
        'updated_at': datetime.now().strftime('%Y-%m-%d'),
        'trade_date': latest_date,
        'stocks': stocks,
    }
    save_json(snapshot, DATA_DIR / 'all_stocks.json')
    print(f"  📊 Total stocks: {len(stocks)}")
    return snapshot


# ============================================
# MAIN
# ============================================
def main():
    print("=" * 60)
    print("🚀 FINRA Short Volume Data Collector")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    DATA_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(exist_ok=True)

    # ---- FINRA 인증 ----
    finra = FINRAClient()
    finra.authenticate()

    # ---- 가져올 날짜 수 결정 ----
    sample_file = HISTORY_DIR / 'SPY.json'
    has_existing = sample_file.exists()
    if has_existing:
        try:
            with open(sample_file) as f:
                spy_data = json.load(f)
            existing_count = len(spy_data.get('short_volume', []))
            print(f"\n📦 Incremental mode (SPY has {existing_count} existing records)")
        except Exception:
            has_existing = False

    num_dates = INCREMENTAL_DAYS if has_existing else INITIAL_DAYS
    if not has_existing:
        print(f"\n📦 Initial mode: will fetch {num_dates} dates")

    # ---- 최근 날짜 발견 ----
    print("\n🔍 Discovering recent trading dates...")
    available_dates = finra.discover_recent_dates(n=num_dates) if finra.token else []

    if not available_dates:
        print("⚠️  No dates found — building with price data only")
        price_data = fetch_price_data(ALL_TRACKED)
        build_market_overview(price_data, {})
        build_korean_favorites(price_data, {})
        return

    latest_date = available_dates[0]

    # ---- 벌크 데이터 가져오기 ----
    print(f"\n📥 Fetching bulk Reg SHO data for {len(available_dates)} dates...")
    symbol_sv_index = finra.fetch_multi_date_bulk(available_dates)

    # 추적 종목 데이터 확인
    tracked_found = sum(1 for s in ALL_TRACKED if s in symbol_sv_index)
    print(f"  📋 Tracked symbols with data: {tracked_found}/{len(ALL_TRACKED)}")
    missing = [s for s in ALL_TRACKED if s not in symbol_sv_index]
    if missing:
        print(f"  ⚠️  Missing: {missing[:10]}{'...' if len(missing)>10 else ''}")

    # ---- 가격 데이터 ----
    price_data = fetch_price_data(ALL_TRACKED)

    # ---- 페이지 빌드 ----
    build_market_overview(price_data, symbol_sv_index)
    build_korean_favorites(price_data, symbol_sv_index)
    build_all_stocks_snapshot(symbol_sv_index, latest_date)

    # ---- 개별 히스토리 파일 저장 (누적) ----
    print("\n🏗️  Saving individual history files...")
    saved = 0
    for symbol in ALL_TRACKED:
        data = build_symbol_data(symbol, price_data, symbol_sv_index)
        data['ticker'] = symbol
        save_json(data, HISTORY_DIR / f'{symbol}.json')
        saved += 1

    # ---- 완료 ----
    print(f"\n{'='*60}")
    print(f"✅ Complete!")
    print(f"  📊 Tracked: {tracked_found}/{len(ALL_TRACKED)} symbols with short data")
    print(f"  📅 Dates: {available_dates[-1]} → {latest_date} ({len(available_dates)} days)")
    print(f"  🌐 Total symbols: {len(symbol_sv_index)}")
    print(f"  💾 History files saved: {saved}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
