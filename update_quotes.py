"""
Fetch latest quotes for the top-50 global stock market indices and write
them to quotes.json for the globe to consume.

Uses yfinance (https://github.com/ranaroussi/yfinance), which is an
open-source library that pulls from Yahoo Finance's public endpoints.
No API key required. Runs server-side so there are no CORS issues.

Designed to be run by a GitHub Actions cron workflow; can also be run
locally with `python update_quotes.py`.
"""

import json
import math
import sys
import time
from datetime import datetime, timezone

import yfinance as yf


# Each entry: (iso_numeric_country_id, country_name, index_name, yahoo_symbol,
#              country_market_cap_usd_tn)
#
# The market cap column is the COMBINED market capitalization of all stock
# exchanges in that country (in USD trillions), as of October 2025, used
# solely for ranking countries on the top-10 table on the main panel.
# Source: Wikipedia "List of major stock exchanges" ($1 Trillion Club).
# Countries with multiple indices share the same country-level figure.
# Small/emerging markets outside the $1 Trillion Club are given small
# positive values so they still get individual per-country ranking but
# always fall below the top 10.
MARKETS = [
    ('840', 'United States',        'S&P 500',               '^GSPC',       86.9),
    ('840', 'United States',        'Dow Jones',             '^DJI',        86.9),
    ('840', 'United States',        'Nasdaq Composite',      '^IXIC',       86.9),
    ('156', 'China',                'Shanghai Composite',    '000001.SS',   14.03),
    ('156', 'China',                'Shenzhen Component',    '399001.SZ',   14.03),
    ('392', 'Japan',                'Nikkei 225',            '^N225',        7.59),
    ('344', 'Hong Kong',            'Hang Seng',             '^HSI',         6.17),
    ('826', 'United Kingdom',       'FTSE 100',              '^FTSE',        3.14),
    ('826', 'United Kingdom',       'FTSE 250',              '^FTMC',        3.14),
    ('276', 'Germany',              'DAX',                   '^GDAXI',       2.04),
    ('250', 'France',               'CAC 40',                '^FCHI',        1.20),  # Paris via Euronext share
    ('356', 'India',                'BSE Sensex',            '^BSESN',      10.57),
    ('356', 'India',                'Nifty 50',              '^NSEI',       10.57),
    ('124', 'Canada',               'S&P/TSX Composite',     '^GSPTSE',      4.00),
    ('410', 'South Korea',          'KOSPI',                 '^KS11',        2.95),
    ('036', 'Australia',            'ASX 200',               '^AXJO',        1.89),
    ('756', 'Switzerland',          'SMI',                   '^SSMI',        1.97),
    ('158', 'Taiwan',               'TAIEX',                 '^TWII',        2.87),
    ('724', 'Spain',                'IBEX 35',               '^IBEX',        0.80),
    ('076', 'Brazil',               'Bovespa',               '^BVSP',        1.01),
    ('380', 'Italy',                'FTSE MIB',              'FTSEMIB.MI',   0.90),
    ('528', 'Netherlands',          'AEX',                   '^AEX',         1.10),
    ('702', 'Singapore',            'STI',                   '^STI',         0.60),
    ('484', 'Mexico',               'IPC',                   '^MXX',         0.50),
    ('752', 'Sweden',               'OMXS30',                '^OMX',         1.00),
    ('056', 'Belgium',              'BEL 20',                '^BFX',         0.40),
    ('710', 'South Africa',         'JSE Top 40',            '^JN0U.JO',     1.15),
    # ('578', 'Norway',               'OBX',                   'OBX.OL',       0.35),  # dropped: history data unreliable
    ('246', 'Finland',              'OMXH25',                '^OMXH25',      0.30),
    ('208', 'Denmark',              'OMXC25',                '^OMXC25',      0.60),
    ('040', 'Austria',              'ATX',                   '^ATX',         0.15),
    ('620', 'Portugal',             'PSI 20',                'PSI20.LS',     0.08),
    ('300', 'Greece',               'ATHEX Composite',       'GD.AT',        0.10),
    ('372', 'Ireland',              'ISEQ Overall',          '^ISEQ',        0.14),
    # ('616', 'Poland',               'WIG20',                 'WIG20.WA',     0.45),  # dropped: broken ticker
    # ('348', 'Hungary',              'BUX',                   '^BUX.BD',      0.05),  # dropped: broken ticker
    # ('203', 'Czechia',              'PX',                    '^PX',          0.07),  # dropped: broken ticker
    ('792', 'Turkey',               'BIST 100',              'XU100.IS',     0.30),
    ('376', 'Israel',               'TA-125',                '^TA125.TA',    0.30),
    ('682', 'Saudi Arabia',         'Tadawul All Share',     '^TASI.SR',     2.73),
    # ('784', 'United Arab Emirates', 'DFM General',           '^DFMGI',       0.95),  # dropped: broken ticker
    # ('634', 'Qatar',                'QE General',            '^QSI',         0.17),  # dropped: broken ticker
    # ('818', 'Egypt',                'EGX 30',                '^CASE30',      0.04),  # dropped: broken ticker
    ('360', 'Indonesia',            'Jakarta Composite',     '^JKSE',        0.70),
    ('458', 'Malaysia',             'KLCI',                  '^KLSE',        0.40),
    ('764', 'Thailand',             'SET',                   '^SET.BK',      0.45),
    ('608', 'Philippines',          'PSEi',                  'PSEI.PS',      0.25),
    # ('704', 'Vietnam',              'VN Index',              '^VNINDEX.VN',  0.28),  # dropped: broken ticker
    # ('586', 'Pakistan',             'KSE 100',               '^KSE',         0.05),  # dropped: broken ticker
    ('554', 'New Zealand',          'NZX 50',                '^NZ50',        0.16),
    ('032', 'Argentina',            'S&P Merval',            '^MERV',        0.10),
    # ('152', 'Chile',                'IPSA',                  '^IPSA',        0.20),  # dropped: broken ticker
    # ('170', 'Colombia',             'COLCAP',                '^COLCAP',      0.10),  # dropped: broken ticker
    # ('604', 'Peru',                 'S&P/BVL Peru General',  'SPBLPGPT.LM',  0.08),  # dropped: broken ticker
    # ('643', 'Russia',               'MOEX',                  'IMOEX.ME',     0.70),  # dropped: broken ticker

    # --- Extended coverage. Previous iteration tried to add ~50 more
    # frontier markets but most of those Yahoo tickers I guessed at don't
    # actually exist (Yahoo's coverage of African/Central-Asian/Caribbean
    # exchanges is very thin). Removed the guesses; kept only tickers
    # that Yahoo actually hosts based on their world-indices page. Other
    # countries still appear on the globe but in the untracked-land color.

    # Additional verified tickers
    ('036', 'Australia',            'All Ordinaries',        '^AORD',        1.89),  # additional AU index
    ('036', 'Australia',            'S&P/ASX 200',           '^AXJO',        1.89),  # already present, leaving to note
]


# dedupe by exact tuple in case the list has accidental duplicates
_seen = set()
_unique = []
for _entry in MARKETS:
    _key = (_entry[0], _entry[3])   # country_id + symbol
    if _key in _seen:
        continue
    _seen.add(_key)
    _unique.append(_entry)
MARKETS = _unique


def fetch_one(symbol: str) -> dict | None:
    """Fetch a single symbol's latest quote plus multi-period changes.

    Uses .history() as the source of truth rather than .fast_info —
    the latter has a known bug where accessing .last_price or
    .previous_close throws AttributeError('_dividends') for some
    instruments (typically indices that don't pay dividends, like ^PX,
    ^QSI, IMOEX.ME etc.). History is more reliable.
    """
    try:
        t = yf.Ticker(symbol)

        # Pull ~14 months of daily history. This is the ONE call we depend
        # on — it also gives us the 1-week, 1-month, 1-year lookbacks.
        hist = t.history(period='14mo', interval='1d', auto_adjust=True)
        if hist is None or hist.empty or 'Close' not in hist.columns:
            print(f'  ! {symbol}: no data returned (invalid ticker or delisted)', file=sys.stderr)
            return None

        # Current price = last close. Previous close = second-to-last.
        # This is standard practice when fast_info is unreliable.
        closes = hist['Close'].dropna()
        if len(closes) < 2:
            print(f'  ! {symbol}: insufficient history ({len(closes)} closes)', file=sys.stderr)
            return None
        price = float(closes.iloc[-1])
        prev  = float(closes.iloc[-2])
        change = price - prev
        pct = (change / prev) * 100 if prev else 0.0

        pct_week  = _pct_from_ago(hist, price, 7)
        pct_month = _pct_from_ago(hist, price, 30)
        pct_year  = _pct_from_ago(hist, price, 365)

        # Day high/low from the latest bar
        last_row = hist.iloc[-1]
        day_high = _num(last_row.get('High'))
        day_low  = _num(last_row.get('Low'))

        # 52-week high/low from the trailing ~1-year window of history
        import pandas as pd
        year_ago = hist.index[-1] - pd.Timedelta(days=365)
        last_year = hist.loc[hist.index >= year_ago]
        year_high = _num(last_year['High'].max()) if 'High' in last_year else None
        year_low  = _num(last_year['Low'].min())  if 'Low'  in last_year else None

        # Currency / exchange metadata — try fast_info but don't fail if
        # it breaks (it often does for indices).
        currency = None
        exchange = None
        try:
            fi = t.fast_info
            currency = fi.currency
            exchange = fi.exchange
        except Exception:
            pass

        # 30-day sparkline: last ~30 daily closes. We emit just the
        # numeric values (floats rounded to 4dp), not the dates — the
        # client draws them as a monotonic time-series since equal spacing
        # reads fine for a 30-day trend visualization.
        spark30 = [
            round(float(v), 4)
            for v in closes.iloc[-30:].tolist()
            if v is not None and math.isfinite(float(v))
        ]

        # 90-day daily returns for correlation analysis. Returns (not
        # prices) is what correlation should be computed on — prices are
        # non-stationary and would just measure shared trends, not
        # co-movement. We compute pct_change on the last ~91 closes to
        # get 90 returns. Stored under '_returns90' (leading underscore
        # = private; main() uses these but does NOT emit them in the
        # final JSON to keep file size down).
        returns_series = closes.pct_change().dropna()
        returns90 = [
            round(float(v), 6)
            for v in returns_series.iloc[-90:].tolist()
            if v is not None and math.isfinite(float(v))
        ]

        return {
            'price': round(price, 4),
            'previousClose': round(prev, 4),
            'change': round(change, 4),
            'changePct': round(pct, 4),
            'changePct1w':  _round(pct_week),
            'changePct1m':  _round(pct_month),
            'changePct1y':  _round(pct_year),
            'currency': currency,
            'exchange': exchange,
            'dayHigh': day_high,
            'dayLow': day_low,
            'yearHigh': year_high,
            'yearLow': year_low,
            'spark30': spark30,
            '_returns90': returns90,   # private: stripped before JSON write
        }
    except Exception as e:
        print(f'  ! {symbol}: {type(e).__name__}: {e}', file=sys.stderr)
        return None


def _pct_from_ago(hist, current_price: float, days: int):
    """Return percent change of current_price vs. the close price ~`days`
    calendar days ago. Uses the trading day on or just before the target
    date; if the target falls before the first bar in the history (can
    happen at 365d for shorter series), falls back to the earliest
    available bar so we return a best-effort figure rather than None."""
    if hist is None or hist.empty or 'Close' not in hist.columns:
        return None
    try:
        import pandas as pd
        target = hist.index[-1] - pd.Timedelta(days=days)
        # Closes on or before the target date; use the last (most recent) one
        eligible = hist.loc[hist.index <= target, 'Close']
        if eligible.empty:
            # Target predates our history. For 1-year lookbacks we'd rather
            # return an approximate number based on the earliest bar than
            # no number at all. Only do this when we've got at least ~80%
            # of the requested span — otherwise the number would be
            # misleading.
            span_days = (hist.index[-1] - hist.index[0]).days
            if span_days >= days * 0.8:
                past = float(hist['Close'].iloc[0])
            else:
                return None
        else:
            past = float(eligible.iloc[-1])
        if past == 0:
            return None
        return ((current_price - past) / past) * 100.0
    except Exception:
        return None


def _round(v):
    """Round to 4 dp, returning None for missing/NaN/inf.

    NaN passes isinstance(v, float) and survives round(), but isn't
    valid JSON — standard JSON.parse() will reject a file containing
    the literal "NaN". We convert to None here so json.dumps can emit
    it as the JSON null.
    """
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    import math
    if not math.isfinite(f):
        return None
    return round(f, 4)


def _num(v):
    """Same as _round. Kept as a separate name for semantic clarity at
    call sites (prices vs percentages)."""
    return _round(v)


def fetch_fx(symbol: str):
    """Fetch one FX pair. Returns dict with price + 1d/1w/1m/1y changes,
    or None on failure."""
    try:
        t = yf.Ticker(symbol)
        # Pull ~14 months so 1-year lookback is always covered.
        hist = t.history(period='14mo', interval='1d', auto_adjust=True)
        if hist is None or hist.empty:
            return None
        closes = hist['Close'].dropna()
        if len(closes) < 1:
            return None
        price = float(closes.iloc[-1])
        if not math.isfinite(price):
            return None
        prev = float(closes.iloc[-2]) if len(closes) >= 2 else price
        change_pct = ((price - prev) / prev * 100) if prev else 0.0
        pct_week  = _pct_from_ago(hist, price, 7)
        pct_month = _pct_from_ago(hist, price, 30)
        pct_year  = _pct_from_ago(hist, price, 365)
        return {
            'price':        round(price, 4),
            'changePct':    round(change_pct, 4),
            'changePct1w':  _round(pct_week),
            'changePct1m':  _round(pct_month),
            'changePct1y':  _round(pct_year),
        }
    except Exception as e:
        print(f'  ! {symbol}: {type(e).__name__}: {e}', file=sys.stderr)
        return None


# Map country ISO numeric id → ISO-4217 currency code. Used to build
# the currencyCode field on each market entry and to decide which FX
# pairs to fetch. Kept as a separate source of truth rather than
# relying on yfinance's flaky fast_info.currency for indices.
COUNTRY_CURRENCY = {
    '840': 'USD',  # United States
    '156': 'CNY',  # China
    '392': 'JPY',  # Japan
    '344': 'HKD',  # Hong Kong
    '826': 'GBP',  # United Kingdom
    '276': 'EUR',  # Germany
    '250': 'EUR',  # France
    '356': 'INR',  # India
    '124': 'CAD',  # Canada
    '410': 'KRW',  # South Korea
    '036': 'AUD',  # Australia
    '756': 'CHF',  # Switzerland
    '158': 'TWD',  # Taiwan
    '724': 'EUR',  # Spain
    '076': 'BRL',  # Brazil
    '380': 'EUR',  # Italy
    '528': 'EUR',  # Netherlands
    '702': 'SGD',  # Singapore
    '484': 'MXN',  # Mexico
    '752': 'SEK',  # Sweden
    '056': 'EUR',  # Belgium
    '710': 'ZAR',  # South Africa
    '246': 'EUR',  # Finland
    '208': 'DKK',  # Denmark
    '040': 'EUR',  # Austria
    '620': 'EUR',  # Portugal
    '300': 'EUR',  # Greece
    '372': 'EUR',  # Ireland
    '792': 'TRY',  # Turkey
    '376': 'ILS',  # Israel
    '682': 'SAR',  # Saudi Arabia
    '360': 'IDR',  # Indonesia
    '458': 'MYR',  # Malaysia
    '764': 'THB',  # Thailand
    '608': 'PHP',  # Philippines
    '554': 'NZD',  # New Zealand
    '032': 'ARS',  # Argentina
}


def _compute_correlation_matrix(markets: list) -> dict:
    """Build a sparse correlation matrix from per-market 90-day returns.

    Pearson correlation on returns (NOT prices — prices share trends
    that aren't real co-movement). Symmetric, so we only need the upper
    triangle, but we emit both directions for cheap client-side lookup.

    Output shape: { country_id: { other_id: corr_float, ... }, ... }
    Diagonal (self-correlation = 1.0) is omitted.

    Only the FIRST index per country contributes — e.g. US has S&P 500
    + Nasdaq + Dow, but for "how does the US move with Germany" we use
    just the S&P 500. Keeps the matrix one-row-per-country.
    """
    # Step 1: collect one returns array per country, keyed by id.
    series = {}
    for m in markets:
        cid = m['id']
        if cid in series:
            continue   # already have a primary index for this country
        q = m.get('quote') or {}
        r = q.get('_returns90')
        if not r or len(r) < 30:
            # Need at least 30 overlapping days for a meaningful correlation.
            continue
        series[cid] = r

    # Step 2: align lengths. Take the minimum length across all series so
    # all correlations use the same window. Pandas would handle this but
    # we want zero deps in the corr step (it's already plenty fast in pure
    # Python for 37×37).
    if not series:
        return {}
    min_len = min(len(r) for r in series.values())
    if min_len < 30:
        return {}
    aligned = {cid: r[-min_len:] for cid, r in series.items()}

    # Step 3: pairwise Pearson correlation. n^2 / 2 pairs * 90 mults each
    # = ~60K mults for 37 countries. Trivial.
    def pearson(a, b):
        n = len(a)
        if n != len(b) or n < 2:
            return None
        ma = sum(a) / n
        mb = sum(b) / n
        num = 0.0
        sa = 0.0
        sb = 0.0
        for x, y in zip(a, b):
            dx = x - ma
            dy = y - mb
            num += dx * dy
            sa += dx * dx
            sb += dy * dy
        denom = (sa * sb) ** 0.5
        if denom == 0:
            return None
        return num / denom

    ids = list(aligned.keys())
    matrix = {cid: {} for cid in ids}
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            a, b = ids[i], ids[j]
            c = pearson(aligned[a], aligned[b])
            if c is None or not math.isfinite(c):
                continue
            c = round(c, 3)
            matrix[a][b] = c
            matrix[b][a] = c   # mirror so client-side lookup is one hop

    # Drop empty rows
    return {k: v for k, v in matrix.items() if v}


def main():
    now = datetime.now(timezone.utc)
    output = {
        'generated_at': now.isoformat(),
        'generated_at_unix': int(now.timestamp()),
        'source': 'Yahoo Finance via yfinance',
        'markets': [],
    }

    ok = 0
    fail = 0

    for i, (country_id, country, index_name, symbol, mcap) in enumerate(MARKETS, 1):
        print(f'[{i:2d}/{len(MARKETS)}] {country_id} {symbol:<18s} {index_name}')
        quote = fetch_one(symbol)

        entry = {
            'id': country_id,
            'country': country,
            'index': index_name,
            'symbol': symbol,
            'countryMarketCapUsdTn': mcap,
            'currencyCode': COUNTRY_CURRENCY.get(country_id),
        }
        if quote:
            entry['quote'] = quote
            ok += 1
        else:
            entry['error'] = 'No data returned'
            fail += 1

        output['markets'].append(entry)

        # Be polite to Yahoo's servers
        time.sleep(0.15)

    # --- Forex block -------------------------------------------------
    # Fetch each unique non-USD, non-GBP currency vs USD and vs GBP so
    # the UI can show "EUR/USD 1.0845" and "EUR/GBP 0.8612" per country.
    # We skip self-pairs (USD/USD, GBP/GBP).
    unique_ccys = {
        COUNTRY_CURRENCY[cid]
        for cid, *_ in MARKETS
        if COUNTRY_CURRENCY.get(cid)
    }
    fx = {}
    for ccy in sorted(unique_ccys):
        for base in ('USD', 'GBP'):
            if ccy == base:
                continue
            pair = f'{ccy}{base}'
            symbol = f'{pair}=X'
            print(f'[fx] {symbol}')
            fx_quote = fetch_fx(symbol)
            if fx_quote:
                fx[pair] = fx_quote
            time.sleep(0.15)
    output['fx'] = fx

    # --- Correlations -----------------------------------------------
    # For each pair of countries (one primary index per country),
    # compute the Pearson correlation of their last 90 daily returns.
    # Output is a sparse matrix: { '826': { '276': 0.85, '250': 0.92, ... }, ... }
    # We dedup to one entry per country (the first index listed for that
    # country in MARKETS) so a country has exactly one correlation row.
    output['correlations'] = _compute_correlation_matrix(output['markets'])

    # Strip private fields before serialization. _returns90 was needed
    # for the correlation calculation but shouldn't bloat the public JSON.
    for m in output['markets']:
        if 'quote' in m and isinstance(m['quote'], dict):
            m['quote'].pop('_returns90', None)

    output['summary'] = {'ok': ok, 'failed': fail, 'total': len(MARKETS)}

    # allow_nan=False -> json.dump will raise ValueError if any NaN/Inf
    # slipped through _num/_round. Better to crash the workflow loudly
    # than silently produce invalid JSON that breaks the globe.
    with open('quotes.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False, allow_nan=False)

    print(f'\nWrote quotes.json — {ok}/{len(MARKETS)} succeeded, {fail} failed.')

    # Exit non-zero if everything failed (likely a systemic issue)
    if ok == 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
