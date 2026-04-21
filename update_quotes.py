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
    ('578', 'Norway',               'OSEAX',                 'OSEAX.OL',     0.35),
    ('246', 'Finland',              'OMXH25',                '^OMXH25',      0.30),
    ('208', 'Denmark',              'OMXC25',                '^OMXC25',      0.60),
    ('040', 'Austria',              'ATX',                   '^ATX',         0.15),
    ('620', 'Portugal',             'PSI 20',                'PSI20.LS',     0.08),
    ('300', 'Greece',               'ATHEX Composite',       'GD.AT',        0.10),
    ('372', 'Ireland',              'ISEQ Overall',          '^ISEQ',        0.14),
    ('616', 'Poland',               'WIG20',                 'WIG20.WA',     0.45),
    ('348', 'Hungary',              'BUX',                   '^BUX.BD',      0.05),
    ('203', 'Czechia',              'PX',                    '^PX',          0.07),
    ('792', 'Turkey',               'BIST 100',              'XU100.IS',     0.30),
    ('376', 'Israel',               'TA-125',                '^TA125.TA',    0.30),
    ('682', 'Saudi Arabia',         'Tadawul All Share',     '^TASI.SR',     2.73),
    ('784', 'United Arab Emirates', 'DFM General',           '^DFMGI',       0.95),
    ('634', 'Qatar',                'QE General',            '^QSI',         0.17),
    ('818', 'Egypt',                'EGX 30',                '^CASE30',      0.04),
    ('360', 'Indonesia',            'Jakarta Composite',     '^JKSE',        0.70),
    ('458', 'Malaysia',             'KLCI',                  '^KLSE',        0.40),
    ('764', 'Thailand',             'SET',                   '^SET.BK',      0.45),
    ('608', 'Philippines',          'PSEi',                  'PSEI.PS',      0.25),
    ('704', 'Vietnam',              'VN Index',              '^VNINDEX.VN',  0.28),
    ('586', 'Pakistan',             'KSE 100',               '^KSE',         0.05),
    ('554', 'New Zealand',          'NZX 50',                '^NZ50',        0.16),
    ('032', 'Argentina',            'S&P Merval',            '^MERV',        0.10),
    ('152', 'Chile',                'IPSA',                  '^IPSA',        0.20),
    ('170', 'Colombia',             'COLCAP',                '^COLCAP',      0.10),
    ('604', 'Peru',                 'S&P/BVL Peru General',  'SPBLPGPT.LM',  0.08),
    ('643', 'Russia',               'MOEX',                  'IMOEX.ME',     0.70),
]


def fetch_one(symbol: str) -> dict | None:
    """Fetch a single symbol's latest quote plus multi-period changes."""
    try:
        t = yf.Ticker(symbol)
        info = t.fast_info
        price = info.last_price
        prev = info.previous_close

        if price is None or prev is None:
            return None

        change = price - prev
        pct = (change / prev) * 100 if prev else 0.0

        # Pull ~14 months of daily history so the 1-year lookback is
        # always covered (a strict `period='1y'` window can end just
        # after the target date for some markets, returning None — this
        # is why e.g. Saudi Arabia was the only one with 1y data). Using
        # '14mo' buys us a margin of ~60 days.
        hist = t.history(period='14mo', interval='1d', auto_adjust=True)
        pct_week  = _pct_from_ago(hist, price, 7)
        pct_month = _pct_from_ago(hist, price, 30)
        pct_year  = _pct_from_ago(hist, price, 365)

        return {
            'price': round(float(price), 4),
            'previousClose': round(float(prev), 4),
            'change': round(float(change), 4),
            'changePct': round(float(pct), 4),
            'changePct1w':  _round(pct_week),
            'changePct1m':  _round(pct_month),
            'changePct1y':  _round(pct_year),
            'currency': info.currency,
            'exchange': info.exchange,
            'dayHigh': _num(info.day_high),
            'dayLow': _num(info.day_low),
            'yearHigh': _num(info.year_high),
            'yearLow': _num(info.year_low),
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
    if v is None:
        return None
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return None


def _num(v):
    if v is None:
        return None
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return None


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

    output['summary'] = {'ok': ok, 'failed': fail, 'total': len(MARKETS)}

    with open('quotes.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f'\nWrote quotes.json — {ok}/{len(MARKETS)} succeeded, {fail} failed.')

    # Exit non-zero if everything failed (likely a systemic issue)
    if ok == 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
