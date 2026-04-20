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


# Each entry: (iso_numeric_country_id, country_name, index_name, yahoo_symbol)
MARKETS = [
    ('840', 'United States',        'S&P 500',               '^GSPC'),
    ('840', 'United States',        'Dow Jones',             '^DJI'),
    ('840', 'United States',        'Nasdaq Composite',      '^IXIC'),
    ('156', 'China',                'Shanghai Composite',    '000001.SS'),
    ('156', 'China',                'Shenzhen Component',    '399001.SZ'),
    ('392', 'Japan',                'Nikkei 225',            '^N225'),
    ('344', 'Hong Kong S.A.R.',     'Hang Seng',             '^HSI'),
    ('826', 'United Kingdom',       'FTSE 100',              '^FTSE'),
    ('276', 'Germany',              'DAX',                   '^GDAXI'),
    ('250', 'France',               'CAC 40',                '^FCHI'),
    ('356', 'India',                'BSE Sensex',            '^BSESN'),
    ('356', 'India',                'Nifty 50',              '^NSEI'),
    ('124', 'Canada',               'S&P/TSX Composite',     '^GSPTSE'),
    ('410', 'South Korea',          'KOSPI',                 '^KS11'),
    ('036', 'Australia',            'ASX 200',               '^AXJO'),
    ('756', 'Switzerland',          'SMI',                   '^SSMI'),
    ('158', 'Taiwan',               'TAIEX',                 '^TWII'),
    ('724', 'Spain',                'IBEX 35',               '^IBEX'),
    ('076', 'Brazil',               'Bovespa',               '^BVSP'),
    ('380', 'Italy',                'FTSE MIB',              'FTSEMIB.MI'),
    ('528', 'Netherlands',          'AEX',                   '^AEX'),
    ('702', 'Singapore',            'STI',                   '^STI'),
    ('484', 'Mexico',               'IPC',                   '^MXX'),
    ('752', 'Sweden',               'OMXS30',                '^OMX'),
    ('056', 'Belgium',              'BEL 20',                '^BFX'),
    ('710', 'South Africa',         'JSE Top 40',            '^JN0U.JO'),
    ('578', 'Norway',               'OSEAX',                 'OSEAX.OL'),
    ('246', 'Finland',              'OMXH25',                '^OMXH25'),
    ('208', 'Denmark',              'OMXC25',                '^OMXC25'),
    ('040', 'Austria',              'ATX',                   '^ATX'),
    ('620', 'Portugal',             'PSI 20',                'PSI20.LS'),
    ('300', 'Greece',               'ATHEX Composite',       'GD.AT'),
    ('372', 'Ireland',              'ISEQ Overall',          '^ISEQ'),
    ('616', 'Poland',               'WIG20',                 'WIG20.WA'),
    ('348', 'Hungary',              'BUX',                   '^BUX.BD'),
    ('203', 'Czechia',              'PX',                    '^PX'),
    ('792', 'Turkey',               'BIST 100',              'XU100.IS'),
    ('376', 'Israel',               'TA-125',                '^TA125.TA'),
    ('682', 'Saudi Arabia',         'Tadawul All Share',     '^TASI.SR'),
    ('784', 'United Arab Emirates', 'DFM General',           '^DFMGI'),
    ('634', 'Qatar',                'QE General',            '^QSI'),
    ('818', 'Egypt',                'EGX 30',                '^CASE30'),
    ('360', 'Indonesia',            'Jakarta Composite',     '^JKSE'),
    ('458', 'Malaysia',             'KLCI',                  '^KLSE'),
    ('764', 'Thailand',             'SET',                   '^SET.BK'),
    ('608', 'Philippines',          'PSEi',                  'PSEI.PS'),
    ('704', 'Vietnam',              'VN Index',              '^VNINDEX.VN'),
    ('586', 'Pakistan',             'KSE 100',               '^KSE'),
    ('554', 'New Zealand',          'NZX 50',                '^NZ50'),
    ('032', 'Argentina',            'S&P Merval',            '^MERV'),
    ('152', 'Chile',                'IPSA',                  '^IPSA'),
    ('170', 'Colombia',             'COLCAP',                '^COLCAP'),
    ('604', 'Peru',                 'S&P/BVL Peru General',  'SPBLPGPT.LM'),
    ('643', 'Russia',               'MOEX',                  'IMOEX.ME'),
]


def fetch_one(symbol: str) -> dict | None:
    """Fetch a single symbol's latest quote. Returns None on failure."""
    try:
        t = yf.Ticker(symbol)
        info = t.fast_info  # faster + more reliable than .info
        price = info.last_price
        prev = info.previous_close

        if price is None or prev is None:
            return None

        change = price - prev
        pct = (change / prev) * 100 if prev else 0.0

        return {
            'price': round(float(price), 4),
            'previousClose': round(float(prev), 4),
            'change': round(float(change), 4),
            'changePct': round(float(pct), 4),
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

    for i, (country_id, country, index_name, symbol) in enumerate(MARKETS, 1):
        print(f'[{i:2d}/{len(MARKETS)}] {country_id} {symbol:<18s} {index_name}')
        quote = fetch_one(symbol)

        entry = {
            'id': country_id,
            'country': country,
            'index': index_name,
            'symbol': symbol,
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
