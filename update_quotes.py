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

    # --- Extended coverage: additional 46 countries to reach 100 distinct
    # markets. Many of these smaller/frontier markets have patchier Yahoo
    # coverage — any symbol that doesn't resolve will simply show as
    # "Data unavailable" in the panel without breaking the globe. ---

    # South & Southeast Asia frontier
    ('050', 'Bangladesh',           'DSEX',                  '^DSEX',        0.04),
    ('144', 'Sri Lanka',            'CSE All Share',         '^CSE',         0.02),
    ('116', 'Cambodia',             'CSX',                   '^CSX',         0.01),
    ('496', 'Mongolia',             'MSE Top 20',            '^MSE20',       0.01),

    # Middle East (non-GCC covered above) + Central Asia
    ('400', 'Jordan',               'ASE General',           '^ASE',         0.02),
    ('414', 'Kuwait',               'Kuwait All Share',      '^BKP',         0.12),
    ('048', 'Bahrain',              'Bahrain All Share',     '^BAX',         0.03),
    ('512', 'Oman',                 'MSX 30',                '^MSX30',       0.02),
    ('422', 'Lebanon',              'BLOM Stock Index',      '^BLOM',        0.01),
    ('368', 'Iraq',                 'ISX 60',                '^ISX60',       0.01),
    ('398', 'Kazakhstan',           'KASE',                  '^KASE',        0.06),

    # Africa
    ('504', 'Morocco',              'MASI',                  '^MASI',        0.07),
    ('566', 'Nigeria',              'NGX All Share',         '^NGX',         0.04),
    ('404', 'Kenya',                'NSE 20',                '^NSE20',       0.02),
    ('288', 'Ghana',                'GSE Composite',         '^GSECI',       0.01),
    ('834', 'Tanzania',             'DSE All Share',         '^DSEI',        0.01),
    ('788', 'Tunisia',              'Tunindex',              '^TUNINDEX',    0.01),
    ('894', 'Zambia',               'LuSE All Share',        '^LASI',        0.01),
    ('480', 'Mauritius',            'SEMDEX',                '^SEMDEX',      0.01),
    ('716', 'Zimbabwe',             'ZSE All Share',         '^ZSEAS',       0.01),
    ('072', 'Botswana',             'BSE Domestic Company',  '^DCI',         0.01),
    ('516', 'Namibia',              'NSX Overall',           '^NOX',         0.02),
    ('384', "Côte d'Ivoire",        'BRVM Composite',        '^BRVM',        0.01),
    ('800', 'Uganda',               'USE All Share',         '^USEALSI',     0.01),

    # Central & Eastern Europe
    ('100', 'Bulgaria',             'SOFIX',                 '^SOFIX',       0.01),
    ('642', 'Romania',              'BET',                   '^BET',         0.04),
    ('191', 'Croatia',              'CROBEX',                '^CRBEX',       0.03),
    ('705', 'Slovenia',             'SBI TOP',               '^SBITOP',      0.01),
    ('703', 'Slovakia',             'SAX',                   '^SAX',         0.01),
    ('688', 'Serbia',               'BELEX 15',              '^BELEX15',     0.01),
    ('070', 'Bosnia and Herz.',     'SASX-10',               '^SASX10',      0.01),
    ('804', 'Ukraine',              'PFTS',                  '^PFTS',        0.01),

    # Baltics / Nordics (beyond Denmark/Sweden/Norway/Finland already covered)
    ('428', 'Latvia',               'OMX Riga',              '^OMXRGI',      0.01),
    ('440', 'Lithuania',            'OMX Vilnius',           '^OMXVGI',      0.01),
    ('233', 'Estonia',              'OMX Tallinn',           '^OMXTGI',      0.01),
    ('352', 'Iceland',              'OMX Iceland',           '^OMXI10',      0.03),

    # Southern Europe / smaller EU
    ('196', 'Cyprus',               'CSE General',           '^CSE',         0.01),
    ('470', 'Malta',                'MSE Equity Price',      '^MALTEX',      0.01),
    ('442', 'Luxembourg',           'LuxX',                  '^LUXX',        0.06),

    # Latin America (beyond Brazil/Mexico/Chile/Colombia/Peru/Argentina)
    ('862', 'Venezuela',            'IBC',                   '^IBC',         0.01),
    ('218', 'Ecuador',              'ECU',                   '^ECU',         0.01),
    ('858', 'Uruguay',              'BVMBG',                 '^BVMBG',       0.01),
    ('068', 'Bolivia',              'BBV General',           '^BBV',         0.01),
    ('600', 'Paraguay',             'PDCA',                  '^PDCA',        0.01),
    ('188', 'Costa Rica',           'CRSMB',                 '^CRSMB',       0.01),

    # Final six to reach 100 distinct countries
    ('031', 'Azerbaijan',           'Baku Stock Exchange',   '^BSE',         0.01),
    ('051', 'Armenia',              'AMX Main Index',        '^AMX',         0.01),
    ('268', 'Georgia',              'Georgia All Share',     '^GSXAS',       0.01),
    ('364', 'Iran',                 'TEDPIX',                '^TEDPIX',      2.00),
    ('760', 'Syria',                'DSE Weighted',          '^DSEWG',       0.01),
    ('887', 'Yemen',                'Yemen Market Index',    '^YEMI',        0.01),
]


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
            return None

        # Current price = last close. Previous close = second-to-last.
        # This is standard practice when fast_info is unreliable.
        closes = hist['Close'].dropna()
        if len(closes) < 2:
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
