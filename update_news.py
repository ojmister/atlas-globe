"""
Fetch the top 3 English-language news headlines about each tracked
country using GDELT's free DOC 2.0 API, and write the results to
news.json for the globe to consume alongside quotes.json.

GDELT is a free open-access global news monitoring project. The DOC 2.0
API is fully public — no auth required, no rate limit published, but we
pace requests politely (0.5s between calls).

Design choices:
  * Query format: `"Country Name" sourcelang:english` — returns English
    coverage that mentions the country, sorted by date. This is more
    useful for a "what's happening in X" view than `sourcecountry:`,
    which would only return articles published BY outlets in X.
  * 24-hour time window. Fresh is better than comprehensive.
  * Top 3 per country (keeps the panel compact; 3 credits of GDELT).
  * If GDELT returns no results or fails, that country's news field is
    left empty — the UI shows "No recent news" gracefully.

Runs after update_quotes.py in the GitHub Actions workflow.
"""

import json
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Country list mirrors update_quotes.py MARKETS but deduplicated to one
# entry per country. The `gdelt_query` value is what we send as the
# sourcecountry / keyword match — GDELT accepts country names with
# spaces removed. Where a country has a short-form name that works
# better in headlines, we use that (e.g. "UK" would collide too much
# with other uses; "UnitedKingdom" is the canonical form).
#
# Fields: (iso_numeric_id, display_name, gdelt_query_name)
COUNTRIES = [
    ('840', 'United States',         'United States'),
    ('156', 'China',                 'China'),
    ('392', 'Japan',                 'Japan'),
    ('344', 'Hong Kong',             'Hong Kong'),
    ('826', 'United Kingdom',        'United Kingdom'),
    ('276', 'Germany',                'Germany'),
    ('250', 'France',                 'France'),
    ('356', 'India',                  'India'),
    ('124', 'Canada',                 'Canada'),
    ('410', 'South Korea',            'South Korea'),
    ('036', 'Australia',              'Australia'),
    ('756', 'Switzerland',            'Switzerland'),
    ('158', 'Taiwan',                 'Taiwan'),
    ('724', 'Spain',                  'Spain'),
    ('076', 'Brazil',                 'Brazil'),
    ('380', 'Italy',                  'Italy'),
    ('528', 'Netherlands',            'Netherlands'),
    ('702', 'Singapore',              'Singapore'),
    ('484', 'Mexico',                 'Mexico'),
    ('752', 'Sweden',                 'Sweden'),
    ('056', 'Belgium',                'Belgium'),
    ('710', 'South Africa',           'South Africa'),
    ('578', 'Norway',                 'Norway'),
    ('246', 'Finland',                'Finland'),
    ('208', 'Denmark',                'Denmark'),
    ('040', 'Austria',                'Austria'),
    ('620', 'Portugal',               'Portugal'),
    ('300', 'Greece',                 'Greece'),
    ('372', 'Ireland',                'Ireland'),
    ('616', 'Poland',                 'Poland'),
    ('348', 'Hungary',                'Hungary'),
    ('203', 'Czechia',                'Czech Republic'),
    ('792', 'Turkey',                 'Turkey'),
    ('376', 'Israel',                 'Israel'),
    ('682', 'Saudi Arabia',           'Saudi Arabia'),
    ('784', 'United Arab Emirates',   'United Arab Emirates'),
    ('634', 'Qatar',                  'Qatar'),
    ('818', 'Egypt',                  'Egypt'),
    ('360', 'Indonesia',              'Indonesia'),
    ('458', 'Malaysia',               'Malaysia'),
    ('764', 'Thailand',               'Thailand'),
    ('608', 'Philippines',            'Philippines'),
    ('704', 'Vietnam',                'Vietnam'),
    ('586', 'Pakistan',               'Pakistan'),
    ('554', 'New Zealand',            'New Zealand'),
    ('032', 'Argentina',              'Argentina'),
    ('152', 'Chile',                  'Chile'),
    ('170', 'Colombia',               'Colombia'),
    ('604', 'Peru',                   'Peru'),
    ('643', 'Russia',                 'Russia'),
    ('050', 'Bangladesh',             'Bangladesh'),
    ('144', 'Sri Lanka',              'Sri Lanka'),
    ('116', 'Cambodia',               'Cambodia'),
    ('496', 'Mongolia',               'Mongolia'),
    ('400', 'Jordan',                 'Jordan'),
    ('414', 'Kuwait',                 'Kuwait'),
    ('048', 'Bahrain',                'Bahrain'),
    ('512', 'Oman',                   'Oman'),
    ('422', 'Lebanon',                'Lebanon'),
    ('368', 'Iraq',                   'Iraq'),
    ('398', 'Kazakhstan',             'Kazakhstan'),
    ('504', 'Morocco',                'Morocco'),
    ('566', 'Nigeria',                'Nigeria'),
    ('404', 'Kenya',                  'Kenya'),
    ('288', 'Ghana',                  'Ghana'),
    ('834', 'Tanzania',               'Tanzania'),
    ('788', 'Tunisia',                'Tunisia'),
    ('894', 'Zambia',                 'Zambia'),
    ('480', 'Mauritius',              'Mauritius'),
    ('716', 'Zimbabwe',               'Zimbabwe'),
    ('072', 'Botswana',               'Botswana'),
    ('516', 'Namibia',                'Namibia'),
    ('384', "Côte d'Ivoire",          'Ivory Coast'),
    ('800', 'Uganda',                 'Uganda'),
    ('100', 'Bulgaria',               'Bulgaria'),
    ('642', 'Romania',                'Romania'),
    ('191', 'Croatia',                'Croatia'),
    ('705', 'Slovenia',               'Slovenia'),
    ('703', 'Slovakia',               'Slovakia'),
    ('688', 'Serbia',                 'Serbia'),
    ('070', 'Bosnia and Herz.',       'Bosnia and Herzegovina'),
    ('804', 'Ukraine',                'Ukraine'),
    ('428', 'Latvia',                 'Latvia'),
    ('440', 'Lithuania',              'Lithuania'),
    ('233', 'Estonia',                'Estonia'),
    ('352', 'Iceland',                'Iceland'),
    ('196', 'Cyprus',                 'Cyprus'),
    ('470', 'Malta',                  'Malta'),
    ('442', 'Luxembourg',             'Luxembourg'),
    ('862', 'Venezuela',              'Venezuela'),
    ('218', 'Ecuador',                'Ecuador'),
    ('858', 'Uruguay',                'Uruguay'),
    ('068', 'Bolivia',                'Bolivia'),
    ('600', 'Paraguay',               'Paraguay'),
    ('188', 'Costa Rica',             'Costa Rica'),
    ('031', 'Azerbaijan',             'Azerbaijan'),
    ('051', 'Armenia',                'Armenia'),
    ('268', 'Georgia',                'Georgia'),
    ('364', 'Iran',                   'Iran'),
    ('760', 'Syria',                  'Syria'),
    ('887', 'Yemen',                  'Yemen'),
]


GDELT_ENDPOINT = 'https://api.gdeltproject.org/api/v2/doc/doc'
USER_AGENT = 'AtlasGlobe/1.0 (+https://github.com; news aggregation)'
TIMEOUT = 15                    # per-request timeout in seconds
PAUSE_BETWEEN_REQUESTS = 0.5    # seconds — GDELT doesn't publish a limit
                                # but we stay polite
ARTICLES_PER_COUNTRY = 3
TIMESPAN = '24h'                # rolling window to search within


def fetch_news(country_name: str) -> list:
    """Fetch up to 3 recent English news headlines about a country."""
    # We query for the exact country name AND filter to English-language
    # coverage. Sorted by date descending.
    query = f'"{country_name}" sourcelang:english'
    params = {
        'query': query,
        'mode': 'artlist',
        'maxrecords': str(ARTICLES_PER_COUNTRY),
        'format': 'json',
        'timespan': TIMESPAN,
        'sort': 'datedesc',
    }
    # Manually build the query string — GDELT wants spaces encoded but
    # not extra aggressive encoding of quotes/colons.
    url = GDELT_ENDPOINT + '?' + '&'.join(
        f'{k}={quote(v, safe=":\"()")}' for k, v in params.items()
    )

    req = Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urlopen(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
    except (URLError, HTTPError, TimeoutError) as e:
        print(f'  ! {country_name}: {type(e).__name__}: {e}', file=sys.stderr)
        return []

    # GDELT sometimes returns empty body for zero results, or an HTML
    # error page. Guard both.
    if not raw.strip() or not raw.strip().startswith('{'):
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    articles = data.get('articles', []) or []
    out = []
    for a in articles[:ARTICLES_PER_COUNTRY]:
        title = (a.get('title') or '').strip()
        url_ = (a.get('url') or '').strip()
        if not title or not url_:
            continue
        out.append({
            'title': title,
            'url': url_,
            'domain': (a.get('domain') or '').strip(),
            'seendate': (a.get('seendate') or '').strip(),
        })
    return out


def main():
    now = datetime.now(timezone.utc)
    output = {
        'generated_at': now.isoformat(),
        'generated_at_unix': int(now.timestamp()),
        'source': 'GDELT 2.0 DOC API',
        'timespan': TIMESPAN,
        'news': {},
    }

    ok = 0
    empty = 0

    for i, (country_id, display_name, query_name) in enumerate(COUNTRIES, 1):
        print(f'[{i:3d}/{len(COUNTRIES)}] {country_id} {display_name}')
        articles = fetch_news(query_name)
        output['news'][country_id] = articles
        if articles:
            ok += 1
        else:
            empty += 1
        time.sleep(PAUSE_BETWEEN_REQUESTS)

    output['summary'] = {
        'with_news':   ok,
        'empty':       empty,
        'total':       len(COUNTRIES),
    }

    with open('news.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f'\nWrote news.json — {ok}/{len(COUNTRIES)} countries had headlines.')


if __name__ == '__main__':
    main()
