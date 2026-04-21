"""
Fetch the most IMPORTANT recent news stories about each tracked country
using GDELT's free DOC 2.0 API, and write the results to news.json for
the globe to consume alongside quotes.json.

GDELT is a free open-access global news monitoring project. The DOC 2.0
API is fully public — no auth required, no rate limit published, but we
pace requests politely (0.5s between calls).

Query strategy (v3 — ranks by importance via story clustering):

  For each country:
    1. Query GDELT for up to 50 articles that match ALL three filters:
       * Exact-phrase country name
       * At least one GKG theme from a curated finance/politics list
         (ECON_STOCKMARKET, ELECTION, LEADER, etc.)
       * Published by one of ~13 reputable global outlets (Reuters,
         Bloomberg, FT, WSJ, BBC, CNBC, Economist, AP, MarketWatch,
         CNN, NYT, Al Jazeera)
    2. Cluster articles into "stories" by title-token overlap. Articles
       sharing 3+ content words in their titles are treated as covering
       the same event.
    3. Score each cluster by (count × source weight), where wire
       services (Reuters, Bloomberg, AP) are weighted heavier than
       second-tier outlets. This approximates "how much did the world's
       major outlets care about this story."
    4. Return the lead article from the top 3 clusters — i.e. the 3
       most-covered, highest-prestige stories of the day.

Why clustering? Sorting by date alone gives you "3 most recent
articles", which could be three low-stakes items from the same outlet.
Clustering surfaces stories that many outlets covered simultaneously —
a strong signal of genuine importance.

Fallback: if no clusters match, return an empty list. UI shows "No
relevant articles in the last 24 hours" gracefully. We do NOT
progressively relax filters; quality over coverage.

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
]


GDELT_ENDPOINT = 'https://api.gdeltproject.org/api/v2/doc/doc'
USER_AGENT = 'AtlasGlobe/1.0 (+https://github.com; news aggregation)'
TIMEOUT = 15                    # per-request timeout in seconds
PAUSE_BETWEEN_REQUESTS = 3.0    # seconds — GDELT's rate limit is very
                                # tight. Their own blog notes that changes
                                # of 0.001 QPS can push error rate to 5%.
                                # 3s/req = 0.33 QPS is a safe baseline.
MAX_RETRIES = 4                 # retries on transient failures (429/5xx)
BACKOFF_BASE_SECONDS = 8        # first retry waits 8s, then 16, 32, 64
ARTICLES_PER_COUNTRY = 3        # final returned, AFTER clustering
FETCH_POOL_SIZE = 50            # articles fetched per country (pre-cluster)
TIMESPAN = '24h'                # rolling window to search within

# Source weights for importance ranking. Wire services and tier-1 global
# outlets get a multiplier; other reputable outlets stay at baseline.
# Heavier weight = story is more important if THIS source covered it.
SOURCE_WEIGHTS = {
    'reuters.com':      2.0,
    'bloomberg.com':    2.0,
    'apnews.com':       2.0,   # AP wire service
    'ft.com':           1.8,
    'wsj.com':          1.8,
    'bbc.com':          1.5,
    'bbc.co.uk':        1.5,
    'economist.com':    1.5,
    'nytimes.com':      1.3,
    'cnbc.com':         1.2,
    'aljazeera.com':    1.2,
    'cnn.com':          1.0,
    'marketwatch.com':  1.0,
}

# Stopwords stripped from titles before clustering. Purpose: let the
# clustering focus on the *content* words, not English glue. Kept small
# and general.
STOPWORDS = {
    'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 'and', 'or',
    'but', 'as', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'has', 'have', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
    'should', 'may', 'might', 'can', 'with', 'from', 'by', 'this',
    'that', 'these', 'those', 'it', 'its', 'his', 'her', 'their', 'our',
    'your', 'my', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
    'says', 'said', 'than', 'over', 'after', 'before', 'amid', 'up',
    'down', 'out', 'new', 'old', 'not', 'no', 'yes', 'all', 'any',
    'some', 'more', 'most', 'less', 'least', 'news', 'report', 'reports',
    'reuters', 'bloomberg', 'bbc', 'cnn', 'ap', 'afp',  # outlet names
}

# Reputable outlets — articles MUST come from one of these domains. This
# solves the "noisy aggregator" problem by only trusting recognized
# global news brands.
REPUTABLE_DOMAINS = [
    'reuters.com',
    'bloomberg.com',
    'ft.com',             # Financial Times
    'wsj.com',            # Wall Street Journal
    'bbc.com',
    'bbc.co.uk',
    'cnbc.com',
    'economist.com',
    'apnews.com',         # Associated Press
    'marketwatch.com',
    'cnn.com',
    'nytimes.com',
    'aljazeera.com',
]

# GDELT Global Knowledge Graph (GKG) themes — articles MUST be tagged
# with at least one of these. Covers finance/business/markets AND
# politics/government, which is what we want per design decision.
#
# Themes are inferred by GDELT's NLP from article content, so they're
# much more accurate than keyword matching. An article tagged with
# ECON_STOCKMARKET is genuinely about stock markets, not incidentally
# mentioning the word "stock".
RELEVANT_THEMES = [
    # Economics / finance / business
    'ECON_STOCKMARKET',
    'ECON_CENTRALBANK',
    'ECON_INTEREST_RATES',
    'ECON_TRADE',
    'ECON_EARNINGSREPORT',
    'ECON_MONOPOLY',
    'ECON_INFLATION',
    'ECON_BANKRUPTCY',
    'ECON_TAXATION',
    'ECON_DEBT',
    'ECON_COST_OF_LIVING',
    'EPU_ECONOMY',         # economic policy uncertainty
    'EPU_POLICY',
    # Politics / government / current affairs
    'GENERAL_GOVERNMENT',
    'ELECTION',
    'LEGISLATION',
    'DIPLOMATIC_REL',
    'LEADER',
]


def _or_block(field: str, values) -> str:
    """Build a GDELT boolean OR block: '(field:v1 OR field:v2 OR ...)'."""
    parts = [f'{field}:{v}' for v in values]
    return '(' + ' OR '.join(parts) + ')'


def _title_tokens(title: str) -> set:
    """Extract content-word tokens from a title for clustering. Lowercase,
    strip punctuation, drop short tokens and stopwords. Returns a set."""
    import re
    # Keep letters, numbers and spaces; normalize the rest to spaces.
    cleaned = re.sub(r"[^a-z0-9\s]", " ", title.lower())
    return {
        w for w in cleaned.split()
        if len(w) >= 3 and w not in STOPWORDS and not w.isdigit()
    }


def _cluster_articles(articles: list) -> list:
    """Group articles covering the same story. Two articles are the same
    story if they share >= 3 content-word tokens in their titles.

    Greedy single-pass clustering: for each article, find the first
    existing cluster it overlaps with enough; otherwise start a new one.
    Returns a list of clusters, each cluster being a list of articles
    (in insertion order — first article is usually the most recent
    because the input is sorted datedesc).
    """
    # 2 shared content words is the right threshold for business news,
    # where different outlets cover the same event with different phrasing
    # (e.g. "Bank of England holds rates" vs "BoE keeps interest rates
    # unchanged" — share only 'rates' and 'holds'/'keeps' is a synonym).
    # Tried 3 initially; merged too little. 2 gives a good balance.
    OVERLAP_THRESHOLD = 2
    clusters = []  # each entry: {'tokens': set, 'articles': list}

    for a in articles:
        toks = _title_tokens(a.get('title', ''))
        if not toks:
            continue
        placed = False
        for c in clusters:
            if len(toks & c['tokens']) >= OVERLAP_THRESHOLD:
                c['articles'].append(a)
                # Keep the cluster's core signature as the UNION of all
                # article tokens so later articles using any of the
                # covered vocabulary can still merge in.
                c['tokens'] |= toks
                placed = True
                break
        if not placed:
            clusters.append({'tokens': toks, 'articles': [a]})

    return clusters


def _score_cluster(cluster: dict) -> float:
    """Importance score for a cluster = sum of source weights of its
    articles. More outlets covering the story + heavier-weighted outlets
    covering it = higher score."""
    return sum(
        SOURCE_WEIGHTS.get(a.get('domain', '').lower(), 1.0)
        for a in cluster['articles']
    )


def _urlopen_with_retry(req, label):
    """Call urlopen with retry+backoff on transient failures (429/5xx).

    GDELT's rate limiter is aggressive and occasionally returns 429 even
    when we're well under the quota — we wait and try again. For other
    network errors (DNS, TLS handshake timeout, etc.) we also retry.
    Returns the decoded response body on success, or None after final
    failure. Errors are printed to stderr.
    """
    attempt = 0
    while True:
        try:
            with urlopen(req, timeout=TIMEOUT) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except HTTPError as e:
            # Respect Retry-After if the server provided one (seconds)
            retry_after = None
            try:
                retry_after = int(e.headers.get('Retry-After', '0')) or None
            except (ValueError, AttributeError):
                pass
            retriable = e.code == 429 or 500 <= e.code < 600
            if retriable and attempt < MAX_RETRIES:
                wait = retry_after or (BACKOFF_BASE_SECONDS * (2 ** attempt))
                print(f'  ~ {label}: HTTP {e.code}, backing off {wait}s (attempt {attempt + 1}/{MAX_RETRIES})', file=sys.stderr)
                time.sleep(wait)
                attempt += 1
                continue
            print(f'  ! {label}: HTTPError {e.code}: {e.reason}', file=sys.stderr)
            return None
        except (URLError, TimeoutError) as e:
            if attempt < MAX_RETRIES:
                wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
                print(f'  ~ {label}: {type(e).__name__}, backing off {wait}s (attempt {attempt + 1}/{MAX_RETRIES})', file=sys.stderr)
                time.sleep(wait)
                attempt += 1
                continue
            print(f'  ! {label}: {type(e).__name__}: {e}', file=sys.stderr)
            return None


def fetch_news(country_name: str) -> list:
    """Fetch the most important finance/business/politics stories about a
    country in the last 24h, from reputable outlets.

    Strategy:
      1. Query GDELT for up to 50 articles matching all three filters:
         country name (exact phrase) + theme (finance/politics) +
         domain (reputable outlets).
      2. Cluster articles covering the same story by title-token overlap.
      3. Score each cluster by (number of articles × source weight),
         which approximates "how much did the world's major outlets
         care about this story."
      4. Return the most recent article from each of the top 3 clusters.

    This surfaces the stories being widely covered by wire services and
    tier-1 outlets, not just the 3 most recent articles (which could be
    a single outlet's minor filings).
    """
    theme_block = _or_block('theme', RELEVANT_THEMES)
    domain_block = _or_block('domain', REPUTABLE_DOMAINS)
    query = f'"{country_name}" {theme_block} {domain_block} sourcelang:english'

    params = {
        'query': query,
        'mode': 'artlist',
        'maxrecords': str(FETCH_POOL_SIZE),
        'format': 'json',
        'timespan': TIMESPAN,
        'sort': 'datedesc',
    }
    # URL-encode each param value. Keep GDELT operators (`:` `"` `(` `)`)
    # in their raw form so the server parses the query correctly, but
    # encode spaces as %20 — Python 3.12+ rejects URLs containing raw
    # spaces before they're even sent, throwing InvalidURL.
    url = GDELT_ENDPOINT + '?' + '&'.join(
        f'{k}={quote(v, safe=":\"()")}' for k, v in params.items()
    )

    req = Request(url, headers={'User-Agent': USER_AGENT})
    raw = _urlopen_with_retry(req, country_name)
    if raw is None:
        return []

    if not raw.strip() or not raw.strip().startswith('{'):
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    raw_articles = data.get('articles', []) or []

    # Normalize + drop entries with no title/url.
    pool = []
    for a in raw_articles:
        title = (a.get('title') or '').strip()
        url_ = (a.get('url') or '').strip()
        if not title or not url_:
            continue
        pool.append({
            'title': title,
            'url': url_,
            'domain': (a.get('domain') or '').strip().lower(),
            'seendate': (a.get('seendate') or '').strip(),
        })
    if not pool:
        return []

    # Cluster, score, and pick top N clusters. From each cluster we return
    # the most recent article (which is the first in the cluster, since
    # the API returned them datedesc and _cluster_articles preserves order).
    clusters = _cluster_articles(pool)
    clusters.sort(
        key=lambda c: (_score_cluster(c), -_earliest_seendate_rank(c)),
        reverse=True,
    )

    out = []
    for c in clusters[:ARTICLES_PER_COUNTRY]:
        lead = c['articles'][0]
        out.append({
            'title':    lead['title'],
            'url':      lead['url'],
            'domain':   lead['domain'],
            'seendate': lead['seendate'],
            # Diagnostic: how many articles in the cluster (shows how
            # widely covered the story is). Could surface this in the UI
            # later if you want to display e.g. "24 outlets".
            'clusterSize': len(c['articles']),
        })
    return out


def _earliest_seendate_rank(cluster: dict) -> int:
    """Sort-key helper: return a string-comparable representation of the
    most recent article's timestamp within a cluster. Used only as a
    tiebreaker between equally-scored clusters so that newer stories
    come first."""
    dates = [a.get('seendate', '') for a in cluster['articles']]
    # The seendate format "YYYYMMDDThhmmssZ" sorts lexically == chronologically.
    # Return negative hash for descending sort within the larger key.
    try:
        return int(max(dates).replace('T', '').replace('Z', ''))
    except (ValueError, AttributeError):
        return 0


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
        # Defence in depth: wrap per-country fetches so one unexpected
        # exception (URL encoding, JSON parsing, clustering bug) doesn't
        # break the whole run. fetch_news already catches network errors
        # internally; this catches anything it misses.
        try:
            articles = fetch_news(query_name)
        except Exception as e:
            print(f'  ! {display_name}: {type(e).__name__}: {e}', file=sys.stderr)
            articles = []
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
