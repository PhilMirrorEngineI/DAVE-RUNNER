import os
import re
import time
import html
import textwrap
from collections import Counter, deque
from html.parser import HTMLParser
from urllib.parse import quote_plus, urlparse, parse_qs, unquote
from datetime import datetime, timezone
import requests

try:
    from standalone.historical_continuity import (
        scan_continuity_archive,
    )
except ImportError:
    from historical_continuity import (
        scan_continuity_archive,
    )

BUILD_STATUS = 'CANDIDATE'
BUILD_NAME = 'v6.8.1-meta-advice-consequence-hardening'

BASE_URL = os.getenv(
    'PMEI_BASE_URL',
    'https://dave-runner.onrender.com'
).rstrip('/')

API_KEY = os.getenv(
    'DAVE_RUNNER_API_KEY',
    ''
).strip()

PMEI_DEPTH = 100
WEB_RESULTS = 8
WEB_FETCH = 4
TIMEOUT = 15
RETRIEVAL_QUALITY_THRESHOLD = 0.5

HISTORY = deque(maxlen=30)
LEARNING_THREADS = {}

LAST_EVIDENCE = []
LAST_PROCESSED = None
LAST_REASON = None

BOOT_STATE = {
    'loaded': False,
    'open_threads': [],
    'successful_patterns': [],
    'failed_patterns': [],
    'learning_events': []
}

session = requests.Session()

session.headers.update({
    'User-Agent':
        'Mozilla/5.0 PMEi deterministic standalone'
})


def utc_now_iso():
    return datetime.now(
        timezone.utc
    ).isoformat()


def empty_pmei_transport():
    return {
        'required': False,
        'attempted': False,
        'success': False,
        'route': None,
        'errors': []
    }

STOPWORDS = set('''
a an and are as at be been being but by can could did do does
for from had has have he her him his how i if in into is it its
me my of on or our she so that the their them then there these
they this those to too us was we were what when where which who
why will with would you your some after than even all much
'''.split())

INTENT_WORDS = set('''
reason reasons
explain explains explained explaining explanation explanations
answer answers answering
tell
understand understanding
find finding
show
mean means meaning
happen happens happening
occur occurs occurring
cause causes caused causing
'''.split())

OPTIONAL_COVERAGE_MODIFIERS = {
    'sometimes',
    'sometime',
    'often',
    'usually',
    'occasionally',
    'several',
    'multiple',
    'repeatedly'
}

BOILERPLATE_WORDS = set('''
home menu search login log sign register account
privacy cookie cookies advertisement advertisements
subscribe subscription newsletter categories category
contact about faq sitemap navigation
facebook instagram youtube twitter reddit tiktok
share print copyright terms conditions accessibility
company careers press developers
shop store products product
news latest archive archives
browse donate donations
podcast podcasts video videos
email inbox
download downloads
discover help
activity forum forums
member members profile profiles
leaderboard staff
'''.split())

UI_PHRASES = (
    'sign up',
    'log in',
    'create new account',
    'existing user',
    'jump to content',
    'all activity',
    'my activity streams',
    'unread content',
    'content i started',
    'since last visit',
    'browse forums',
    'online users',
    'terms and conditions',
    'privacy policy',
    'contact us',
    'cookie settings',
    'powered by',
    'go to topic listing',
    'topic listing',
    'followers 0',
    'share this',
    'more sharing options',
    'subscribe to our newsletter',
    'facebook instagram youtube',
    'home building trades',
    'all shows',
    'latest magazine',
    'current issue archives',
    'subscriber support'
)

EXPLANATION_TERMS = set('''
because due cause causes caused reason results result
leads lead therefore occurs happens mechanism process
produces produce produced released releases forms form
formed creates create created depends effect interaction
transfer reaction reflects reflection reflective absorbs
scatters changes change pressure temperature density
evaporation condensation vibration frequency energy heat
charge chemical chemistry molecule molecules material
current flow friction gravity moisture humidity light sound
wavelength atmosphere refraction nucleation dissolved gas
gases bubble bubbles means meaning function timer delay
stopping stop switched wiring wire wires thermal shock
tension expansion contraction contracts expands layer
layers defect defects cooling heating difference differences
internal external connected connection surface surfaces
smooth rough water film films
'''.split())

MECHANISM_SIGNAL_WORDS = set('''
because due caused causes cause
therefore leads lead result results
pressure temperature heat cooling heating
expansion expands contraction contracts
tension stress internal external
layer layers connected connection
defect defects dissolved oxygen
vibration frequency energy current flow
condensation evaporation moisture humidity
reaction chemical molecule molecules
transfer friction gravity
forms formed creates created
allows allow prevents prevent
depends effect interaction
reflection reflective reflect
refraction atmosphere turbulence
surface film water
'''.split())

ADVICE_OPENERS = (
    "don't ",
    'do not ',
    'avoid ',
    'try ',
    'use ',
    'start with ',
    'hold ',
    'place ',
    'put ',
    'make sure ',
    'remember to ',
    'always ',
    'never ',
    'you should ',
    'you can ',
    "it's best to ",
    'it is best to ',
    'for best results ',
    'the best way to ',
    'the most effective way to ',
    'one effective way to ',
    'to prevent ',
    'to avoid ',
    'to reduce ',
    'to stop '
)

CONSEQUENCE_PHRASES = (
    'as a result',
    'therefore',
    'consequently',
    'resulting in',
    'results in',
    'result in',
    'leads to',
    'lead to',
    'causing',
    'which causes',
    'which can cause',
    'which makes',
    'making it',
    'making the',
    'leaving',
    'producing',
    'creating',
    'so that',
    'so the',
    'allowing'
)

COMPARATIVE_TERMS = {
    'more',
    'less',
    'higher',
    'lower',
    'warmer',
    'colder',
    'hotter',
    'cooler',
    'faster',
    'slower',
    'older',
    'younger',
    'better',
    'worse',
    'greater',
    'smaller'
}

NEGATION_WORDS = {
    'not',
    'never',
    'cannot',
    "can't",
    'cant',
    "doesn't",
    'doesnt',
    "isn't",
    'isnt',
    "aren't",
    'arent',
    "wasn't",
    'wasnt',
    "weren't",
    'werent',
    "won't",
    'wont'
}

REFERENCE_PRONOUNS = {
    'he',
    'she',
    'him',
    'her',
    'his',
    'hers',
    'it',
    'its',
    'they',
    'them',
    'their',
    'theirs',
    'this',
    'that',
    'these',
    'those'
}

ESSENTIAL_CONDITION_WORDS = {
    'wet',
    'dry',
    'damp',
    'moist',
    'hot',
    'cold',
    'warm',
    'cool',
    'frozen',
    'freezing',
    'night',
    'nighttime',
    'day',
    'daytime',
    'morning',
    'evening',
    'inside',
    'outside',
    'indoors',
    'outdoors',
    'underwater',
    'underground',
    'open',
    'closed',
    'empty',
    'full',
    'sealed',
    'unsealed'
}

CONDITION_NORMALISATION = {
    'nighttime': 'night',
    'daytime': 'day',
    'outdoors': 'outside',
    'indoors': 'inside',
    'moist': 'wet',
    'damp': 'wet'
}

PHENOMENON_TRIGGER_WORDS = {
    'look',
    'looks',
    'looking',
    'appear',
    'appears',
    'appearing',
    'feel',
    'feels',
    'feeling',
    'sound',
    'sounds',
    'sounding',
    'become',
    'becomes',
    'becoming',
    'seem',
    'seems'
}

PHENOMENON_IGNORE_WORDS = {
    'like',
    'very',
    'really',
    'much',
    'more',
    'less',
    'so',
    'quite',
    'rather',
    'usually',
    'sometimes',
    'sometime',
    'often',
    'occasionally',
    'when',
    'after',
    'before',
    'while',
    'even'
}

NORMALISE_WORD = {
    'aeroplane': 'airplane',
    'aeroplanes': 'airplane',
    'airplanes': 'airplane',
    'windows': 'window',
    'holes': 'hole',
    'bubbles': 'bubble',
    'candles': 'candle',
    'lakes': 'lake',
    'books': 'book',
    'onions': 'onion',
    'balloons': 'balloon',
    'railings': 'railing',
    'mirrors': 'mirror',
    'shines': 'shiny'
}


class TextExtractor(HTMLParser):

    BLOCK_TAGS = {
        'article', 'section', 'main', 'aside', 'header',
        'footer', 'nav', 'div', 'p', 'li', 'ul', 'ol',
        'table', 'tr', 'td', 'th', 'blockquote',
        'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'br'
    }

    SKIP_TAGS = {
        'script',
        'style',
        'noscript',
        'svg',
        'template'
    }

    def __init__(self):
        super().__init__()
        self.parts = []
        self.skip = 0

    def boundary(self):
        if not self.parts or self.parts[-1] != '\n':
            self.parts.append('\n')

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()

        if tag in self.SKIP_TAGS:
            self.skip += 1
            return

        if not self.skip and tag in self.BLOCK_TAGS:
            self.boundary()

    def handle_endtag(self, tag):
        tag = tag.lower()

        if tag in self.SKIP_TAGS and self.skip:
            self.skip -= 1
            return

        if not self.skip and tag in self.BLOCK_TAGS:
            self.boundary()

    def handle_data(self, data):
        if self.skip:
            return

        text = ' '.join(data.split())

        if text:
            self.parts.append(text)

    def text(self):
        raw = ' '.join(self.parts)
        raw = re.sub('\\s*\\n\\s*', '\n', raw)
        raw = re.sub('[ \\t]+', ' ', raw)
        return raw.strip()


class DDGParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.results = []
        self.current = None
        self.in_title = False
        self.in_snippet = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        cls = attrs.get('class', '')

        if tag == 'a' and 'result__a' in cls:
            self.current = {
                'url': clean_url(
                    attrs.get(
                        'href',
                        ''
                    )
                ),
                'title': '',
                'snippet': ''
            }
            self.in_title = True

        elif self.current and 'result__snippet' in cls:
            self.in_snippet = True

    def handle_endtag(self, tag):
        if tag == 'a' and self.in_title:
            self.in_title = False

            if self.current and self.current['url']:
                self.results.append(self.current)

            self.current = None

        if self.in_snippet and tag in {'a', 'div'}:
            self.in_snippet = False

    def handle_data(self, data):
        if self.current and self.in_title:
            self.current['title'] += data

        elif self.current and self.in_snippet:
            self.current['snippet'] += data


def clean_url(url):
    if url.startswith('//'):
        url = 'https:' + url

    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if 'uddg' in query:
            return unquote(query['uddg'][0])

    except Exception:
        pass

    return url


def raw_words(text):
    return re.findall(
        "[a-z0-9]+(?:'[a-z0-9]+)?",
        (
            text
            or ''
        ).lower()
    )


def normalise_word(word):
    word = (
        word
        or ''
    ).lower()

    if word in NORMALISE_WORD:
        return NORMALISE_WORD[word]

    if len(word) > 5 and word.endswith('ies'):
        return word[:-3] + 'y'

    if (
        len(word) > 4
        and
        word.endswith('s')
        and
        not word.endswith('ss')
    ):
        return word[:-1]

    return word


def normalised_words(text):
    return [
        normalise_word(word)
        for word in raw_words(text)
    ]


def content_words(text):
    return [
        word
        for word in normalised_words(text)
        if (
            word not in STOPWORDS
            and
            len(word) > 1
        )
    ]


def subject_words(text):
    return [
        word
        for word in content_words(text)
        if word not in INTENT_WORDS
    ]


def coverage_required_words(text):
    return [
        word
        for word in subject_words(text)
        if word not in OPTIONAL_COVERAGE_MODIFIERS
    ]


def canonical(text):
    return ' '.join(
        re.sub(
            '[^a-z0-9]+',
            ' ',
            (
                text
                or ''
            ).lower()
        ).split()
    )


def unique_list(values, limit=50):
    output = []
    seen = set()

    for value in values:
        value = str(value).strip()

        if not value:
            continue

        key = canonical(value)

        if key in seen:
            continue

        seen.add(key)
        output.append(value)

        if len(output) >= limit:
            break

    return output


def wrap(text, indent=''):
    print(
        textwrap.fill(
            str(text),
            width=92,
            initial_indent=indent,
            subsequent_indent=indent
        )
    )


def normalise_condition(word):
    word = normalise_word(word)
    return CONDITION_NORMALISATION.get(
        word,
        word
    )


def essential_conditions(question):
    output = []

    for word in normalised_words(question):
        if word in ESSENTIAL_CONDITION_WORDS:
            word = normalise_condition(word)

            if word not in output:
                output.append(word)

    return output


def phenomenon_anchors(question):
    words = normalised_words(question)
    anchors = []

    for index, word in enumerate(words):
        if word not in PHENOMENON_TRIGGER_WORDS:
            continue

        for candidate in words[
            index + 1:
            index + 6
        ]:
            if candidate in PHENOMENON_TRIGGER_WORDS:
                continue

            if candidate in PHENOMENON_IGNORE_WORDS:
                continue

            if candidate in STOPWORDS:
                continue

            if candidate in INTENT_WORDS:
                continue

            if candidate in OPTIONAL_COVERAGE_MODIFIERS:
                continue

            if candidate in ESSENTIAL_CONDITION_WORDS:
                continue

            if len(candidate) < 3:
                continue

            if candidate not in anchors:
                anchors.append(candidate)

            if len(anchors) >= 3:
                return anchors

    return anchors


def text_conditions(text):
    output = set()

    for word in normalised_words(text):
        if word in ESSENTIAL_CONDITION_WORDS:
            output.add(
                normalise_condition(word)
            )

    return output


def condition_coverage(text, question):
    required = set(
        essential_conditions(question)
    )

    if not required:
        return {
            'score': 1.0,
            'required': [],
            'covered': [],
            'missing': [],
            'pass': True
        }

    found = text_conditions(text)
    covered = required & found

    score = (
        len(covered)
        /
        len(required)
    )

    return {
        'score': score,
        'required': sorted(required),
        'covered': sorted(covered),
        'missing': sorted(
            required - covered
        ),
        'pass': len(
            required - covered
        ) == 0
    }


def phenomenon_coverage(text, question):
    required = set(
        phenomenon_anchors(question)
    )

    if not required:
        return {
            'score': 1.0,
            'required': [],
            'covered': [],
            'missing': [],
            'pass': True
        }

    found = set(
        subject_words(text)
    )

    covered = required & found

    score = (
        len(covered)
        /
        len(required)
    )

    return {
        'score': score,
        'required': sorted(required),
        'covered': sorted(covered),
        'missing': sorted(
            required - covered
        ),
        'pass': bool(covered)
    }


def anchor_bonus(text, question):
    conditions = condition_coverage(
        text,
        question
    )

    phenomenon = phenomenon_coverage(
        text,
        question
    )

    return (
        conditions['score'] * 2.5
        +
        phenomenon['score'] * 2.0
    )



def get_pmei_records():
    meta = empty_pmei_transport()
    meta['required'] = True

    if not API_KEY:
        meta['errors'].append(
            'DAVE_RUNNER_API_KEY is not set'
        )
        return [], meta

    routes = [
        '/memory/continuity',
        '/get_continuity',
        '/memory/continuity/get'
    ]

    meta['attempted'] = True

    for route in routes:
        try:
            response = session.post(
                BASE_URL + route,
                headers={
                    'X-API-KEY': API_KEY,
                    'Content-Type':
                        'application/json'
                },
                json={
                    'limit': PMEI_DEPTH
                },
                timeout=TIMEOUT
            )

            response.raise_for_status()

            payload = response.json()
            data = payload.get(
                'data',
                payload
            )

            if isinstance(
                data,
                dict
            ):
                records = (
                    data.get('items')
                    or
                    data.get('records')
                    or
                    []
                )

            elif isinstance(
                data,
                list
            ):
                records = data

            else:
                records = []

            if records:
                meta['success'] = True
                meta['route'] = route
                return records, meta

            meta['errors'].append(
                f'{route}: valid response but no continuity records'
            )

        except Exception as error:
            meta['errors'].append(
                f'{route}: {type(error).__name__}: {error}'
            )

    return [], meta


def get_pmei_historical_records(
    page_size=200
):
    meta = empty_pmei_transport()
    meta['required'] = True
    meta['route'] = '/memory/continuity/get'
    meta['exhaustive'] = False
    meta['scanned_count'] = 0
    meta['available_count'] = None
    meta['pages'] = 0

    try:
        requested_page_size = int(page_size)
    except (TypeError, ValueError):
        requested_page_size = 200

    meta['page_size'] = min(
        max(requested_page_size, 1),
        200
    )

    if not API_KEY:
        meta['errors'].append(
            'DAVE_RUNNER_API_KEY is not set'
        )
        return [], meta

    meta['attempted'] = True

    def fetch_page(request_payload):
        response = session.post(
            BASE_URL + '/memory/continuity/get',
            headers={
                'X-API-KEY': API_KEY,
                'Content-Type':
                    'application/json'
            },
            json=request_payload,
            timeout=TIMEOUT
        )

        response.raise_for_status()

        payload = response.json()
        data = payload.get(
            'data',
            payload
        )

        if isinstance(data, dict):
            records = (
                data.get('items')
                or
                data.get('records')
                or
                []
            )

        elif isinstance(data, list):
            records = data

        else:
            raise ValueError(
                'Historical continuity response '
                'did not contain a record list'
            )

        if not isinstance(records, list):
            raise ValueError(
                'Historical continuity records '
                'were not returned as a list'
            )

        return records

    records, scan_meta = scan_continuity_archive(
        fetch_page,
        page_size=page_size,
    )

    meta['exhaustive'] = scan_meta['exhaustive']
    meta['scanned_count'] = scan_meta['scanned_count']
    meta['available_count'] = scan_meta['available_count']
    meta['pages'] = scan_meta['pages']
    meta['page_size'] = scan_meta['page_size']

    meta['errors'].extend(
        scan_meta['errors']
    )

    meta['success'] = bool(
        scan_meta['exhaustive']
    )

    return records, meta

def load_boot_learning(records):
    global BOOT_STATE

    open_threads = []
    successes = []
    failures = []
    events = []

    for record in records:
        open_threads.extend(
            record.get(
                'open_threads'
            )
            or
            []
        )

        layer = (
            record.get(
                'learning_layer'
            )
            or
            {}
        )

        successes.extend(
            layer.get(
                'successful_patterns'
            )
            or
            []
        )

        failures.extend(
            layer.get(
                'failed_patterns'
            )
            or
            []
        )

        events.extend(
            layer.get(
                'learning_events'
            )
            or
            []
        )

    BOOT_STATE = {
        'loaded': True,
        'open_threads':
            unique_list(
                open_threads,
                100
            ),
        'successful_patterns':
            unique_list(
                successes,
                100
            ),
        'failed_patterns':
            unique_list(
                failures,
                100
            ),
        'learning_events':
            unique_list(
                events,
                100
            )
    }


def strong_reference_dependency(question):
    q = (
        question
        or ''
    ).strip().lower()

    explicit_patterns = (
        '^\\s*what about\\s+'
        '(him|her|it|them|this|that|those|these)\\b',

        '^\\s*how about\\s+'
        '(him|her|it|them|this|that|those|these)\\b',

        '^\\s*and what about\\s+',

        '^\\s*and how about\\s+'
    )

    for pattern in explicit_patterns:
        if re.search(
            pattern,
            q
        ):
            return True

    wh_pronoun_pattern = (
        '^\\s*'
        '(who|what|where|when|why|how)'
        '\\s+'
        '(is|are|was|were|did|does|do|'
        'has|have|had|can|could|would|will|should)'
        '\\s+'
        '(he|she|him|her|it|they|them|'
        'this|that|these|those)'
        '\\b'
    )

    if re.search(
        wh_pronoun_pattern,
        q
    ):
        return True

    auxiliary_pronoun_pattern = (
        '^\\s*'
        '(is|are|was|were|did|does|do|'
        'has|have|had|can|could|would|will|should)'
        '\\s+'
        '(he|she|him|her|it|they|them|'
        'this|that|these|those)'
        '\\b'
    )

    if re.search(
        auxiliary_pronoun_pattern,
        q
    ):
        return True

    words = raw_words(q)

    if len(words) <= 4:
        if any(
            word in REFERENCE_PRONOUNS
            for word in words
        ):
            if not re.match(
                '^\\s*'
                '(is|are|was|were)'
                '\\s+it\\s+\\w+\\??\\s*$',
                q
            ):
                return True

    return False


def personal_orientation(question):
    current = canonical(question)

    repeat_count = sum(
        1
        for old in HISTORY
        if canonical(
            old['question']
        ) == current
    )

    follow_up = bool(
        re.match(
            '^\\s*('
            'so\\b|'
            'and\\b|'
            'but\\b|'
            'then\\b|'
            'now\\b|'
            'also\\b|'
            'what about\\b|'
            'how about\\b'
            ')',
            question.lower()
        )
    )

    reference = strong_reference_dependency(
        question
    )

    return {
        'repeat_count': repeat_count,
        'follow_up': follow_up,
        'reference': reference
    }


def thread_similarity(question, thread):
    a = set(
        subject_words(question)
    )

    b = set(
        thread['topic_words']
    )

    if not a or not b:
        return 0.0

    return (
        len(
            a.intersection(b)
        )
        /
        len(
            a.union(b)
        )
    )


def get_learning_thread(question):
    best = None
    best_score = 0.0

    for key, thread in LEARNING_THREADS.items():
        score = thread_similarity(
            question,
            thread
        )

        if score > best_score:
            best = key
            best_score = score

    if (
        best
        and
        best_score >= 0.25
    ):
        return (
            best,
            LEARNING_THREADS[best],
            False
        )

    words = unique_list(
        subject_words(question),
        12
    )

    key = '|'.join(
        sorted(
            words[:4]
        )
    ) or 'general'

    base = key
    n = 2

    while key in LEARNING_THREADS:
        key = (
            base
            +
            f'#{n}'
        )
        n += 1

    LEARNING_THREADS[key] = {
        'topic_words': words,
        'turns': 0,
        'successful_query_terms': [],
        'quality_gate_passes': 0,
        'quality_gate_fails': 0,
        'processing_successes': 0,
        'processing_failures': 0,
        'coverage_failures': 0,
        'condition_failures': 0,
        'phenomenon_failures': 0,
        'causal_support_failures': 0,
        'contradictions': 0,
        'learning_events': []
    }

    return (
        key,
        LEARNING_THREADS[key],
        True
    )


def build_subject_terms(question):
    terms = []

    for word in subject_words(question):
        if word not in terms:
            terms.append(word)

    return terms


def build_query(
    question,
    signals,
    thread
):
    terms = build_subject_terms(
        question
    )

    if HISTORY:
        if (
            signals['reference']
            or
            signals['follow_up']
        ):
            recent = Counter()

            for old in list(
                HISTORY
            )[-3:]:
                recent.update(
                    subject_words(
                        old['question']
                    )
                )

            for word, count in recent.most_common(
                6
            ):
                if word not in terms:
                    terms.append(word)

    for word in thread[
        'successful_query_terms'
    ][:4]:
        if (
            word not in terms
            and
            word not in INTENT_WORDS
        ):
            terms.append(word)

    if not terms:
        terms = content_words(
            question
        )

    return ' '.join(
        terms[:20]
    )


def determine_domain(
    question,
    signals
):
    lower = question.lower()

    markers = [
        'pmei',
        'my api',
        'continuity record',
        'my continuity',
        'my project',
        'dave engineering',
        'dave architecture',
        'dave governance',
        'dave steward',
        'knobhead dave',
        'builder dave'
    ]

    if any(
        marker in lower
        for marker in markers
    ):
        return 'PMEI_LOOKUP'

    if (
        HISTORY
        and
        (
            signals['follow_up']
            or
            signals['reference']
        )
    ):
        return HISTORY[-1]['route']

    return 'WEB_LOOKUP'


def coverage_terms(question):
    terms = []

    for word in coverage_required_words(
        question
    ):
        if word not in terms:
            terms.append(word)

    return terms


def subject_coverage(
    text,
    question
):
    required = set(
        coverage_terms(question)
    )

    found = set(
        subject_words(text)
    )

    if not required:
        return {
            'score': 0.0,
            'required': [],
            'found': [],
            'missing': []
        }

    matched = required & found

    return {
        'score':
            len(matched)
            /
            len(required),

        'required':
            sorted(required),

        'found':
            sorted(matched),

        'missing':
            sorted(
                required - matched
            )
    }


def is_meta_explanation(text):
    lower = (
        text
        or ''
    ).strip().lower()

    meta_subjects = (
        'this article',
        'the article',
        'this guide',
        'the guide',
        'this page',
        'the page',
        'this post',
        'the post',
        'this blog',
        'the blog',
        'this video',
        'the video',
        'this resource',
        'the resource',
        'this section',
        'the section'
    )

    meta_actions = (
        'explains',
        'explain',
        'explores',
        'explore',
        'looks at',
        'looks into',
        'discusses',
        'discuss',
        'covers',
        'cover',
        'examines',
        'examine',
        'describes why',
        'provides reasons',
        'outlines the reasons',
        'reveals why'
    )

    if any(
        lower.startswith(subject)
        for subject in meta_subjects
    ):
        if any(
            action in lower
            for action in meta_actions
        ):
            return True

    embedded_meta_patterns = (
        r'\bwe\s+will\s+(?:discuss|explore|cover|examine|look at)\b',
        r"\bwe['’]?ll\s+(?:discuss|explore|cover|examine|look at)\b",
        r'\blater\s+in\s+this\s+(?:article|guide|post|page)\b',
        r'\bin\s+this\s+(?:article|guide|post|page)\s+we\b',
        r'\b(?:this|the)\s+(?:article|guide|post|page)\s+will\b',
        r'\bread\s+on\b',
        r'\b(?:below|later)\s+we\s+(?:discuss|explore|cover|examine)\b'
    )

    for pattern in embedded_meta_patterns:
        if re.search(
            pattern,
            lower
        ):
            return True

    patterns = (
        '\\b(article|guide|page|post|blog|video|resource)\\b'
        '.{0,50}'
        '\\b(explains|explores|discusses|covers|examines)\\b',

        '\\b(explains|explores|discusses|covers|examines)\\b'
        '.{0,50}'
        '\\b(article|guide|page|post|blog|video|resource)\\b'
    )

    for pattern in patterns:
        if re.search(
            pattern,
            lower
        ):
            return True

    return False


def explanation_score(text):
    if is_meta_explanation(text):
        return 0

    words = set(
        content_words(text)
    )

    hits = len(
        words.intersection(
            EXPLANATION_TERMS
        )
    )

    phrases = (
        'because',
        'due to',
        'caused by',
        'results from',
        'leads to',
        'occurs when',
        'happens when',
        'as a result',
        'the reason',
        'means it',
        'meaning it',
        'supposed to',
        'time delay',
        'thermal shock',
        'internal tension',
        'temperature difference',
        'differential expansion'
    )

    lower = text.lower()

    hits += sum(
        2
        for phrase in phrases
        if phrase in lower
    )

    return min(
        hits,
        12
    )


def boilerplate_ratio(text):
    words = normalised_words(text)

    if not words:
        return 1.0

    hits = sum(
        1
        for word in words
        if word in BOILERPLATE_WORDS
    )

    return (
        hits
        /
        len(words)
    )


def ui_phrase_hits(text):
    lower = (
        text
        or ''
    ).lower()

    return sum(
        1
        for phrase in UI_PHRASES
        if phrase in lower
    )


def repeated_navigation_score(text):
    words = normalised_words(text)

    if not words:
        return 1.0

    nav_hits = [
        word
        for word in words
        if word in BOILERPLATE_WORDS
    ]

    unique_nav = len(
        set(nav_hits)
    )

    repetition = len(nav_hits)

    score = (
        unique_nav * 0.04
        +
        repetition * 0.015
    )

    return min(
        score,
        1.0
    )


def looks_like_navigation_block(text):
    text = ' '.join(
        (
            text
            or ''
        ).split()
    )

    if not text:
        return True

    words = normalised_words(text)
    word_count = len(words)

    if word_count == 0:
        return True

    phrase_hits = ui_phrase_hits(text)
    ratio = boilerplate_ratio(text)
    nav_score = repeated_navigation_score(text)
    explanation = explanation_score(text)

    if phrase_hits >= 2:
        return True

    if (
        word_count <= 16
        and
        phrase_hits >= 1
    ):
        return True

    if (
        ratio >= 0.24
        and
        explanation == 0
    ):
        return True

    if (
        nav_score >= 0.46
        and
        explanation == 0
    ):
        return True

    punctuation = len(
        re.findall(
            '[.!?]',
            text
        )
    )

    if (
        word_count >= 20
        and
        ratio >= 0.16
        and
        punctuation == 0
        and
        explanation == 0
    ):
        return True

    return False


def hard_boilerplate_reject(text):
    if looks_like_navigation_block(text):
        return True

    lower = (
        text
        or ''
    ).lower()

    chrome_phrases = (
        'log in create new account',
        'subscribe to our newsletter',
        'privacy policy terms',
        'discover shop news help',
        'home news business entertainment',
        'facebook instagram youtube',
        'contact us privacy policy',
        'jump to content existing user',
        'all activity home',
        'go to topic listing'
    )

    if any(
        phrase in lower
        for phrase in chrome_phrases
    ):
        return True

    return False


def sanitise_page_text(text):
    if not text:
        return ''

    raw_blocks = re.split(
        '\\n+',
        text
    )

    kept = []
    seen = set()

    for block in raw_blocks:
        block = ' '.join(
            block.split()
        )

        if len(block) < 20:
            continue

        if hard_boilerplate_reject(block):
            continue

        key = canonical(block)

        if not key:
            continue

        if key in seen:
            continue

        seen.add(key)
        kept.append(block)

    return '\n'.join(kept)


def information_gain(
    text,
    question
):
    q = set(
        subject_words(question)
    )

    t = set(
        subject_words(text)
    )

    if not t:
        return 0.0

    new = t - q

    return (
        len(new)
        /
        len(t)
    )


def passage_usefulness(
    text,
    question
):
    if not text:
        return 0.0

    if hard_boilerplate_reject(text):
        return 0.0

    if is_meta_explanation(text):
        return 0.0

    words = raw_words(text)

    if len(words) < 8:
        return 0.0

    coverage = subject_coverage(
        text,
        question
    )['score']

    explanation = min(
        explanation_score(text) / 4,
        1.0
    )

    gain = information_gain(
        text,
        question
    )

    length = min(
        len(words) / 80,
        1.0
    )

    boiler = boilerplate_ratio(text)

    anchors = min(
        anchor_bonus(
            text,
            question
        ) / 4.5,
        1.0
    )

    value = (
        coverage * 0.28
        +
        explanation * 0.27
        +
        gain * 0.15
        +
        length * 0.10
        +
        anchors * 0.20
    )

    value -= min(
        boiler * 2,
        0.60
    )

    return max(
        0.0,
        min(
            value,
            1.0
        )
    )


def source_subject_relevance(
    title,
    text,
    question
):
    subject = set(
        build_subject_terms(question)
    )

    if not subject:
        return 0.0

    title_words = set(
        subject_words(title)
    )

    body_words = set(
        subject_words(text)
    )

    combined = (
        title_words
        |
        body_words
    )

    return (
        len(
            subject.intersection(
                combined
            )
        )
        /
        len(subject)
    )


def best_passages(
    text,
    query,
    question,
    limit=4
):
    pieces = re.split(
        '\\n+|(?<=[.!?])\\s+',
        text
    )

    ranked = []

    for sentence in pieces:
        sentence = ' '.join(
            sentence.split()
        )

        if len(sentence) < 35:
            continue

        if hard_boilerplate_reject(sentence):
            continue

        if is_meta_explanation(sentence):
            continue

        usefulness = passage_usefulness(
            sentence,
            question
        )

        if usefulness < 0.16:
            continue

        coverage = subject_coverage(
            sentence,
            question
        )['score']

        query_overlap = len(
            set(
                subject_words(sentence)
            )
            &
            set(
                query.split()
            )
        )

        anchors = anchor_bonus(
            sentence,
            question
        )

        rank = (
            usefulness * 8
            +
            coverage * 5
            +
            query_overlap
            +
            anchors * 2.5
        )

        ranked.append(
            (
                rank,
                usefulness,
                sentence[:750]
            )
        )

    ranked.sort(
        reverse=True,
        key=lambda item:
            item[0]
    )

    return [
        (
            sentence,
            usefulness
        )
        for rank, usefulness, sentence
        in ranked[:limit]
    ]


def web_search(query):
    response = session.get(
        'https://html.duckduckgo.com/html/?q='
        +
        quote_plus(query),
        timeout=TIMEOUT
    )

    response.raise_for_status()

    parser = DDGParser()
    parser.feed(response.text)

    output = []
    seen = set()

    for item in parser.results:
        if not item['url']:
            continue

        if item['url'] in seen:
            continue

        seen.add(item['url'])

        item['title'] = html.unescape(
            ' '.join(
                item['title'].split()
            )
        )

        item['snippet'] = html.unescape(
            ' '.join(
                item['snippet'].split()
            )
        )

        output.append(item)

        if len(output) >= WEB_RESULTS:
            break

    return output


def fetch_page(url):
    try:
        response = session.get(
            url,
            timeout=TIMEOUT
        )

        response.raise_for_status()

        parser = TextExtractor()

        parser.feed(
            response.text[:2_000_000]
        )

        raw = parser.text()[:160_000]

        return sanitise_page_text(
            raw
        )[:120_000]

    except Exception:
        return ''



def retrieve_web(
    query,
    question
):
    evidence = []
    results = web_search(query)

    for index, item in enumerate(results):
        snippet = item['snippet']
        retrieved_at = utc_now_iso()

        if (
            snippet
            and
            not is_meta_explanation(
                snippet
            )
        ):
            source_relevance = source_subject_relevance(
                item['title'],
                snippet,
                question
            )

            usefulness = passage_usefulness(
                snippet,
                question
            )

            if (
                usefulness >= 0.16
                and
                source_relevance >= 0.20
            ):
                evidence.append({
                    'source':
                        item['title'],
                    'url':
                        item['url'],
                    'retrieval_type':
                        'WEB_SNIPPET',
                    'retrieved_at_utc':
                        retrieved_at,
                    'text':
                        snippet,
                    'usefulness':
                        usefulness,
                    'coverage':
                        subject_coverage(
                            snippet,
                            question
                        )['score']
                })

        if index < WEB_FETCH:
            page = fetch_page(
                item['url']
            )

            for passage, usefulness in best_passages(
                page,
                query,
                question,
                4
            ):
                relevance = source_subject_relevance(
                    item['title'],
                    passage,
                    question
                )

                if relevance < 0.20:
                    continue

                evidence.append({
                    'source':
                        item['title'],
                    'url':
                        item['url'],
                    'retrieval_type':
                        'WEB_PAGE_PASSAGE',
                    'retrieved_at_utc':
                        utc_now_iso(),
                    'text':
                        passage,
                    'usefulness':
                        usefulness,
                    'coverage':
                        subject_coverage(
                            passage,
                            question
                        )['score']
                })

    deduped = []
    seen = set()

    for item in evidence:
        key = (
            canonical(
                item['source']
            ),
            canonical(
                item['text']
            )
        )

        if key in seen:
            continue

        seen.add(key)
        deduped.append(item)

    deduped.sort(
        reverse=True,
        key=lambda item:
            (
                anchor_bonus(
                    item['text'],
                    question
                ),
                item['coverage'],
                item['usefulness']
            )
    )

    return deduped[:20]


def record_text(record):
    brief = (
        record.get(
            'human_brief'
        )
        or
        {}
    )

    return ' '.join([
        str(
            brief.get(
                'title',
                ''
            )
        ),
        str(
            brief.get(
                'summary',
                ''
            )
        ),
        str(
            brief.get(
                'decision_made',
                ''
            )
        ),
        str(
            brief.get(
                'why_it_matters',
                ''
            )
        ),
        str(
            record.get(
                'last_stable_state',
                ''
            )
        ),
        str(
            record.get(
                'context_shard',
                ''
            )
        )
    ])



def retrieve_pmei(
    records,
    query,
    question,
    transport=None
):
    evidence = []

    subject = set(
        build_subject_terms(question)
    )

    transport = (
        transport
        or
        empty_pmei_transport()
    )

    for record in records:
        text = record_text(record)

        text_subject = set(
            subject_words(text)
        )

        if not subject.intersection(
            text_subject
        ):
            continue

        record_id = record.get(
            'id',
            '?'
        )

        for passage, usefulness in best_passages(
            text,
            query,
            question,
            3
        ):
            evidence.append({
                'source':
                    'PMEi Record '
                    +
                    str(record_id),

                'url':
                    None,

                'record_id':
                    record_id,

                'retrieval_type':
                    'PMEI_CONTINUITY_RECORD',

                'pmei_route':
                    transport.get('route'),

                'retrieved_at_utc':
                    utc_now_iso(),

                'text':
                    passage,

                'usefulness':
                    usefulness,

                'coverage':
                    subject_coverage(
                        passage,
                        question
                    )['score']
            })

    evidence.sort(
        reverse=True,
        key=lambda item:
            (
                anchor_bonus(
                    item['text'],
                    question
                ),
                item['coverage'],
                item['usefulness']
            )
    )

    return evidence[:20]


def question_type(question):
    words = set(
        normalised_words(question)
    )

    if words.intersection(
        COMPARATIVE_TERMS
    ):
        return 'COMPARE'

    if 'why' in words:
        return 'CAUSE'

    q = question.lower()

    if q.startswith('how '):
        return 'MECHANISM'

    if q.startswith('who '):
        return 'IDENTITY'

    if any(
        q.startswith(prefix)
        for prefix in (
            'is ',
            'are ',
            'was ',
            'were ',
            'did ',
            'does ',
            'do '
        )
    ):
        return 'BOOLEAN'

    return 'GENERAL'


def similarity(a, b):
    aa = set(
        subject_words(a)
    )

    bb = set(
        subject_words(b)
    )

    if not aa or not bb:
        return 0.0

    return (
        len(
            aa.intersection(bb)
        )
        /
        len(
            aa.union(bb)
        )
    )


def is_advice_claim(text):
    lower = (
        text
        or ''
    ).strip().lower()

    if any(
        lower.startswith(opener)
        for opener in ADVICE_OPENERS
    ):
        return True

    patterns = (
        '\\byou should\\b',
        '\\byou can try\\b',
        '\\byou can use\\b',
        '\\bmake sure\\b',
        '\\bavoid using\\b',
        '\\bstart with\\b',
        '\\bfor best results\\b',
        '\\bit is better to\\b',
        "\\bit's better to\\b",
        '\\bhold the\\b',
        '\\bplace the\\b',
        '\\bput the\\b',
        '\\bthe best way to\\b',
        '\\bthe most effective way to\\b',
        '\\bone effective way to\\b',
        '\\bto prevent\\b',
        '\\bto avoid\\b',
        '\\bto reduce\\b',
        '\\bto stop\\b',
        '\\bprevention tip\\b',
        '\\bprevent .* by\\b',
        r'\btry\s+(?:not\s+to\s+)?[a-z]+',
        r'\bconsider\s+[a-z]+(?:ing)?\b',
        r'\bremember\s+to\b',
        r'\bdo\s+not\s+(?:panic|use|drink|place|put|leave|open|close|run|turn|switch)\b',
        r'\b(?:switch|turn|move|point|direct|set|keep)\s+(?:the\s+)?(?:vents?|fan|heater|air|ac|a/c)\b'
    )

    for pattern in patterns:
        if re.search(
            pattern,
            lower
        ):
            return True

    return False


def mechanism_words(
    text,
    question
):
    q_words = set(
        subject_words(question)
    )

    candidate_words = set(
        subject_words(text)
    )

    generic = {
        'article',
        'guide',
        'page',
        'post',
        'video',
        'question',
        'answer',
        'thing',
        'things',
        'case',
        'way'
    }

    return {
        word
        for word in candidate_words
        if (
            word not in q_words
            and
            word not in generic
            and
            len(word) >= 4
        )
    }


def mechanism_signal_score(text):
    words = set(
        subject_words(text)
    )

    score = len(
        words.intersection(
            MECHANISM_SIGNAL_WORDS
        )
    )

    lower = text.lower()

    phrases = (
        'thermal shock',
        'internal tension',
        'temperature difference',
        'differential expansion',
        'because of',
        'caused by',
        'due to',
        'leads to',
        'results in',
        'results from',
        'allow the',
        'allows the',
        'depends on',
        'reflects light',
        'light reflection',
        'water film',
        'smooth surface'
    )

    score += sum(
        2
        for phrase in phrases
        if phrase in lower
    )

    return score


def looks_like_title_claim(text):
    raw = ' '.join(
        (
            text
            or ''
        ).split()
    )

    if not raw:
        return True

    lower = raw.lower().strip()

    if re.search(
        '\\s[-|:]\\s*'
        '(youtube|wikipedia|reddit|quora|'
        'slashgear|medium|substack|news\\s*\\d+)'
        '\\s*$',
        lower
    ):
        return True

    if re.match(
        '^('
        'why|what|how|when|where|who|which|'
        'does|do|can|could|is|are|should|will|would'
        ')\\b',
        lower
    ):
        if '?' in raw:
            return True

        words = raw_words(raw)

        if (
            len(words) <= 18
            and
            not re.search(
                '[.!]$',
                raw
            )
        ):
            return True

    if re.search(
        '\\b('
        'science explained|'
        'everything you need to know|'
        'the science behind|'
        'guide to|'
        'explained'
        ')$',
        lower
    ):
        if not re.search(
            '[.!]$',
            raw
        ):
            return True

    words = raw.split()

    titleish = sum(
        1
        for word in words
        if (
            word[:1].isupper()
            and
            len(word) > 1
        )
    )

    if (
        6 <= len(words) <= 18
        and
        titleish / len(words) >= 0.55
        and
        not re.search(
            '[.!?]$',
            raw
        )
    ):
        return True

    return False


def is_asserted_proposition(text):
    raw = ' '.join(
        (
            text
            or ''
        ).split()
    )

    if looks_like_title_claim(raw):
        return False

    if is_meta_explanation(raw):
        return False

    words = raw_words(raw)

    if len(words) < 8:
        return False

    return bool(
        re.search(
            '\\b('
            'is|are|was|were|has|have|had|'
            'does|do|did|means|meaning|'
            'runs|run|running|keeps|keep|'
            'stops|stop|stopping|'
            'causes|cause|because|due|'
            'works|work|working|'
            'turns|turn|turned|'
            'switches|switch|switched|'
            'supposed|allows|allow|'
            'prevents|prevent|'
            'controls|control|'
            'creates|create|'
            'becomes|become|'
            'makes|make|'
            'forms|form|'
            'condenses|condense|'
            'depends|depend|'
            'leads|lead|'
            'results|result|'
            'connects|connect|connected|'
            'expands|expand|'
            'contracts|contract|'
            'reflects|reflect|'
            'appears|appear|'
            'looks|look'
            ')\\b',
            raw.lower()
        )
    )


def build_claims(
    evidence,
    question
):
    claims = []

    for item in evidence:
        text = ' '.join(
            item['text'].split()
        )

        if hard_boilerplate_reject(text):
            continue

        if is_meta_explanation(text):
            continue

        usefulness = item[
            'usefulness'
        ]

        coverage = subject_coverage(
            text,
            question
        )

        words = raw_words(text)

        if len(words) < 8:
            continue

        if usefulness < 0.18:
            continue

        if coverage['score'] <= 0:
            continue

        if not is_asserted_proposition(text):
            continue

        if any(
            similarity(
                text,
                existing['text']
            ) >= 0.75
            for existing in claims
        ):
            continue

        explanation = explanation_score(text)
        mechanism = mechanism_signal_score(text)
        advice = is_advice_claim(text)

        condition_data = condition_coverage(
            text,
            question
        )

        phenomenon_data = phenomenon_coverage(
            text,
            question
        )

        claims.append({
            'text':
                text,

            'source':
                item['source'],

            'url':
                item.get('url'),

            'record_id':
                item.get('record_id'),

            'retrieval_type':
                item.get('retrieval_type'),

            'pmei_route':
                item.get('pmei_route'),

            'retrieved_at_utc':
                item.get('retrieved_at_utc'),

            'usefulness':
                usefulness,

            'coverage':
                coverage['score'],

            'explanation':
                explanation,

            'mechanism':
                mechanism,

            'advice':
                advice,

            'condition_score':
                condition_data['score'],

            'phenomenon_score':
                phenomenon_data['score']
        })

    claims.sort(
        reverse=True,
        key=lambda item:
            (
                item['condition_score'],
                item['phenomenon_score'],
                item['mechanism'],
                item['explanation'],
                item['coverage'],
                item['usefulness']
            )
    )

    return claims[:12]


def has_negation(text):
    return bool(
        set(
            normalised_words(text)
        )
        &
        NEGATION_WORDS
    )


def proposition_words(text):
    return {
        word
        for word in subject_words(text)
        if word not in NEGATION_WORDS
    }


def proposition_similarity(a, b):
    aa = proposition_words(a)
    bb = proposition_words(b)

    if not aa or not bb:
        return 0.0

    return (
        len(
            aa.intersection(bb)
        )
        /
        len(
            aa.union(bb)
        )
    )


def contradictions(claims):
    conflicts = []

    for i in range(
        len(claims)
    ):
        for j in range(
            i + 1,
            len(claims)
        ):
            a = claims[i]
            b = claims[j]

            a_neg = has_negation(
                a['text']
            )

            b_neg = has_negation(
                b['text']
            )

            if a_neg == b_neg:
                continue

            prop_similarity = proposition_similarity(
                a['text'],
                b['text']
            )

            if prop_similarity < 0.68:
                continue

            conflicts.append({
                'claim_a': a,
                'claim_b': b,
                'similarity':
                    prop_similarity
            })

    return conflicts


def base_claim_score(
    claim,
    qtype
):
    explanation = claim.get(
        'explanation',
        0
    )

    mechanism = claim.get(
        'mechanism',
        0
    )

    coverage = claim.get(
        'coverage',
        0.0
    )

    usefulness = claim.get(
        'usefulness',
        0.0
    )

    condition_score = claim.get(
        'condition_score',
        1.0
    )

    phenomenon_score = claim.get(
        'phenomenon_score',
        1.0
    )

    advice_penalty = 0.0

    if (
        qtype in {
            'CAUSE',
            'MECHANISM'
        }
        and
        claim.get(
            'advice',
            False
        )
    ):
        advice_penalty = 6.0

    if qtype in {
        'CAUSE',
        'MECHANISM'
    }:
        return (
            condition_score * 5.0
            +
            phenomenon_score * 4.0
            +
            mechanism * 3.0
            +
            explanation * 2.0
            +
            coverage * 1.5
            +
            usefulness
            -
            advice_penalty
        )

    return (
        condition_score * 3.0
        +
        phenomenon_score * 2.0
        +
        coverage * 2.0
        +
        usefulness
        +
        explanation * 0.5
    )


def select_claims(
    claims,
    question
):
    if not claims:
        return []

    qtype = question_type(
        question
    )

    if qtype not in {
        'CAUSE',
        'MECHANISM'
    }:
        ranked = sorted(
            claims,
            reverse=True,
            key=lambda claim:
                base_claim_score(
                    claim,
                    qtype
                )
        )

        selected = []

        for claim in ranked:
            if any(
                similarity(
                    claim['text'],
                    existing['text']
                ) >= 0.55
                for existing in selected
            ):
                continue

            selected.append(claim)

            if len(selected) >= 3:
                break

        return selected

    remaining = list(claims)
    selected = []
    used_mechanism_words = set()

    while (
        remaining
        and
        len(selected) < 3
    ):
        best_claim = None
        best_score = None

        for claim in remaining:

            # v6.7:
            # WHY/HOW advice cannot enter the selected answer.
            if claim.get(
                'advice',
                False
            ):
                continue

            if any(
                similarity(
                    claim['text'],
                    existing['text']
                ) >= 0.55
                for existing in selected
            ):
                continue

            base = base_claim_score(
                claim,
                qtype
            )

            claim_mechanism_words = mechanism_words(
                claim['text'],
                question
            )

            new_mechanism = (
                claim_mechanism_words
                -
                used_mechanism_words
            )

            contribution_bonus = min(
                len(new_mechanism),
                8
            ) * 0.75

            redundancy_penalty = 0.0

            if (
                selected
                and
                not new_mechanism
            ):
                redundancy_penalty = 2.0

            score = (
                base
                +
                contribution_bonus
                -
                redundancy_penalty
            )

            if (
                best_score is None
                or
                score > best_score
            ):
                best_score = score
                best_claim = claim

        if best_claim is None:
            break

        selected.append(
            best_claim
        )

        used_mechanism_words.update(
            mechanism_words(
                best_claim['text'],
                question
            )
        )

        remaining.remove(
            best_claim
        )

    return selected


def causal_support(
    claims,
    question
):
    qtype = question_type(
        question
    )

    if qtype not in {
        'CAUSE',
        'MECHANISM'
    }:
        return {
            'required': False,
            'explanatory_claims': 0,
            'mechanism_claims': 0,
            'strong_causal_claims': 0,
            'advice_selected': 0,
            'score': 1.0,
            'pass': True
        }

    explanatory_claims = sum(
        1
        for claim in claims
        if (
            not claim.get(
                'advice',
                False
            )
            and
            claim.get(
                'explanation',
                0
            ) >= 2
        )
    )

    mechanism_claims = sum(
        1
        for claim in claims
        if (
            not claim.get(
                'advice',
                False
            )
            and
            claim.get(
                'mechanism',
                0
            ) >= 1
        )
    )

    strong_causal_claims = sum(
        1
        for claim in claims
        if (
            not claim.get(
                'advice',
                False
            )
            and
            claim.get(
                'explanation',
                0
            ) >= 2
            and
            claim.get(
                'mechanism',
                0
            ) >= 1
        )
    )

    advice_selected = sum(
        1
        for claim in claims
        if claim.get(
            'advice',
            False
        )
    )

    score = min(
        1.0,
        strong_causal_claims / 2.0
    )

    return {
        'required': True,

        'explanatory_claims':
            explanatory_claims,

        'mechanism_claims':
            mechanism_claims,

        'strong_causal_claims':
            strong_causal_claims,

        'advice_selected':
            advice_selected,

        'score':
            score,

        'pass':
            (
                strong_causal_claims >= 1
                and
                advice_selected == 0
            )
    }


def consequence_signal_score(text):
    lower = (
        text
        or ''
    ).lower()

    score = 0

    for phrase in CONSEQUENCE_PHRASES:
        if phrase in lower:
            score += 2

    if re.search(
        '\\b('
        'result|results|effect|effects|'
        'consequence|consequences'
        ')\\b',
        lower
    ):
        score += 1

    return score



def extract_consequence(
    claims,
    selected,
    question
):
    abstain = {
        'established': False,
        'independent': False,
        'distinct_from_selected': False,
        'different_source': False,
        'downstream_signal': False,
        'downstream_new_terms': [],
        'text':
            'Not independently established from retrieved evidence.',
        'source': None,
        'url': None,
        'retrieval_type': None,
        'retrieved_at_utc': None,
        'score': 0
    }

    if not claims or not selected:
        return abstain

    selected_sources = {
        claim.get('source')
        for claim in selected
        if claim.get('source')
    }

    ranked = []

    for claim in claims:
        if claim.get(
            'advice',
            False
        ):
            continue

        # A consequence must be a distinct proposition, not a
        # recycled selected explanation.
        max_selected_similarity = max(
            (
                similarity(
                    claim['text'],
                    selected_claim['text']
                )
                for selected_claim in selected
            ),
            default=0.0
        )

        distinct_from_selected = (
            max_selected_similarity < 0.62
        )

        if not distinct_from_selected:
            continue

        signal = consequence_signal_score(
            claim['text']
        )

        downstream_signal = (
            signal >= 2
        )

        # Require an explicit downstream/result signal.
        if not downstream_signal:
            continue

        question_relation = subject_coverage(
            claim['text'],
            question
        )['score']

        if question_relation < 0.20:
            continue

        # Conservative independence rule: consequence evidence
        # must come from a source not already used by the selected
        # explanation. Otherwise abstain rather than overclaim.
        independent = (
            claim.get('source')
            not in selected_sources
        )

        if not independent:
            continue

        selected_terms = set(
            subject_words(
                selected_text
            )
        )

        question_terms = set(
            subject_words(
                question
            )
        )

        consequence_terms = set(
            subject_words(
                claim['text']
            )
        )

        downstream_new_terms = (
            consequence_terms
            -
            selected_terms
            -
            question_terms
        )

        # Require at least one new downstream concept so the
        # consequence is not merely a differently worded restatement.
        if not downstream_new_terms:
            continue

        score = (
            signal * 3.0
            +
            question_relation * 2.0
            +
            min(
                len(downstream_new_terms),
                4
            ) * 0.5
            +
            claim.get(
                'usefulness',
                0.0
            )
        )

        ranked.append(
            (
                score,
                claim,
                {
                    'distinct_from_selected': distinct_from_selected,
                    'different_source': independent,
                    'downstream_signal': downstream_signal,
                    'downstream_new_terms': sorted(
                        downstream_new_terms
                    ),
                    'max_selected_similarity': max_selected_similarity
                }
            )
        )

    if not ranked:
        return abstain

    ranked.sort(
        reverse=True,
        key=lambda item:
            item[0]
    )

    best_score, best_claim, qualification = ranked[0]

    return {
        'established': True,
        'independent': True,
        'distinct_from_selected':
            qualification['distinct_from_selected'],
        'different_source':
            qualification['different_source'],
        'downstream_signal':
            qualification['downstream_signal'],
        'downstream_new_terms':
            qualification['downstream_new_terms'],
        'max_selected_similarity':
            qualification['max_selected_similarity'],
        'text':
            best_claim['text'],
        'source':
            best_claim.get('source'),
        'url':
            best_claim.get('url'),
        'retrieval_type':
            best_claim.get('retrieval_type'),
        'retrieved_at_utc':
            best_claim.get('retrieved_at_utc'),
        'score':
            best_score
    }



def answer_support(
    coverage,
    conditions,
    phenomenon,
    causal,
    retrieval_quality_pass
):
    return {
        'retrieval_quality_pass':
            retrieval_quality_pass,

        'coverage_pass':
            coverage['score'] >= 0.30,

        'essential_condition_pass':
            conditions['pass'],

        'phenomenon_anchor_pass':
            phenomenon['pass'],

        'causal_language_support_pass':
            causal['pass'],

        'pass':
            (
                retrieval_quality_pass
                and
                coverage['score'] >= 0.30
                and
                conditions['pass']
                and
                phenomenon['pass']
                and
                causal['pass']
            )
    }


def combined_text(claims):
    return ' '.join(
        claim['text']
        for claim in claims
    )


def combined_coverage(
    claims,
    question
):
    return subject_coverage(
        combined_text(claims),
        question
    )


def process_evidence(
    question,
    evidence,
    retrieval_quality_pass
):
    qtype = question_type(question)

    claims = build_claims(
        evidence,
        question
    )

    empty_coverage = subject_coverage(
        '',
        question
    )

    empty_conditions = condition_coverage(
        '',
        question
    )

    empty_phenomenon = phenomenon_coverage(
        '',
        question
    )

    empty_causal = causal_support(
        [],
        question
    )

    empty_answer = answer_support(
        empty_coverage,
        empty_conditions,
        empty_phenomenon,
        empty_causal,
        retrieval_quality_pass
    )

    empty_consequence = {
        'established': False,
        'independent': False,
        'distinct_from_selected': False,
        'different_source': False,
        'downstream_signal': False,
        'downstream_new_terms': [],
        'text':
            'Not independently established from retrieved evidence.',
        'source': None,
        'url': None,
        'retrieval_type': None,
        'retrieved_at_utc': None,
        'score': 0
    }

    reason = {
        'question_type':
            qtype,

        'retrieval_terms':
            build_subject_terms(
                question
            ),

        'coverage_required_terms':
            coverage_terms(
                question
            ),

        'essential_conditions':
            essential_conditions(
                question
            ),

        'phenomenon_anchors':
            phenomenon_anchors(
                question
            ),

        'evidence_count':
            len(evidence),

        'claim_count':
            len(claims),

        'contradictions':
            0,

        'selected_claims':
            [],

        'coverage':
            None,

        'condition_coverage':
            None,

        'phenomenon_coverage':
            None,

        'causal_support':
            empty_causal,

        'answer_support':
            empty_answer,

        'consequence':
            empty_consequence,

        'decision':
            None
    }

    if not claims:
        reason[
            'decision'
        ] = 'NO_USEFUL_CLAIMS'

        return {
            'status':
                'LOW-VALUE EVIDENCE',

            'question_type':
                qtype,

            'claims':
                [],

            'conflicts':
                [],

            'selected':
                [],

            'coverage':
                empty_coverage,

            'condition_coverage':
                empty_conditions,

            'phenomenon_coverage':
                empty_phenomenon,

            'causal_support':
                empty_causal,

            'answer_support':
                empty_answer,

            'consequence':
                empty_consequence,

            'finding':
                (
                    'The retrieval pass did not produce '
                    'sufficient subject-relevant explanatory evidence.'
                ),

            'sources':
                [],

            'reason':
                reason
        }

    conflicts = contradictions(claims)

    reason[
        'contradictions'
    ] = len(conflicts)

    if conflicts:
        reason[
            'decision'
        ] = 'DIRECT_NEGATION_CONTRADICTION'

        return {
            'status':
                'CONTRADICTION DETECTED',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                conflicts,

            'selected':
                [],

            'coverage':
                empty_coverage,

            'condition_coverage':
                empty_conditions,

            'phenomenon_coverage':
                empty_phenomenon,

            'causal_support':
                empty_causal,

            'answer_support':
                empty_answer,

            'consequence':
                empty_consequence,

            'finding':
                (
                    'The evidence contains near-matching '
                    'propositions where one is explicitly negated.'
                ),

            'sources':
                unique_list(
                    [
                        conflicts[0][
                            'claim_a'
                        ][
                            'source'
                        ],
                        conflicts[0][
                            'claim_b'
                        ][
                            'source'
                        ]
                    ],
                    5
                ),

            'reason':
                reason
        }

    selected = select_claims(
        claims,
        question
    )

    selected_text = combined_text(
        selected
    )

    coverage = subject_coverage(
        selected_text,
        question
    )

    conditions = condition_coverage(
        selected_text,
        question
    )

    phenomenon = phenomenon_coverage(
        selected_text,
        question
    )

    causal = causal_support(
        selected,
        question
    )

    support = answer_support(
        coverage,
        conditions,
        phenomenon,
        causal,
        retrieval_quality_pass
    )

    consequence = extract_consequence(
        claims,
        selected,
        question
    )

    reason[
        'selected_claims'
    ] = [
        item['text']
        for item in selected
    ]

    reason[
        'coverage'
    ] = coverage

    reason[
        'condition_coverage'
    ] = conditions

    reason[
        'phenomenon_coverage'
    ] = phenomenon

    reason[
        'causal_support'
    ] = causal

    reason[
        'answer_support'
    ] = support

    reason[
        'consequence'
    ] = consequence

    explanatory = any(
        explanation_score(
            claim['text']
        ) > 0
        for claim in selected
    )

    if not retrieval_quality_pass:
        reason[
            'decision'
        ] = 'RETRIEVAL_QUALITY_NOT_ESTABLISHED'

        return {
            'status':
                'RETRIEVAL QUALITY FAILURE',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                [],

            'selected':
                selected,

            'coverage':
                coverage,

            'condition_coverage':
                conditions,

            'phenomenon_coverage':
                phenomenon,

            'causal_support':
                causal,

            'answer_support':
                support,

            'consequence':
                {
                    'established': False,
                    'independent': False,
                    'distinct_from_selected': False,
                    'different_source': False,
                    'downstream_signal': False,
                    'downstream_new_terms': [],
                    'text':
                        'Not independently established from retrieved evidence.',
                    'source': None,
                    'url': None,
                    'retrieval_type': None,
                    'retrieved_at_utc': None,
                    'score': 0
                },

            'finding':
                (
                    'Relevant material was retrieved, but the retrieval '
                    'quality gate failed. No supported finding is returned.'
                ),

            'sources':
                unique_list(
                    [
                        claim['source']
                        for claim in selected
                    ],
                    5
                ),

            'reason':
                reason
        }

    if (
        coverage['score'] < 0.30
        or
        (
            qtype in {
                'CAUSE',
                'MECHANISM'
            }
            and
            not explanatory
        )
    ):
        reason[
            'decision'
        ] = 'SUBJECT_RELATION_NOT_ESTABLISHED'

        return {
            'status':
                'QUESTION COVERAGE FAILURE',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                [],

            'selected':
                selected,

            'coverage':
                coverage,

            'condition_coverage':
                conditions,

            'phenomenon_coverage':
                phenomenon,

            'causal_support':
                causal,

            'answer_support':
                support,

            'consequence':
                consequence,

            'finding':
                (
                    'Relevant material was retrieved, but it '
                    'did not establish enough of the subject '
                    'relationship requested by the question.'
                ),

            'sources':
                unique_list(
                    [
                        claim['source']
                        for claim in selected
                    ],
                    5
                ),

            'reason':
                reason
        }

    if not conditions['pass']:
        reason[
            'decision'
        ] = 'ESSENTIAL_CONDITION_NOT_ESTABLISHED'

        return {
            'status':
                'ESSENTIAL CONDITION FAILURE',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                [],

            'selected':
                selected,

            'coverage':
                coverage,

            'condition_coverage':
                conditions,

            'phenomenon_coverage':
                phenomenon,

            'causal_support':
                causal,

            'answer_support':
                support,

            'consequence':
                consequence,

            'finding':
                (
                    'The evidence is related to the general subject, '
                    'but it does not preserve all essential conditions '
                    'stated in the question. Missing essential conditions: '
                    +
                    ', '.join(
                        conditions['missing']
                    )
                ),

            'sources':
                unique_list(
                    [
                        claim['source']
                        for claim in selected
                    ],
                    5
                ),

            'reason':
                reason
        }

    if not phenomenon['pass']:
        reason[
            'decision'
        ] = 'PHENOMENON_NOT_ESTABLISHED'

        return {
            'status':
                'PHENOMENON COVERAGE FAILURE',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                [],

            'selected':
                selected,

            'coverage':
                coverage,

            'condition_coverage':
                conditions,

            'phenomenon_coverage':
                phenomenon,

            'causal_support':
                causal,

            'answer_support':
                support,

            'consequence':
                consequence,

            'finding':
                (
                    'The evidence covers the broad subject and conditions, '
                    'but it does not establish the specific phenomenon '
                    'described in the question. Missing phenomenon anchors: '
                    +
                    ', '.join(
                        phenomenon['missing']
                    )
                ),

            'sources':
                unique_list(
                    [
                        claim['source']
                        for claim in selected
                    ],
                    5
                ),

            'reason':
                reason
        }

    if not causal['pass']:
        reason[
            'decision'
        ] = 'CAUSAL_SUPPORT_NOT_ESTABLISHED'

        return {
            'status':
                'CAUSAL-LANGUAGE SUPPORT FAILURE',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                [],

            'selected':
                selected,

            'coverage':
                coverage,

            'condition_coverage':
                conditions,

            'phenomenon_coverage':
                phenomenon,

            'causal_support':
                causal,

            'answer_support':
                support,

            'consequence':
                consequence,

            'finding':
                (
                    'The evidence covers the subject and stated conditions, '
                    'but the selected claims do not establish sufficient causal or mechanistic language.'
                ),

            'sources':
                unique_list(
                    [
                        claim['source']
                        for claim in selected
                    ],
                    5
                ),

            'reason':
                reason
        }

    average_usefulness = (
        sum(
            claim['usefulness']
            for claim in selected
        )
        /
        len(selected)
    )

    if average_usefulness < 0.30:
        reason[
            'decision'
        ] = 'LOW_EVIDENCE_USEFULNESS'

        return {
            'status':
                'LOW-VALUE EVIDENCE',

            'question_type':
                qtype,

            'claims':
                claims,

            'conflicts':
                [],

            'selected':
                selected,

            'coverage':
                coverage,

            'condition_coverage':
                conditions,

            'phenomenon_coverage':
                phenomenon,

            'causal_support':
                causal,

            'answer_support':
                support,

            'consequence':
                consequence,

            'finding':
                (
                    'The retrieved material is related to the '
                    'subject but is not sufficiently informative '
                    'to support a finding.'
                ),

            'sources':
                unique_list(
                    [
                        claim['source']
                        for claim in selected
                    ],
                    5
                ),

            'reason':
                reason
        }

    reason[
        'decision'
    ] = 'SUPPORTED'

    sources = unique_list(
        [
            claim['source']
            for claim in selected
        ]
        +
        (
            [
                consequence['source']
            ]
            if consequence['source']
            else
            []
        ),
        5
    )

    return {
        'status':
            'SUPPORTED FROM RETRIEVED EVIDENCE',

        'question_type':
            qtype,

        'claims':
            claims,

        'conflicts':
            [],

        'selected':
            selected,

        'coverage':
            coverage,

        'condition_coverage':
            conditions,

        'phenomenon_coverage':
            phenomenon,

        'causal_support':
            causal,

        'answer_support':
            support,

        'consequence':
            consequence,

        'finding':
            ' '.join(
                claim['text']
                for claim in selected
            ),

        'sources':
            sources,

        'reason':
            reason
    }


def retrieval_quality(
    question,
    evidence
):
    if not evidence:
        return 0.0

    useful = [
        item
        for item in evidence
        if item['usefulness'] >= 0.18
    ]

    if not useful:
        return 0.0

    usefulness = (
        sum(
            item['usefulness']
            for item in useful[:8]
        )
        /
        len(useful[:8])
    )

    coverage = (
        sum(
            item['coverage']
            for item in useful[:8]
        )
        /
        len(useful[:8])
    )

    sources = len(
        set(
            item['source']
            for item in useful
        )
    )

    diversity = min(
        sources / 3,
        1.0
    )

    explanatory = (
        sum(
            1
            for item in useful[:8]
            if explanation_score(
                item['text']
            ) > 0
        )
        /
        len(useful[:8])
    )

    anchor_quality = (
        sum(
            min(
                anchor_bonus(
                    item['text'],
                    question
                ) / 4.5,
                1.0
            )
            for item in useful[:8]
        )
        /
        len(useful[:8])
    )

    return min(
        1.0,
        usefulness * 0.32
        +
        coverage * 0.20
        +
        diversity * 0.13
        +
        explanatory * 0.18
        +
        anchor_quality * 0.17
    )


def build_telemetry(
    evidence,
    processed,
    quality
):
    return {
        'evidence_packet_present':
            bool(evidence),

        'usable_claims_present':
            bool(
                processed['claims']
            ),

        'retrieval_quality_score':
            quality,

        'retrieval_quality_threshold':
            RETRIEVAL_QUALITY_THRESHOLD,

        'retrieval_quality_pass':
            quality >= RETRIEVAL_QUALITY_THRESHOLD,

        'essential_condition_pass':
            processed[
                'condition_coverage'
            ]['pass'],

        'phenomenon_anchor_pass':
            processed[
                'phenomenon_coverage'
            ]['pass'],

        'causal_support_pass':
            processed[
                'causal_support'
            ]['pass'],

        'answer_support_pass':
            processed[
                'answer_support'
            ]['pass'],

        'consequence_established':
            processed[
                'consequence'
            ]['established'],

        'consequence_distinct_from_selected':
            processed[
                'consequence'
            ].get(
                'distinct_from_selected',
                False
            ),

        'consequence_different_source':
            processed[
                'consequence'
            ].get(
                'different_source',
                False
            ),

        'consequence_downstream_signal':
            processed[
                'consequence'
            ].get(
                'downstream_signal',
                False
            ),

        'consequence_downstream_new_terms':
            processed[
                'consequence'
            ].get(
                'downstream_new_terms',
                []
            )
    }


def learn(
    thread,
    question,
    evidence,
    processed,
    telemetry
):
    thread['turns'] += 1

    if telemetry[
        'retrieval_quality_pass'
    ]:
        thread[
            'quality_gate_passes'
        ] += 1
    else:
        thread[
            'quality_gate_fails'
        ] += 1

    status = processed['status']

    if status == 'SUPPORTED FROM RETRIEVED EVIDENCE':
        thread[
            'processing_successes'
        ] += 1

        thread[
            'learning_events'
        ].append(
            'PROCESSING_SUCCESS'
        )

    else:
        thread[
            'processing_failures'
        ] += 1

        if status == 'QUESTION COVERAGE FAILURE':
            thread[
                'coverage_failures'
            ] += 1

        elif status == 'ESSENTIAL CONDITION FAILURE':
            thread[
                'condition_failures'
            ] += 1

        elif status == 'PHENOMENON COVERAGE FAILURE':
            thread[
                'phenomenon_failures'
            ] += 1

        elif status == 'CAUSAL-LANGUAGE SUPPORT FAILURE':
            thread[
                'causal_support_failures'
            ] += 1

        elif status == 'CONTRADICTION DETECTED':
            thread[
                'contradictions'
            ] += 1

        thread[
            'learning_events'
        ].append(
            'PROCESSING_FAILURE '
            +
            status
        )

    if (
        telemetry[
            'retrieval_quality_pass'
        ]
        and
        telemetry[
            'essential_condition_pass'
        ]
        and
        telemetry[
            'phenomenon_anchor_pass'
        ]
        and
        telemetry[
            'causal_support_pass'
        ]
        and
        telemetry[
            'answer_support_pass'
        ]
        and
        status
        ==
        'SUPPORTED FROM RETRIEVED EVIDENCE'
    ):
        original = set(
            build_subject_terms(
                question
            )
        )

        counter = Counter()

        for claim in processed[
            'selected'
        ]:
            for word in subject_words(
                claim['text']
            ):
                if (
                    word not in original
                    and
                    word not in INTENT_WORDS
                    and
                    len(word) > 4
                ):
                    counter[word] += 1

        learned = [
            word
            for word, count
            in counter.most_common(5)
            if count >= 2
        ]

        thread[
            'successful_query_terms'
        ] = unique_list(
            thread[
                'successful_query_terms'
            ]
            +
            learned,
            12
        )

    thread[
        'learning_events'
    ] = thread[
        'learning_events'
    ][-20:]



def print_source_basis(processed):
    sources = []

    for claim in processed.get(
        'selected',
        []
    ):
        sources.append(claim)

    consequence = processed.get(
        'consequence',
        {}
    )

    if consequence.get(
        'established'
    ):
        sources.append({
            'source':
                consequence.get('source'),
            'url':
                consequence.get('url'),
            'retrieval_type':
                consequence.get('retrieval_type'),
            'retrieved_at_utc':
                consequence.get('retrieved_at_utc'),
            'text':
                consequence.get('text')
        })

    seen = set()

    if not sources:
        print('  none')
        return

    for item in sources:
        source = item.get(
            'source'
        ) or 'unknown source'

        key = (
            source,
            item.get('url'),
            item.get('record_id'),
            canonical(
                item.get(
                    'text',
                    ''
                )
            )
        )

        if key in seen:
            continue

        seen.add(key)

        print(
            ' ',
            source
        )

        if item.get('url'):
            print(
                '    URL:',
                item.get('url')
            )

        if item.get('record_id') is not None:
            print(
                '    PMEi record_id:',
                item.get('record_id')
            )

        if item.get('retrieval_type'):
            print(
                '    Retrieval type:',
                item.get('retrieval_type')
            )

        if item.get('pmei_route'):
            print(
                '    PMEi route:',
                item.get('pmei_route')
            )

        if item.get('retrieved_at_utc'):
            print(
                '    Retrieved UTC:',
                item.get('retrieved_at_utc')
            )

        if item.get('text'):
            wrap(
                item.get('text'),
                '    Passage: '
            )


def show_result(
    question,
    query,
    route,
    signals,
    thread_key,
    evidence,
    processed,
    telemetry,
    timings,
    pmei_transport
):
    thread = LEARNING_THREADS[
        thread_key
    ]

    coverage = processed[
        'coverage'
    ]

    conditions = processed[
        'condition_coverage'
    ]

    phenomenon = processed[
        'phenomenon_coverage'
    ]

    causal = processed[
        'causal_support'
    ]

    consequence = processed[
        'consequence'
    ]

    print()
    print('=' * 92)
    print('PMEi STANDALONE')
    print(f'BUILD STATUS: {BUILD_STATUS}')
    print(f'BUILD: {BUILD_NAME}')
    print('=' * 92)
    print()

    print('QUESTION:')
    wrap(
        '  '
        +
        question
    )

    print()
    print('CONVERSATIONAL ORIENTATION')
    print(
        '  Explicit follow-up:',
        signals['follow_up']
    )
    print(
        '  Strong reference dependency:',
        signals['reference']
    )
    print(
        '  Previous-turn context eligible:',
        (
            signals['follow_up']
            or
            signals['reference']
        )
    )

    print()
    print('QUESTION ANCHORS')
    print(
        '  Essential conditions:',
        ', '.join(
            essential_conditions(
                question
            )
        )
        or
        'none'
    )

    print(
        '  Phenomenon anchors:',
        ', '.join(
            phenomenon_anchors(
                question
            )
        )
        or
        'none'
    )

    print()
    print('RETRIEVAL WORDING:')
    wrap(
        '  '
        +
        query
    )

    print()
    print('PASS 1 - RETRIEVAL')
    print('  Sweeps: 1')
    print('  Route:', route)
    print('  Block-aware sanitation: TRUE')
    print('  Meta-explanation rejection: TRUE')
    print('  Anchor-aware ranking: TRUE')
    print(
        '  Evidence items surviving sanitation:',
        len(evidence)
    )
    print('  Evidence frozen: TRUE')

    print()
    print('PASS 2 - PROCESSING')
    print(
        '  Question type:',
        processed[
            'question_type'
        ]
    )
    print(
        '  Claims:',
        len(
            processed['claims']
        )
    )
    print('  Mechanism-contribution ranking: TRUE')
    print('  Essential-condition gate: TRUE')
    print('  Phenomenon-anchor gate: TRUE')
    print('  Proposition validation: TRUE')
    print('  Causal-language support gate: TRUE')
    print('  Answer-support gate: TRUE')
    print('  Evidence-backed consequence: TRUE')
    print(
        '  Direct-negation contradictions:',
        len(
            processed['conflicts']
        )
    )

    if not processed['conflicts']:
        print(
            '  Contradiction result: no direct-negation contradiction detected'
        )

    print()
    print('GENERAL COVERAGE')
    print(
        f"  Score: "
        f"{coverage['score']:.2f}"
    )
    print(
        '  Required:',
        ', '.join(
            coverage['required']
        )
        or
        'none'
    )
    print(
        '  Covered:',
        ', '.join(
            coverage['found']
        )
        or
        'none'
    )
    print(
        '  Missing:',
        ', '.join(
            coverage['missing']
        )
        or
        'none'
    )

    print()
    print('ESSENTIAL CONDITIONS')
    print(
        f"  Score: "
        f"{conditions['score']:.2f}"
    )
    print(
        '  Required:',
        ', '.join(
            conditions['required']
        )
        or
        'none'
    )
    print(
        '  Covered:',
        ', '.join(
            conditions['covered']
        )
        or
        'none'
    )
    print(
        '  Missing:',
        ', '.join(
            conditions['missing']
        )
        or
        'none'
    )
    print(
        '  Pass:',
        conditions['pass']
    )

    print()
    print('PHENOMENON ANCHORS')
    print(
        f"  Score: "
        f"{phenomenon['score']:.2f}"
    )
    print(
        '  Required:',
        ', '.join(
            phenomenon['required']
        )
        or
        'none'
    )
    print(
        '  Covered:',
        ', '.join(
            phenomenon['covered']
        )
        or
        'none'
    )
    print(
        '  Missing:',
        ', '.join(
            phenomenon['missing']
        )
        or
        'none'
    )
    print(
        '  Pass:',
        phenomenon['pass']
    )

    print()
    print('CAUSAL-LANGUAGE SUPPORT')
    print(
        f"  Score: "
        f"{causal['score']:.2f}"
    )
    print(
        '  Explanatory claims:',
        causal.get(
            'explanatory_claims',
            0
        )
    )
    print(
        '  Mechanism claims:',
        causal.get(
            'mechanism_claims',
            0
        )
    )
    print(
        '  Advice selected:',
        causal.get(
            'advice_selected',
            0
        )
    )
    print(
        '  Pass:',
        causal['pass']
    )

    print()
    print(
        'STATUS:',
        processed['status']
    )

    print()
    print('FINDING')
    print('-' * 92)
    wrap(
        processed['finding']
    )

    print()
    print('CONSEQUENCE')
    print('-' * 92)
    wrap(
        consequence['text']
    )

    print()
    print('CONSEQUENCE QUALIFICATION')
    print(
        '  Distinct from causal finding:',
        consequence.get(
            'distinct_from_selected',
            False
        )
    )
    print(
        '  Different source from mechanism:',
        consequence.get(
            'different_source',
            False
        )
    )
    print(
        '  Downstream-effect signal:',
        consequence.get(
            'downstream_signal',
            False
        )
    )
    print(
        '  New downstream terms:',
        ', '.join(
            consequence.get(
                'downstream_new_terms',
                []
            )
        )
        or
        'none'
    )

    print()
    print('SOURCE BASIS')
    print_source_basis(
        processed
    )

    print()
    print('RETRIEVAL TELEMETRY')
    print(
        '  Evidence packet present:',
        telemetry[
            'evidence_packet_present'
        ]
    )
    print(
        '  Usable claims present:',
        telemetry[
            'usable_claims_present'
        ]
    )
    print(
        f"  Retrieval quality score: "
        f"{telemetry['retrieval_quality_score']:.2f}"
    )
    print(
        f"  Retrieval quality threshold: "
        f"{telemetry['retrieval_quality_threshold']:.2f}"
    )
    print(
        '  Retrieval quality pass:',
        telemetry[
            'retrieval_quality_pass'
        ]
    )
    print(
        '  Essential condition pass:',
        telemetry[
            'essential_condition_pass'
        ]
    )
    print(
        '  Phenomenon anchor pass:',
        telemetry[
            'phenomenon_anchor_pass'
        ]
    )
    print(
        '  Causal-language support pass:',
        telemetry[
            'causal_support_pass'
        ]
    )
    print(
        '  Consequence established:',
        telemetry[
            'consequence_established'
        ]
    )
    print(
        '  Consequence distinct:',
        telemetry[
            'consequence_distinct_from_selected'
        ]
    )
    print(
        '  Consequence different source:',
        telemetry[
            'consequence_different_source'
        ]
    )
    print(
        '  Consequence downstream signal:',
        telemetry[
            'consequence_downstream_signal'
        ]
    )

    print()
    print('PMEi TRANSPORT')

    if route == 'PMEI_LOOKUP':
        print(
            '  Required: True'
        )
        print(
            '  Attempted:',
            pmei_transport.get(
                'attempted',
                False
            )
        )
        print(
            '  Success:',
            pmei_transport.get(
                'success',
                False
            )
        )
        print(
            '  Compatibility route:',
            pmei_transport.get(
                'route'
            )
            or
            'none'
        )

        errors = pmei_transport.get(
            'errors',
            []
        )

        if errors:
            print(
                '  Bounded errors:'
            )
            for error in errors[-3:]:
                wrap(
                    error,
                    '    '
                )
        else:
            print(
                '  Bounded errors: none'
            )
    else:
        print(
            '  Required: False'
        )
        print(
            '  Retrieval skipped: TRUE'
        )

    print()
    print('ANSWER SUPPORT GATE')
    print(
        '  Retrieval packet quality:',
        (
            'PASS'
            if telemetry[
                'retrieval_quality_pass'
            ]
            else
            'FAIL'
        )
    )
    print(
        '  Essential conditions:    ',
        (
            'PASS'
            if telemetry[
                'essential_condition_pass'
            ]
            else
            'FAIL'
        )
    )
    print(
        '  Phenomenon anchors:       ',
        (
            'PASS'
            if telemetry[
                'phenomenon_anchor_pass'
            ]
            else
            'FAIL'
        )
    )
    print(
        '  Causal-language support:  ',
        (
            'PASS'
            if telemetry[
                'causal_support_pass'
            ]
            else
            'FAIL'
        )
    )
    print(
        '  Answer support:           ',
        (
            'PASS'
            if telemetry[
                'answer_support_pass'
            ]
            else
            'FAIL'
        )
    )

    print()
    print('LEARNING PASS')
    print(
        '  Quality gate pass/fail:',
        thread[
            'quality_gate_passes'
        ],
        '/',
        thread[
            'quality_gate_fails'
        ]
    )
    print(
        '  Processing success/fail:',
        thread[
            'processing_successes'
        ],
        '/',
        thread[
            'processing_failures'
        ]
    )
    print(
        '  Coverage failures:',
        thread[
            'coverage_failures'
        ]
    )
    print(
        '  Essential-condition failures:',
        thread[
            'condition_failures'
        ]
    )
    print(
        '  Phenomenon failures:',
        thread[
            'phenomenon_failures'
        ]
    )
    print(
        '  Causal-language support failures:',
        thread[
            'causal_support_failures'
        ]
    )
    print(
        '  Contradictions:',
        thread[
            'contradictions'
        ]
    )
    print(
        '  Learning persisted to PMEi: FALSE'
    )

    print()
    print('WORKING STATE')
    print(
        '  RAM learning threads:',
        len(
            LEARNING_THREADS
        )
    )
    print(
        '  Persistent write: FALSE'
    )
    print(
        '  Graph DB:         FALSE'
    )
    print(
        '  LLM loaded:       FALSE'
    )
    print(
        '  LLM invoked:      FALSE'
    )
    print(
        '  Build promoted:    FALSE'
    )

    print()
    print('TRACE')
    print(
        f"  PMEi orientation:      "
        f"{timings['pmei']:.3f}s"
    )
    print(
        f"  Pass 1 retrieval:      "
        f"{timings['retrieval']:.3f}s"
    )
    print(
        f"  Pass 2 processing:     "
        f"{timings['processing']:.6f}s"
    )
    print(
        f"  Total:                 "
        f"{timings['total']:.3f}s"
    )
    print('=' * 92)
    print()



def ask(question):
    global LAST_EVIDENCE
    global LAST_PROCESSED
    global LAST_REASON

    total_start = time.perf_counter()

    t = time.perf_counter()

    signals = personal_orientation(
        question
    )

    (
        thread_key,
        thread,
        new_thread
    ) = get_learning_thread(
        question
    )

    query = build_query(
        question,
        signals,
        thread
    )

    # Route before any PMEi retrieval. Ordinary web questions
    # do not depend on PMEi availability or pay PMEi latency.
    route = determine_domain(
        question,
        signals
    )

    records = []
    pmei_transport = empty_pmei_transport()

    if route == 'PMEI_LOOKUP':
        records, pmei_transport = get_pmei_records()

        if (
            records
            and
            not BOOT_STATE['loaded']
        ):
            load_boot_learning(
                records
            )

    pmei_time = (
        time.perf_counter()
        -
        t
    )

    t = time.perf_counter()

    if route == 'PMEI_LOOKUP':
        evidence = retrieve_pmei(
            records,
            query,
            question,
            pmei_transport
        )
    else:
        evidence = retrieve_web(
            query,
            question
        )

    retrieval_time = (
        time.perf_counter()
        -
        t
    )

    LAST_EVIDENCE = list(
        evidence
    )

    # Retrieval quality is now a real authority gate, so it is
    # computed before deterministic processing/final support.
    quality = retrieval_quality(
        question,
        evidence
    )

    quality_pass = (
        quality
        >=
        RETRIEVAL_QUALITY_THRESHOLD
    )

    t = time.perf_counter()

    processed = process_evidence(
        question,
        evidence,
        quality_pass
    )

    processing_time = (
        time.perf_counter()
        -
        t
    )

    LAST_PROCESSED = processed

    telemetry = build_telemetry(
        evidence,
        processed,
        quality
    )

    LAST_REASON = {
        'question':
            question,

        'query':
            query,

        'route':
            route,

        'signals':
            signals,

        'retrieval_terms':
            build_subject_terms(
                question
            ),

        'coverage_terms':
            coverage_terms(
                question
            ),

        'essential_conditions':
            essential_conditions(
                question
            ),

        'phenomenon_anchors':
            phenomenon_anchors(
                question
            ),

        'telemetry':
            telemetry,

        'pmei_transport':
            pmei_transport,

        'processor':
            processed[
                'reason'
            ]
    }

    learn(
        thread,
        question,
        evidence,
        processed,
        telemetry
    )

    HISTORY.append({
        'question':
            question,

        'route':
            route,

        'thread':
            thread_key
    })

    total_time = (
        time.perf_counter()
        -
        total_start
    )

    show_result(
        question,
        query,
        route,
        signals,
        thread_key,
        evidence,
        processed,
        telemetry,
        {
            'pmei':
                pmei_time,

            'retrieval':
                retrieval_time,

            'processing':
                processing_time,

            'total':
                total_time
        },
        pmei_transport
    )


def show_reason():
    print()
    print(
        'PMEi REASON PATH'
    )
    print('=' * 92)

    if not LAST_REASON:
        print(
            'No previous question.'
        )
        print()
        return

    print()
    print(
        'ORIGINAL QUESTION'
    )
    wrap(
        '  '
        +
        LAST_REASON['question']
    )

    signals = LAST_REASON[
        'signals'
    ]

    print()
    print(
        'CONVERSATIONAL ORIENTATION'
    )
    print(
        '  Explicit follow-up:',
        signals['follow_up']
    )
    print(
        '  Strong reference dependency:',
        signals['reference']
    )
    print(
        '  Previous-turn context eligible:',
        (
            signals['follow_up']
            or
            signals['reference']
        )
    )

    print()
    print(
        'QUESTION ANCHORS'
    )
    print(
        '  Essential conditions:',
        ', '.join(
            LAST_REASON[
                'essential_conditions'
            ]
        )
        or
        'none'
    )
    print(
        '  Phenomenon anchors:',
        ', '.join(
            LAST_REASON[
                'phenomenon_anchors'
            ]
        )
        or
        'none'
    )

    print()
    print(
        'RETRIEVAL QUERY'
    )
    wrap(
        '  '
        +
        LAST_REASON['query']
    )

    print()
    print(
        'DOMAIN'
    )
    print(
        '  ',
        LAST_REASON['route']
    )

    telemetry = LAST_REASON[
        'telemetry'
    ]

    print()
    print(
        'RETRIEVAL TELEMETRY'
    )
    print(
        '  Evidence packet present:',
        telemetry[
            'evidence_packet_present'
        ]
    )
    print(
        '  Usable claims present:',
        telemetry[
            'usable_claims_present'
        ]
    )
    print(
        f"  Quality score: "
        f"{telemetry['retrieval_quality_score']:.2f}"
    )
    print(
        f"  Quality threshold: "
        f"{telemetry['retrieval_quality_threshold']:.2f}"
    )
    print(
        '  Quality pass:',
        telemetry[
            'retrieval_quality_pass'
        ]
    )
    print(
        '  Essential condition pass:',
        telemetry[
            'essential_condition_pass'
        ]
    )
    print(
        '  Phenomenon anchor pass:',
        telemetry[
            'phenomenon_anchor_pass'
        ]
    )
    print(
        '  Causal-language support pass:',
        telemetry[
            'causal_support_pass'
        ]
    )
    print(
        '  Consequence established:',
        telemetry[
            'consequence_established'
        ]
    )
    print(
        '  Answer support pass:',
        telemetry[
            'answer_support_pass'
        ]
    )

    pmei_transport = LAST_REASON.get(
        'pmei_transport',
        empty_pmei_transport()
    )

    print()
    print(
        'PMEi TRANSPORT'
    )

    if LAST_REASON[
        'route'
    ] == 'PMEI_LOOKUP':
        print(
            '  Attempted:',
            pmei_transport.get(
                'attempted',
                False
            )
        )
        print(
            '  Success:',
            pmei_transport.get(
                'success',
                False
            )
        )
        print(
            '  Compatibility route:',
            pmei_transport.get(
                'route'
            )
            or
            'none'
        )
    else:
        print(
            '  Retrieval skipped: TRUE'
        )

    processor = LAST_REASON[
        'processor'
    ]

    print()
    print(
        'QUESTION TYPE'
    )
    print(
        '  ',
        processor[
            'question_type'
        ]
    )

    print()
    print(
        'EVIDENCE'
    )
    print(
        '  Frozen evidence items:',
        processor[
            'evidence_count'
        ]
    )
    print(
        '  Usable claims:',
        processor[
            'claim_count'
        ]
    )

    print()
    print(
        'SELECTED CLAIMS'
    )

    if processor[
        'selected_claims'
    ]:
        for index, claim in enumerate(
            processor[
                'selected_claims'
            ],
            1
        ):
            print()
            print(
                f'  [{index}]'
            )
            wrap(
                claim,
                '    '
            )
    else:
        print(
            '  none'
        )

    print()
    print(
        'GENERAL COVERAGE'
    )

    coverage = processor[
        'coverage'
    ]

    if coverage:
        print(
            f"  Score: "
            f"{coverage['score']:.2f}"
        )
        print(
            '  Required:',
            ', '.join(
                coverage[
                    'required'
                ]
            )
            or
            'none'
        )
        print(
            '  Covered:',
            ', '.join(
                coverage[
                    'found'
                ]
            )
            or
            'none'
        )
        print(
            '  Missing:',
            ', '.join(
                coverage[
                    'missing'
                ]
            )
            or
            'none'
        )

    conditions = processor[
        'condition_coverage'
    ]

    print()
    print(
        'ESSENTIAL CONDITIONS'
    )
    print(
        f"  Score: "
        f"{conditions['score']:.2f}"
    )
    print(
        '  Required:',
        ', '.join(
            conditions[
                'required'
            ]
        )
        or
        'none'
    )
    print(
        '  Covered:',
        ', '.join(
            conditions[
                'covered'
            ]
        )
        or
        'none'
    )
    print(
        '  Missing:',
        ', '.join(
            conditions[
                'missing'
            ]
        )
        or
        'none'
    )
    print(
        '  Pass:',
        conditions[
            'pass'
        ]
    )

    phenomenon = processor[
        'phenomenon_coverage'
    ]

    print()
    print(
        'PHENOMENON ANCHORS'
    )
    print(
        f"  Score: "
        f"{phenomenon['score']:.2f}"
    )
    print(
        '  Required:',
        ', '.join(
            phenomenon[
                'required'
            ]
        )
        or
        'none'
    )
    print(
        '  Covered:',
        ', '.join(
            phenomenon[
                'covered'
            ]
        )
        or
        'none'
    )
    print(
        '  Missing:',
        ', '.join(
            phenomenon[
                'missing'
            ]
        )
        or
        'none'
    )
    print(
        '  Pass:',
        phenomenon[
            'pass'
        ]
    )

    causal = processor[
        'causal_support'
    ]

    print()
    print(
        'CAUSAL-LANGUAGE SUPPORT'
    )
    print(
        f"  Score: "
        f"{causal['score']:.2f}"
    )
    print(
        '  Explanatory claims:',
        causal.get(
            'explanatory_claims',
            0
        )
    )
    print(
        '  Mechanism claims:',
        causal.get(
            'mechanism_claims',
            0
        )
    )
    print(
        '  Advice selected:',
        causal.get(
            'advice_selected',
            0
        )
    )
    print(
        '  Pass:',
        causal[
            'pass'
        ]
    )

    consequence = processor[
        'consequence'
    ]

    print()
    print(
        'CONSEQUENCE'
    )
    wrap(
        consequence['text'],
        '  '
    )

    print()
    print(
        'CONSEQUENCE QUALIFICATION'
    )
    print(
        '  Distinct from causal finding:',
        consequence.get(
            'distinct_from_selected',
            False
        )
    )
    print(
        '  Different source from mechanism:',
        consequence.get(
            'different_source',
            False
        )
    )
    print(
        '  Downstream-effect signal:',
        consequence.get(
            'downstream_signal',
            False
        )
    )
    print(
        '  New downstream terms:',
        ', '.join(
            consequence.get(
                'downstream_new_terms',
                []
            )
        )
        or
        'none'
    )

    print()
    print(
        'FINAL DETERMINISTIC DECISION'
    )
    print(
        '  ',
        processor[
            'decision'
        ]
    )
    print('=' * 92)
    print()


def show_evidence():
    print()
    print(
        'FROZEN EVIDENCE'
    )
    print('=' * 92)

    if not LAST_EVIDENCE:
        print(
            'None.'
        )
        print()
        return

    for index, item in enumerate(
        LAST_EVIDENCE,
        1
    ):
        print()
        print(
            f'[{index}]',
            item['source']
        )
        print(
            f"  usefulness="
            f"{item['usefulness']:.2f}"
        )
        print(
            f"  coverage="
            f"{item['coverage']:.2f}"
        )
        wrap(
            item['text'],
            '  '
        )

    print()


def show_claims():
    print()
    print(
        'NORMALISED CLAIMS'
    )
    print('=' * 92)

    if not LAST_PROCESSED:
        print(
            'None.'
        )
        print()
        return

    for index, claim in enumerate(
        LAST_PROCESSED[
            'claims'
        ],
        1
    ):
        print()
        print(
            f'[{index}]',
            claim['source']
        )
        print(
            f"  usefulness="
            f"{claim['usefulness']:.2f}"
        )
        print(
            f"  coverage="
            f"{claim['coverage']:.2f}"
        )
        print(
            f"  explanation="
            f"{claim.get('explanation', 0)}"
        )
        print(
            f"  mechanism="
            f"{claim.get('mechanism', 0)}"
        )
        print(
            f"  condition_score="
            f"{claim.get('condition_score', 0):.2f}"
        )
        print(
            f"  phenomenon_score="
            f"{claim.get('phenomenon_score', 0):.2f}"
        )
        print(
            f"  advice="
            f"{claim.get('advice', False)}"
        )
        print(
            f"  consequence_signal="
            f"{consequence_signal_score(claim['text'])}"
        )
        wrap(
            claim['text'],
            '  '
        )

    print()


def show_learning():
    print()
    print(
        'RAM LEARNING THREADS'
    )
    print('=' * 92)

    if not LEARNING_THREADS:
        print(
            'None.'
        )
        print()
        return

    for key, thread in LEARNING_THREADS.items():
        print()

        print(
            'THREAD:',
            key
        )

        print(
            '  Turns:',
            thread[
                'turns'
            ]
        )

        print(
            '  Quality gate pass/fail:',
            thread[
                'quality_gate_passes'
            ],
            '/',
            thread[
                'quality_gate_fails'
            ]
        )

        print(
            '  Processing success/fail:',
            thread[
                'processing_successes'
            ],
            '/',
            thread[
                'processing_failures'
            ]
        )

        print(
            '  Coverage failures:',
            thread[
                'coverage_failures'
            ]
        )

        print(
            '  Essential-condition failures:',
            thread[
                'condition_failures'
            ]
        )

        print(
            '  Phenomenon failures:',
            thread[
                'phenomenon_failures'
            ]
        )

        print(
            '  Causal-language support failures:',
            thread[
                'causal_support_failures'
            ]
        )

        print(
            '  Contradictions:',
            thread[
                'contradictions'
            ]
        )

        print(
            '  Learned query terms:',
            ', '.join(
                thread[
                    'successful_query_terms'
                ]
            )
            or
            'none'
        )

    print()


def reset():
    global LAST_EVIDENCE
    global LAST_PROCESSED
    global LAST_REASON

    HISTORY.clear()
    LEARNING_THREADS.clear()

    LAST_EVIDENCE = []
    LAST_PROCESSED = None
    LAST_REASON = None

    print(
        'RAM cleared.'
    )


def main():
    print()

    print(
        'PMEi STANDALONE'
    )

    print(
        f'BUILD STATUS: {BUILD_STATUS}'
    )

    print(
        f'BUILD: {BUILD_NAME}'
    )

    print()

    print(
        'No LLM.'
    )

    print(
        'No graph database.'
    )

    print(
        'No PMEi writes.'
    )

    print()

    print(
        'v6 sanitation retained.'
    )

    print(
        'v6.1 explanatory ranking retained.'
    )

    print(
        'v6.2 conditional reference retained.'
    )

    print(
        'v6.3 meta-explanation rejection retained.'
    )

    print(
        'v6.4 mechanism-contribution ranking retained.'
    )

    print(
        'v6.5 essential-condition preservation retained.'
    )

    print(
        'v6.5 phenomenon-anchor preservation retained.'
    )

    print(
        'v6.6 proposition validation retained.'
    )

    print(
        'v6.6 answer-support separation retained.'
    )

    print(
        'v6.7 strict WHY/HOW advice exclusion added.'
    )

    print(
        'v6.7 independent causal-support gate added.'
    )

    print(
        'v6.7 evidence-backed consequence retained.'
    )

    print(
        'v6.8 retrieval-quality authority gate added.'
    )

    print(
        'v6.8 route-before-PMEi retrieval added.'
    )

    print(
        'v6.8 provenance and PMEi transport telemetry added.'
    )

    print(
        'v6.8 conservative independent consequence gate added.'
    )

    print()

    print(
        'v6.8.1 embedded meta-language rejection added.'
    )

    print(
        'v6.8.1 embedded advice rejection added.'
    )

    print(
        'v6.8.1 consequence qualification telemetry added.'
    )

    print()

    print(
        'Commands:'
    )

    print(
        '  /reason'
    )

    print(
        '  /evidence'
    )

    print(
        '  /claims'
    )

    print(
        '  /learn'
    )

    print(
        '  /reset'
    )

    print(
        '  /quit'
    )

    print()

    while True:
        try:
            question = input(
                'You > '
            ).strip()

        except (
            EOFError,
            KeyboardInterrupt
        ):
            break

        if not question:
            continue

        command = question.lower()

        if command in {
            '/quit',
            '/exit',
            'quit',
            'exit'
        }:
            print(
                'Bye.'
            )
            break

        if command == '/reason':
            show_reason()
            continue

        if command == '/evidence':
            show_evidence()
            continue

        if command == '/claims':
            show_claims()
            continue

        if command == '/learn':
            show_learning()
            continue

        if command == '/reset':
            reset()
            continue

        try:
            ask(
                question
            )

        except Exception as error:
            print()
            print(
                'ERROR:',
                error
            )
            print()


if __name__ == '__main__':
    main()
