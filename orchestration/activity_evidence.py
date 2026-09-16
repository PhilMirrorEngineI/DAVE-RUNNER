"""Bounded grammatical extraction of attributed activity passages.

Candidates are quotations, not verified events. No person/project-specific rules.
Unknown dates stay unknown; provenance timestamps never supply activity dates.
"""
from datetime import date, datetime
import re

# General English irregular past forms; regular -ed forms are handled below.
_PAST = frozenset('ran built made wrote read sent went took gave bought sold brought found chose drew drove spoke taught learnt learned met began kept led paid put set spent told won worked'.split())
_NON_ACTION = frozenset('is are was were remains remained'.split())
_AUX = frozenset({'has','have','had'})
_FRAMING = re.compile(r"\b(if|unless|suppose|supposing|imagine|hypothetical|example|prompt|question|whether|must|should|could|would|might)\b|\b(?:do not|don't|no evidence|not established|not verified)\b",re.I)
_MONTHS = 'January February March April May June July August September October November December'.split()
_DATE_PATTERN = r'(?:\d{4}-\d{2}-\d{2}|\d{1,2}\s+(?:'+'|'.join(_MONTHS)+r')\s+\d{4})'


def _dates(text):
    values=[]
    for match in re.finditer(_DATE_PATTERN,text,re.I):
        raw=match.group()
        try:
            if '-' in raw:
                value=date.fromisoformat(raw)
            else:
                day,month,year=raw.split()
                value=date(int(year),next(i+1 for i,m in enumerate(_MONTHS) if m.casefold()==month.casefold()),int(day))
        except (ValueError,StopIteration):
            continue
        values.append((match.start(),match.end(),value))
    return values


def _time_position(passage, subject_start, request):
    """Only a leading explicit date scoped to the subject supplies event time.

    Dates elsewhere in a sentence may belong to an object/recollection and are
    left unresolved. This deliberately under-extracts instead of fabricating
    temporal scope. Relative source dates require a separate provenance rule.
    """
    start=request.get('start_date');end=request.get('end_date')
    prefix=passage[:subject_start].strip()
    dates=_dates(prefix)
    event_date=None
    if len(dates)==1:
        left,right,value=dates[0]
        if re.fullmatch(r'(?:on\s+)?',prefix[:left],re.I) and re.fullmatch(r'[, :\-]*',prefix[right:]):
            event_date=value
    if event_date is None:
        return dict(position='DATE_UNRESOLVED',event_date=None)
    if not start or not end:
        return dict(position='DATED_CONTEXT',event_date=event_date.isoformat())
    position='IN_REQUESTED_WINDOW' if date.fromisoformat(start)<=event_date<=date.fromisoformat(end) else 'OUTSIDE_REQUESTED_WINDOW'
    return dict(position=position,event_date=event_date.isoformat())


def activity_passages(record, request):
    subject=request.get('subject')
    if not request.get('ready') or not subject:
        return []
    subject_pattern=r'(?<!\w)'+r'\s+'.join(re.escape(x) for x in subject.split())+r'(?!\w)\s+'
    brief=record.get('human_brief')
    brief=brief if isinstance(brief,dict) else {}
    # Keep source fields distinct. Titles and policy/decision fields are not
    # silently treated as event narratives.
    fields=[('human_brief.summary',brief.get('summary')),
            ('context_shard',record.get('context_shard')),
            ('last_stable_state',record.get('last_stable_state'))]
    found=[];seen=set()
    for field,text in fields:
        if not isinstance(text,str):continue
        pieces=re.split(r'\n+|(?<=[.!?])\s+',text)
        for index,raw in enumerate(pieces):
            passage=' '.join(raw.split()).strip()
            # Preserve an immediately following qualification/denial instead
            # of extracting the positive clause alone and losing its boundary.
            if index+1<len(pieces):
                following=' '.join(pieces[index+1].split()).strip()
                if re.match(r'^(?:this|that|it|however|but|the claim|the account)\b',following,re.I) and re.search(r'\b(?:not|never|unverified|hypothetical|disputed|rejected|uncertain|alleged|denied|false)\b',following,re.I):
                    passage += ' ' + following
            if not passage or len(passage)>3000 or passage.endswith('?'):continue
            for match in re.finditer(subject_pattern,passage,re.I):
                prefix=passage[:match.start()]
                if _FRAMING.search(prefix) or re.search(r'["“”]',prefix):continue
                rest=passage[match.end():]
                words=re.findall(r"[A-Za-z]+(?:'[A-Za-z]+)?",rest)
                if not words:continue
                verb=words[0].lower()
                if verb.endswith('ly') and len(words)>1:
                    words=words[1:]
                    verb=words[0].lower()
                if verb in _AUX:
                    if len(words)<2:continue
                    verb=words[1].lower()
                if verb in _NON_ACTION:continue
                if not (verb in _PAST or (len(verb)>3 and verb.endswith('ed'))):continue
                if re.search(r'\b(?:did not|never|not actually)\b',rest,re.I):continue
                time=_time_position(passage,match.start(),request)
                key=passage.casefold()
                if key in seen:break
                seen.add(key)
                found.append(dict(text=passage,source_field=field,
                    relation='ATTRIBUTED_ACTION_CANDIDATE',**time))
                break
    return found


def make_activity_selector(request, authority_classifier, counters):
    """Callback for the existing retrieve_pmei traversal, before its limits."""
    def select(record,query,question):
        authority=authority_classifier(record)
        if authority not in {'LAWFUL_EVIDENCE','READ_ONLY_EVIDENCE'}:
            counters['authority_excluded_records']+=1
            return []
        candidates=activity_passages(record,request)
        output=[]
        for item in candidates:
            if item['position']=='OUTSIDE_REQUESTED_WINDOW':
                counters['outside_window_passages']+=1
                continue
            counters['activity_candidate_passages']+=1
            # Fixed grammar-match score; source verbosity is not evidence quality.
            output.append((item['text'],1.0,item))
        if output:counters['activity_matching_records']+=1
        return output
    return select
