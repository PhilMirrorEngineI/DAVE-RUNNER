"""Deterministic progress relevance; never current-state or approval authority.

Shared by the existing intent, passage ranking and qualification path. No
project names, saved record IDs, network calls, inference or state mutation.
"""
import re
from .request_interpretation import text_mentions_subject

FACETS = ('IMPLEMENTED', 'TESTED', 'UNRESOLVED')


def clean(text):
    return ' '.join(str(text or '').split())


def implementation_progress_topic(question):
    """Recognise a named implementation audit requesting both history and gaps."""
    value = clean(question)
    first = re.split(r'[?!\n]', str(question or '').strip(), maxsplit=1)[0].strip()
    if not all(re.search(pattern, value, re.I) for pattern in (
        r'\b(?:built|implemented|installed|implementation)\b',
        r'\b(?:tested|tests?|verification|verified\s+behavio[ur]+)\b',
        r'\b(?:incomplete|unverified|unresolved|blocking|blockers?|remaining|remains|unfinished|outstanding)\b',
    )):
        return None
    patterns = (
        r'^What\s+needs\s+to\s+happen\s+to\s+get\s+(?P<topic>.+?)[\u2019\']s\s+(?:existing\s+)?(?:logic|implementation|software|system)\b',
        r'^(?:Review|Audit|Assess|Summari[sz]e|Check)\s+(?:the\s+)?(?:implementation|development)\s+(?:status|progress|evidence)\s+(?:of|for)\s+(?P<topic>[^:;.]+)',
        r'^(?:Review|Audit|Assess|Summari[sz]e|Check)\s+(?P<topic>.+?)[\u2019\']s\s+(?:implementation|development)\s+(?:status|progress|evidence)\b',
    )
    for pattern in patterns:
        match = re.search(pattern, first, re.I)
        if match:
            topic = match.group('topic').strip()
            if len(topic) <= 100 and len(topic.split()) <= 8 and topic.lower() not in {
                'it', 'this', 'that', 'the system', 'the project', 'my project', 'our project',
                'this project', 'that project', 'this system', 'that system'}:
                return topic
    return None


def report_facets(text):
    """Classify the subject matter of an asserted report, not its truth.

    Headings, source lists, questions, instructions and hypothetical results
    cannot become progress reports merely by containing status vocabulary.
    Negative findings are retained as limitations, not positive achievements.
    """
    value = clean(text)
    if not value or value.endswith('?') or re.match(
        r'^(?:source saves?\s*:|what\b|which\b|whether\b|how\b|why\b|if\b|'
        r'do not\b|don\x27t\b|must\b|should\b|use\b|run\b|please\b|'
        r'assert\b|ensure\b|verify\b|check\b|confirm\b|expect\b|example\s*:|'
        r'propos(?:e|ed|al)\b|recommended\s+next\b|next steps?\s*:)', value, re.I):
        return ()
    # Constraints are not execution reports, even when they name tests.
    if re.search(r"\b(?:do not|don't|must not|should not)\b", value, re.I):
        return ()
    # Do not accept imagined/conditional accomplishments as observations.
    if re.search(r'\b(?:would|could|should|might|may|will|aim\s+for|plan\s+to|proposed|proposal|hypothetical)\b', value, re.I):
        return ()
    facets = []
    negative = bool(re.search(
        r'\b(?:not(?:\s+yet)?|never|no)\s+(?:been\s+|actually\s+|independently\s+|fully\s+)?'
        r'(?:installed|built|implemented|tested|verified|proven|proved|deployed|executed|completed)\b'
        r'|\b(?:does not|cannot|no evidence).{0,100}\b(?:prove|establish|support)\b', value, re.I))
    if not negative and re.search(
        r'\b(?:was|were|has been|have been|is now|are now)\s+(?:successfully\s+)?'
        r'(?:built|implemented|installed|added|fixed|connected|restored|updated|replaced)\b'
        r'|\b(?:implemented|installed|added|fixed|connected|restored|updated|replaced)\s+'
        r'(?:a\b|an\b|the\b|its\b|[^\s]+\.py\b)'
        r'|\b(?:implementation|patch|module|adapter|route|schema|renderer)\b.{0,60}\b(?:was added|was created|is installed|now uses|now routes|now requires)\b', value, re.I):
        facets.append('IMPLEMENTED')
    if not negative and (re.search(r'\b\d+\s+(?:(?:sub)?tests?\s+)?(?:passed|failed|passing|failures)\b',value,re.I)
        or re.search(r'\b(?:test|tests|probe|suite|pytest|checks?|inference|chain)\b.{0,80}\b(?:passed|failed|returned|retrieved|showed|reported|succeeded|timed out)\b',value,re.I)
        or re.search(r'\b(?:test|probe|suite|verification)\s+PASS\b',value)):
        facets.append('TESTED')
    if negative and not re.search(r'\b(?:implementation|implemented|installed|built|tested|verified|deployment|runtime|execution|orchestration|worker|gate|tests?|build|chain|code|state|logic|working|behavio[ur]+)\b', value, re.I):
        negative = False
    if negative or re.search(
        r'\b(?:remains?|still|is|are|was|were)\s+(?:explicitly\s+|currently\s+|partly\s+)?'
        r'(?:incomplete|unverified|unresolved|unfinished|pending|blocked|open|unproved|unproven|missing)\b'
        r'|\b(?:timed out|not yet evidenced|not yet authoritative|not evidenced|not proven|not independently evidenced)\b'
        r'|\b(?:test|tests|probe|suite|pytest|checks?|inference|chain|retrieval|endpoint)\b.{0,80}\b(?:failed|fails|failure|timeout|503|422)\b'
        r'|\b(?:remaining|unresolved|unfinished|pending)\s+(?:work|issue|seam|blocker|test|task|step)\b',value,re.I):
        facets.append('UNRESOLVED')
    return tuple(facets)


def progress_topic(question):
    # Lazy import avoids a cycle with the intent classifier.
    from .question_intent import classify_question_intent
    intent = classify_question_intent(question)
    return intent.topic if intent.intent == 'PROGRESS_HISTORY' else None


def record_fields(record):
    """Only source-owned prose fields; control/learning metadata is not prose."""
    values=[]
    for key in ('context_shard','last_stable_state','human_title','human_summary'):
        if isinstance(record.get(key),str):values.append((key,record[key]))
    brief=record.get('human_brief')
    if isinstance(brief,dict):
        for key in ('title','summary','decision_made','why_it_matters'):
            if isinstance(brief.get(key),str):values.append(('human_brief.'+key,brief[key]))
    for key in ('key_insights','anchor_points','open_threads'):
        if isinstance(record.get(key),list):
            values.extend((key,str(v)) for v in record[key] if isinstance(v,str))
    return values


def bound_report(text, record, topic):
    """Return facets only for a verbatim source passage with subject binding."""
    facets=report_facets(text)
    if not facets or not topic:return ()
    passage=clean(text)
    if len(passage) > 3000:return ()
    sources=[(key,value) for key,value in record_fields(record) if passage in clean(value)]
    if not sources:return ()
    # Reject explicit attribution to a different named project.
    named=re.search(r'\b([A-Z][\w-]*(?:\s+[A-Z][\w-]*){0,3})\s+project\b',passage)
    if named and not text_mentions_subject(named.group(1),topic):return ()
    if text_mentions_subject(passage,topic):return facets
    brief=record.get('human_brief') if isinstance(record.get('human_brief'),dict) else {}
    headings=[str(record.get('human_title') or ''),str(brief.get('title') or '')]
    headings.extend(re.split(r'[.!?\n]',value,maxsplit=1)[0] for key,value in record_fields(record)
                    if key in {'context_shard','human_brief.summary','last_stable_state'})
    return facets if any(text_mentions_subject(h,topic) for h in headings) else ()


def select_coverage(items, limit, facets):
    """Keep each evidenced facet within the existing cap, then fill by rank."""
    selected=[];seen=set()
    for facet in FACETS:
        if facet in seen:continue
        item=next((item for item in items if facet in facets(item)),None)
        if item is not None and item not in selected and len(selected)<limit:
            selected.append(item);seen.update(facets(item))
    for item in items:
        if len(selected)>=limit:break
        if item not in selected:selected.append(item)
    return selected
