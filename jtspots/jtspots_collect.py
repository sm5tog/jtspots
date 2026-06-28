"""DX News-hämtning för JTSpots — porterad logik från collect.py."""

import re
import io
import threading
import urllib.request
import urllib.error
from datetime import datetime, timedelta

# ── Konstanter ────────────────────────────────────────────────────────────────

WINDOW_DAYS = 8

DXWORLD_BASE_NUMBER = 665
DXWORLD_BASE_DATE   = datetime(2026, 5, 30)
DXWORLD_URL         = "https://www.dx-world.net/wp-content/uploads/{year}/{month:02d}/DX_{num}.pdf"
URL_425             = "https://www.425dxn.org/wcalpdf.php"
URL_NG3K            = "https://www.ng3k.com/Misc/adxo.html"
URL_DXWORLD_TIMELINE = "https://www.hamradiotimeline.com/timeline/dxw_timeline_1_1.php"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

JUNK_TOKENS = {
    "HF","VHF","UHF","CW","SSB","FT8","FT4","RTTY","FM","AM",
    "LOTW","OQRS","QSL","EME","POTA","IOTA","SOTA","QO","QO-100",
    "WPX","ARRL","CQ","DXCC","SOAB","ATNO","BURO","OPDX","TDDX",
    "DXW","MHZ","KHZ","USA","DX","VALUE","JA","EU","NA","SA",
    "AF","OC","AN","UTC","GMT","SASE","MSHV","WARC","SES",
    "DARC","DCL","RSGB","NZART","EQSL","OK",
}

SPECIAL_KEYWORDS = [
    "special callsign","special call sign","special call",
    "special event station","special event call",
    "commemorative call","commemorative callsign","special prefix",
]

DXCC_PREFIXES = {
    "1A","1S","3A","3B6","3B7","3B8","3B9","3C","3C0","3D2","3DA","3V","3W","3X","3Y",
    "4J","4K","4L","4O","4S","4U","4W","4X","5A","5B","5H","5N","5R","5T","5U","5V",
    "5W","5X","5Z","5Z4","6O","6W","6Y","7O","7P","7Q","7X","8P","8Q","8R","9A","9G",
    "9H","9J","9K","9L","9M2","9M6","9N","9Q","9T","9U","9V","9X","9Y",
    "A2","A3","A4","A5","A6","A7","A9","AP","BS7","BV","BV9P","BY",
    "C2","C3","C5","C6","C8","C9","CE","CE0","CE9","CM","CN","CP","CT","CT3","CU","CX","CY0","CY9",
    "D2","D4","D6","DL","DU","E3","E4","E5","E6","E7","EA","EA6","EA8","EA9","EI","EK",
    "EL","EP","ER","ES","ET","EU","EX","EY","EZ","F","FG","FH","FJ","FK","FM","FO","FP",
    "FR","FS","FT","FT5W","FT5X","FT5Z","FW","FY","G","GD","GI","GJ","GM","GU","GW",
    "H4","H40","HA","HB","HB0","HC","HC8","HH","HI","HK","HK0","HL","HP","HR","HS","HV","HZ",
    "I","IS","IS0","J2","J3","J5","J6","J7","J8","JA","JD1","JT","JW","JX","JY",
    "K","KG4","KH0","KH1","KH2","KH3","KH4","KH5","KH6","KH7","KH7K","KH8","KH9","KL",
    "KP1","KP2","KP4","KP5","LA","LU","LX","LY","LZ",
    "OA","OD","OE","OH","OH0","OJ0","OK","OM","ON","OX","OY","OZ",
    "P2","P29","P4","P5","PA","PJ2","PJ4","PJ5","PJ7","PY","PY0F","PY0S","PY0T","PZ",
    "R1FJ","RA","RA0","RA1","RA2","RA9","S0","S2","S5","S7","S9","SM","SP","ST","SU","SV","SV5","SV9",
    "T2","T30","T31","T32","T33","T5","T7","T8","TA","TF","TG","TI","TI9","TJ","TK","TL","TN","TR",
    "TT","TU","TY","TZ","UA","UA2","UA9","UK","UN","UR",
    "V2","V3","V4","V5","V6","V7","V8","VE","VK","VK0H","VK0M","VK9C","VK9L","VK9M","VK9N",
    "VK9W","VK9X","VP2E","VP2M","VP2V","VP5","VP6","VP8","VP9","VR","VU","VU4","VU7","VY0","VY1","VY2",
    "W","XE","XF4","XT","XU","XW","XX9","XY","XZ","YA","YB","YI","YJ","YK","YL","YN","YO","YS",
    "YU","YV","YV0","Z2","Z3","Z6","Z8","ZA","ZB","ZC4","ZD7","ZD8","ZD9","ZF","ZK3","ZL","ZL7",
    "ZL8","ZL9","ZP","ZS","ZS8",
}

# ── HTTP ──────────────────────────────────────────────────────────────────────

_dxworld_session = None


HTTP_TIMEOUT = 15  # sekunder per anrop


def _make_session():
    import requests
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    try:
        s.get("https://www.dx-world.net/", timeout=HTTP_TIMEOUT)
    except Exception:
        pass
    return s


def http_get(url, binary=False):
    global _dxworld_session
    try:
        import requests
        if "dx-world.net" in url:
            if _dxworld_session is None:
                _dxworld_session = _make_session()
            resp = _dxworld_session.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
        else:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
        return resp.content if binary else resp.text
    except ImportError:
        pass
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        data = r.read()
    if binary:
        return data
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


# ── PDF / HTML ────────────────────────────────────────────────────────────────

def pdf_to_text(pdf_bytes):
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    try:
        from pypdf import PdfReader
        return "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(pdf_bytes)).pages)
    except ImportError:
        pass
    from PyPDF2 import PdfReader
    return "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(pdf_bytes)).pages)


def html_to_text(html):
    s = re.sub(r"</(tr|p|div|li|h\d)>", "\n", html, flags=re.IGNORECASE)
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(td|th)>", "\t", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    import html as _h
    s = _h.unescape(s)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in s.splitlines()]
    return "\n".join(l for l in lines if l)


# ── Datum ─────────────────────────────────────────────────────────────────────

def _parse_ddmm(s, ref_year):
    m = re.match(r"^(\d{1,2})/(\d{1,2})$", s.strip())
    if not m:
        return None
    try:
        d = datetime(ref_year, int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None
    if (datetime.now() - d).days > 180:
        try:
            d = datetime(ref_year + 1, int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return d


def _parse_425_date(s, ref_year):
    s = s.strip()
    m = re.match(r"^(\d{1,2}/\d{1,2})\s*[-–]\s*(\d{1,2}/\d{1,2})$", s)
    if m:
        start = _parse_ddmm(m.group(1), ref_year)
        end   = _parse_ddmm(m.group(2), ref_year)
        if start and end and end < start:
            end = datetime(end.year + 1, end.month, end.day)
        return start, end
    m = re.match(r"^(\d{1,2}/\d{1,2})$", s)
    if m:
        d = _parse_ddmm(s, ref_year)
        return d, d
    return None, None


def _in_window(start, end, ws, we):
    if start is None and end is None:
        return False
    if end is None:
        end = start
    if start is None:
        return end >= ws
    return start <= we and end >= ws


def _check_date_in_text(text, ws, we, ref_year):
    month_map = {
        "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
        "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
        "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    }
    tl = text.lower()
    dates = []
    for m in re.finditer(
        r"\b(january|february|march|april|may|june|july|august|september|october|november|december"
        r"|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?", tl
    ):
        mo = month_map.get(m.group(1))
        if not mo:
            continue
        try:
            dates.append(datetime(ref_year, mo, int(m.group(2))))
        except ValueError:
            pass
        if m.group(3):
            try:
                dates.append(datetime(ref_year, mo, int(m.group(3))))
            except ValueError:
                pass
    for m in re.finditer(r"\b(\d{1,2})/(\d{1,2})\b", text):
        d = _parse_ddmm(f"{m.group(1)}/{m.group(2)}", ref_year)
        if d:
            dates.append(d)
    if not dates:
        return False
    dates.sort()
    return dates[0] <= we and dates[-1] >= ws


# ── Anropsklassificering ──────────────────────────────────────────────────────

_VALID_CALL_RE = re.compile(r"^[A-Z0-9]{3,}$")

COUNTRY_ANTARCTIC_SUFFIXES = [("Z", re.compile(r"^LU\d"))]


def _is_valid_call(s):
    if not s or not (3 <= len(s) <= 12):
        return False
    if not _VALID_CALL_RE.match(s):
        return False
    if s in JUNK_TOKENS:
        return False
    return any(c.isalpha() for c in s) and any(c.isdigit() for c in s)


def _is_prefix_only(s):
    if not s:
        return True
    s = s.upper()
    if not any(c.isdigit() for c in s):
        return True
    last_digit = max(i for i, c in enumerate(s) if c.isdigit())
    after = s[last_digit + 1:]
    return not (after and any(c.isalpha() for c in after))


def _is_dxcc_prefix(s):
    return s.upper() in DXCC_PREFIXES if s else False


def _normalize_slash(call):
    if "/" not in call:
        return call, call, True
    parts = [p for p in call.split("/") if p]
    if not parts:
        return call, call, False
    complete = [p for p in parts if not _is_prefix_only(p) and _is_valid_call(p)]
    base = max(complete, key=len) if complete else max(parts, key=len)
    others = [p for p in parts if p != base]
    dxcc_rel = any(_is_dxcc_prefix(p) for p in others)
    if not dxcc_rel:
        for o in others:
            for sfx, pat in COUNTRY_ANTARCTIC_SUFFIXES:
                if o.upper() == sfx and pat.match(base.upper()):
                    dxcc_rel = True
    return call, base, dxcc_rel


def _find_calls_in_info(info):
    found = []
    if not info:
        return found
    for m in re.finditer(r"\bas\s+([A-Z0-9]+(?:/[A-Z0-9]+)*)", info, re.IGNORECASE):
        tok = m.group(1).upper()
        if "/" in tok:
            if any(_is_valid_call(p) for p in tok.split("/") if p):
                found.append(tok)
        elif _is_valid_call(tok):
            found.append(tok)
    if not found:
        for m in re.finditer(r"\bBy\s+([A-Z][A-Z0-9]+(?:/[A-Z0-9]+)*)\b", info):
            tok = m.group(1).upper()
            if "/" in tok:
                if any(_is_valid_call(p) for p in tok.split("/") if p):
                    found.append(tok)
            elif _is_valid_call(tok):
                found.append(tok)
    return found


def _process_call(call, is_special, calls, specials):
    call = call.strip().rstrip(",.;:").upper()
    if not call:
        return
    if "/" in call:
        parts = [p for p in call.split("/") if p]
        if not any(_is_valid_call(p) for p in parts):
            return
        _, base, dxcc_rel = _normalize_slash(call)
        if dxcc_rel and _is_valid_call(base) and not _is_prefix_only(base):
            if is_special:
                if base not in calls:
                    specials.add(base)
            else:
                calls.add(base)
                specials.discard(base)
    else:
        if not _is_valid_call(call) or _is_prefix_only(call):
            return
        if is_special:
            if call not in calls:
                specials.add(call)
        else:
            calls.add(call)
            specials.discard(call)


# ── Extraktion per källa ──────────────────────────────────────────────────────

def _extract_425(text, ws, we):
    calls, specials = set(), set()
    ref = datetime.now().year
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^till\s+(\d{1,2}/\d{1,2})\s+([A-Z0-9/]+)[:\s]", line)
        if m:
            start, end = None, _parse_ddmm(m.group(1), ref)
            seg_call = m.group(2).strip().rstrip(":")
        else:
            m = re.match(r"^(\d{1,2}/\d{1,2}(?:[-–]\d{1,2}/\d{1,2})?)\s+([A-Z0-9/]+)[:\s]", line)
            if not m:
                continue
            start, end = _parse_425_date(m.group(1), ref)
            seg_call = m.group(2).strip().rstrip(":")
        if not _in_window(start, end, ws, we):
            continue
        is_special = any(kw in line.lower() for kw in SPECIAL_KEYWORDS)
        seg_m = re.match(r"^(?:till\s+\d{1,2}/\d{1,2}|\d{1,2}/\d{1,2}(?:[-–]\d{1,2}/\d{1,2})?)\s+(.+?)[:\s]", line)
        if seg_m:
            for p in re.split(r"[,]|\s+and\s+", seg_m.group(1)):
                p = p.strip().rstrip(":").strip()
                if p and ("/" in p or _is_valid_call(p)):
                    _process_call(p, is_special, calls, specials)
        else:
            _process_call(seg_call, is_special, calls, specials)
    return calls, specials


def _extract_ng3k(text, ws, we):
    calls, specials = set(), set()
    month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                 "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    date_re = re.compile(r"(\d{4})\s*([A-Z][a-z]{2})(\d{1,2})")

    def parse_date(s):
        m = date_re.search(s)
        if not m:
            return None
        mo = month_map.get(m.group(2))
        if not mo:
            return None
        try:
            return datetime(int(m.group(1)), mo, int(m.group(3)))
        except ValueError:
            return None

    for line in text.splitlines():
        dates = list(date_re.finditer(line))
        if len(dates) < 2:
            continue
        start, end = parse_date(dates[0].group(0)), parse_date(dates[1].group(0))
        if not start or not end or not _in_window(start, end, ws, we):
            continue
        rest = line[dates[1].end():]
        is_special = any(kw in line.lower() for kw in SPECIAL_KEYWORDS)
        tokens = re.findall(r"[A-Z0-9]+(?:/[A-Z0-9]+)*", rest)
        call_field = next((t for t in tokens if t not in JUNK_TOKENS and len(t) >= 2), None)
        if not call_field:
            continue
        if "/" in call_field or not _is_prefix_only(call_field):
            _process_call(call_field, is_special, calls, specials)
            info_start = rest.find(call_field)
            if info_start >= 0:
                for ec in _find_calls_in_info(rest[info_start + len(call_field):]):
                    if ec != call_field:
                        _process_call(ec, is_special, calls, specials)
        else:
            info_start = rest.find(call_field)
            info = rest[info_start + len(call_field):] if info_start >= 0 else rest
            for fc in _find_calls_in_info(info):
                _process_call(fc, is_special, calls, specials)
    return calls, specials


def _extract_dxworld(text, ws, we):
    calls, specials = set(), set()
    ref = datetime.now().year
    lines = text.splitlines()
    in_skip = False
    as_pat = re.compile(r"\bas\s+([A-Z0-9]+(?:/[A-Z0-9]+)*)\b")
    op_pat = re.compile(
        r"\b(QSL\s+via|via|operator|operators|Look\s+for|Team|by|with|signs|include|including|joined\s+by)"
        r"\s+([A-Z0-9]+(?:/[A-Z0-9]+)?)", re.IGNORECASE)
    name_pat = re.compile(
        r"\b([A-Z][a-zé]{2,15}),?\s+([A-Z0-9]+(?:/[A-Z0-9]+)?)\s+(is|will|are|operating|active|signs|works|plans)")

    for i, line in enumerate(lines):
        ls = line.strip()
        ll = ls.lower()
        if re.search(r"\bannouncements?\b", ll) and len(ls) < 50:
            in_skip = True
            continue
        if re.search(r"\b(reminders|the reminders)\b", ll) and len(ls) < 50:
            in_skip = True
            continue
        if re.search(r"\b(this week on air|qsl preview|coming up soon|iota|special calls)\b", ll) and len(ls) < 50:
            in_skip = False
        if in_skip:
            continue
        is_special = any(kw in ll for kw in SPECIAL_KEYWORDS)
        prev = lines[i-1].strip() if i > 0 else ""
        if not _check_date_in_text(ls, ws, we, ref) and not _check_date_in_text(prev, ws, we, ref):
            continue
        accepted, rejected = set(), set()
        for m in as_pat.finditer(ls):
            tok = m.group(1).upper()
            if tok in JUNK_TOKENS:
                continue
            if "/" in tok:
                if any(_is_valid_call(p) for p in tok.split("/") if p):
                    accepted.add(tok)
            elif _is_valid_call(tok) and not _is_prefix_only(tok):
                accepted.add(tok)
        for m in op_pat.finditer(ls):
            tok = m.group(2).upper().rstrip(",.;:")
            if tok and (_is_valid_call(tok) or "/" in tok):
                rejected.add(tok)
        for m in name_pat.finditer(ls):
            tok = m.group(2).upper().rstrip(",.;:")
            if tok and tok not in accepted:
                rejected.add(tok)
        tokens = re.findall(
            r"\b[A-Z0-9]{2,}(?:/[A-Z0-9]+)+\b|\b[A-Z]{1,2}[0-9]+[A-Z]+\b|\b[0-9][A-Z]+[0-9]+[A-Z]+\b", ls)
        final = set(accepted)
        for tok in tokens:
            if tok in JUNK_TOKENS or tok in rejected or tok in accepted:
                continue
            if _is_dxcc_prefix(tok) and _is_prefix_only(tok):
                continue
            if "/" in tok:
                if any(_is_valid_call(p) for p in tok.split("/") if p):
                    final.add(tok)
            elif _is_valid_call(tok) and not _is_prefix_only(tok):
                if not accepted:
                    final.add(tok)
        for c in final:
            _process_call(c, is_special, calls, specials)
    return calls, specials


# ── DX-World PDF-hämtning ─────────────────────────────────────────────────────

def _dxworld_fetch():
    import time
    global _dxworld_session

    today = datetime.now()
    weeks_diff = (today - DXWORLD_BASE_DATE).days // 7
    base_num = DXWORLD_BASE_NUMBER + weeks_diff

    attempts = []
    attempts.append((base_num, today.year, today.month))
    for no in (0, -1, 1, -2, 2):
        for mo in (0, -1, 1):
            if no == 0 and mo == 0:
                continue
            num = base_num + no
            year, month = today.year, today.month + mo
            if month < 1:
                month += 12; year -= 1
            elif month > 12:
                month -= 12; year += 1
            attempts.append((num, year, month))
    seen, unique = set(), []
    for a in attempts:
        if a not in seen:
            seen.add(a); unique.append(a)

    try:
        import requests
        HTTPError_r = requests.exceptions.HTTPError
    except ImportError:
        HTTPError_r = None

    def _is_404(exc):
        if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
            return True
        if HTTPError_r and isinstance(exc, HTTPError_r):
            resp = getattr(exc, "response", None)
            return resp is not None and resp.status_code == 404
        return False

    def _is_403(exc):
        if isinstance(exc, urllib.error.HTTPError) and exc.code == 403:
            return True
        if HTTPError_r and isinstance(exc, HTTPError_r):
            resp = getattr(exc, "response", None)
            return resp is not None and resp.status_code == 403
        return False

    for num, year, month in unique:
        url = DXWORLD_URL.format(year=year, month=month, num=num)
        for attempt in range(3):
            try:
                data = http_get(url, binary=True)
                if data[:4] == b"%PDF":
                    return data
                break  # 200 men ej PDF — prova nästa URL
            except Exception as e:
                if _is_404(e):
                    break  # filen finns inte — nästa URL
                if _is_403(e):
                    _dxworld_session = None  # ny session vid bot-skydd
                    if attempt < 2:
                        time.sleep(3)
                        continue
                    break
                if attempt < 2:
                    time.sleep(2)
    return None


# ── DX-World Timeline-extraktion ──────────────────────────────────────────────

def _extract_dxworld_timeline(html, ws, we):
    calls, specials = set(), set()
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
    }
    month_m = re.search(r'content="DX-World\.net - ([A-Z]+) Featured', html)
    month_str = month_m.group(1)[:3] if month_m else None
    month_num = month_map.get(month_str)
    if not month_num:
        return calls, specials
    year = datetime.now().year

    labels_m = re.search(r"var labels\s*=\s*\[([^\]]+)\]", html)
    if not labels_m:
        return calls, specials
    labels = re.findall(r"'([^']*)'", labels_m.group(1))

    data_m = re.search(r'data\s*=\s*\[(.*?)\];', html, re.DOTALL)
    if not data_m:
        return calls, specials
    rows = re.findall(r'\[\[([^\]]*)\],\[([^\]]*)\]\]', data_m.group(1))

    bars = []
    for a, b in rows:
        for item in [a, b]:
            parts = item.split(',')
            try:
                start = int(parts[0].strip())
            except (ValueError, IndexError):
                start = None
            try:
                dur = int(parts[1].strip())
            except (ValueError, IndexError):
                dur = None
            bars.append((start, dur))

    for label, (start, dur) in zip(labels, bars):
        if not label or start is None or dur is None:
            continue
        try:
            start_date = datetime(year, month_num, start + 1)
            end_date = start_date + timedelta(days=dur - 1)
        except ValueError:
            continue
        if not _in_window(start_date, end_date, ws, we):
            continue
        raw_call = label.split(' ')[0].split('-')[0].strip().upper()
        if raw_call:
            _process_call(raw_call, False, calls, specials)

    return calls, specials


# ── Publik API ────────────────────────────────────────────────────────────────

def fetch_dx_news(on_progress=None, on_done=None):
    """
    Hämtar DX News från alla tre källor i bakgrundstråd.
    on_progress(msg): statusuppdateringar
    on_done(union_calls, special_calls, error_msg): resultat
    """
    def _run():
        today    = datetime.now()
        ws       = today
        we       = today + timedelta(days=WINDOW_DAYS)
        union    = set()
        specials = set()
        errors   = []

        def _prog(msg):
            if on_progress:
                on_progress(msg)

        # DX-World
        _prog("Hämtar DX-World PDF...")
        try:
            pdf = _dxworld_fetch()
            if pdf:
                text = pdf_to_text(pdf)
                c, s = _extract_dxworld(text, ws, we)
                union |= c; specials |= s
                _prog(f"DX-World: {len(c)} union, {len(s)} special")
            else:
                errors.append("DX-World: ingen PDF hittades")
                _prog("DX-World: misslyckades")
        except Exception as e:
            errors.append(f"DX-World: {e}")
            _prog(f"DX-World fel: {e}")

        # 425 DX
        _prog("Hämtar 425 DX Bulletin...")
        try:
            pdf = http_get(URL_425, binary=True)
            text = pdf_to_text(pdf)
            c, s = _extract_425(text, ws, we)
            union |= c; specials |= s
            _prog(f"425 DX: {len(c)} union, {len(s)} special")
        except Exception as e:
            errors.append(f"425 DX: {e}")
            _prog(f"425 DX fel: {e}")

        # NG3K
        _prog("Hämtar NG3K...")
        try:
            html = http_get(URL_NG3K, binary=False)
            text = html_to_text(html)
            c, s = _extract_ng3k(text, ws, we)
            union |= c; specials |= s
            _prog(f"NG3K: {len(c)} union, {len(s)} special")
        except Exception as e:
            errors.append(f"NG3K: {e}")
            _prog(f"NG3K fel: {e}")

        # DX-World Timeline
        _prog("Hämtar DX-World Timeline...")
        try:
            html = http_get(URL_DXWORLD_TIMELINE, binary=False)
            c, s = _extract_dxworld_timeline(html, ws, we)
            union |= c; specials |= s
            _prog(f"DX-World Timeline: {len(c)} union, {len(s)} special")
        except Exception as e:
            errors.append(f"DX-World Timeline: {e}")
            _prog(f"DX-World Timeline fel: {e}")

        specials -= union
        err_msg = "; ".join(errors) if errors else None
        if on_done:
            on_done(union, specials, err_msg)

    threading.Thread(target=_run, daemon=True).start()
