#!/usr/bin/env python3
"""JTSpots — JTAlert/WSJT-X → Log4OM DX-spot bridge med Clublog-filter."""

import struct
import socket
import threading
import re
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
import customtkinter as ctk

SETTINGS_FILE = Path(__file__).parent / 'jtspots_settings.json'

WSJTX_MAGIC = 0xADBCCBDA
MSG_STATUS  = 1
MSG_DECODE  = 2

DEFAULT_MCAST = "224.0.0.1"
DEFAULT_UPORT = 2237
DEFAULT_TPORT = 7300

# ── QDataStream reader ──────────────────────────────────────────────────────

class QStream:
    def __init__(self, buf):
        self._b = buf
        self._p = 0

    def u8(self):
        v = self._b[self._p]; self._p += 1; return v

    def bool_(self):
        return bool(self.u8())

    def u32(self):
        v, = struct.unpack_from('>I', self._b, self._p); self._p += 4; return v

    def i32(self):
        v, = struct.unpack_from('>i', self._b, self._p); self._p += 4; return v

    def u64(self):
        v, = struct.unpack_from('>Q', self._b, self._p); self._p += 8; return v

    def f64(self):
        v, = struct.unpack_from('>d', self._b, self._p); self._p += 8; return v

    def str_(self):
        n = self.u32()
        if n == 0xFFFFFFFF:
            return ''
        v = self._b[self._p:self._p + n].decode('utf-8', errors='replace')
        self._p += n
        return v


def parse_packet(data):
    try:
        s = QStream(data)
        if s.u32() != WSJTX_MAGIC:
            return None
        s.u32()
        mtype = s.u32()
        cid   = s.str_()

        if mtype == MSG_STATUS:
            freq = s.u64()
            mode = s.str_()
            return {'t': 'status', 'id': cid, 'freq': freq, 'mode': mode}

        if mtype == MSG_DECODE:
            _new  = s.bool_()
            _tms  = s.u32()
            snr   = s.i32()
            _dt   = s.f64()
            df    = s.u32()
            mode  = s.str_()
            msg   = s.str_()
            return {'t': 'decode', 'id': cid, 'snr': snr,
                    'df': df, 'mode': mode, 'msg': msg}
    except Exception:
        pass
    return None


# ── Frekvens → mode / band ───────────────────────────────────────────────────

_FT8_FREQS = {1840, 3573, 5357, 7074, 10136, 14074, 18100, 21074, 24915, 28074, 50313, 50323, 144174}
_FT4_FREQS = {3575, 7047, 14080, 18104, 21140, 24919, 28180, 50318}

def mode_from_freq(freq_khz: float) -> str:
    khz = round(freq_khz)
    for f in _FT8_FREQS:
        if abs(khz - f) <= 5:
            return 'FT8'
    for f in _FT4_FREQS:
        if abs(khz - f) <= 5:
            return 'FT4'
    return ''

def freq_to_band(freq_khz: float) -> str:
    f = freq_khz
    if 1800   <= f <= 2000:   return '160'
    if 3500   <= f <= 4000:   return '80'
    if 5300   <= f <= 5410:   return '60'
    if 7000   <= f <= 7300:   return '40'
    if 10100  <= f <= 10150:  return '30'
    if 14000  <= f <= 14350:  return '20'
    if 18068  <= f <= 18168:  return '17'
    if 21000  <= f <= 21450:  return '15'
    if 24890  <= f <= 24990:  return '12'
    if 28000  <= f <= 29700:  return '10'
    if 50000  <= f <= 54000:  return '6'
    if 144000 <= f <= 148000: return '2'
    return ''


# ── CQ-parser ────────────────────────────────────────────────────────────────

_CQ_RE = re.compile(
    r'^CQ(?:\s+(?:DX|[A-Z]{2,3}))?\s+([A-Z0-9/]+)\s+[A-R]{2}[0-9]{2}',
    re.IGNORECASE
)
_VALID_CALL_RE = re.compile(r'^[A-Z0-9]{3,}(?:/[A-Z0-9]+)?$', re.IGNORECASE)
_JUNK_CALLS = {'73', 'RR73', 'RRR', 'TNX', 'TU', 'DE', 'CQ', 'DX', 'QSL'}

def extract_cq_call(msg: str):
    m = _CQ_RE.match(msg.strip())
    return m.group(1).upper() if m else None

def is_valid_callsign(s: str) -> bool:
    if not s or '<' in s or '>' in s:
        return False
    if s.upper() in _JUNK_CALLS:
        return False
    if not _VALID_CALL_RE.match(s):
        return False
    has_letter = any(c.isalpha() for c in s)
    has_digit  = any(c.isdigit() for c in s)
    return has_letter and has_digit

# DX cluster spot format: "DX de SPOTTER:   FREQ  CALL  COMMENT  TIME"
_SPOT_RE = re.compile(
    r'^DX de\s+(\S+?):?\s+(\d+\.?\d*)\s+(\S+)\s+(.*?)\s+(\d{4}Z)',
    re.IGNORECASE
)
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[mK]')


# ── Clublog-klient ───────────────────────────────────────────────────────────

class ClublogClient:
    MATRIX_URL = 'https://clublog.org/json_dxccchart.php'
    DXCC_URL   = 'https://clublog.org/dxcc'

    def __init__(self):
        self._matrix      = {}
        self._dxcc_cache  = {}
        self._lock        = threading.Lock()
        self.api_key      = ''
        self.email        = ''
        self.password     = ''
        self.callsign     = ''
        self.last_fetch   = None
        self.entity_count = 0

    def fetch_matrix(self, on_done=None):
        threading.Thread(target=self._fetch_matrix_bg, args=(on_done,),
                         daemon=True).start()

    def _fetch_matrix_bg(self, on_done):
        try:
            params = urllib.parse.urlencode({
                'call':     self.callsign,
                'api':      self.api_key,
                'email':    self.email,
                'password': self.password,
                'mode':     0,
            })
            url = f'{self.MATRIX_URL}?{params}'
            debug_url = re.sub(r'(password=)[^&]+', r'\1***', url)
            debug_url = re.sub(r'(api=)[^&]+', r'\1***', debug_url)
            if on_done:
                on_done(None, f'Anropar: {debug_url}')
            req = urllib.request.Request(url, headers={'User-Agent': 'JTSpots/1.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                raw = r.read().decode()
            if not raw.strip().startswith('{'):
                raise ValueError(f'Oväntat svar: {raw[:120]}')
            data = json.loads(raw)
            with self._lock:
                self._matrix      = data
                self._dxcc_cache  = {}
                self.last_fetch   = datetime.now()
                self.entity_count = len(data)
            if on_done:
                on_done(True, f'{self.entity_count} enheter hämtade')
        except urllib.error.HTTPError as e:
            if on_done:
                on_done(False, f'HTTP {e.code}: {e.reason}')
        except urllib.error.URLError as e:
            if on_done:
                on_done(False, f'Nätverksfel: {e.reason}')
        except Exception as e:
            if on_done:
                on_done(False, f'{type(e).__name__}: {e}')

    def get_dxcc(self, callsign: str) -> str:
        with self._lock:
            if callsign in self._dxcc_cache:
                return self._dxcc_cache[callsign]
        try:
            params = urllib.parse.urlencode({'call': callsign, 'api': self.api_key})
            url = f'{self.DXCC_URL}?{params}'
            with urllib.request.urlopen(url, timeout=5) as r:
                adif = r.read().decode().strip()
            with self._lock:
                self._dxcc_cache[callsign] = adif
            return adif
        except Exception:
            return ''

    def is_needed(self, callsign: str, freq_khz: float) -> tuple:
        with self._lock:
            if not self._matrix:
                return True, ''
        adif = self.get_dxcc(callsign)
        if not adif:
            return True, '?DXCC'
        band = freq_to_band(freq_khz)
        with self._lock:
            if adif not in self._matrix:
                return True, 'ATNO'
            if band and band not in self._matrix[adif]:
                return True, f'Ny {band}m'
        return False, ''

    @property
    def ready(self) -> bool:
        with self._lock:
            return bool(self._matrix)


# ── Telnet DX-cluster server (mot Log4OM) ────────────────────────────────────

class SpotServer:
    def __init__(self, port):
        self._port    = port
        self._clients = []
        self._lock    = threading.Lock()
        self._running = False
        self._sock    = None

    def start(self):
        self._running = True
        threading.Thread(target=self._serve, daemon=True).start()

    def stop(self):
        self._running = False
        if self._sock:
            try: self._sock.close()
            except Exception: pass

    def _serve(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', self._port))
        self._sock.listen(5)
        self._sock.settimeout(1.0)
        while self._running:
            try:
                conn, _ = self._sock.accept()
                with self._lock:
                    self._clients.append(conn)
                conn.sendall(b'JTSpots DX Cluster\r\n')
            except socket.timeout:
                pass
            except Exception:
                break

    def send_spot(self, line: str):
        dead = []
        with self._lock:
            for c in self._clients:
                try:
                    c.sendall((line + '\r\n').encode())
                except Exception:
                    dead.append(c)
            for c in dead:
                self._clients.remove(c)

    @property
    def client_count(self):
        with self._lock:
            return len(self._clients)


# ── UDP multicast-lyssnare ────────────────────────────────────────────────────

class UDPListener:
    def __init__(self, group, port, callback):
        self._group    = group
        self._port     = port
        self._callback = callback
        self._running  = False

    def start(self):
        self._running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._running = False

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self._port))
        mreq = struct.pack('4sL', socket.inet_aton(self._group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(1.0)
        while self._running:
            try:
                data, _ = sock.recvfrom(4096)
                self._callback(data)
            except socket.timeout:
                pass
        sock.close()


# ── DX Cluster-klient (mot externa cluster) ───────────────────────────────────

# ── Tidsbuffert ───────────────────────────────────────────────────────────────

class SpotBuffer:
    """Håller reda på nyligen sedda (callsign, band)-kombinationer."""

    def __init__(self):
        self._seen = {}   # {(call, band): timestamp}
        self._lock = threading.Lock()
        self.minutes = 10

    def is_duplicate(self, call: str, band: str) -> bool:
        key = (call, band)
        now = time.monotonic()
        with self._lock:
            if key in self._seen:
                if now - self._seen[key] < self.minutes * 60:
                    return True
            self._seen[key] = now
            return False

    def clear(self):
        with self._lock:
            self._seen.clear()


# ── Filterregler ──────────────────────────────────────────────────────────────

COND_TYPES = {
    'atno':        'ATNO (kräver Clublog)',
    'new_band':    'Ny bandländer (kräver Clublog)',
    'wanted_call': 'Wanted callsign (kommasep.)',
    'band':        'Band (kommasep. t.ex. 6,2)',
    'mode':        'Mode (kommasep. t.ex. FT8,CW)',
    'snr':         'Min SNR (dB)',
}

class RuleEngine:
    def __init__(self, clublog):
        self._clublog = clublog

    def evaluate(self, call, freq_khz, snr, mode, rules):
        """Returns (passed, rule_name). If no active rules: pass all."""
        active = [r for r in rules if r.get('enabled', True)]
        if not active:
            return True, ''
        for rule in active:
            if self._matches(rule, call, freq_khz, snr, mode):
                return True, rule.get('name', '')
        return False, ''

    def _matches(self, rule, call, freq_khz, snr, mode):
        return all(self._cond_ok(c, call, freq_khz, snr, mode)
                   for c in rule.get('conditions', []))

    def _cond_ok(self, cond, call, freq_khz, snr, mode):
        t = cond.get('type', '')
        if t == 'atno':
            _, reason = self._clublog.is_needed(call, freq_khz)
            return reason == 'ATNO'
        if t == 'new_band':
            needed, reason = self._clublog.is_needed(call, freq_khz)
            return needed
        if t == 'wanted_call':
            calls = {c.strip().upper() for c in cond.get('value', '').split(',') if c.strip()}
            return call.upper() in calls
        if t == 'band':
            bands = {b.strip() for b in cond.get('value', '').split(',') if b.strip()}
            return freq_to_band(freq_khz) in bands
        if t == 'mode':
            modes = {m.strip().upper() for m in cond.get('value', '').split(',') if m.strip()}
            return mode.upper() in modes
        if t == 'snr':
            try:
                return snr >= int(cond.get('value', '-99'))
            except ValueError:
                return True
        return True


class ClusterClient:
    def __init__(self, cfg: dict, on_spot, on_status):
        self._cfg       = cfg        # {name, host, port, callsign, password, init}
        self._on_spot   = on_spot    # callback(line: str)
        self._on_status = on_status  # callback(name: str, connected: bool, msg: str)
        self._running   = False
        self._connected = False

    @property
    def name(self):
        return self._cfg.get('name', '?')

    @property
    def connected(self):
        return self._connected

    def connect(self):
        self._running = True
        threading.Thread(target=self._run, daemon=True).start()

    def disconnect(self):
        self._running = False

    def _run(self):
        host = self._cfg.get('host', '')
        port = int(self._cfg.get('port', 7373))
        call = self._cfg.get('callsign', '')
        pwd  = self._cfg.get('password', '')
        init = self._cfg.get('init', '<CALLSIGN>')

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            self._connected = True
            self._on_status(self.name, True, f'Ansluten till {host}:{port}')

            for cmd in init.splitlines():
                cmd = cmd.strip()
                if not cmd or cmd.startswith('//'):
                    continue
                if cmd == '<DELAY>':
                    time.sleep(1)
                    continue
                cmd = cmd.replace('<CALLSIGN>', call).replace('<PASSWORD>', pwd)
                sock.sendall((cmd + '\r\n').encode())
                time.sleep(0.3)

            buf = ''
            sock.settimeout(1.0)
            while self._running:
                try:
                    data = sock.recv(2048).decode('utf-8', errors='replace')
                    if not data:
                        break
                    buf += data
                    while '\n' in buf:
                        line, buf = buf.split('\n', 1)
                        line = _ANSI_RE.sub('', line).strip()
                        if line:
                            self._on_spot(line)
                except socket.timeout:
                    pass
        except Exception as e:
            self._on_status(self.name, False, f'Fel: {e}')
        finally:
            self._connected = False
            self._on_status(self.name, False, f'Frånkopplad från {host}')
            try: sock.close()
            except Exception: pass


# ── Huvud-GUI ────────────────────────────────────────────────────────────────

DEFAULT_INIT = '<CALLSIGN>\n<PASSWORD>\nSH/DX 30'

class JTSpots(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title('JTSpots')
        self.geometry('720x820')
        self.resizable(True, True)
        ctk.set_appearance_mode('dark')
        ctk.set_default_color_theme('blue')

        self._freq_hz       = 0
        self._running       = False
        self._udp           = None
        self._telnet        = None
        self._rules         = []
        self._spot_count    = 0
        self._spot_log      = []   # [{call, freq_khz, snr, mode, line, source}]
        self._clublog       = ClublogClient()
        self._buffer        = SpotBuffer()
        self._engine        = RuleEngine(self._clublog)
        self._clusters      = []    # list of ClusterClient
        self._cluster_cfgs  = []    # list of dicts (sparade servrar)
        self._selected_idx  = None  # vald server i listan

        self._build_ui()
        self._load_settings()
        self.protocol('WM_DELETE_WINDOW', self._on_close)
        self.after(2000, self._tick)
        self.after(100, self._start)

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        p = {'padx': 10, 'pady': 4}

        # Statusrad
        top = ctk.CTkFrame(self)
        top.pack(fill='x', padx=10, pady=(8, 4))
        self._dot = ctk.CTkLabel(top, text='●', text_color='gray', font=('', 20))
        self._dot.pack(side='left', padx=(6, 2))
        self._lbl_status = ctk.CTkLabel(top, text='Startar...')
        self._lbl_status.pack(side='left', padx=4)
        self._lbl_clients = ctk.CTkLabel(top, text='', text_color='gray')
        self._lbl_clients.pack(side='left', padx=10)
        self._btn = ctk.CTkButton(top, text='Stoppa', width=90, command=self._toggle)
        self._btn.pack(side='right', padx=6)

        # Flikar
        tabs = ctk.CTkTabview(self)
        tabs.pack(fill='x', **p)
        for t in ('WSJT-X', 'DX Cluster', 'Clublog', 'Filter'):
            tabs.add(t)

        self._build_wsjtx_tab(tabs.tab('WSJT-X'))
        self._build_cluster_tab(tabs.tab('DX Cluster'))
        self._build_clublog_tab(tabs.tab('Clublog'))
        self._build_filter_tab(tabs.tab('Filter'))

        # Spotlogg (alltid synlig)
        lf = ctk.CTkFrame(self)
        lf.pack(fill='both', expand=True, **p)
        hdr = ctk.CTkFrame(lf, fg_color='transparent')
        hdr.pack(fill='x')
        ctk.CTkLabel(hdr, text='Spotlogg',
                     font=ctk.CTkFont(weight='bold')).pack(side='left', padx=8, pady=(6, 2))
        self._lbl_count = ctk.CTkLabel(hdr, text='0 spots', text_color='gray')
        self._lbl_count.pack(side='left', padx=4)
        ctk.CTkButton(hdr, text='Rensa', width=70,
                      command=self._clear_log).pack(side='right', padx=8, pady=4)

        log_tabs = ctk.CTkTabview(lf, height=260)
        log_tabs.pack(fill='both', expand=True, padx=4, pady=(0, 4))
        log_tabs.add('Alla')
        log_tabs.add('Filtrerat')

        self._log     = self._make_log_box(log_tabs.tab('Alla'))
        self._log_flt = self._make_log_box(log_tabs.tab('Filtrerat'))

    def _build_wsjtx_tab(self, tab):
        self._mk_label(tab, 'Multicast IP:', 0, 0)
        self._e_mcast = self._mk_entry(tab, DEFAULT_MCAST, 0, 1, 140)
        self._mk_label(tab, 'UDP-port:', 0, 2)
        self._e_uport = self._mk_entry(tab, str(DEFAULT_UPORT), 0, 3, 70)

        self._mk_label(tab, 'Telnet-port:', 1, 0)
        self._e_tport = self._mk_entry(tab, str(DEFAULT_TPORT), 1, 1, 70)
        self._mk_label(tab, 'Mitt callsign:', 1, 2)
        self._e_call = self._mk_entry(tab, 'SM5K', 1, 3, 100)

    def _build_cluster_tab(self, tab):
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=2)

        # Vänster — serverlista
        left = ctk.CTkFrame(tab)
        left.grid(row=0, column=0, sticky='nsew', padx=(0, 6), pady=4)

        ctk.CTkLabel(left, text='Sparade servrar',
                     font=ctk.CTkFont(weight='bold')).pack(anchor='w', padx=6, pady=(6, 2))

        self._cluster_list = ctk.CTkScrollableFrame(left, height=160)
        self._cluster_list.pack(fill='both', expand=True, padx=4)

        btns = ctk.CTkFrame(left, fg_color='transparent')
        btns.pack(fill='x', padx=4, pady=4)
        ctk.CTkButton(btns, text='+', width=36,
                      command=self._cluster_new).pack(side='left', padx=2)
        ctk.CTkButton(btns, text='−', width=36,
                      command=self._cluster_del).pack(side='left', padx=2)
        self._btn_connect = ctk.CTkButton(btns, text='Koppla', width=80,
                                          command=self._cluster_toggle)
        self._btn_connect.pack(side='right', padx=2)

        # Höger — formulär
        right = ctk.CTkFrame(tab)
        right.grid(row=0, column=1, sticky='nsew', pady=4)

        ctk.CTkLabel(right, text='Serverinformation',
                     font=ctk.CTkFont(weight='bold')).grid(
            row=0, column=0, columnspan=2, sticky='w', padx=8, pady=(6, 2))

        fields = [('Namn:', 'name', 1), ('Host:', 'host', 2),
                  ('Port:', 'port', 3), ('Callsign:', 'callsign', 4),
                  ('Lösenord:', 'password', 5)]
        self._cl_entries = {}
        for label, key, row in fields:
            self._mk_label(right, label, row, 0)
            show = '*' if key == 'password' else ''
            e = ctk.CTkEntry(right, width=180, show=show)
            e.grid(row=row, column=1, sticky='w', padx=4, pady=2)
            self._cl_entries[key] = e

        self._mk_label(right, 'Init-kommandon:', 6, 0)
        self._cl_init = ctk.CTkTextbox(right, width=180, height=80, font=('Courier', 11))
        self._cl_init.grid(row=6, column=1, sticky='w', padx=4, pady=2)
        self._cl_init.insert('end', DEFAULT_INIT)

        ctk.CTkButton(right, text='Spara server', width=120,
                      command=self._cluster_save).grid(
            row=7, column=1, sticky='e', padx=4, pady=6)

        self._refresh_cluster_list()

    def _build_clublog_tab(self, tab):
        self._mk_label(tab, 'Callsign:', 0, 0)
        self._e_cl_call = self._mk_entry(tab, 'SM5K', 0, 1, 160)

        self._mk_label(tab, 'E-post:', 1, 0)
        self._e_cl_email = self._mk_entry(tab, '', 1, 1, 200)

        self._mk_label(tab, 'Lösenord:', 2, 0)
        self._e_cl_pass = ctk.CTkEntry(tab, width=200, show='*')
        self._e_cl_pass.grid(row=2, column=1, sticky='w', padx=4, pady=3)

        self._mk_label(tab, 'API-nyckel:', 3, 0)
        self._e_cl_api = ctk.CTkEntry(tab, width=200, show='*')
        self._e_cl_api.grid(row=3, column=1, sticky='w', padx=4, pady=3)

        br = ctk.CTkFrame(tab, fg_color='transparent')
        br.grid(row=4, column=0, columnspan=3, sticky='w', padx=4, pady=(4, 6))
        ctk.CTkButton(br, text='Hämta matris', width=120,
                      command=self._fetch_clublog).pack(side='left', padx=4)
        self._lbl_cl_status = ctk.CTkLabel(br, text='Ingen matris hämtad',
                                            text_color='gray')
        self._lbl_cl_status.pack(side='left', padx=8)

    def _build_filter_tab(self, tab):
        tab.columnconfigure(0, weight=1)

        # Globala filter
        self._flt_cq = self._mk_chk(tab, 'Bara CQ-anrop (WSJT-X)', 0, 0, True)
        buf_row = ctk.CTkFrame(tab, fg_color='transparent')
        buf_row.grid(row=1, column=0, sticky='w', pady=2)
        self._flt_buf = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(buf_row, text='Ignore dupes (min):', variable=self._flt_buf).pack(side='left', padx=(8, 4))
        self._e_buf = ctk.CTkEntry(buf_row, width=55)
        self._e_buf.insert(0, '10')
        self._e_buf.pack(side='left', padx=4)
        ctk.CTkButton(buf_row, text='Rensa buffert', width=110,
                      command=self._buffer.clear).pack(side='left', padx=8)

        ctk.CTkFrame(tab, height=1, fg_color='gray30').grid(
            row=2, column=0, sticky='ew', padx=4, pady=8)

        # Regelrubrik
        rh = ctk.CTkFrame(tab, fg_color='transparent')
        rh.grid(row=3, column=0, sticky='ew', padx=4)
        ctk.CTkLabel(rh, text='Regler', font=ctk.CTkFont(weight='bold')).pack(side='left', padx=8)
        ctk.CTkButton(rh, text='+ Lägg till regel', width=130,
                      command=self._add_rule).pack(side='right', padx=8)

        # Regellist
        self._rule_frame = ctk.CTkScrollableFrame(tab, height=160)
        self._rule_frame.grid(row=4, column=0, sticky='ew', padx=4, pady=4)
        self._rule_frame.columnconfigure(1, weight=1)
        self._refresh_rule_list()

    def _refresh_rule_list(self):
        for w in self._rule_frame.winfo_children():
            w.destroy()
        for i, rule in enumerate(self._rules):
            var = ctk.BooleanVar(value=rule.get('enabled', True))
            def on_toggle(v=var, r=rule):
                r['enabled'] = v.get()
                self._rerender_filtered()
            ctk.CTkCheckBox(self._rule_frame, text='', variable=var, width=30,
                            command=on_toggle
                            ).grid(row=i, column=0, padx=(4, 0), pady=2)
            cond_summary = ', '.join(COND_TYPES.get(c['type'], c['type'])
                                     for c in rule.get('conditions', []))
            label = f"{rule.get('name','?')}  —  {cond_summary}" if cond_summary else rule.get('name', '?')
            ctk.CTkLabel(self._rule_frame, text=label, anchor='w').grid(
                row=i, column=1, sticky='w', padx=6, pady=2)
            ctk.CTkButton(self._rule_frame, text='Redigera', width=80,
                          command=lambda r=rule: self._open_rule_editor(r)).grid(
                row=i, column=2, padx=4, pady=2)
            ctk.CTkButton(self._rule_frame, text='Ta bort', width=75,
                          fg_color='#662222', hover_color='#882222',
                          command=lambda r=rule: self._delete_rule(r)).grid(
                row=i, column=3, padx=4, pady=2)

    def _add_rule(self):
        rule = {'id': str(time.monotonic()), 'name': 'Ny regel',
                'enabled': True, 'conditions': []}
        self._rules.append(rule)
        self._refresh_rule_list()
        self._open_rule_editor(rule)

    def _delete_rule(self, rule):
        if rule in self._rules:
            self._rules.remove(rule)
        self._refresh_rule_list()

    def _open_rule_editor(self, rule):
        dlg = ctk.CTkToplevel(self)
        dlg.title('Redigera regel')
        dlg.geometry('520x480')
        dlg.grab_set()

        # Namn
        nf = ctk.CTkFrame(dlg, fg_color='transparent')
        nf.pack(fill='x', padx=12, pady=8)
        ctk.CTkLabel(nf, text='Namn:', width=60).pack(side='left')
        e_name = ctk.CTkEntry(nf, width=320)
        e_name.insert(0, rule.get('name', ''))
        e_name.pack(side='left', padx=4)

        ctk.CTkLabel(dlg, text='Villkor  (AND — alla måste stämma):',
                     anchor='w').pack(fill='x', padx=12, pady=(4, 2))

        cond_frame = ctk.CTkScrollableFrame(dlg, height=240)
        cond_frame.pack(fill='both', expand=True, padx=12, pady=4)
        cond_frame.columnconfigure(1, weight=1)

        working = [dict(c) for c in rule.get('conditions', [])]
        entry_refs = []

        def refresh():
            nonlocal entry_refs
            entry_refs = []
            for w in cond_frame.winfo_children():
                w.destroy()
            for i, cond in enumerate(working):
                lbl = COND_TYPES.get(cond['type'], cond['type'])
                ctk.CTkLabel(cond_frame, text=lbl, anchor='w', width=210).grid(
                    row=i, column=0, sticky='w', padx=4, pady=3)
                if cond['type'] in ('band', 'mode', 'wanted_call', 'snr'):
                    e = ctk.CTkEntry(cond_frame, width=160)
                    e.insert(0, cond.get('value', ''))
                    e.grid(row=i, column=1, sticky='ew', padx=4, pady=3)
                    entry_refs.append((cond, e))
                else:
                    ctk.CTkLabel(cond_frame, text='').grid(row=i, column=1)
                    entry_refs.append((cond, None))
                ctk.CTkButton(cond_frame, text='✕', width=32,
                              fg_color='#662222', hover_color='#882222',
                              command=lambda c=cond: (working.remove(c), refresh())
                              ).grid(row=i, column=2, padx=4, pady=3)

        refresh()

        # Lägg till villkor
        add_row = ctk.CTkFrame(dlg, fg_color='transparent')
        add_row.pack(fill='x', padx=12, pady=4)
        cond_var = ctk.StringVar(value=list(COND_TYPES.values())[0])
        ctk.CTkOptionMenu(add_row, variable=cond_var,
                          values=list(COND_TYPES.values()), width=280).pack(side='left', padx=(0, 8))

        def add_cond():
            key = next(k for k, v in COND_TYPES.items() if v == cond_var.get())
            working.append({'type': key, 'value': ''})
            refresh()

        ctk.CTkButton(add_row, text='+ Lägg till', width=100, command=add_cond).pack(side='left')

        # Spara / Avbryt
        btn_row = ctk.CTkFrame(dlg, fg_color='transparent')
        btn_row.pack(fill='x', padx=12, pady=8)

        def save():
            for cond, entry in entry_refs:
                if entry is not None:
                    cond['value'] = entry.get()
            rule['name']       = e_name.get().strip() or 'Namnlös regel'
            rule['conditions'] = working
            self._refresh_rule_list()
            dlg.destroy()

        ctk.CTkButton(btn_row, text='Spara', width=100, command=save).pack(side='left', padx=(0, 8))
        ctk.CTkButton(btn_row, text='Avbryt', width=100,
                      fg_color='gray30', hover_color='gray40',
                      command=dlg.destroy).pack(side='left')

    def _make_log_box(self, parent):
        box = ctk.CTkTextbox(parent, font=('Courier', 11), state='disabled')
        box.pack(fill='both', expand=True, padx=4, pady=4)
        box._textbox.tag_config('cluster', foreground='#88ccff')
        box._textbox.tag_config('rule',    foreground='#ffcc00')
        return box

    # ── Hjälpwidgets ──────────────────────────────────────────────────────────

    def _mk_label(self, parent, text, row, col):
        ctk.CTkLabel(parent, text=text).grid(
            row=row, column=col, sticky='e', padx=(8, 4), pady=3)

    def _mk_entry(self, parent, default, row, col, width):
        e = ctk.CTkEntry(parent, width=width)
        e.insert(0, default)
        e.grid(row=row, column=col, sticky='w', padx=4, pady=3)
        return e

    def _mk_chk(self, parent, text, row, col, default):
        var = ctk.BooleanVar(value=default)
        ctk.CTkCheckBox(parent, text=text, variable=var).grid(
            row=row, column=col, sticky='w', padx=8, pady=4)
        return var

    # ── Start / Stop (WSJT-X + Telnet-server) ────────────────────────────────

    def _on_close(self):
        self._save_settings()
        self._stop()
        for c in self._clusters:
            c.disconnect()
        self.destroy()

    def _toggle(self):
        if self._running:
            self._stop()
        else:
            self._start()

    def _start(self):
        try:
            mcast = self._e_mcast.get().strip()
            uport = int(self._e_uport.get())
            tport = int(self._e_tport.get())
        except ValueError as e:
            self._log_line(f'FEL i inställningar: {e}')
            return

        self._telnet = SpotServer(tport)
        self._telnet.start()

        self._udp = UDPListener(mcast, uport, self._on_packet)
        self._udp.start()

        self._running = True
        self._dot.configure(text_color='#00cc44')
        self._lbl_status.configure(text='Aktiv')
        self._btn.configure(text='Stoppa')
        self._log_line(f'=== Startad — UDP {mcast}:{uport}  |  Telnet 127.0.0.1:{tport} ===')

    def _stop(self):
        if self._udp:    self._udp.stop()
        if self._telnet: self._telnet.stop()
        self._running = False
        self._dot.configure(text_color='gray')
        self._lbl_status.configure(text='Stoppad')
        self._lbl_clients.configure(text='')
        self._btn.configure(text='Starta')
        self._log_line('=== Stoppad ===')

    # ── DX Cluster-hantering ──────────────────────────────────────────────────

    def _refresh_cluster_list(self):
        for w in self._cluster_list.winfo_children():
            w.destroy()
        for i, cfg in enumerate(self._cluster_cfgs):
            connected = any(c.name == cfg['name'] and c.connected
                            for c in self._clusters)
            dot   = '●' if connected else '○'
            color = '#00cc44' if connected else 'gray'
            row   = ctk.CTkFrame(self._cluster_list, fg_color='transparent')
            row.pack(fill='x', pady=1)
            ctk.CTkLabel(row, text=dot, text_color=color, width=16).pack(side='left')
            ctk.CTkButton(row, text=cfg['name'], anchor='w',
                          fg_color='transparent', hover_color=('#3a3a3a', '#3a3a3a'),
                          command=lambda idx=i: self._cluster_select(idx)).pack(
                side='left', fill='x', expand=True)

    def _cluster_select(self, idx):
        self._selected_idx = idx
        cfg = self._cluster_cfgs[idx]
        for key, e in self._cl_entries.items():
            e.delete(0, 'end')
            e.insert(0, str(cfg.get(key, '')))
        self._cl_init.delete('1.0', 'end')
        self._cl_init.insert('end', cfg.get('init', DEFAULT_INIT))
        connected = any(c.name == cfg['name'] and c.connected for c in self._clusters)
        self._btn_connect.configure(text='Koppla ned' if connected else 'Koppla upp')

    def _cluster_new(self):
        self._cluster_cfgs.append({
            'name': 'Ny server', 'host': '', 'port': '7373',
            'callsign': self._e_call.get(), 'password': '',
            'init': DEFAULT_INIT,
        })
        self._refresh_cluster_list()
        self._cluster_select(len(self._cluster_cfgs) - 1)

    def _cluster_del(self):
        if self._selected_idx is None:
            return
        cfg = self._cluster_cfgs[self._selected_idx]
        for c in list(self._clusters):
            if c.name == cfg['name']:
                c.disconnect()
                self._clusters.remove(c)
        self._cluster_cfgs.pop(self._selected_idx)
        self._selected_idx = None
        self._refresh_cluster_list()

    def _cluster_save(self):
        cfg = {key: e.get() for key, e in self._cl_entries.items()}
        cfg['init'] = self._cl_init.get('1.0', 'end').strip()
        if self._selected_idx is None:
            self._cluster_cfgs.append(cfg)
        else:
            self._cluster_cfgs[self._selected_idx] = cfg
        self._refresh_cluster_list()

    def _cluster_toggle(self):
        if self._selected_idx is None:
            return
        cfg = self._cluster_cfgs[self._selected_idx]
        existing = next((c for c in self._clusters if c.name == cfg['name']), None)
        if existing and existing.connected:
            existing.disconnect()
            self._clusters.remove(existing)
            self._btn_connect.configure(text='Koppla upp')
        else:
            self._cluster_save()
            cfg = self._cluster_cfgs[self._selected_idx]
            client = ClusterClient(cfg, self._on_cluster_line, self._on_cluster_status)
            self._clusters.append(client)
            client.connect()
            self._btn_connect.configure(text='Koppla ned')

    def _on_cluster_status(self, name, connected, msg):
        self.after(0, lambda: self._log_line(f'[{name}] {msg}'))
        self.after(0, self._refresh_cluster_list)
        if self._selected_idx is not None:
            cfg = self._cluster_cfgs[self._selected_idx]
            if cfg.get('name') == name:
                self.after(0, lambda: self._btn_connect.configure(
                    text='Koppla ned' if connected else 'Koppla upp'))

    def _on_cluster_line(self, line):
        m = _SPOT_RE.match(line)
        if m:
            spotter, freq_str, call, comment, utc = m.groups()
            try:
                freq_khz = float(freq_str)
            except ValueError:
                return
            if self._flt_buf.get():
                try: self._buffer.minutes = float(self._e_buf.get())
                except ValueError: pass
                if self._buffer.is_duplicate(call, freq_to_band(freq_khz)):
                    return
            mode_cl = mode_from_freq(freq_khz)
            out = (f'DX de {spotter.rstrip(":")+":":<11}{freq_khz:>9.1f}  '
                   f'{call:<13} {comment:<20} {utc}')
            spot = {'call': call, 'freq_khz': freq_khz, 'snr': -99,
                    'mode': mode_cl, 'line': out, 'source': 'cluster'}
            self._spot_log.append(spot)
            if len(self._spot_log) > 1000:
                self._spot_log.pop(0)
            passed, rule_name = self._engine.evaluate(call, freq_khz, -99, mode_cl, self._rules)
            if passed and self._telnet:
                self._telnet.send_spot(out)
            self._spot_count += 1
            self.after(0, lambda l=out: self._log_line(l, tag='cluster'))
            if passed:
                suffix = f' [{rule_name}]' if rule_name else ''
                self.after(0, lambda l=out, s=suffix: self._append_to_box(
                    self._log_flt, l + s, tag='rule' if s else 'cluster'))
        else:
            self.after(0, lambda l=line: self._log_line(f'  {l}'))

    # ── Clublog ───────────────────────────────────────────────────────────────

    def _fetch_clublog(self):
        self._clublog.callsign = self._e_cl_call.get().strip()
        self._clublog.email    = self._e_cl_email.get().strip()
        self._clublog.password = self._e_cl_pass.get()
        self._clublog.api_key  = self._e_cl_api.get().strip()
        self._lbl_cl_status.configure(text='Hämtar...', text_color='gray')
        self._clublog.fetch_matrix(on_done=self._on_clublog_done)

    def _on_clublog_done(self, ok, msg):
        self.after(0, lambda: self._log_line(f'Clublog: {msg}'))
        if ok is None:
            return
        color = '#00cc44' if ok else '#cc4444'
        self.after(0, lambda: self._lbl_cl_status.configure(text=msg, text_color=color))

    # ── WSJT-X pakethantering ─────────────────────────────────────────────────

    def _on_packet(self, data):
        pkt = parse_packet(data)
        if not pkt:
            return
        if pkt['t'] == 'status':
            self._freq_hz = pkt['freq']
        elif pkt['t'] == 'decode':
            self._handle_decode(pkt)

    def _handle_decode(self, pkt):
        msg  = pkt.get('msg', '')
        snr  = pkt.get('snr', 0)
        mode = pkt.get('mode', '')

        callsign = extract_cq_call(msg)
        if self._flt_cq.get() and callsign is None:
            return
        if callsign is None:
            parts = msg.strip().split()
            callsign = parts[1] if len(parts) >= 2 else (parts[0] if parts else '')
        if not is_valid_callsign(callsign):
            return

        freq_khz = (self._freq_hz + pkt.get('df', 0)) / 1000.0

        if self._flt_buf.get():
            try: self._buffer.minutes = float(self._e_buf.get())
            except ValueError: pass
            if self._buffer.is_duplicate(callsign, freq_to_band(freq_khz)):
                return

        passed, rule_name = self._engine.evaluate(callsign, freq_khz, snr, mode, self._rules)
        if not passed:
            return

        self._emit_spot(callsign, freq_khz, snr, mode, rule_name)

    def _emit_spot(self, call, freq_khz, snr, mode, rule_name=''):
        if not mode or mode == '~':
            mode = mode_from_freq(freq_khz)
        de      = self._e_call.get().strip() or 'JTSpots'
        utc     = datetime.now(timezone.utc).strftime('%H%MZ')
        comment = f'{mode} {snr:+d}dB'
        line = (f'DX de {de + ":":<11}{freq_khz:>9.1f}  {call:<13} '
                f'{comment:<20} {utc}')
        spot = {'call': call, 'freq_khz': freq_khz, 'snr': snr,
                'mode': mode, 'line': line, 'source': 'wsjt'}
        self._spot_log.append(spot)
        if len(self._spot_log) > 1000:
            self._spot_log.pop(0)
        if self._telnet and rule_name:
            self._telnet.send_spot(line)
        self._spot_count += 1
        self.after(0, lambda l=line: self._log_line(l))
        if rule_name:
            suffix = f' [{rule_name}]'
            self.after(0, lambda l=line, s=suffix: self._append_to_box(self._log_flt, l + s, tag='rule'))

    # ── Logg + tick ───────────────────────────────────────────────────────────

    def _tick(self):
        if self._running and self._telnet:
            n = self._telnet.client_count
            self._lbl_clients.configure(
                text=f'{n} klient{"er" if n != 1 else ""}')
        self._lbl_count.configure(text=f'{self._spot_count} spots')
        self.after(2000, self._tick)

    def _append_to_box(self, box, text, tag=None):
        ts = datetime.now().strftime('%H:%M:%S')
        box.configure(state='normal')
        box.insert('end', f'{ts}  {text}\n')
        if tag:
            end = box._textbox.index('end-1c')
            box._textbox.tag_add(tag, f'{end} linestart', end)
        box.see('end')
        box.configure(state='disabled')

    def _log_line(self, text, tag=None):
        self._append_to_box(self._log, text, tag)

    def _rerender_filtered(self):
        self._log_flt.configure(state='normal')
        self._log_flt.delete('1.0', 'end')
        self._log_flt.configure(state='disabled')
        for s in self._spot_log:
            passed, rule_name = self._engine.evaluate(
                s['call'], s['freq_khz'], s['snr'], s['mode'], self._rules)
            if passed:
                suffix = f' [{rule_name}]' if rule_name else ''
                self._append_to_box(self._log_flt, s['line'] + suffix, tag=s['source'])

    def _clear_log(self):
        for box in (self._log, self._log_flt):
            box.configure(state='normal')
            box.delete('1.0', 'end')
            box.configure(state='disabled')
        self._spot_log.clear()
        self._spot_count = 0

    # ── Spara / ladda inställningar ───────────────────────────────────────────

    def _save_settings(self):
        data = {
            'mcast':       self._e_mcast.get(),
            'uport':       self._e_uport.get(),
            'tport':       self._e_tport.get(),
            'callsign':    self._e_call.get(),
            'cl_call':     self._e_cl_call.get(),
            'cl_email':    self._e_cl_email.get(),
            'cl_pass':     self._e_cl_pass.get(),
            'cl_api':      self._e_cl_api.get(),
            'flt_cq':      self._flt_cq.get(),
            'flt_buf':     self._flt_buf.get(),
            'buf_min':     self._e_buf.get(),
            'rules':       self._rules,
            'clusters':    self._cluster_cfgs,
        }
        try:
            SETTINGS_FILE.write_text(json.dumps(data, indent=2), encoding='utf-8')
        except Exception:
            pass

    def _load_settings(self):
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding='utf-8'))
        except Exception:
            return

        def se(e, key):
            if key in data:
                e.delete(0, 'end')
                e.insert(0, data[key])

        se(self._e_mcast,    'mcast')
        se(self._e_uport,    'uport')
        se(self._e_tport,    'tport')
        se(self._e_call,     'callsign')
        se(self._e_cl_call,  'cl_call')
        se(self._e_cl_email, 'cl_email')
        se(self._e_cl_pass,  'cl_pass')
        se(self._e_cl_api,   'cl_api')
        if 'flt_cq'  in data: self._flt_cq.set(data['flt_cq'])
        if 'flt_buf' in data: self._flt_buf.set(data['flt_buf'])
        se(self._e_buf, 'buf_min')
        if 'rules' in data:
            self._rules = data['rules']
            self._refresh_rule_list()
        if 'clusters'    in data:
            self._cluster_cfgs = data['clusters']
            self._refresh_cluster_list()


# ── Start ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    app = JTSpots()
    app.mainloop()
