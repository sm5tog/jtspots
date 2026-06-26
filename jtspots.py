#!/usr/bin/env python3
"""JTWatch — JTAlert/WSJT-X → Log4OM DX-spot bridge."""

import struct
import socket
import threading
import re
from datetime import datetime, timezone
import customtkinter as ctk

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
        s.u32()             # schema
        mtype = s.u32()
        cid   = s.str_()

        if mtype == MSG_STATUS:
            freq = s.u64()  # dial frequency Hz
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


# ── FT8/FT4 CQ parser ───────────────────────────────────────────────────────

_CQ_RE = re.compile(
    r'^CQ(?:\s+(?:DX|[A-Z]{2,3}))?\s+([A-Z0-9/]+)\s+[A-R]{2}[0-9]{2}',
    re.IGNORECASE
)

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


def extract_cq_call(msg: str):
    m = _CQ_RE.match(msg.strip())
    return m.group(1).upper() if m else None


# ── Telnet DX-cluster server ─────────────────────────────────────────────────

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
                conn.sendall(b'JTWatch DX Cluster\r\n')
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


# ── UDP multicast listener ───────────────────────────────────────────────────

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
        mreq = struct.pack('4sL', socket.inet_aton(self._group),
                           socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(1.0)
        while self._running:
            try:
                data, _ = sock.recvfrom(4096)
                self._callback(data)
            except socket.timeout:
                pass
        sock.close()


# ── Main GUI ─────────────────────────────────────────────────────────────────

class JTWatch(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title('JTWatch')
        self.geometry('640x680')
        self.resizable(True, True)
        ctk.set_appearance_mode('dark')
        ctk.set_default_color_theme('blue')

        self._freq_hz = 0
        self._running = False
        self._udp     = None
        self._telnet  = None
        self._spot_count = 0

        self._build_ui()
        self.after(2000, self._tick)

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self):
        p = {'padx': 10, 'pady': 5}

        # Status bar
        top = ctk.CTkFrame(self)
        top.pack(fill='x', **p)
        self._dot = ctk.CTkLabel(top, text='●', text_color='gray', font=('', 20))
        self._dot.pack(side='left', padx=(6, 2))
        self._lbl_status = ctk.CTkLabel(top, text='Stoppad')
        self._lbl_status.pack(side='left', padx=4)
        self._lbl_clients = ctk.CTkLabel(top, text='', text_color='gray')
        self._lbl_clients.pack(side='left', padx=10)
        self._btn = ctk.CTkButton(top, text='Starta', width=90, command=self._toggle)
        self._btn.pack(side='right', padx=6)

        # Settings
        sf = ctk.CTkFrame(self)
        sf.pack(fill='x', **p)
        ctk.CTkLabel(sf, text='Inställningar',
                     font=ctk.CTkFont(weight='bold')).grid(
            row=0, column=0, columnspan=4, sticky='w', padx=8, pady=(6, 2))

        self._mk_label(sf, 'Multicast IP:', 1, 0)
        self._e_mcast = self._mk_entry(sf, DEFAULT_MCAST, 1, 1, 140)

        self._mk_label(sf, 'UDP-port:', 1, 2)
        self._e_uport = self._mk_entry(sf, str(DEFAULT_UPORT), 1, 3, 70)

        self._mk_label(sf, 'Telnet-port:', 2, 0)
        self._e_tport = self._mk_entry(sf, str(DEFAULT_TPORT), 2, 1, 70)

        self._mk_label(sf, 'Mitt callsign:', 2, 2)
        self._e_call = self._mk_entry(sf, 'SM5TOG', 2, 3, 100)

        # Filters
        ff = ctk.CTkFrame(self)
        ff.pack(fill='x', **p)
        ctk.CTkLabel(ff, text='Filter',
                     font=ctk.CTkFont(weight='bold')).grid(
            row=0, column=0, columnspan=4, sticky='w', padx=8, pady=(6, 2))

        self._flt_cq  = self._mk_chk(ff, 'Bara CQ-anrop', 1, 0, True)
        self._flt_snr = self._mk_chk(ff, 'Min SNR (dB):', 2, 0, False)
        self._e_snr   = self._mk_entry(ff, '-15', 2, 1, 55)

        ctk.CTkLabel(ff,
                     text='Tips: aktivera JTAlert-flaggor (ny DXCC/band) via JTAlert → Network → UDP',
                     text_color='gray', font=('', 11)).grid(
            row=3, column=0, columnspan=4, sticky='w', padx=8, pady=(2, 6))

        # Spot log
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

        self._log = ctk.CTkTextbox(lf, font=('Courier', 11), state='disabled')
        self._log.pack(fill='both', expand=True, padx=8, pady=(0, 8))

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
            row=row, column=col, sticky='w', padx=8, pady=3)
        return var

    # ── Start / Stop ─────────────────────────────────────────────────────────

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

    # ── Packet handling ───────────────────────────────────────────────────────

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
            callsign = parts[1] if len(parts) >= 2 else (parts[0] if parts else '?')

        if self._flt_snr.get():
            try:
                if snr < int(self._e_snr.get()):
                    return
            except ValueError:
                pass

        freq_khz = (self._freq_hz + pkt.get('df', 0)) / 1000.0
        self._emit_spot(callsign, freq_khz, snr, mode)

    def _emit_spot(self, call, freq_khz, snr, mode):
        if not mode or mode == '~':
            mode = mode_from_freq(freq_khz)
        de   = self._e_call.get().strip() or 'JTWatch'
        utc  = datetime.now(timezone.utc).strftime('%H%MZ')
        line = (f'DX de {de + ":":<11}{freq_khz:>9.1f}  {call:<13} '
                f'{mode} {snr:+d}dB                {utc}')
        self._telnet.send_spot(line)
        self._spot_count += 1
        self.after(0, self._log_line, line)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _tick(self):
        if self._running and self._telnet:
            n = self._telnet.client_count
            self._lbl_clients.configure(
                text=f'{n} klient{"er" if n != 1 else ""}')
        self._lbl_count.configure(text=f'{self._spot_count} spots')
        self.after(2000, self._tick)

    def _log_line(self, text):
        ts = datetime.now().strftime('%H:%M:%S')
        self._log.configure(state='normal')
        self._log.insert('end', f'{ts}  {text}\n')
        self._log.see('end')
        self._log.configure(state='disabled')

    def _clear_log(self):
        self._log.configure(state='normal')
        self._log.delete('1.0', 'end')
        self._log.configure(state='disabled')
        self._spot_count = 0


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    app = JTWatch()
    app.mainloop()
