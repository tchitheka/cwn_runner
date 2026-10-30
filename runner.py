#!/usr/bin/env python3
"""Phase Three trial runner.

Runs on the gateway. Serves the controller page, drives the fault scripts, takes
independent probe readings (never from the dashboards under test) and keeps every
event and trial in SQLite. Standard library only (Python 3.10+).

    sudo python3 runner.py                      # real testbed
    python3 runner.py --simulate --speed 10     # dry run with a fake testbed
"""
import argparse, csv, hmac, io, json, math, os, random, re, secrets, shlex, signal, sqlite3, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode
import urllib.request

VERSION = 'runner-1.1'
HERE = os.path.dirname(os.path.abspath(__file__))

ap = argparse.ArgumentParser()
ap.add_argument('--host', default='0.0.0.0')
ap.add_argument('--port', type=int, default=8080)
ap.add_argument('--data-dir', default=os.path.join(HERE, 'data'))
ap.add_argument('--config', default=os.path.join(HERE, 'scenarios.json'))
ap.add_argument('--fault-dir', default='')
ap.add_argument('--token', default=os.environ.get('CWN_TOKEN', ''))
ap.add_argument('--simulate', action='store_true', help='fake testbed, no commands are executed')
ap.add_argument('--speed', type=float, default=1.0, help='simulation time multiplier (only with --simulate)')
ARGS = ap.parse_args()
SIM = ARGS.simulate
SPEED = ARGS.speed if SIM else 1.0

CFG = json.load(open(ARGS.config))
import hashlib
CFG_HASH = hashlib.sha1(open(ARGS.config, 'rb').read()).hexdigest()[:8]   # changes whenever scenarios.json changes
RC = CFG['runner']
SC = {s['id']: s for s in CFG['scenarios']}
REAL_IDS = [s['id'] for s in CFG['scenarios'] if s['id'] != 'CATCH']
FAULT_DIR = os.path.expanduser(ARGS.fault_dir or os.environ.get('CWN_FAULT_DIR', '') or RC['fault_dir'])
os.makedirs(ARGS.data_dir, exist_ok=True)
os.makedirs(os.path.join(ARGS.data_dir, 'logs'), exist_ok=True)
WAN = RC['wan_iface']


def sub(t):
    return t.replace('{wan_iface}', WAN) if isinstance(t, str) else t


RC['safety_clear'] = [sub(c) for c in RC['safety_clear']]
for _s in SC.values():
    _s['runner']['stop_cmd'] = sub(_s['runner'].get('stop_cmd', ''))
PD = {}
for k, v in RC['probe_defs'].items():
    PD[k] = {kk: sub(vv) for kk, vv in v.items()}
    PD[k]['name'] = k

LOCK = threading.RLock()
T0 = time.time(); M0 = time.monotonic()


def clk():
    return T0 + (time.time() - T0) * SPEED


def stamp():
    return {'w': clk(), 'm': M0 + (time.monotonic() - M0) * SPEED}


def iso(s):
    return datetime.fromtimestamp(s['w'], timezone.utc).isoformat(timespec='milliseconds') if s else ''


def between(a, b):
    return b['m'] - a['m']


def since(s):
    return stamp()['m'] - s['m']


# ------------------------------------------------------------------ storage
DB = sqlite3.connect(os.path.join(ARGS.data_dir, 'trials.db'), check_same_thread=False)
DB.executescript('''
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, session TEXT, pid TEXT, trial TEXT, scenario TEXT, event TEXT, win TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS trials(trial_id TEXT PRIMARY KEY, seq INTEGER, j TEXT);
CREATE TABLE IF NOT EXISTS samples(trial_id TEXT, t_ms INTEGER, rel_s REAL, state TEXT, probe TEXT, value REAL);
''')


def fresh():
    return {'settings': {'include': {}, 'window': {}, 'recovery': {}, 'catchN': CFG['defaults']['catch_trials'],
                         'baseline_s': CFG['defaults']['baseline_s'], 'blur': False,
                         'early_end': False, 'early_grace_s': 5, 'early_recovery': False, 'settle_s': 15,
                         'dashboard_version': {'A': '', 'B': ''}},
            'session': {'pid': '', 'condition': 'A', 'sessionId': '', 'seed': '', 'order': [], 'observer': ''},
            'cur': None, 'idle': 0, 'safety': ''}


def load_state():
    r = DB.execute("SELECT v FROM kv WHERE k='state'").fetchone()
    s = fresh()
    if r:
        d = json.loads(r[0])
        s['settings'].update(d.get('settings', {})); s['session'].update(d.get('session', {}))
        s['cur'] = d.get('cur'); s['idle'] = d.get('idle', 0)
    return s


ST = load_state()
SAMPLES = []          # in-memory samples of the current trial: (m, rel, state, probe, value)
PROCS = {}


def save_state():
    DB.execute("INSERT OR REPLACE INTO kv VALUES('state',?)", (json.dumps({k: ST[k] for k in ('settings', 'session', 'cur', 'idle')}),))
    DB.commit()


def log_event(ev, detail='', win=None):
    c = ST['cur']
    DB.execute('INSERT INTO events(ts,session,pid,trial,scenario,event,win,detail) VALUES(?,?,?,?,?,?,?,?)',
               (iso(stamp()), ST['session']['sessionId'], ST['session']['pid'], c['trialId'] if c else '',
                c['scenarioId'] if c else '', ev, win or (c['state'] if c else 'IDLE'), str(detail)))
    save_state()


# ------------------------------------------------------------------ command execution
SSH = 'ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 -o ControlMaster=auto -o ControlPath=/tmp/cwn-ssh-%C -o ControlPersist=120'


def sh(cmd, timeout=60, cwd=None):
    try:
        p = subprocess.run(['bash', '-c', cmd], cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr)
    except subprocess.TimeoutExpired:
        return 124, 'timeout'
    except Exception as e:
        return 125, str(e)


def on(host, remote, timeout=8):
    target = RC['hosts'].get(host)
    if target is None:
        return sh(remote, timeout)
    return sh('%s %s %s' % (SSH, shlex.quote(target), shlex.quote(remote)), timeout)


# ------------------------------------------------------------------ simulation
class Sim:
    def __init__(self):
        self.fault = None; self.t = 0; self.rng = random.Random(7); self.flaps = 100

    def launch(self, sid):
        self.fault = sid; self.t = clk()

    def stop(self):
        self.fault = None

    def probe(self, spec):
        n = spec['name']; f = self.fault; el = clk() - self.t if f else -1; r = self.rng
        if n == 'ap2_cpu': return 96 + r.random() if f == 'CPU' and el > 4 else 8 + r.random() * 3
        if n == 'ap1_cpu': return 5 + r.random() * 2
        if n == 'ap1_mem_used': return 40 + r.random()
        if n == 'ap1_reach': return 0.0
        if n == 'ap2_mem_used': return 92 if f == 'MEMORY' and el > 4 else 45 + r.random()
        if n == 'gw_rtt': return 224 + r.random() * 4 if f == 'LATENCY' and el > 2 else 24 + r.random() * 2
        if n == 'gw_loss': return r.choice([10, 20, 20, 10, 0]) if f in ('WAN-LOSS', 'LOSS-JITTER') and el > 2 else 0.0
        if n == 'netem': return 1.0 if f in ('WAN-LOSS', 'LATENCY', 'LOSS-JITTER') and el > 1 else 0.0
        if n == 'memfill_pid': return 1.0 if f == 'MEMORY' and el > 2 else 0.0
        if n == 'gw_jitter': return 40 + r.random() * 5 if f == 'LOSS-JITTER' and el > 2 else 1 + r.random()
        if n == 'dns_pid': return 1.0 if f == 'DNS-FAIL' and el > 2 else 0.0
        if n == 'flap_pid': return 1.0 if f == 'WIFI-FLAP' and 2 < el < 84 else 0.0
        if n == 'cpu_pid': return 1.0 if f == 'CPU' and el > 2 else 0.0
        if n == 'ap2_carrier': return 100.0 + (int(min(el, 84) / 7) * 2 if f == 'WIFI-FLAP' and el > 2 else 0)
        if n == 'dns_ms': return 800.0 if f == 'DNS-FAIL' and el > 2 else 20 + r.random()
        if n == 'bwsat_pid': return 1.0 if f == 'BW-SAT' and el > 2 else 0.0
        if n in ('bw_up_mbps', 'bw_down_mbps'): return min(20.0, 0.12 * max(0, el)) + r.random() * 0.2 if f == 'BW-SAT' and n == 'bw_up_mbps' else r.random() * 0.2
        if n == 'qoe_rtt_p90': return 224 + r.random() * 6 if f == 'LATENCY' and el > 10 else 24 + r.random() * 2
        if n == 'qoe_loss_pct': return 15.0 if f == 'WAN-LOSS' and el > 8 else 0.0
        if n == 'dns_fail_pct': return min(60.0, 3.0 * (el - 8)) + r.random() if f == 'DNS-FAIL' and el > 8 else r.random() * 0.3
        if n == 'ap2_reach': return 100.0 if f == 'AVAIL' and 2 < el < 40 else 0.0
        if n == 'apoutage_pid': return 1.0 if f == 'AVAIL' and 2 < el < 45 else 0.0
        if n.startswith('d_'):
            out = f == 'AVAIL' and 2 < el < 40
            if n == 'd_clients': return 1.0
            if n == 'd_rtt_p90': return 224 + r.random() * 6 if f == 'LATENCY' and el > 10 else 24 + r.random() * 2
            if n == 'd_jitter': return 40 + r.random() * 5 if f == 'LOSS-JITTER' and el > 8 else 1 + r.random()
            if n == 'd_inet_loss': return 15.0 if f in ('WAN-LOSS', 'LOSS-JITTER') and el > 8 else 0.0
            if n == 'd_dns_fail': return min(60.0, 3.0 * (el - 8)) + r.random() if f == 'DNS-FAIL' and el > 8 else r.random() * 0.3
            if n == 'd_up_mbps': return min(20.0, 0.3 * max(0, el)) if f == 'BW-SAT' else r.random() * 0.2
            if n == 'd_down_mbps': return r.random() * 0.2
            if n == 'd_data_24h_gb': return 3.2
            if n == 'd_chan_util': return 2 + r.random()
            if n == 'd_mesh_snr': return None if out else 38 + r.random() * 2
            if n == 'd_mesh_rssi': return None if out else -55 + r.random() * 2
            if n == 'd_mesh_loss': return 100.0 if out else 0.0
            if n == 'd_ap1_up': return 1.0
            if n == 'd_ap2_up': return 0.0 if out else 1.0
            if n == 'd_ap1_cpu': return 5 + r.random() * 2
            if n == 'd_ap2_cpu': return 90 if f == 'CPU' and el > 60 else 8 + r.random() * 3
            if n == 'd_ap1_mem': return 40 + r.random()
            if n == 'd_ap2_mem': return 92 if f == 'MEMORY' and el > 4 else 45 + r.random()
            if n in ('d_ap1_wifi_loss', 'd_ap2_wifi_loss'): return 0.0
            if n == 'd_ap1_flaps': return 0.0
            if n == 'd_ap2_flaps': return 2.0 if f == 'WIFI-FLAP' and 2 < el < 84 else 0.0
            if n == 'd_ap1_min_snr': return 34 + r.random()
            if n == 'd_ap2_min_snr': return None
        return None


SIMW = Sim() if SIM else None

# ------------------------------------------------------------------ probes
_ping_cache = {}


def ping_stats(host, target, count):
    key = (host, target, count)
    now = time.time()
    with LOCK:
        hit = _ping_cache.get(key)
        if hit and now - hit[0] < 1.5:
            return hit[1]
    rc, out = on(host, 'ping -c %d -i 0.2 -W 1 %s' % (count, shlex.quote(target)), 6 + count * 0.3)
    loss = re.search(r'([\d.]+)% packet loss', out)
    rtt = re.search(r'= [\d.]+/([\d.]+)/[\d.]+/([\d.]+)', out)
    res = {'loss': float(loss.group(1)) if loss else None, 'rtt': float(rtt.group(1)) if rtt else None,
           'jitter': float(rtt.group(2)) if rtt else None}
    with LOCK:
        _ping_cache[key] = (now, res)
    return res


def probe(spec):
    try:
        if SIM:
            return SIMW.probe(spec)
        k = spec['kind']
        if k == 'cpu_busy':
            rc, out = on(spec['host'], 'head -1 /proc/stat; sleep 1; head -1 /proc/stat', 8)
            ls = [l.split() for l in out.splitlines() if l.startswith('cpu')][:2]
            a, b = [list(map(int, l[1:])) for l in ls]
            idle = lambda x: x[3] + (x[4] if len(x) > 4 else 0)
            return 100.0 * (1 - (idle(b) - idle(a)) / max(1, sum(b) - sum(a)))
        if k == 'mem_used':
            rc, out = on(spec['host'], 'grep -E "^(MemTotal|MemAvailable|MemFree|Buffers|Cached):" /proc/meminfo', 6)
            m = {l.split(':')[0]: float(l.split()[1]) for l in out.splitlines() if ':' in l}
            avail = m.get('MemAvailable', m.get('MemFree', 0) + m.get('Buffers', 0) + m.get('Cached', 0))
            return 100.0 * (1 - avail / m['MemTotal'])
        if k == 'ping':
            return ping_stats(spec['host'], spec['target'], spec['count'])[spec['field']]
        if k == 'netem':
            rc, out = sh('tc qdisc show dev %s' % shlex.quote(spec['iface']), 5)
            return 1.0 if rc == 0 and 'netem' in out else (0.0 if rc == 0 else None)
        if k == 'pid_alive':
            rc, out = on(spec['host'], 'kill -0 $(cat %s 2>/dev/null) 2>/dev/null && echo 1 || echo 0' % shlex.quote(spec['path']), 6)
            return float(out.strip().splitlines()[-1]) if rc == 0 and out.strip() else None
        if k == 'carrier':
            i = shlex.quote(spec['iface'])
            rc, out = on(spec['host'], 'cat /sys/class/net/%s/carrier_down_count 2>/dev/null || cat /sys/class/net/%s/carrier_changes' % (i, i), 6)
            return float(out.strip().splitlines()[-1]) if rc == 0 and out.strip() else None
        if k == 'prom':
            url = RC.get('prometheus_url', 'http://localhost:9090').rstrip('/') + '/api/v1/query?' + urlencode({'query': spec['query']})
            with urllib.request.urlopen(url, timeout=3) as r:
                res = json.load(r)['data']['result']
            vals = [float(x['value'][1]) for x in res if x['value'][1] not in ('NaN', '+Inf', '-Inf')]
            if not vals:
                return None
            agg = {'min': min, 'max': max, 'sum': sum, 'avg': lambda v: sum(v) / len(v)}[spec.get('agg', 'max')]
            return agg(vals) * spec.get('scale', 1)
        if k == 'dns_ms':
            rc, out = sh('dig +tries=1 +time=2 +noall +stats %s.example.com' % secrets.token_hex(4), 5)
            m = re.search(r'Query time: (\d+) msec', out)
            return float(m.group(1)) if m else None
    except Exception:
        return None
    return None


POOL = ThreadPoolExecutor(32)


def probes_for(sid):
    names = list(RC['common_probes'])
    for n in SC[sid]['runner'].get('extra', []):
        if n not in names:
            names.append(n)
    return names


def sample(names):
    futs = {n: POOL.submit(probe, PD[n]) for n in names}
    out = {}
    for n, f in futs.items():
        try:
            out[n] = f.result(timeout=12)
        except Exception:
            out[n] = None
    return out


# ------------------------------------------------------------------ helpers
def eff_window(i): return int(ST['settings']['window'].get(i, SC[i]['window_s']))
def eff_rec(i): return int(ST['settings']['recovery'].get(i, SC[i]['min_recovery_s']))
def included(i): return ST['settings']['include'].get(i, True) is not False


def gen_order():
    s = ST['session']; rng = random.Random(s['seed'] or s['pid'] or 'seed')
    ids = [i for i in REAL_IDS if included(i)]
    last = [i for i in ids if SC[i].get('order') == 'last']
    free = [i for i in ids if SC[i].get('order') != 'last']
    rng.shuffle(free)
    for _ in range(max(0, int(ST['settings']['catchN']))):
        if len(free) < 2: break
        for _t in range(40):
            p = 1 + rng.randrange(len(free))
            if free[p - 1] != 'CATCH' and (p >= len(free) or free[p] != 'CATCH'):
                break
        free.insert(p, 'CATCH')
    return free + last


def n_done():
    return sum(1 for r in DB.execute('SELECT j FROM trials') if json.loads(r[0])['session'] == ST['session']['sessionId'])


def next_planned():
    o = ST['session']['order']; d = n_done()
    return o[d] if d < len(o) else ''


def mean(v):
    v = [x for x in v if x is not None]
    return sum(v) / len(v) if v else None


def rule_ok(rule, val, base):
    if val is None or rule is None: return False
    ref = rule['value']
    if rule.get('mode') == 'delta':
        b = base.get(rule['probe'])
        if b is None: return False
        ref += b
    return {'gte': val >= ref, 'lte': val <= ref, 'eq': abs(val - ref) < 1e-9}[rule['op']]


def window_tag(c):
    if not c: return 'idle'
    if c['state'] == 'ACTIVE': return 'catch_active' if c['scenarioId'] == 'CATCH' else 'active'
    return {'BASELINE': 'baseline', 'STARTING': 'pre_onset', 'CLEANUP': 'post_window', 'RECOVERY': 'recovery',
            'FINALISING': 'finalising', 'ERROR': 'error'}.get(c['state'], 'idle')


def baseline_means():
    by = {}
    for (m, rel, st, n, v) in SAMPLES:
        if st == 'BASELINE' and v is not None: by.setdefault(n, []).append(v)
    return {n: mean(v) for n, v in by.items()}


def summarise(c):
    sc = SC[c['scenarioId']]; out = {}
    names = probes_for(c['scenarioId'])
    for n in names:
        act = [(m, v) for (m, rel, st, nn, v) in SAMPLES if nn == n and st == 'ACTIVE' and v is not None]
        post = [v for (m, rel, st, nn, v) in SAMPLES if nn == n and st == 'RECOVERY' and v is not None]
        end = [v for _, v in act[-3:]]
        out[n] = {'baseline': c['baseline'].get(n), 'end_window': mean(end),
                  'peak': (max(v for _, v in act) if act else None) if sc['runner']['gt_dir'] == 'up' else (min(v for _, v in act) if act else None),
                  'post': mean(post[-3:]), 'n_active': len(act)}
    return out


# ------------------------------------------------------------------ fault control (worker threads)
def do_launch(tid, cmd, minutes):
    log = open(os.path.join(ARGS.data_dir, 'logs', tid + '.log'), 'ab')
    if SIM:
        with LOCK:
            c = ST['cur']
            if c and c['trialId'] == tid: SIMW.launch(c['scenarioId'])
        return
    try:
        p = subprocess.Popen(['bash', '-c', cmd], cwd=FAULT_DIR, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        PROCS[tid] = p
    except Exception as e:
        with LOCK:
            c = ST['cur']
            if c and c['trialId'] == tid: c['launch_error'] = str(e); save_state()
        return
    time.sleep(2.5)
    rc = p.poll()
    if rc not in (None, 0):
        with LOCK:
            c = ST['cur']
            if c and c['trialId'] == tid: c['launch_error'] = 'launch script exited with code %s' % rc; save_state()


def do_stop(tid, cmd):
    if SIM:
        SIMW.stop()
        with LOCK:
            c = ST['cur']
            if c and c['trialId'] == tid: c['stop_done'] = True
        return
    p = PROCS.pop(tid, None)
    if p and p.poll() is None:
        try: os.killpg(p.pid, signal.SIGTERM)
        except Exception: pass
        try: p.wait(8)
        except Exception:
            try: os.killpg(p.pid, signal.SIGKILL)
            except Exception: pass
    rc, out = sh(cmd, 70, FAULT_DIR) if cmd else (0, '')
    with LOCK:
        c = ST['cur']
        if c and c['trialId'] == tid:
            c['stop_done'] = True; c['stop_rc'] = rc; c['stop_out'] = out[-300:]; save_state()


def start_stop(c):
    c['stop_done'] = False; c['stop_started'] = stamp(); c['stop_n'] = c.get('stop_n', 0) + 1
    log_event('stop_issued', 'attempt %d' % c['stop_n'])
    threading.Thread(target=do_stop, args=(c['trialId'], SC[c['scenarioId']]['runner'].get('stop_cmd', '')), daemon=True).start()


def safety_clear(reason):
    if SIM:
        SIMW.stop(); ST['safety'] = 'simulated clear (%s)' % reason; return
    res = []
    for cmd in RC['safety_clear']:
        rc, out = sh(cmd, 40, FAULT_DIR); res.append('%s -> %s' % (cmd, rc))
    ST['safety'] = '%s: %s' % (reason, '; '.join(res))
    with LOCK:
        log_event('safety_clear', ST['safety'], 'idle')


# ------------------------------------------------------------------ state machine
def to_error(c, why):
    c['problem'] = why; c['state'] = 'ERROR'; c['aborted'] = True; c['abortReason'] = 'error: ' + why; c['streak'] = 0; c['first'] = None
    log_event('error', why)
    if c['mode'] == 'auto': start_stop(c)
    save_state()


def advance(c):
    """Called every tick with LOCK held, after new samples were recorded."""
    sc = SC[c['scenarioId']]; r = sc['runner']; st = c['state']; auto = c['mode'] == 'auto'
    if c.get('launch_error') and st in ('STARTING', 'ACTIVE'):
        to_error(c, c.pop('launch_error')); return
    latest = c['latest']
    if st == 'BASELINE' and auto and since(c['t']['prep']) >= ST['settings']['baseline_s'] and SAMPLES:
        c['baseline'] = baseline_means()
        if c['scenarioId'] == 'CATCH':
            c['t']['onset'] = log_stamp('catch_window_started'); c['onset_method'] = 'catch_window'; c['state'] = 'ACTIVE'
        else:
            cmd = r['launch_cmd'].format(minutes=int(math.ceil((c['windowS'] + 60) / 60.0)), wan_iface=WAN)
            c['launch_cmd'] = cmd; c['t']['command'] = log_stamp('fault_command_issued', cmd)
            c['state'] = 'STARTING'; c['streak'] = 0; c['first'] = None
            threading.Thread(target=do_launch, args=(c['trialId'], cmd, 0), daemon=True).start()
        save_state(); return
    if st == 'STARTING':
        rule = r.get('onset')
        if auto and rule:
            ok = rule_ok(rule, latest.get(rule['probe']), c['baseline'])
            if ok:
                if c['streak'] == 0: c['first'] = c['last_sample_stamp']
                c['streak'] += 1
            else:
                c['streak'] = 0; c['first'] = None
            if c['streak'] >= rule['consec']:
                c['t']['onset'] = c['first']; c['onset_method'] = 'probe'; log_event('fault_verified_onset', 'probe %s = %s' % (rule['probe'], latest.get(rule['probe'])))
                c['state'] = 'ACTIVE'; save_state(); return
            if since(c['t']['command']) > RC['onset_timeout_s']:
                to_error(c, 'fault onset not verified within %d s' % RC['onset_timeout_s'])
        return
    if st == 'ACTIVE':
        S = ST['settings']
        if (S.get('early_end') and c['detected'] and c['scenarioId'] != 'CATCH' and c['t'].get('detect')
                and since(c['t']['detect']) >= S.get('early_grace_s', 5) and since(c['t']['onset']) < c['windowS']):
            end_window(c, 'early: participant reported, grace %ds elapsed' % S.get('early_grace_s', 5), True)
        elif since(c['t']['onset']) >= c['windowS']:
            end_window(c, 'timer', False)
        return
    if st in ('CLEANUP', 'ERROR') and auto:
        rule = r.get('cleared')
        if c['scenarioId'] == 'CATCH' or not rule:
            c['cleared_ok'] = True
            if st == 'CLEANUP':
                c['t']['clear'] = log_stamp('fault_cleared_verified', 'no fault to clear'); c['state'] = 'RECOVERY'; save_state()
            return
        if not c.get('stop_done'): return
        ok = rule_ok(rule, latest.get(rule['probe']), c['baseline'])
        if ok:
            if c['streak'] == 0: c['first'] = c['last_sample_stamp']
            c['streak'] += 1
        else:
            c['streak'] = 0; c['first'] = None
        if c['streak'] >= rule['consec']:
            c['cleared_ok'] = True
            if st == 'CLEANUP':
                c['t']['clear'] = c['first']; log_event('fault_cleared_verified', 'probe %s = %s' % (rule['probe'], latest.get(rule['probe'])))
                c['state'] = 'RECOVERY'; save_state()
            return
        waited = since(c['stop_started'])
        if waited > RC['clear_timeout_s'] and c.get('stop_n', 1) < 2:
            start_stop(c)
        elif waited > RC['clear_timeout_s'] and st == 'CLEANUP':
            to_error(c, 'fault not verified as cleared')
        return
    if st == 'RECOVERY' and auto:
        S = ST['settings']
        waited = since(c['t']['clear'])
        if waited >= c['minRecS'] or (S.get('early_recovery') and waited >= S.get('settle_s', 15)) or c.get('skip_recovery'):
            rule = r.get('cleared')
            if c['scenarioId'] == 'CATCH' or not rule or rule_ok(rule, latest.get(rule['probe']), c['baseline']):
                finish_recovery(c)
        return


def end_window(c, how, early):
    c['t']['windowEnd'] = log_stamp('window_ended', how); c['window_early'] = bool(early)
    c['state'] = 'CLEANUP'; c['streak'] = 0; c['first'] = None
    if c['mode'] == 'auto' and c['scenarioId'] != 'CATCH': start_stop(c)
    save_state()


def log_stamp(ev, detail=''):
    s = stamp(); log_event(ev, detail); return s


def finish_recovery(c, note=''):
    c['recovery_early'] = bool(c['t'].get('clear')) and since(c['t']['clear']) < c['minRecS']
    c['t']['recovered'] = log_stamp('recovery_confirmed', note + (' (before the minimum recovery time)' if c['recovery_early'] else ''))
    c['state'] = 'FINALISING'; c['summary'] = summarise(c); save_state()


def engine():
    last = 0.0
    while True:
        time.sleep(0.2 / max(1.0, SPEED / 5.0))
        with LOCK:
            c = ST['cur']
            if not c or c['state'] in ('FINALISING',) or (c['state'] == 'ERROR' and c['mode'] != 'auto'): continue
            tid = c['trialId']; sid = c['scenarioId']
            due = clk() - c.get('_ls', 0) >= RC['sample_s']
            if due: c['_ls'] = clk()
            t_start = stamp()
        if due:
            vals = sample(probes_for(sid))
        with LOCK:
            c = ST['cur']
            if not c or c['trialId'] != tid: continue
            if due:
                st = c['state']; rel = between(c['t']['prep'], t_start)
                rows = []
                for n, v in vals.items():
                    SAMPLES.append((t_start['m'], rel, st, n, v)); rows.append((tid, int(t_start['w'] * 1000), rel, st, n, v))
                DB.executemany('INSERT INTO samples VALUES(?,?,?,?,?,?)', rows); DB.commit()
                c['latest'] = {n: v for n, v in vals.items()}; c['last_sample_stamp'] = t_start
                if not c['baseline'] and st == 'BASELINE': pass
            try:
                if due: advance(c)
                elif c['state'] == 'ACTIVE' or (c['state'] == 'RECOVERY' and c['mode'] == 'auto'): advance(c)
            except Exception as e:
                log_event('engine_exception', repr(e))


# ------------------------------------------------------------------ API actions
class ApiError(Exception):
    pass


def need_cur(states=None):
    c = ST['cur']
    if not c: raise ApiError('No trial in progress.')
    if states and c['state'] not in states: raise ApiError('Not allowed in state %s.' % c['state'])
    return c


def norm_pid(x):
    p = re.sub(r'\s+', '', str(x)).upper()
    if re.match(r'^P[O0-9]{1,3}$', p): p = 'P' + p[1:].replace('O', '0')     # P08 typed as PO8
    return p


def a_session(b):
    with LOCK:
        if ST['session']['sessionId'] and (norm_pid(b.get('pid', '')) != ST['session']['pid'] or b.get('condition') != ST['session']['condition']):
            raise ApiError('Participant and condition are locked once the session has started.')
        for k in ('pid', 'condition', 'seed', 'observer'):
            if k in b: ST['session'][k] = norm_pid(b[k]) if k == 'pid' else str(b[k]).strip()
        if 'blur' in b: ST['settings']['blur'] = bool(b['blur'])
        save_state()


def a_settings(b):
    with LOCK:
        s = ST['settings']
        for k in ('include', 'window', 'recovery'):
            if k in b: s[k].update(b[k])
        if 'catchN' in b: s['catchN'] = max(0, min(4, int(b['catchN'])))
        if 'baseline_s' in b: s['baseline_s'] = max(5, int(b['baseline_s']))
        if 'dashboard_version' in b:
            dv = s.setdefault('dashboard_version', {'A': '', 'B': ''})
            for k2 in ('A', 'B'):
                if k2 in b['dashboard_version']: dv[k2] = str(b['dashboard_version'][k2]).strip()[:80]
        for k in ('early_end', 'early_recovery'):
            if k in b: s[k] = bool(b[k])
        if 'early_grace_s' in b: s['early_grace_s'] = max(0, min(120, int(b['early_grace_s'])))
        if 'settle_s' in b: s['settle_s'] = max(5, min(300, int(b['settle_s'])))
        save_state()


def a_order(b):
    with LOCK:
        s = ST['session']
        if not s['pid']: raise ApiError('Enter a participant ID first.')
        if ST['cur'] or n_done(): raise ApiError('The order cannot change after the first trial.')
        s['order'] = gen_order(); save_state()


def a_newparticipant(b):
    with LOCK:
        if ST['cur']: raise ApiError('Finish or abort the current trial first.')
        ST['session'] = {'pid': '', 'condition': 'A', 'sessionId': '', 'seed': '', 'order': [], 'observer': ST['session'].get('observer', '')}; ST['idle'] = 0; save_state()


def preflight(sid):
    fails = []
    for kind, ref in SC[sid]['runner'].get('preflight', []):
        if SIM: break
        if kind == 'ssh_ok':
            rc, _ = on(ref, 'true', 6)
            if rc != 0: fails.append('cannot reach %s over SSH' % ref)
        elif kind == 'pid_dead':
            v = probe(PD[ref])
            if v is None: fails.append('cannot read %s' % ref)
            elif v != 0: fails.append('a previous fault is still running (%s)' % ref)
        elif kind == 'no_netem':
            v = probe(PD[ref])
            if v is None: fails.append('cannot read tc state')
            elif v != 0: fails.append('a netem rule is already active on the WAN interface')
    return fails


def a_prepare(b):
    with LOCK:
        if ST['cur']: raise ApiError('A trial is already in progress.')
        s = ST['session']
        if not s['pid']: raise ApiError('Enter a participant ID first.')
        sid = b.get('scenario'); mode = b.get('mode', 'auto')
        if sid not in SC: raise ApiError('Unknown scenario.')
        if mode == 'auto' and not SC[sid]['runner']['auto']: raise ApiError('This scenario has no automatic script yet. Use manual mode.')
        if not s['sessionId']:
            s['sessionId'] = 'S-%s-%s' % (s['pid'], datetime.now().strftime('%Y%m%d%H%M'))
            if not s['order']: s['order'] = gen_order()
            log_event('session_started', 'condition=%s' % s['condition'], 'idle')
        if sid != 'CATCH':
            for r in DB.execute('SELECT j FROM trials'):
                j = json.loads(r[0])
                if j['session'] == s['sessionId'] and j['scenarioId'] == sid and j['valid']:
                    raise ApiError('This scenario already has a valid trial for this participant.')
        ST['preparing'] = True
    try:
        fails = preflight(sid)
    finally:
        ST['preparing'] = False
    if fails: raise ApiError('Preflight failed: ' + '; '.join(fails))
    with LOCK:
        pos = n_done() + 1
        planned = next_planned()
        SAMPLES.clear()
        c = {'trialId': '%s-T%02d' % (ST['session']['sessionId'], pos), 'scenarioId': sid, 'position': pos, 'mode': mode, 'state': 'BASELINE',
             't': {'prep': stamp()}, 'windowS': eff_window(sid), 'minRecS': eff_rec(sid), 'reported': '', 'desc': '', 'notes': '',
             'falseAlarms': 0, 'preOnset': 0, 'late': 0, 'extra': 0, 'reports': [], 'onset_method': '', 'pdesc': '', 'detected': False, 'aborted': False, 'abortReason': '', 'problem': '',
             'baseline': {}, 'latest': {}, 'streak': 0, 'first': None, 'summary': {}, 'launch_cmd': '', 'last_sample_stamp': None, '_ls': 0}
        ST['cur'] = c
        if planned and planned != sid: log_event('order_override', 'planned=%s; chosen=%s' % (planned, sid), 'idle')
        log_event('trial_prepared', 'mode=%s window_s=%d' % (mode, c['windowS']), 'BASELINE')


def a_command(b):
    with LOCK:
        c = need_cur(['BASELINE'])
        if c['mode'] != 'manual': raise ApiError('Automatic trial: the runner issues the command.')
        c['baseline'] = baseline_means()
        if c['scenarioId'] == 'CATCH':
            c['t']['onset'] = log_stamp('catch_window_started'); c['state'] = 'ACTIVE'
        else:
            c['t']['command'] = log_stamp('fault_command_issued', 'manual')
            c['state'] = 'STARTING'
            if SIM: SIMW.launch(c['scenarioId'])
        save_state()


def a_onset(b):
    with LOCK:
        c = need_cur(['STARTING'])
        if c['mode'] != 'manual': raise ApiError('Automatic trial: onset is verified by the probes.')
        rule = SC[c['scenarioId']]['runner'].get('onset')
        c['onset_method'] = 'manual_forced' if b.get('force') else 'manual_probe_confirmed'
        if rule and not b.get('force') and not rule_ok(rule, (c.get('latest') or {}).get(rule['probe']), c.get('baseline') or {}):
            c['onset_method'] = ''
            raise ApiError('NOT_APPLIED: the probes do not show the fault yet (%s). Check that the command is really running.' % rule['probe'])
        c['t']['onset'] = log_stamp('fault_verified_onset', 'manual' + (' (forced, probes did not confirm)' if b.get('force') else '')); c['state'] = 'ACTIVE'; save_state()


def a_cleared(b):
    with LOCK:
        c = need_cur(['CLEANUP'])
        if c['mode'] != 'manual': raise ApiError('Automatic trial: clearing is verified by the probes.')
        if SIM: SIMW.stop()
        c['t']['clear'] = log_stamp('fault_cleared_verified', 'manual'); c['state'] = 'RECOVERY'
        save_state()


def a_recovered(b):
    with LOCK:
        c = need_cur(['RECOVERY'])
        early = since(c['t']['clear']) < c['minRecS']
        if early and not b.get('override'): raise ApiError('Minimum recovery time has not elapsed.')
        finish_recovery(c, 'override_before_minimum' if early else '')


def a_endwindow(b):
    with LOCK:
        c = need_cur(['ACTIVE'])
        if c['scenarioId'] == 'CATCH': raise ApiError('A catch window runs for its full length.')
        end_window(c, 'early: ended by the researcher', True)


def a_skiprecovery(b):
    with LOCK:
        c = need_cur(['RECOVERY'])
        if c['mode'] != 'auto': raise ApiError('Manual trial: use the Testbed recovered button.')
        rule = SC[c['scenarioId']]['runner'].get('cleared')
        if rule and not rule_ok(rule, c['latest'].get(rule['probe']), c['baseline']):
            raise ApiError('The probes do not show the testbed back to normal yet.')
        finish_recovery(c, 'skipped by the researcher')


def a_hand(b):
    with LOCK:
        c = ST['cur']
        if not ST['session']['sessionId']: raise ApiError('Start a trial before recording reports.')
        if not c:
            ST['idle'] += 1; log_event('idle_report', '', 'idle'); return
        w = window_tag(c); st = c['state']
        if st == 'ACTIVE':
            c.setdefault('reports', []).append(round(between(c['t']['onset'], stamp()), 1))
            if c['scenarioId'] == 'CATCH':
                c['falseAlarms'] += 1
                if not c['detected']: c['detected'] = True; c['t']['detect'] = stamp()
                log_event('false_alarm', 'catch trial report', w)
            elif not c['detected']:
                c['detected'] = True; c['t']['detect'] = stamp(); log_event('detection', 'elapsed_s=%.1f' % between(c['t']['onset'], c['t']['detect']), w)
            else:
                c['extra'] += 1; log_event('additional_report', '', w)
        elif st == 'STARTING': c['preOnset'] += 1; log_event('pre_onset_report', '', w)
        elif st == 'CLEANUP': c['late'] += 1; log_event('post_window_report', '', w)
        else: c['falseAlarms'] += 1; log_event('false_alarm', '', w)
        save_state()


def a_outcome(b):
    with LOCK:
        c = need_cur()
        for k, f in (('reported', 'reported'), ('desc', 'desc'), ('notes', 'notes'), ('pdesc', 'pdesc')):
            if k in b and b[k] != c.get(f, ''):
                log_event('outcome_amended' if c.get(f) else 'outcome_entered', '%s=%s' % (k, b[k])); c[f] = str(b[k])
        save_state()


def a_abort(b):
    with LOCK:
        c = need_cur(['BASELINE', 'STARTING', 'ACTIVE'])
        c['aborted'] = True; c['abortReason'] = str(b.get('reason', 'Other')); log_event('trial_aborted', c['abortReason'])
        if c['state'] == 'BASELINE' and not c.get('t', {}).get('command'):
            return finalise_save(c, True)
        c['t']['windowEnd'] = stamp(); c['state'] = 'CLEANUP'; c['streak'] = 0; c['first'] = None
        if c['mode'] == 'auto' and c['scenarioId'] != 'CATCH': start_stop(c)
        save_state()


def a_problem(b):
    with LOCK:
        c = need_cur()
        if c['state'] in ('ERROR', 'FINALISING'): raise ApiError('Not allowed now.')
        c['problem'] = str(b.get('reason', 'Other')); log_event('problem_reported', c['problem'])
        c['aborted'] = True; c['abortReason'] = 'error: ' + c['problem']; c['state'] = 'ERROR'; c['streak'] = 0; c['first'] = None
        if c['mode'] == 'auto': start_stop(c)
        save_state()


def a_resolve(b):
    with LOCK:
        c = need_cur(['ERROR'])
        if not (c.get('cleared_ok') or b.get('confirm_manual')):
            raise ApiError('Testbed not yet verified clean. Wait for the probes or confirm you checked it by hand.')
        log_event('error_resolved', 'testbed verified clean' if c.get('cleared_ok') else 'confirmed manually')
        c['summary'] = summarise(c); finalise_save(c, True)


def a_save(b):
    with LOCK:
        c = need_cur(['FINALISING'])
        iscatch = c['scenarioId'] == 'CATCH'
        if not iscatch and not c['aborted'] and c['detected'] and (not c['reported'] or not c['desc']):
            raise ApiError('Select what the participant reported and the description judgement first.')
        finalise_save(c, False)


def finalise_save(c, invalid):
    sc = SC[c['scenarioId']]; iscatch = c['scenarioId'] == 'CATCH'; valid = not c['aborted'] and not invalid
    if not c.get('summary'): c['summary'] = summarise(c)
    rep = c['reported']
    if iscatch: rep = (c['reported'] or 'OTHER') if c['detected'] else 'NONE'
    elif not c['detected']: rep = 'NONE'
    corr = (not c['detected']) if iscatch else (c['detected'] and rep == c['scenarioId'])
    final = corr if iscatch else (corr and c['desc'] == 'yes')
    raw = between(c['t']['onset'], c['t']['detect']) if (not iscatch and c['detected'] and c['t'].get('onset') and c['t'].get('detect')) else None
    g = c['summary'].get(sc['runner']['gt_probe'], {})
    f1 = lambda x: '' if x is None else ('0' if x == 0 else ('%.3f' if abs(x) < 1 else ('%.2f' if abs(x) < 10 else '%.1f')) % x)
    t = c['t']
    row = {'trial_id': c['trialId'], 'session': ST['session']['sessionId'], 'pid': ST['session']['pid'], 'condition': ST['session']['condition'],
           'observer': ST['session'].get('observer', ''), 'dashboard_version': ST['settings'].get('dashboard_version', {}).get(ST['session']['condition'], ''),
           'task_order_code': '|'.join(ST['session'].get('order', [])), 'order_seed': ST['session'].get('seed', ''),
           'position': c['position'], 'scenarioId': c['scenarioId'], 'name': sc['name'], 'h4_type': sc['h4_type'], 'mode': c['mode'],
           'window_s': c['windowS'], 'min_recovery_s': c['minRecS'],
           'window_actual_s': f1(between(t['onset'], t['windowEnd'])) if t.get('onset') and t.get('windowEnd') else '', 'window_ended_early': bool(c.get('window_early')),
           'recovery_actual_s': f1(between(t['clear'], t['recovered'])) if t.get('clear') and t.get('recovered') else '', 'recovery_ended_early': bool(c.get('recovery_early')), 't_prepared': iso(t.get('prep')), 't_command': iso(t.get('command')),
           't_onset_verified': iso(t.get('onset')), 't_detect': iso(t.get('detect')), 't_window_end': iso(t.get('windowEnd')),
           't_cleared': iso(t.get('clear')), 't_recovered': iso(t.get('recovered')),
           'onset_lag_s': f1(between(t['command'], t['onset'])) if t.get('command') and t.get('onset') else '', 'onset_method': c.get('onset_method', ''),
           'detected': c['detected'], 'reported_scenario': rep, 'correct_indicator': corr if valid else '',
           'description_correct': '' if iscatch else c['desc'], 'final_correct': final if valid else '',
           'ttd_raw_s': f1(raw), 'ttd_s': f1(raw) if (valid and final and raw is not None and not iscatch) else '',
           'timed_out': (not iscatch and not c['detected'] and valid), 'false_alarm_events': c['falseAlarms'],
           'pre_onset_reports': c['preOnset'], 'post_window_reports': c['late'], 'additional_reports': c['extra'], 'report_times_s': ' '.join('%.1f' % x for x in c.get('reports', [])),
           'gt_metric': sc['gt_label'], 'gt_probe': sc['runner']['gt_probe'], 'baseline_value': f1(g.get('baseline')),
           'active_value': f1(g.get('end_window')), 'peak_value': f1(g.get('peak')), 'post_value': f1(g.get('post')),
           'probe_summary': json.dumps(c['summary']), 'launch_cmd': c.get('launch_cmd', ''),
           'valid': valid, 'invalid_reason': '' if valid else (c['abortReason'] or 'invalid'), 'participant_description': c.get('pdesc', ''), 'notes': c['notes'],
           'clock_source': 'server', 'runner_version': VERSION, 'config_version': CFG['version'] + '+' + CFG_HASH}
    DB.execute('INSERT OR REPLACE INTO trials VALUES(?,?,?)', (c['trialId'], c['position'], json.dumps(row)))
    log_event('trial_saved', 'valid=%s' % valid, 'FINALISING')
    ST['cur'] = None; SAMPLES.clear(); save_state()


def a_safety(b):
    with LOCK:
        if ST['cur'] and ST['cur']['state'] in ('STARTING', 'ACTIVE'): raise ApiError('Abort the trial first.')
    threading.Thread(target=safety_clear, args=('manual',), daemon=True).start()


def a_clear(b):
    with LOCK:
        if b.get('confirm') != 'DELETE': raise ApiError('Type DELETE to confirm.')
        if ST['cur']: raise ApiError('Finish or abort the current trial first.')
        global DB
        DB.commit(); DB.close()
        os.rename(os.path.join(ARGS.data_dir, 'trials.db'), os.path.join(ARGS.data_dir, 'trials-%s.db.bak' % datetime.now().strftime('%Y%m%d-%H%M%S')))
        DB = sqlite3.connect(os.path.join(ARGS.data_dir, 'trials.db'), check_same_thread=False)
        DB.executescript('''CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, session TEXT, pid TEXT, trial TEXT, scenario TEXT, event TEXT, win TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS trials(trial_id TEXT PRIMARY KEY, seq INTEGER, j TEXT);
CREATE TABLE IF NOT EXISTS samples(trial_id TEXT, t_ms INTEGER, rel_s REAL, state TEXT, probe TEXT, value REAL);''')
        keep = fresh()['settings']; ST.update(fresh()); ST['settings'] = keep; save_state()


ACTIONS = {'session': a_session, 'settings': a_settings, 'order': a_order, 'newparticipant': a_newparticipant, 'prepare': a_prepare,
           'command': a_command, 'onset': a_onset, 'cleared': a_cleared, 'recovered': a_recovered, 'hand': a_hand, 'endwindow': a_endwindow, 'skiprecovery': a_skiprecovery, 'outcome': a_outcome,
           'abort': a_abort, 'problem': a_problem, 'resolve': a_resolve, 'save': a_save, 'safety_clear': a_safety, 'clear': a_clear}


# ------------------------------------------------------------------ state view and exports
def view():
    with LOCK:
        c = ST['cur']; cv = None
        if c:
            cv = {k: v for k, v in c.items() if not k.startswith('_') and k not in ('t', 'last_sample_stamp', 'first')}
            t = c['t']; st = c['state']
            ref = {'BASELINE': t.get('prep'), 'STARTING': t.get('command'), 'ACTIVE': t.get('onset'), 'CLEANUP': t.get('windowEnd'),
                   'RECOVERY': t.get('clear')}.get(st)
            cv['state_elapsed_s'] = since(ref) if ref else 0
            cv['detect_s'] = between(t['onset'], t['detect']) if t.get('onset') and t.get('detect') and c['scenarioId'] != 'CATCH' else None
            cv['stop_tail'] = c.get('stop_out', '')
            cv['window_tag'] = window_tag(c)
            lp = os.path.join(ARGS.data_dir, 'logs', c['trialId'] + '.log')
            try: cv['launch_log'] = open(lp, 'rb').read()[-400:].decode('utf8', 'replace')
            except Exception: cv['launch_log'] = ''
        rows = [json.loads(r[0]) for r in DB.execute('SELECT j FROM trials ORDER BY rowid DESC LIMIT 80')]
        total = DB.execute('SELECT COUNT(*) FROM trials').fetchone()[0]
        return {'version': VERSION, 'sim': SIM, 'speed': SPEED, 'config': CFG, 'session': ST['session'], 'settings': ST['settings'], 'cur': cv,
                'idle': ST['idle'], 'safety': ST['safety'], 'trials': rows, 'total': total, 'done': n_done() if ST['session']['sessionId'] else 0,
                'planned': next_planned(), 'preparing': bool(ST.get('preparing')), 'version': VERSION}


TRIAL_COLS = ['trial_id', 'session', 'pid', 'condition', 'observer', 'dashboard_version', 'task_order_code', 'order_seed', 'position', 'scenarioId', 'name', 'h4_type', 'mode', 'window_s', 'min_recovery_s', 'window_actual_s', 'window_ended_early', 'recovery_actual_s', 'recovery_ended_early', 't_prepared',
              't_command', 't_onset_verified', 't_detect', 't_window_end', 't_cleared', 't_recovered', 'onset_lag_s', 'onset_method', 'detected', 'reported_scenario',
              'correct_indicator', 'description_correct', 'final_correct', 'ttd_raw_s', 'ttd_s', 'timed_out', 'false_alarm_events', 'pre_onset_reports',
              'post_window_reports', 'additional_reports', 'report_times_s', 'gt_metric', 'gt_probe', 'baseline_value', 'active_value', 'peak_value', 'post_value',
              'probe_summary', 'launch_cmd', 'valid', 'invalid_reason', 'participant_description', 'notes', 'clock_source', 'runner_version', 'config_version']


def to_csv(cols, rows, sep=','):
    out = io.StringIO(); w = csv.writer(out, delimiter=sep, quoting=csv.QUOTE_MINIMAL); w.writerow(cols)
    for r in rows: w.writerow(['' if x is None else x for x in r])
    return '\ufeff' + out.getvalue()      # BOM so Excel reads UTF-8 correctly


_NUM = re.compile(r'^-?(0|[1-9]\d{0,14})(\.\d+)?$')
_BAD = re.compile('[\x00-\x08\x0b\x0c\x0e-\x1f]')


def _col(i):
    s = ''
    i += 1
    while i: i, m = divmod(i - 1, 26); s = chr(65 + m) + s
    return s


def _cell(ref, v):
    if v is None or v == '': return ''
    if isinstance(v, bool): return '<c r="%s" t="b"><v>%d</v></c>' % (ref, 1 if v else 0)
    if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v): return '<c r="%s"><v>%r</v></c>' % (ref, v)
    s = str(v)
    if _NUM.match(s): return '<c r="%s"><v>%s</v></c>' % (ref, s)
    s = _BAD.sub('', s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    return '<c r="%s" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>' % (ref, s)


def make_xlsx(sheets):
    import zipfile
    buf = io.BytesIO(); z = zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED)
    names = list(sheets)
    z.writestr('[Content_Types].xml', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>' + ''.join('<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' % (i + 1) for i in range(len(names))) + '</Types>')
    z.writestr('_rels/.rels', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
    z.writestr('xl/workbook.xml', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>' + ''.join('<sheet name="%s" sheetId="%d" r:id="rId%d"/>' % (n, i + 1, i + 1) for i, n in enumerate(names)) + '</sheets></workbook>')
    z.writestr('xl/_rels/workbook.xml.rels', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + ''.join('<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>' % (i + 1, i + 1) for i in range(len(names))) + '</Relationships>')
    for i, n in enumerate(names):
        cols, rows = sheets[n]
        parts = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><sheetData>']
        parts.append('<row r="1">' + ''.join('<c r="%s1" t="inlineStr"><is><t>%s</t></is></c>' % (_col(j), c) for j, c in enumerate(cols)) + '</row>')
        for k, row in enumerate(rows):
            rn = k + 2
            parts.append('<row r="%d">' % rn + ''.join(_cell('%s%d' % (_col(j), rn), v) for j, v in enumerate(row)) + '</row>')
        parts.append('</sheetData></worksheet>')
        z.writestr('xl/worksheets/sheet%d.xml' % (i + 1), ''.join(parts))
    z.close()
    return buf.getvalue()


def export(kind, sep=','):
    with LOCK:
        if kind == 'trials':
            rows = [json.loads(r[0]) for r in DB.execute('SELECT j FROM trials ORDER BY rowid')]
            return to_csv(TRIAL_COLS, [[r.get(k, '') for k in TRIAL_COLS] for r in rows], sep), 'text/csv'
        if kind == 'events':
            cols = ['id', 'ts', 'session', 'pid', 'trial', 'scenario', 'event', 'win', 'detail']
            return to_csv(cols, DB.execute('SELECT %s FROM events ORDER BY id' % ','.join(cols)), sep), 'text/csv'
        if kind == 'samples':
            cols = ['trial_id', 't_ms', 'rel_s', 'state', 'probe', 'value']
            return to_csv(cols, DB.execute('SELECT %s FROM samples ORDER BY rowid' % ','.join(cols)), sep), 'text/csv'
        if kind == 'xlsx':
            rows = [json.loads(x[0]) for x in DB.execute('SELECT j FROM trials ORDER BY rowid')]
            ec = ['id', 'ts', 'session', 'pid', 'trial', 'scenario', 'event', 'win', 'detail']
            sc_ = ['trial_id', 't_ms', 'rel_s', 'state', 'probe', 'value']
            MAP = {'BW-SAT': 'T1', 'DNS-FAIL': 'T2', 'TRAFFIC-MIX': 'T3', 'LOSS-JITTER': 'T5', 'LATENCY': 'T6', 'CPU': 'T7', 'MEMORY': 'T7', 'AVAIL': 'T10'}
            imap = [[s_['id'], s_['name'], s_['h4_type'], s_['panel'], s_['gt_label'], s_['runner']['gt_probe'], MAP.get(s_['id'], ''), 'suggested, confirm' if s_['id'] in MAP else ('not an indicator (catch trial)' if s_['id'] == 'CATCH' else 'no matching instrument task: decide'), ''] for s_ in CFG['scenarios']]
            return make_xlsx({'indicator_map': (['scenarioId', 'name', 'h4_type', 'panel', 'ground_truth_metric', 'ground_truth_probe', 'instrument_task', 'mapping_status', 'phase2_criticality'], imap),
                              'trials': (TRIAL_COLS, [[x.get(k, '') for k in TRIAL_COLS] for x in rows]),
                              'events': (ec, [list(x) for x in DB.execute('SELECT %s FROM events ORDER BY id' % ','.join(ec))]),
                              'samples': (sc_, [list(x) for x in DB.execute('SELECT %s FROM samples ORDER BY rowid' % ','.join(sc_))])}), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        if kind == 'backup':
            d = {t: [list(r) for r in DB.execute('SELECT * FROM %s' % t)] for t in ('kv', 'events', 'trials', 'samples')}
            return json.dumps(d), 'application/json'
    return '', 'text/plain'


# ------------------------------------------------------------------ http
TOKEN_FILE = os.path.join(ARGS.data_dir, 'token.txt')
if ARGS.token: TOKEN = ARGS.token
elif os.path.exists(TOKEN_FILE): TOKEN = open(TOKEN_FILE).read().strip()
else:
    TOKEN = secrets.token_urlsafe(9); open(TOKEN_FILE, 'w').write(TOKEN)


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _auth(self, q):
        t = self.headers.get('X-Token') or (q.get('t') or [''])[0]
        return hmac.compare_digest(t, TOKEN)

    def _send(self, code, body, ctype='application/json', extra=None):
        b = body if isinstance(body, bytes) else body.encode('utf8')
        self.send_response(code); self.send_header('Content-Type', ctype + '; charset=utf-8'); self.send_header('Content-Length', str(len(b)))
        self.send_header('Cache-Control', 'no-store')
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        if u.path == '/favicon.ico': return self._send(204, '')
        if u.path == '/':
            return self._send(200, open(os.path.join(HERE, 'ui.html'), 'rb').read(), 'text/html')
        if not self._auth(q): return self._send(401, '{"error":"unauthorised"}')
        if u.path == '/api/state': return self._send(200, json.dumps(view()))
        m = re.match(r'^/api/(trials|events|samples)\.csv$', u.path)
        if m:
            body, ct = export(m.group(1), ';' if (q.get('sep') or [''])[0] == 'semi' else ',')
            return self._send(200, body, ct, {'Content-Disposition': 'attachment; filename="phase3_%s_%s.csv"' % (m.group(1), datetime.now().strftime('%Y%m%d-%H%M%S'))})
        if u.path == '/api/phase3_data.xlsx':
            body, ct = export('xlsx')
            return self._send(200, body, ct, {'Content-Disposition': 'attachment; filename="phase3_data_%s.xlsx"' % datetime.now().strftime('%Y%m%d-%H%M%S')})
        if u.path == '/api/backup.json':
            body, ct = export('backup')
            return self._send(200, body, ct, {'Content-Disposition': 'attachment; filename="phase3_backup_%s.json"' % datetime.now().strftime('%Y%m%d-%H%M%S')})
        self._send(404, '{"error":"not found"}')

    def do_POST(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        if not self._auth(q): return self._send(401, '{"error":"unauthorised"}')
        name = u.path.replace('/api/', '', 1)
        fn = ACTIONS.get(name)
        if not fn: return self._send(404, '{"error":"not found"}')
        try:
            n = int(self.headers.get('Content-Length') or 0)
            body = json.loads(self.rfile.read(n) or b'{}')
            fn(body)
            self._send(200, json.dumps({'ok': True}))
        except ApiError as e:
            self._send(400, json.dumps({'error': str(e)}))
        except Exception as e:
            self._send(500, json.dumps({'error': 'internal error: %r' % e}))


def startup_reconcile():
    with LOCK:
        c = ST['cur']
        if c and c['state'] != 'FINALISING':
            c['aborted'] = True; c['abortReason'] = 'runner restarted during trial'
            c['t'].setdefault('windowEnd', stamp()); log_event('trial_invalidated', 'runner restarted during trial', c['state'])
            c['summary'] = {}; finalise_save(c, True)
        elif c:
            c['summary'] = c.get('summary') or {}
    threading.Thread(target=safety_clear, args=('startup',), daemon=True).start()


if __name__ == '__main__':
    startup_reconcile()
    threading.Thread(target=engine, daemon=True).start()
    srv = ThreadingHTTPServer((ARGS.host, ARGS.port), H)
    print('Phase Three runner %s%s on http://%s:%d/?t=%s' % (VERSION, ' (SIMULATION x%g)' % SPEED if SIM else '', ARGS.host, ARGS.port, TOKEN), flush=True)
    try: srv.serve_forever()
    except KeyboardInterrupt: pass
