#!/usr/bin/env python3
"""
f3_all.py — F3 Jitter Testing (Unificado) — VERSIÓN CORREGIDA
RFC 1889 / RFC 8239 — Testbed SDN Spine-Leaf vs Jerárquica 3 Capas — UASLP 2026

CORRECCIONES (2026-07-08):
  - SL ahora usa destino común H5 para todos los emisores (congruente con j3c)
  - Emisores SL: H1, H2, H7 (2 desde Leaf1, 1 desde Leaf3) → todos a H5 (Leaf2)
  - j3c: H1, H2, H3 → todos a H8 (Edge2)
  - Ambas topologías: 3 flujos, todos al mismo destino

Ejecutar desde H3 (host de control):
    python3 f3_all.py --topology sl
    python3 f3_all.py --topology j3c
    python3 f3_all.py --dry-run
"""

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN GLOBAL — HOMOGENIZADA
# ══════════════════════════════════════════════════════════════════════════════

EXPERIMENT = "f3"
LINE_RATE_MBPS = 1000

DURATION = 30
COOLDOWN = 5          # ESTANDARIZADO: 5s
RATE_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
PKT_SIZE = 1024       # Payload UDP (≈ 1518B en cable con overhead Ethernet)

UDP_RATES = [10, 50, 100, 500, 900]

KEY_PATH = Path.home() / ".ssh" / "id_rsa_testbed"
OUTPUT_BASE = Path.home() / "experimentos"

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN POR TOPOLOGÍA — CORREGIDA PARA CONGRUENCIA
# ══════════════════════════════════════════════════════════════════════════════

TOPOLOGIES = {
    "sl": {
        "hosts": {
            "H1": {"ip": "10.0.1.1", "user": "h1"},
            "H2": {"ip": "10.0.1.2", "user": "h2"},
            "H3": {"ip": "10.0.1.3", "user": "h3"},
            "H4": {"ip": "10.0.2.1", "user": "h4"},
            "H5": {"ip": "10.0.2.2", "user": "h5"},
            "H6": {"ip": "10.0.2.3", "user": "h6"},
            "H7": {"ip": "10.0.3.1", "user": "h7"},
            "H8": {"ip": "10.0.3.2", "user": "h8"},
        },
        # CORREGIDO: todos los emisores apuntan al mismo destino H5
        # 2 desde Leaf1 (H1, H2) + 1 desde Leaf3 (H7) → H5 (Leaf2)
        "emisores": ["H1", "H2", "H7"],
        "destino": {"H1": "H5", "H2": "H5", "H7": "H5"},  # Todos a H5
        "puertos": {"H1": 5301, "H2": 5302, "H7": 5303},
        "scenario_desc": "H1+H2 (Leaf1) + H7 (Leaf3) → H5 (Leaf2) — 3 flows, todos al mismo destino",
    },
    "j3c": {
        "hosts": {
            "H1": {"ip": "10.0.1.1", "user": "h1"},
            "H2": {"ip": "10.0.1.2", "user": "h2"},
            "H3": {"ip": "10.0.1.3", "user": "h3"},
            "H4": {"ip": "10.0.1.4", "user": "h4"},
            "H5": {"ip": "10.0.2.1", "user": "h5"},
            "H6": {"ip": "10.0.2.2", "user": "h6"},
            "H7": {"ip": "10.0.2.3", "user": "h7"},
            "H8": {"ip": "10.0.2.4", "user": "h8"},
        },
        "emisores": ["H1", "H2", "H3"],
        "destino": {"H1": "H8", "H2": "H8", "H3": "H8"},  # Todos a H8 (Edge2)
        "puertos": {"H1": 5301, "H2": 5302, "H3": 5303},
        "scenario_desc": "H1+H2+H3 (Edge1) → H8 (Edge2) — 3 flows, todos al mismo destino",
    }
}

# ══════════════════════════════════════════════════════════════════════════════
#  COLORES
# ══════════════════════════════════════════════════════════════════════════════

class C:
    OK = "\033[92m"
    WARN = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    END = "\033[0m"

def ok(msg): print(f"  {C.OK}✔{C.END}  {msg}")
def warn(msg): print(f"  {C.WARN}⚠{C.END}  {msg}")
def fail(msg): print(f"  {C.FAIL}✘{C.END}  {msg}")
def info(msg): print(f"  →  {msg}")

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS SSH
# ══════════════════════════════════════════════════════════════════════════════

_SSH_OPTS = [
    "ssh", "-i", str(KEY_PATH),
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=15",
]

def ssh_run(host_key, cmd, timeout=90):
    h = HOSTS[host_key]
    return subprocess.run(
        _SSH_OPTS + [f"{h['user']}@{h['ip']}", cmd],
        capture_output=True, text=True, timeout=timeout,
    )

def ssh_bg(host_key, cmd):
    h = HOSTS[host_key]
    subprocess.Popen(
        _SSH_OPTS + [f"{h['user']}@{h['ip']}", f"nohup {cmd} >/dev/null 2>&1 &"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

def kill_iperf(host_key):
    ssh_run(host_key, "pkill -9 iperf3 2>/dev/null; true", timeout=8)

def kill_iperf_all(hosts):
    """Mata iperf3 en la lista de hosts dada (clientes y/o servidores).
    Necesario porque un timeout de SSH local no garantiza que el proceso
    iperf3 remoto muera con la sesión."""
    for h in hosts:
        kill_iperf(h)

# ══════════════════════════════════════════════════════════════════════════════
#  SERVIDORES Y FLUJOS UDP (CON BALANCEO MANUAL)
# ══════════════════════════════════════════════════════════════════════════════

def start_servers(dry_run):
    """Inicia servidores UDP en los destinos configurados."""
    if dry_run:
        return
    
    # Obtener todos los destinos únicos
    destinos = set(SCENARIO["destino"].values())
    
    for destino in destinos:
        kill_iperf(destino)
        time.sleep(0.5)
        # Iniciar servidor en cada puerto asociado a este destino
        for emisor, puerto in SCENARIO["puertos"].items():
            if SCENARIO["destino"][emisor] == destino:
                ssh_bg(destino, f"iperf3 -s -p {puerto}")
    time.sleep(2)
    ok(f"Servidores UDP activos en {', '.join(destinos)}")

def stop_servers(dry_run):
    if not dry_run:
        destinos = set(SCENARIO["destino"].values())
        for destino in destinos:
            kill_iperf(destino)

def run_udp_flow(host_key, rate_mbps, results, errors, dry_run):
    """
    Lanza iperf3 UDP desde host_key hacia su destino configurado.
    El destino varía según el emisor (balanceo manual).
    """
    if dry_run:
        results[host_key] = {
            "jitter_ms": round(0.1 + rate_mbps * 0.0001, 4),
            "lost_packets": 0,
            "throughput_mbps": rate_mbps * 0.98,
        }
        return

    destino = SCENARIO["destino"][host_key]
    ip_destino = HOSTS[destino]["ip"]
    puerto = SCENARIO["puertos"][host_key]

    cmd = (
        f"iperf3 -c {ip_destino} -p {puerto}"
        f" -u -b {rate_mbps}M"
        f" -l {PKT_SIZE}"
        f" -t {DURATION}"
        f" -J"
    )

    try:
        res = ssh_run(host_key, cmd, timeout=DURATION + 20)

        if res.returncode != 0 or not res.stdout.strip():
            errors.append(f"{host_key}: iperf3 rc={res.returncode}")
            results[host_key] = None
            return

        data = json.loads(res.stdout)
        end = data.get("end", {})
        s = end.get("sum", {})

        jitter_ms = s.get("jitter_ms")
        lost_packets = s.get("lost_packets", 0)
        throughput_bps = s.get("bits_per_second", 0)

        results[host_key] = {
            "jitter_ms": round(jitter_ms, 4) if jitter_ms is not None else None,
            "lost_packets": lost_packets,
            "throughput_mbps": round(throughput_bps / 1e6, 2),
        }

    except json.JSONDecodeError as e:
        errors.append(f"{host_key}: JSON inválido — {e}")
        results[host_key] = None
    except subprocess.TimeoutExpired:
        errors.append(f"{host_key}: timeout ({DURATION + 20}s)")
        results[host_key] = None
        kill_iperf(host_key)   # el iperf3 remoto pudo quedar vivo tras el timeout
    except Exception as e:
        errors.append(f"{host_key}: error — {e}")
        results[host_key] = None
        kill_iperf(host_key)

# ══════════════════════════════════════════════════════════════════════════════
#  ESTADÍSTICAS
# ══════════════════════════════════════════════════════════════════════════════

def compute_rsd(values):
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def nombre_archivo(topology, rate_mbps, rep):
    return f"{topology}_{EXPERIMENT}_udp_rate{rate_mbps:04d}_rep{rep:02d}.json"

def safe_stats(vals):
    clean = [v for v in vals if v is not None]
    if not clean:
        return None, None, None, None, None
    return (
        round(min(clean), 4),
        round(statistics.mean(clean), 4),
        round(max(clean), 4),
        round(statistics.stdev(clean), 4) if len(clean) > 1 else None,
        round(compute_rsd(clean) or 0, 2)
    )

# ══════════════════════════════════════════════════════════════════════════════
#  LOOP PRINCIPAL POR TASA
# ══════════════════════════════════════════════════════════════════════════════

def run_rate(topology, rate_mbps, out_dir, dry_run):
    emisores = SCENARIO["emisores"]
    jitters_by_host = {h: [] for h in emisores}
    jitters_avg = []

    rep = 0
    converged = False

    print(f"\n  {C.BOLD}── Tasa: {rate_mbps} Mbps "
          f"({(rate_mbps/LINE_RATE_MBPS)*100:.0f}% LR) ──{C.END}")

    while rep < REPS_MAX:
        rep += 1

        if rep > 1:
            for i in range(COOLDOWN, 0, -1):
                print(f"\r  Rep {rep:02d}/{REPS_MAX} enfriando {i}s...   ",
                      end="", flush=True)
                time.sleep(1)

        print(f"\r  Rep {rep:02d}/{REPS_MAX} midiendo {rate_mbps}Mbps...  ",
              end="", flush=True)

        results = {}
        errors = []
        threads = [
            threading.Thread(target=run_udp_flow,
                             args=(h, rate_mbps, results, errors, dry_run))
            for h in emisores
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=DURATION + 25)

        for e in errors:
            print(f"\n    ⚠  {e}", end="")

        # Extraer jitter de cada emisor
        jitter_vals = []
        for h in emisores:
            r = results.get(h)
            if r and r.get("jitter_ms") is not None:
                jitter_vals.append(r["jitter_ms"])
                jitters_by_host[h].append(r["jitter_ms"])

        if jitter_vals:
            rep_jitter_avg = statistics.mean(jitter_vals)
            jitters_avg.append(rep_jitter_avg)

        rsd = compute_rsd(jitters_avg) if len(jitters_avg) > 1 else 99.0
        j_str = f"{rep_jitter_avg:.3f}ms" if jitter_vals else "N/A"
        rsd_str = f"{rsd:.1f}%" if rsd is not None else "N/A"

        print(f"\r  Rep {rep:02d}/{REPS_MAX} "
              f"jitter_avg={j_str} "
              f"RSD={C.OK if rsd < RSD_TARGET else C.WARN}{rsd_str}{C.END}   ")

        # ── Guardar JSON ──
        fname = nombre_archivo(topology, rate_mbps, rep)
        record = {
            "_meta": {
                "experiment": EXPERIMENT,
                "topology": topology,
                "rate_mbps": rate_mbps,
                "line_rate_pct": round((rate_mbps / LINE_RATE_MBPS) * 100, 1),
                "pkt_size_b": PKT_SIZE,
                "rep": rep,
                "duration_s": DURATION,
                "cooldown_s": COOLDOWN,
                "protocol": "udp",
                "tcp_congestion_control": "N/A (UDP)",
                "snd_cwnd_avg_bytes": None,
                "snd_cwnd_max_bytes": None,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "rfc_reference": "RFC1889 jitter / RFC8239 §F3",
                "emisores": emisores,
                "destino_por_emisor": SCENARIO["destino"],
            },
            **{h: results.get(h) for h in emisores},
        }
        if not dry_run:
            (out_dir / fname).write_text(json.dumps(record, indent=2))

        if rep >= REPS_MIN and rsd is not None and rsd < RSD_TARGET:
            converged = True
            break

    if not converged:
        warn(f"RSD no convergió en {REPS_MAX} reps para {rate_mbps} Mbps")

    # Estadísticas finales
    j_min, j_avg, j_max, j_std, j_rsd = safe_stats(jitters_avg)
    j_p95 = round(sorted(jitters_avg)[int(len(jitters_avg) * 0.95)], 4) if jitters_avg else None

    info(f"jitter min/avg/max/p95: {j_min}/{j_avg}/{j_max}/{j_p95} ms | RSD: {j_rsd}% | reps: {rep}")

    result = {
        "rate_mbps": rate_mbps,
        "lr_pct": round((rate_mbps / LINE_RATE_MBPS) * 100, 1),
        "reps": rep,
        "converged": converged,
        "jitter_min_ms": j_min,
        "jitter_avg_ms": j_avg,
        "jitter_max_ms": j_max,
        "jitter_std_ms": j_std,
        "jitter_p95_ms": j_p95,
        "jitter_rsd_pct": j_rsd,
    }
    for h in emisores:
        vals = jitters_by_host[h]
        result[f"jitter_{h.lower()}_avg"] = round(statistics.mean(vals), 4) if vals else None

    return result

# ══════════════════════════════════════════════════════════════════════════════
#  EXPERIMENTO COMPLETO
# ══════════════════════════════════════════════════════════════════════════════

def run_experiment(topology, rates, dry_run):
    out_dir = OUTPUT_BASE / topology / "fase3_jitter"
    out_dir.mkdir(parents=True, exist_ok=True)

    emisores_str = " + ".join(f"{h} ({HOSTS[h]['ip']}->{SCENARIO['destino'][h]})" for h in SCENARIO["emisores"])
    print(f"\n{'='*65}")
    print(f"  F3 JITTER  |  {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Escenario: {SCENARIO['desc']}")
    print(f"  Emisores:  {emisores_str}")
    print(f"  Tasas:     {rates} Mbps")
    print(f"  Cooldown:  {COOLDOWN}s")
    print(f"  Duración:  {DURATION}s por run")
    print(f"  Reps:      {REPS_MIN}–{REPS_MAX} (RSD < {RSD_TARGET}%)")
    print(f"  Salida:    {out_dir}")
    if dry_run:
        print("  MODO:      DRY-RUN")
    print(f"{'='*65}\n")

    # Preflight
    if not dry_run:
        print("-- Preflight " + "-"*50)
        all_ok = True
        all_hosts = list(SCENARIO["emisores"]) + list(set(SCENARIO["destino"].values()))
        for name in all_hosts:
            res = ssh_run(name, "iperf3 --version 2>&1 | head -1", timeout=10)
            ok_flag = res.returncode == 0
            ver = res.stdout.strip()[:40] if ok_flag else "no encontrado"
            print(f"  {'OK' if ok_flag else 'FAIL'} {name}  {HOSTS[name]['ip']}  {ver}")
            if not ok_flag:
                all_ok = False
        if not all_ok:
            fail("Preflight falló — verifica iperf3 en todos los hosts")
            sys.exit(1)
        print()

    start_servers(dry_run)

    all_results = []
    for i, rate in enumerate(rates):
        result = run_rate(topology, rate, out_dir, dry_run)
        all_results.append(result)
        if i < len(rates) - 1 and not dry_run:
            time.sleep(RATE_PAUSE)

    stop_servers(dry_run)

    # TABLA RESUMEN
    print(f"\n\n{'='*65}")
    print(f"  RESUMEN F3 JITTER — {topology.upper()}")
    print(f"  {'Tasa':>6}  {'LR%':>6}  {'Jitter avg':>12}  "
          f"{'Jitter p95':>12}  {'Jitter max':>12}  {'RSD%':>7}  {'Reps':>5}")
    print(f"  {'─'*6}  {'─'*6}  {'─'*12}  {'─'*12}  {'─'*12}  {'─'*7}  {'─'*5}")
    for r in all_results:
        rfc_ok = (r["jitter_rsd_pct"] or 99) < RSD_TARGET
        rsd_str = f"{r['jitter_rsd_pct']:.1f}%" if r["jitter_rsd_pct"] else "N/A"
        p95_str = f"{r['jitter_p95_ms']:.4f}ms" if r["jitter_p95_ms"] is not None else "N/A"
        avg_str = f"{r['jitter_avg_ms']:.4f}ms" if r["jitter_avg_ms"] is not None else "N/A"
        flag = f"{C.OK}✔{C.END}" if rfc_ok else f"{C.WARN}⚠{C.END}"
        print(f"  {r['rate_mbps']:>5}M  {r['lr_pct']:>5.0f}%"
              f"  {avg_str:>12}"
              f"  {p95_str:>12}"
              f"  {str(r['jitter_max_ms'])+'ms':>12}"
              f"  {rsd_str:>7} {flag}"
              f"  {r['reps']:>5}")
    print(f"{'='*65}\n")

    summary_path = out_dir / f"{topology}_{EXPERIMENT}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment": EXPERIMENT,
        "topology": topology,
        "rfc_reference": "RFC1889 jitter / RFC8239 §F3",
        "emisores": SCENARIO["emisores"],
        "destino_por_emisor": SCENARIO["destino"],
        "udp_rates_mbps": rates,
        "duration_s": DURATION,
        "cooldown_s": COOLDOWN,
        "rsd_target": RSD_TARGET,
        "reps_min": REPS_MIN,
        "reps_max": REPS_MAX,
        "line_rate_mbps": LINE_RATE_MBPS,
        "payload_bytes": PKT_SIZE,
        "balanced": (topology == "sl"),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "results": all_results,
    }, indent=2))
    ok(f"Summary: {summary_path}")

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="F3: Jitter Testing Unificado — CON BALANCEO MANUAL"
    )
    parser.add_argument("--topology", choices=["sl", "j3c"], default="sl")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--rates", nargs="+", type=int, default=UDP_RATES)
    args = parser.parse_args()

    global HOSTS, SCENARIO
    cfg = TOPOLOGIES[args.topology]
    HOSTS = cfg["hosts"]
    SCENARIO = {
        "emisores": cfg["emisores"],
        "destino": cfg["destino"],
        "puertos": cfg["puertos"],
        "desc": cfg["scenario_desc"],
    }

    run_experiment(
        topology=args.topology,
        rates=args.rates,
        dry_run=args.dry_run,
    )

if __name__ == "__main__":
    main()