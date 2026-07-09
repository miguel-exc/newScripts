#!/usr/bin/env python3
"""
f2_all.py — F2 Latency Testing (Unificado) — VERSIÓN CORREGIDA
RFC 8239 §3 — Testbed SDN Spine-Leaf vs Jerárquica 3 Capas — UASLP 2026

CORRECCIONES (2026-07-09):
  - Métrica: ahora usa mean_rtt de iperf3 (JSON), NO ping
  - Carga fija: 50-50 Mbps para evitar saturación (NO oversubscripción variable)
  - Receptor común: todos los flujos van a H5 (SL y j3c)
  - SL S2 usa H1+H2 (Leaf1) + H7 (Leaf3) → H5 (2 leafs origen)
  - Tamaños: 6 tamaños (64B a 1518B) como en F1
  - Cooldown: 5s (estandarizado)

Ejecutar desde H3 (host de control):
    python3 f2_all.py --scenario all --topology sl
    python3 f2_all.py --scenario s1 --topology j3c
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

# ============================================================
# CONFIGURACIÓN GLOBAL
# ============================================================

EXPERIMENT_BASE = "f2"
LINE_RATE_MBPS = 1000

DURATION = 30
COOLDOWN = 5           # Estandarizado: 5s
PKT_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30

# Carga fija para evitar saturación (50-100 Mbps)
MAIN_RATE_MBPS = 50    # Flujo de fondo
PROBE_RATE_MBPS = 50   # Flujo que mide RTT
# Total: 100 Mbps (bien dentro del rango 50-100 Mbps)

# Tamaños de paquete: 6 tamaños como en F1
PKT_SIZES = [64, 128, 256, 512, 1024, 1518]

KEY_PATH = Path.home() / ".ssh" / "id_rsa_testbed"
OUTPUT_BASE = Path.home() / "experimentos"

# ============================================================
# CONFIGURACIÓN POR TOPOLOGÍA Y ESCENARIO
# ============================================================

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
        }
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
        }
    }
}

# Escenarios: S1 (2 flows) y S2 (4 flows)
# TODOS los flujos van al mismo receptor H5 (SL y j3c)
SCENARIOS = {
    "s1": {
        "id": "s1",
        "desc": {
            "sl": "H1 (main) + H4 (probe) → H5 — 2 flows, mismo destino",
            "j3c": "H1 (main) + H4 (probe) → H5 — 2 flows, mismo destino"
        },
        "main_host": "H1",
        "probe_host": "H4",
        "target_host": {"sl": "H5", "j3c": "H5"},  # MISMO receptor
        "extra_hosts": [],
        "main_port": 5201,
        "probe_port": 5202,
        "extra_ports": [],
        "extra_targets": {},
    },
    "s2": {
        "id": "s2",
        "desc": {
            "sl": "H1+H2 (Leaf1) + H7 (Leaf3) → H5 — 4 flows, mismo destino, 2 leafs origen",
            "j3c": "H1+H2+H3 (Edge1) → H5 — 4 flows, mismo destino"
        },
        "main_host": "H1",
        "probe_host": "H4",
        "target_host": {"sl": "H5", "j3c": "H5"},
        "extra_hosts": {"sl": ["H2", "H7"], "j3c": ["H2", "H3"]},
        "main_port": 5201,
        "probe_port": 5202,
        "extra_ports": [5203, 5204],
        "extra_targets": {"sl": {"H2": "H5", "H7": "H5"}},  # Todos a H5
    }
}

# ============================================================
# COLORES
# ============================================================

class C:
    OK = "\033[92m"
    WARN = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    END = "\033[0m"

def ok(msg): print(f"  {C.OK}OK{C.END}  {msg}")
def warn(msg): print(f"  {C.WARN}WARN{C.END}  {msg}")
def fail(msg): print(f"  {C.FAIL}FAIL{C.END}  {msg}")
def info(msg): print(f"  ->  {msg}")

# ============================================================
# HELPERS SSH
# ============================================================

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
    for h in hosts:
        kill_iperf(h)

def extra_target_hosts(topology):
    """Hosts destino secundarios (distintos de target_host)"""
    extra_targets = SCENARIO.get("extra_targets", {}).get(topology, {})
    return {h for h in extra_targets.values() if h != SCENARIO["target_host"]}

# ============================================================
# ESTADÍSTICAS
# ============================================================

def compute_rsd(values):
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def nombre_archivo(topology, scenario, pkt_size, rep):
    return (
        f"{topology}_{EXPERIMENT_BASE}{scenario}"
        f"_pkt{pkt_size:04d}"
        f"_rep{rep:02d}.json"
    )

# ============================================================
# SERVIDORES
# ============================================================

def start_iperf_servers(topology, dry_run):
    if dry_run:
        return
    
    target = SCENARIO["target_host"]
    kill_iperf(target)
    time.sleep(0.5)
    
    # Servidor para main flow
    ssh_bg(target, f"iperf3 -s -p {SCENARIO['main_port']}")
    
    # Servidor para probe flow
    ssh_bg(target, f"iperf3 -s -p {SCENARIO['probe_port']}")
    
    # Servidores para extra flows
    for port in SCENARIO["extra_ports"]:
        ssh_bg(target, f"iperf3 -s -p {port}")
    
    time.sleep(2)
    ok(f"Servidores activos en {target}")

def stop_iperf_servers(topology, dry_run):
    if not dry_run:
        kill_iperf(SCENARIO["target_host"])

# ============================================================
# FLUJOS IPERF3
# ============================================================

def run_main_flow(pkt_size, results, errors, dry_run):
    """
    Flujo main (fondo) con carga fija de MAIN_RATE_MBPS
    """
    if dry_run:
        results["throughput_mbps"] = MAIN_RATE_MBPS * 0.98
        results["mean_rtt_ms"] = 2.5
        return
    
    ip_target = HOSTS[SCENARIO["target_host"]]["ip"]
    cmd = (
        f"iperf3 -c {ip_target} -p {SCENARIO['main_port']}"
        f" -t {DURATION} -b {MAIN_RATE_MBPS}M -C cubic -l {pkt_size} -J"
    )
    
    try:
        res = ssh_run(SCENARIO["main_host"], cmd, timeout=DURATION + 20)
        if res.returncode != 0 or not res.stdout.strip():
            errors.append(f"main flow rc={res.returncode}")
            results["throughput_mbps"] = None
            results["mean_rtt_ms"] = None
            return
        
        data = json.loads(res.stdout)
        end = data.get("end", {})
        
        # Throughput
        bps = end.get("sum_sent", {}).get("bits_per_second", 0)
        results["throughput_mbps"] = round(bps / 1e6, 2)
        
        # RTT: extraer mean_rtt de los streams
        streams = end.get("streams", [])
        rtts = [s.get("rtt_ms") for s in streams if s.get("rtt_ms") is not None]
        results["mean_rtt_ms"] = round(statistics.mean(rtts), 3) if rtts else None
        
    except Exception as e:
        errors.append(f"main: {e}")
        results["throughput_mbps"] = None
        results["mean_rtt_ms"] = None

def run_probe_flow(pkt_size, results, errors, dry_run):
    """
    Flujo probe (mide RTT) con carga fija de PROBE_RATE_MBPS
    """
    if dry_run:
        results["throughput_mbps"] = PROBE_RATE_MBPS * 0.98
        results["mean_rtt_ms"] = 2.5
        return
    
    ip_target = HOSTS[SCENARIO["target_host"]]["ip"]
    cmd = (
        f"iperf3 -c {ip_target} -p {SCENARIO['probe_port']}"
        f" -t {DURATION} -b {PROBE_RATE_MBPS}M -C cubic -l {pkt_size} -J"
    )
    
    try:
        res = ssh_run(SCENARIO["probe_host"], cmd, timeout=DURATION + 20)
        if res.returncode != 0 or not res.stdout.strip():
            errors.append(f"probe rc={res.returncode}")
            results["throughput_mbps"] = None
            results["mean_rtt_ms"] = None
            return
        
        data = json.loads(res.stdout)
        end = data.get("end", {})
        
        # Throughput
        bps = end.get("sum_sent", {}).get("bits_per_second", 0)
        results["throughput_mbps"] = round(bps / 1e6, 2)
        
        # RTT: extraer mean_rtt de los streams
        streams = end.get("streams", [])
        rtts = [s.get("rtt_ms") for s in streams if s.get("rtt_ms") is not None]
        results["mean_rtt_ms"] = round(statistics.mean(rtts), 3) if rtts else None
        
    except Exception as e:
        errors.append(f"probe: {e}")
        results["throughput_mbps"] = None
        results["mean_rtt_ms"] = None

def run_extra_flow(host_key, port, pkt_size, results, errors, dry_run):
    """
    Flujo extra (fondo) con carga fija de MAIN_RATE_MBPS
    """
    if dry_run:
        results["throughput_mbps"] = MAIN_RATE_MBPS * 0.98
        results["mean_rtt_ms"] = 2.5
        return
    
    # Determinar destino (balanceo manual si existe)
    extra_targets = SCENARIO.get("extra_targets", {}).get(TOPOLOGY, {})
    target_host = extra_targets.get(host_key, SCENARIO["target_host"])
    ip_target = HOSTS[target_host]["ip"]
    
    cmd = (
        f"iperf3 -c {ip_target} -p {port}"
        f" -t {DURATION} -b {MAIN_RATE_MBPS}M -C cubic -l {pkt_size} -J"
    )
    
    try:
        res = ssh_run(host_key, cmd, timeout=DURATION + 20)
        if res.returncode != 0 or not res.stdout.strip():
            errors.append(f"{host_key} extra rc={res.returncode}")
            results["throughput_mbps"] = None
            results["mean_rtt_ms"] = None
            return
        
        data = json.loads(res.stdout)
        end = data.get("end", {})
        
        bps = end.get("sum_sent", {}).get("bits_per_second", 0)
        results["throughput_mbps"] = round(bps / 1e6, 2)
        
        streams = end.get("streams", [])
        rtts = [s.get("rtt_ms") for s in streams if s.get("rtt_ms") is not None]
        results["mean_rtt_ms"] = round(statistics.mean(rtts), 3) if rtts else None
        
    except Exception as e:
        errors.append(f"{host_key} extra: {e}")
        results["throughput_mbps"] = None
        results["mean_rtt_ms"] = None

# ============================================================
# LOOP PRINCIPAL
# ============================================================

def run_protocol(topology, scenario, pkt_size, out_dir, dry_run):
    print(f"\n  {C.BOLD}── PKT {pkt_size}B ──{C.END}")
    
    rtts = []
    reps = 0
    converged = False
    
    while reps < REPS_MAX:
        reps += 1
        
        if reps > 1:
            for i in range(COOLDOWN, 0, -1):
                print(f"\r  Rep {reps:02d}/{REPS_MAX} enfriando {i}s...  ",
                      end="", flush=True)
                time.sleep(1)
        
        print(f"\r  Rep {reps:02d}/{REPS_MAX} midiendo...  ",
              end="", flush=True)
        
        # Preparar threads
        res_main = {}; err_main = []
        res_probe = {}; err_probe = []
        
        threads = [
            threading.Thread(target=run_main_flow,
                             args=(pkt_size, res_main, err_main, dry_run)),
            threading.Thread(target=run_probe_flow,
                             args=(pkt_size, res_probe, err_probe, dry_run)),
        ]
        
        # Extra flows (S2)
        extra_results = []
        extra_errors = []
        if SCENARIO["extra_hosts"]:
            for i, host in enumerate(SCENARIO["extra_hosts"]):
                res = {}; err = []
                threads.append(threading.Thread(
                    target=run_extra_flow,
                    args=(host, SCENARIO["extra_ports"][i], pkt_size, res, err, dry_run)
                ))
                extra_results.append(res)
                extra_errors.append(err)
        
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=DURATION + 35)
        
        # Recolectar errores
        for e in err_main + err_probe:
            print(f"\n    WARN {e}", end="")
        for err_list in extra_errors:
            for e in err_list:
                print(f"\n    WARN {e}", end="")
        
        # Extraer RTT del probe
        probe_rtt = res_probe.get("mean_rtt_ms")
        if probe_rtt is not None:
            rtts.append(probe_rtt)
        
        rsd = compute_rsd(rtts) if len(rtts) > 1 else 99.0
        avg_str = f"{probe_rtt:.3f}ms" if probe_rtt else "N/A"
        rsd_str = f"{rsd:.1f}%" if rsd is not None else "N/A"
        
        print(f"\r  Rep {reps:02d}/{REPS_MAX} "
              f"RTT={avg_str} RSD={rsd_str}   ",
              end="", flush=True)
        
        # Guardar JSON
        fname = nombre_archivo(topology, scenario, pkt_size, reps)
        fpath = out_dir / fname
        record = {
            "_meta": {
                "experiment": f"{EXPERIMENT_BASE}{scenario}",
                "topology": topology,
                "scenario": SCENARIO["id"],
                "pkt_size_b": pkt_size,
                "rep": reps,
                "duration_s": DURATION,
                "cooldown_s": COOLDOWN,
                "main_rate_mbps": MAIN_RATE_MBPS,
                "probe_rate_mbps": PROBE_RATE_MBPS,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "rfc_reference": "RFC8239 §3 — Latency Testing",
                "mean_rtt_ms": probe_rtt,
            },
            "main_flow": res_main,
            "probe_flow": res_probe,
        }
        if extra_results:
            for i, res in enumerate(extra_results):
                record[f"extra_{i+1}_flow"] = res
        
        if not dry_run:
            fpath.write_text(json.dumps(record, indent=2))
        
        if reps >= REPS_MIN and rsd is not None and rsd < RSD_TARGET:
            converged = True
            break
    
    avg_rtt = statistics.mean(rtts) if rtts else None
    print(f"\n  -> {reps} reps | RTT avg={avg_rtt:.3f}ms | "
          f"{'OK converge' if converged else 'WARN no converge'}")
    
    return {
        "pkt_size_b": pkt_size,
        "mean_rtt_ms": round(avg_rtt, 3) if avg_rtt else None,
        "reps": reps,
        "converged": converged,
        "rsd_pct": round(compute_rsd(rtts), 2) if rtts else None,
    }

# ============================================================
# EXPERIMENTO COMPLETO
# ============================================================

def run_experiment(topology, scenario, dry_run):
    out_dir = OUTPUT_BASE / topology / "fase2_latency"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*70}")
    print(f"  F2-{scenario.upper()}  |  {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Escenario: {SCENARIO['desc'][topology]}")
    print(f"  Main rate: {MAIN_RATE_MBPS} Mbps | Probe rate: {PROBE_RATE_MBPS} Mbps")
    print(f"  Tamaños: {PKT_SIZES} bytes")
    print(f"  Reps: {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    print(f"  Cooldown: {COOLDOWN}s")
    print(f"  Salida: {out_dir}")
    if dry_run:
        print("  MODO: DRY-RUN")
    print(f"{'='*70}\n")
    
    # Preflight
    if not dry_run:
        print("-- Preflight " + "-"*50)
        all_hosts = [SCENARIO["main_host"], SCENARIO["probe_host"], SCENARIO["target_host"]]
        all_hosts += SCENARIO["extra_hosts"] if isinstance(SCENARIO["extra_hosts"], list) else []
        all_ok = True
        for name in set(all_hosts):
            res = ssh_run(name, "iperf3 --version 2>&1 | head -1", timeout=10)
            ok_flag = res.returncode == 0
            ver = res.stdout.strip()[:40] if ok_flag else "no encontrado"
            print(f"  {'OK' if ok_flag else 'FAIL'} {name}  {HOSTS[name]['ip']}  {ver}")
            if not ok_flag:
                all_ok = False
        if not all_ok:
            fail("Preflight falló")
            sys.exit(1)
        print()
    
    start_iperf_servers(topology, dry_run)
    
    all_results = []
    for i, pkt_size in enumerate(PKT_SIZES):
        result = run_protocol(topology, scenario, pkt_size, out_dir, dry_run)
        all_results.append(result)
        if i < len(PKT_SIZES) - 1 and not dry_run:
            time.sleep(PKT_PAUSE)
    
    stop_iperf_servers(topology, dry_run)
    
    # Resumen
    print(f"\n\n{'='*70}")
    print(f"  RESUMEN F2-{scenario.upper()} — {topology.upper()}")
    print(f"  {'PKT':>8}  {'RTT avg (ms)':>14}  {'RSD%':>8}  {'Reps':>6}  {'Converge'}")
    print(f"  {'─'*8}  {'─'*14}  {'─'*8}  {'─'*6}  {'─'*8}")
    for r in all_results:
        rtt_str = f"{r['mean_rtt_ms']:.3f}" if r['mean_rtt_ms'] else "N/A"
        rsd_str = f"{r['rsd_pct']:.1f}%" if r['rsd_pct'] else "N/A"
        conv = "OK" if r['converged'] else "WARN"
        print(f"  {r['pkt_size_b']:>8}  {rtt_str:>14}  {rsd_str:>8}  {r['reps']:>6}  {conv}")
    print(f"{'='*70}\n")
    
    summary_path = out_dir / f"{topology}_{EXPERIMENT_BASE}{scenario}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment": f"{EXPERIMENT_BASE}{scenario}",
        "topology": topology,
        "scenario": SCENARIO["id"],
        "rfc_reference": "RFC8239 §3 — Latency Testing",
        "main_rate_mbps": MAIN_RATE_MBPS,
        "probe_rate_mbps": PROBE_RATE_MBPS,
        "pkt_sizes": PKT_SIZES,
        "rsd_target": RSD_TARGET,
        "reps_min": REPS_MIN,
        "reps_max": REPS_MAX,
        "duration_s": DURATION,
        "cooldown_s": COOLDOWN,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "results": all_results,
    }, indent=2))
    ok(f"Summary: {summary_path}")

# ============================================================
# ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="F2: Latency Testing (CORREGIDO) — RTT con iperf3"
    )
    parser.add_argument(
        "--scenario",
        choices=["s1", "s2", "all"],
        required=True,
        help="Escenario: s1 (2 flows) o s2 (4 flows)"
    )
    parser.add_argument(
        "--topology",
        choices=["sl", "j3c"],
        default="sl",
        help="Topología: sl (spine-leaf) o j3c (jerárquica 3 capas)"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-preflight", action="store_true")
    args = parser.parse_args()
    
    global HOSTS, SCENARIO, TOPOLOGY
    TOPOLOGY = args.topology
    
    HOSTS = TOPOLOGIES[args.topology]["hosts"]
    
    if args.scenario == "all":
        scenarios = ["s1", "s2"]
    else:
        scenarios = [args.scenario]
    
    for sc in scenarios:
        sc_cfg = SCENARIOS[sc]
        SCENARIO = {
            "id": sc_cfg["id"],
            "desc": sc_cfg["desc"],
            "main_host": sc_cfg["main_host"],
            "probe_host": sc_cfg["probe_host"],
            "target_host": sc_cfg["target_host"][args.topology],
            "extra_hosts": sc_cfg["extra_hosts"].get(args.topology, []) if isinstance(sc_cfg["extra_hosts"], dict) else sc_cfg["extra_hosts"],
            "main_port": sc_cfg["main_port"],
            "probe_port": sc_cfg["probe_port"],
            "extra_ports": sc_cfg["extra_ports"],
            "extra_targets": sc_cfg.get("extra_targets", {}),
        }
        
        run_experiment(
            topology=args.topology,
            scenario=sc,
            dry_run=args.dry_run
        )

if __name__ == "__main__":
    main()