#!/usr/bin/env python3
"""
f1_all.py — F1 Line-Rate Testing (Unificado) — VERSIÓN CORREGIDA
RFC 8239 §2 — Testbed SDN Spine-Leaf vs Jerárquica 3 Capas — UASLP 2026

OBJETIVO: Comparar Spine-Leaf (con balanceo manual) vs Jerárquica 3 Capas
en términos de throughput, equidad, latencia y jitter.

CORRECCIONES (2026-07-09):
  - j3c A1 corregido: H1→H5 (Edge1→Edge2, cruza Core1)
  - j3c B1 corregido: H2→H4 (intra-Edge1), H3→H7 (Edge1→Edge2)
  - j3c B2 corregido: H1→H5, H2→H6, H3→H7, H4→H8 (todos Edge1→Edge2)
  - SL se mantiene con balanceo manual S1/S2

Spine-Leaf: usa ambos spines (S1 y S2) de forma balanceada (pseudo-ECMP).
Jerárquica 3 Capas: usa Core1 como punto de convergencia.

Ejecutar desde H3 (host de control):
    python3 f1_all.py --experiment all --topology sl
    python3 f1_all.py --experiment all --topology j3c
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
from typing import Dict, List, Optional, Tuple, Any

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN GLOBAL — HOMOGENIZADA
# ══════════════════════════════════════════════════════════════════════════════

DURATION = 30
COOLDOWN = 5          # ESTANDARIZADO: 5s para F1
PKT_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
PKT_SIZES = [64, 128, 256, 512, 1024, 1518]
TCP_CONGESTION = "cubic"

KEY_PATH = Path.home() / ".ssh" / "id_rsa_testbed"
OUTPUT_BASE = Path.home() / "experimentos"

# ══════════════════════════════════════════════════════════════════════════════
#  DEFINICIÓN DE EXPERIMENTOS — CORREGIDA
# ══════════════════════════════════════════════════════════════════════════════

# NOTA: Los experimentos se definen de forma genérica y se adaptan según topología.
# Para j3c, los pares se reasignan en tiempo de ejecución.

EXPERIMENTS = {
    "a1": {
        "desc": "Par único (carga baja) — baseline",
        "note": "F1-A1: par único cruzando el punto central de la topología",
    },
    "b1": {
        "desc": "2 pares cross-leaf",
        "note": "F1-B1: 2 pares cross-leaf simultáneos",
    },
    "b2": {
        "desc": "4 pares full-mesh",
        "note": "F1-B2: 4 flujos full-mesh",
    },
}

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN POR TOPOLOGÍA — CORREGIDA
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
        # SL: Pares específicos por escenario (balanceado entre S1 y S2)
        "pairs": {
            "a1": [
                {"id": "p1", "client": "H1", "server": "H4", "port": 5201},  # Leaf1→Leaf2 (S1)
            ],
            "b1": [
                {"id": "p1", "client": "H2", "server": "H4", "port": 5201},  # Leaf1→Leaf2 (S1)
                {"id": "p2", "client": "H3", "server": "H7", "port": 5202},  # Leaf1→Leaf3 (S2)
            ],
            "b2": [
                {"id": "p1", "client": "H1", "server": "H4", "port": 5201},  # Leaf1→Leaf2 (S1)
                {"id": "p2", "client": "H2", "server": "H7", "port": 5202},  # Leaf1→Leaf3 (S2)
                {"id": "p3", "client": "H3", "server": "H5", "port": 5203},  # Leaf1→Leaf2 (S1)
                {"id": "p4", "client": "H6", "server": "H8", "port": 5204},  # Leaf2→Leaf3 (S2)
            ],
        },
        "desc": "Spine-Leaf (SL) — balanceado entre S1 y S2",
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
        # j3c: Pares específicos por escenario (todos cruzan Core1 cuando es cross-edge)
        "pairs": {
            "a1": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # Edge1→Edge2 (CRUZA Core1)
            ],
            "b1": [
                {"id": "p1", "client": "H2", "server": "H4", "port": 5201},  # Intra-Edge1 (no cruza Core)
                {"id": "p2", "client": "H3", "server": "H7", "port": 5202},  # Edge1→Edge2 (CRUZA Core1)
            ],
            "b2": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # Edge1→Edge2 (CRUZA Core1)
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # Edge1→Edge2 (CRUZA Core1)
                {"id": "p3", "client": "H3", "server": "H7", "port": 5203},  # Edge1→Edge2 (CRUZA Core1)
                {"id": "p4", "client": "H4", "server": "H8", "port": 5204},  # Edge1→Edge2 (CRUZA Core1)
            ],
        },
        "desc": "Jerárquica 3 Capas (j3c) — flujos cruzan Core1 cuando son cross-edge",
    },
}

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS SSH
# ══════════════════════════════════════════════════════════════════════════════

_SSH_OPTS = [
    "ssh",
    "-i", str(KEY_PATH),
    "-o", "StrictHostKeyChecking=no",
    "-o", "ConnectTimeout=10",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=15",
]

def ssh_run(host_key: str, cmd: str, timeout: int = 90) -> subprocess.CompletedProcess:
    h = HOSTS[host_key]
    return subprocess.run(
        _SSH_OPTS + [f"{h['user']}@{h['ip']}", cmd],
        capture_output=True, text=True, timeout=timeout,
    )

def ssh_bg(host_key: str, cmd: str) -> None:
    h = HOSTS[host_key]
    subprocess.Popen(
        _SSH_OPTS + [f"{h['user']}@{h['ip']}", f"nohup {cmd} >/dev/null 2>&1 &"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

def kill_iperf(host_key: str) -> None:
    ssh_run(host_key, "pkill -9 iperf3 2>/dev/null; true", timeout=10)

def kill_iperf_all(pairs: List[dict]) -> None:
    """Mata iperf3 en clientes Y servidores de los pares dados."""
    hosts = {p["client"] for p in pairs} | {p["server"] for p in pairs}
    for h in hosts:
        kill_iperf(h)

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS DE ESTADÍSTICAS
# ══════════════════════════════════════════════════════════════════════════════

def inject_meta(data: dict, pair: dict, pkt_size: int, rep: int, 
                topology: str, experiment: str, note: str) -> dict:
    # Extraer cwnd de los intervalos
    snd_cwnd_values = []
    try:
        intervals = data.get("intervals", [])
        for interval in intervals:
            cwnd = interval.get("sum", {}).get("snd_cwnd")
            if cwnd is not None:
                snd_cwnd_values.append(cwnd)
    except (KeyError, ValueError, TypeError):
        pass
    
    cwnd_avg = round(statistics.mean(snd_cwnd_values), 0) if snd_cwnd_values else None
    cwnd_max = round(max(snd_cwnd_values), 0) if snd_cwnd_values else None

    data["_meta"] = {
        "experiment":    experiment,
        "topology":      topology,
        "pair_id":       pair["id"],
        "client_host":   pair["client"],
        "server_host":   pair["server"],
        "server_ip":     HOSTS[pair["server"]]["ip"],
        "pkt_size_b":    pkt_size,
        "rep":           rep,
        "duration_s":    DURATION,
        "cooldown_s":    COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rfc_reference": "RFC8239 §2 — Line-Rate Testing",
        "note":          note,
        "snd_cwnd_avg_bytes": cwnd_avg,
        "snd_cwnd_max_bytes": cwnd_max,
    }
    return data

def compute_rsd(values: List[float]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def extract_mbps(data: dict) -> Optional[float]:
    try:
        return data["end"]["sum_sent"]["bits_per_second"] / 1e6
    except (KeyError, TypeError):
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  LÓGICA DE EXPERIMENTO
# ══════════════════════════════════════════════════════════════════════════════

def start_servers(pairs: List[dict]) -> None:
    kill_iperf_all(pairs)
    time.sleep(1)
    for p in pairs:
        ssh_bg(p["server"], f"iperf3 -s -p {p['port']} ")
    time.sleep(2)

def run_single_pair(pair: dict, pkt_size: int, rep: int, topology: str,
                    experiment: str, note: str, out_dir: Path,
                    results: dict, errors: list, dry_run: bool) -> None:
    fname = (
        f"{topology}_{experiment}_tcp"
        f"_pkt{pkt_size:04d}"
        f"_{pair['id']}"
        f"_rep{rep:02d}.json"
    )
    fpath = out_dir / fname
    srv_ip = HOSTS[pair["server"]]["ip"]

    if dry_run:
        print(f"    [DRY] {pair['id']}: iperf3 -c {srv_ip} -p {pair['port']}"
              f" -t {DURATION} -Z -C {TCP_CONGESTION} -l {pkt_size} -J  ->  {fname}")
        results[pair["id"]] = None
        return

    cmd = (f"iperf3 -c {srv_ip} -p {pair['port']}"
           f" -t {DURATION} -Z -C {TCP_CONGESTION} -l {pkt_size} -J")
    try:
        res = ssh_run(pair["client"], cmd, timeout=DURATION + 20)

        if res.returncode != 0 or not res.stdout.strip():
            errors.append(f"{pair['id']}: iperf3 rc={res.returncode}")
            results[pair["id"]] = None
            return

        data = json.loads(res.stdout)
        data = inject_meta(data, pair, pkt_size, rep, topology, experiment, note)
        fpath.write_text(json.dumps(data, indent=2))
        results[pair["id"]] = extract_mbps(data)

    except Exception as e:
        errors.append(f"{pair['id']}: {e}")
        results[pair["id"]] = None
        kill_iperf(pair["client"])

def run_rep(pairs: List[dict], pkt_size: int, rep: int, topology: str,
            experiment: str, note: str, out_dir: Path, 
            dry_run: bool) -> List[Optional[float]]:
    results = {}
    errors = []
    threads = [
        threading.Thread(
            target=run_single_pair,
            args=(p, pkt_size, rep, topology, experiment, note, out_dir,
                  results, errors, dry_run),
        )
        for p in pairs
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DURATION + 35)

    for err in errors:
        print(f"    WARN {err}")

    return [results.get(p["id"]) for p in pairs]

# ══════════════════════════════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_experiment(experiment: str, topology: str, dry_run: bool) -> None:
    # Obtener pares según topología
    pairs = TOPOLOGIES[topology]["pairs"][experiment]
    exp_config = EXPERIMENTS[experiment]
    note = exp_config["note"]
    desc = exp_config["desc"]
    topo_desc = TOPOLOGIES[topology]["desc"]
    
    out_dir = OUTPUT_BASE / topology / "fase1_linerate"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  F1-{experiment.upper()}  |  Topología: {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Descripción: {desc}")
    print(f"  Topología:   {topo_desc}")
    print(f"  Cooldown:    {COOLDOWN}s  |  TCP: {TCP_CONGESTION}")
    print(f"  Pares:  " + "  ".join(f"{p['id']}:{p['client']}->{p['server']}" for p in pairs))
    print(f"  Salida: {out_dir}")
    print(f"  PKTs:   {PKT_SIZES}")
    print(f"  Reps:   {REPS_MIN}-{REPS_MAX}  (RSD < {RSD_TARGET}%)")
    if dry_run:
        print("  MODO:   DRY-RUN")
    print(f"{'='*70}\n")

    summary = {}

    for pkt_size in PKT_SIZES:
        print(f"-- PKT {pkt_size:>4d} B " + "-"*50)

        throughputs_per_rep = []

        if not dry_run:
            start_servers(pairs)

        rep = 1
        converged = False
        while rep <= REPS_MAX:
            print(f"  Rep {rep:02d}/{REPS_MAX}  ", end="", flush=True)

            mbps_list = run_rep(pairs, pkt_size, rep, topology, 
                               experiment, note, out_dir, dry_run)
            valid = [v for v in mbps_list if v is not None]

            if valid:
                rep_mean = statistics.mean(valid)
                throughputs_per_rep.append(rep_mean)
                print(f"  {rep_mean:8.2f} Mbps  [{len(valid)}/{len(pairs)} ok]",
                      end="")
            else:
                print("  FAIL todos los pares fallaron", end="")

            if rep >= REPS_MIN and len(throughputs_per_rep) >= REPS_MIN:
                rsd = compute_rsd(throughputs_per_rep)
                if rsd is not None:
                    print(f"  RSD={rsd:.1f}%", end="")
                    if rsd < RSD_TARGET:
                        print("  OK converge")
                        converged = True
                        break

            print()
            if rep < REPS_MAX:
                time.sleep(COOLDOWN)
            rep += 1

        if not converged:
            print(f"\n  WARN RSD no convergió en {REPS_MAX} reps")

        final_rsd = compute_rsd(throughputs_per_rep)
        final_mean = statistics.mean(throughputs_per_rep) if throughputs_per_rep else 0.0
        lr_pct = (final_mean / 1000.0) * 100

        summary[pkt_size] = {
            "reps": rep,
            "mean_mbps": round(final_mean, 2),
            "rsd_pct": round(final_rsd, 2) if final_rsd is not None else None,
            "lr_pct": round(lr_pct, 1),
        }

        rsd_str = f"{final_rsd:.1f}%" if final_rsd is not None else "N/A"
        print(f"  -> {rep} reps | {final_mean:.2f} Mbps | RSD={rsd_str} | {lr_pct:.1f}% LR\n")

        if not dry_run:
            kill_iperf_all(pairs)
            if pkt_size != PKT_SIZES[-1]:
                time.sleep(PKT_PAUSE)

    # Tabla resumen
    print(f"\n{'='*70}")
    print(f"  RESUMEN F1-{experiment.upper()} -- {topology.upper()}")
    print(f"  Cooldown: {COOLDOWN}s | TCP: {TCP_CONGESTION}")
    print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
    print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*7}  {'-'*8}")
    for pkt, s in summary.items():
        rsd_str = f"{s['rsd_pct']:.1f}" if s["rsd_pct"] is not None else "N/A"
        print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}"
              f"  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
    print(f"{'='*70}\n")

    summary_path = out_dir / f"{topology}_{experiment}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment":    experiment,
        "topology":      topology,
        "rfc_reference": "RFC8239 §2",
        "desc":          desc,
        "topo_desc":     topo_desc,
        "cooldown_s":    COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "pairs": [{"id": p["id"], "client": p["client"],
                   "server": p["server"]} for p in pairs],
        "pkt_sizes":     PKT_SIZES,
        "rsd_target":    RSD_TARGET,
        "reps_min":      REPS_MIN,
        "reps_max":      REPS_MAX,
        "duration_s":    DURATION,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "results":       {str(k): v for k, v in summary.items()},
    }, indent=2))
    print(f"  Resumen: {summary_path}\n")

# ══════════════════════════════════════════════════════════════════════════════
#  PREFLIGHT
# ══════════════════════════════════════════════════════════════════════════════

def preflight(experiment: str, topology: str) -> bool:
    pairs = TOPOLOGIES[topology]["pairs"][experiment]
    involved = sorted(
        {p["client"] for p in pairs} | {p["server"] for p in pairs}
    )
    print("-- Preflight check " + "-"*52)
    all_ok = True
    for name in involved:
        res = ssh_run(name, "iperf3 --version 2>&1 | head -1", timeout=10)
        ok = res.returncode == 0
        ver = res.stdout.strip()[:55] if ok else res.stderr.strip()[:55]
        print(f"  {'OK' if ok else 'FAIL'} {name:4s}  {HOSTS[name]['ip']:15s}  {ver}")
        if not ok:
            all_ok = False
    print()
    return all_ok

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="F1: Line-Rate Testing Unificado (CORREGIDO)"
    )
    parser.add_argument(
        "--experiment",
        choices=["a1", "b1", "b2", "all"],
        required=True,
        help="Experimento a ejecutar (all = todos secuencialmente)"
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

    global HOSTS
    HOSTS = TOPOLOGIES[args.topology]["hosts"]

    if args.experiment == "all":
        experiments = ["a1", "b1", "b2"]
    else:
        experiments = [args.experiment]

    for exp in experiments:
        if not args.dry_run and not args.skip_preflight:
            if not preflight(exp, args.topology):
                print("  FAIL: Preflight falló. Verifica SSH antes de continuar.")
                sys.exit(1)
        
        run_experiment(
            experiment=exp,
            topology=args.topology,
            dry_run=args.dry_run
        )

if __name__ == "__main__":
    main()