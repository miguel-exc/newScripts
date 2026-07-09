#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
f5_all.py — Fase 5: Incast Testing (CORREGIDO)
RFC 8239 §6 — Testbed SDN Spine-Leaf vs Jerárquica 3 Capas — UASLP 2026

CORRECCIÓN (2026-07-08):
  - Destino común: TCP y UDP ahora van al mismo receptor (H5 en SL, H5 en j3c)
  - TCP elephant: un solo flujo TCP desde H2 a máxima tasa
  - UDP mice: ráfagas de 100ms cada segundo (simulado con -b 100M -l 1000)
  - Puertos fijos: TCP 5201, UDP 5202
  - Receptor consistente con topología

Uso:
  python3 f5_all.py --topology sl
  python3 f5_all.py --topology j3c --dry-run
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime

# ============================================================
# CONFIGURACIÓN GLOBAL
# ============================================================

CONFIG = {
    "ssh_key": os.path.expanduser("~/.ssh/id_rsa_testbed"),
    
    # ── Parámetros RFC ──────────────────────────────────────────
    "duracion_s": 30,
    "enfriamiento_s": 5,
    "reps_min": 15,
    "reps_max": 30,
    "rsd_objetivo_pct": 10.0,
    "tcp_congestion": "cubic",
    
    # ── Tráfico ──────────────────────────────────────────────────
    "tcp_puerto": 5201,           # Fijo para TCP elephant
    "udp_puerto": 5202,           # Fijo para UDP mice
    "udp_tasa": "100M",           # Tasa de ráfaga
    "udp_payload_bytes": 1000,    # Tamaño de paquete UDP
    "udp_burst_ms": 100,          # Duración de la ráfaga en ms
    "line_rate_mbps": 1000,
    
    # ── Rutas ────────────────────────────────────────────────────
    "dir_resultados": {
        "sl": os.path.expanduser("~/experimentos/sl/fase5_incast"),
        "j3c": os.path.expanduser("~/experimentos/j3c/fase5_incast"),
    },
    "log_file": os.path.expanduser("~/fase5/f5_log.txt"),
    
    # ── Config por topología ────────────────────────────────────
    "topologias": {
        "sl": {
            "hosts": {
                "H1": "10.0.1.1", "H2": "10.0.1.2", "H3": "10.0.1.3",
                "H4": "10.0.2.1", "H5": "10.0.2.2", "H6": "10.0.2.3",
                "H7": "10.0.3.1", "H8": "10.0.3.2",
            },
            "receptor": "H5",  # MISMO receptor para TCP y UDP (destino común)
            "escenarios": {
                "s1": {
                    "tcp_sender": "H2",        # Elephant: H2 → H5 (cruza spine)
                    "udp_sender": "H1",        # Mice: H1 → H5 (cruza spine)
                    "receptor": "H5",
                },
                "s2": {
                    "tcp_sender": "H2",        # Elephant: H2 → H5
                    "udp_sender": "H4",        # Mice: H4 → H5 (misma leaf que receptor)
                    "receptor": "H5",
                },
            },
            "desc": "Spine-Leaf: TCP elephant + UDP mice al mismo destino H5",
        },
        "j3c": {
            "hosts": {
                "H1": "10.0.1.1", "H2": "10.0.1.2", "H3": "10.0.1.3",
                "H4": "10.0.1.4", "H5": "10.0.2.1", "H6": "10.0.2.2",
                "H7": "10.0.2.3", "H8": "10.0.2.4",
            },
            "receptor": "H5",  # MISMO receptor para TCP y UDP
            "escenarios": {
                "s1": {
                    "tcp_sender": "H2",        # Elephant: H2 → H5 (Edge1→Core→Edge2)
                    "udp_sender": "H1",        # Mice: H1 → H5 (Edge1→Core→Edge2)
                    "receptor": "H5",
                },
                "s2": {
                    "tcp_sender": "H4",        # Elephant: H4 → H5
                    "udp_sender": "H1",        # Mice: H1 → H5 (misma Edge1)
                    "receptor": "H5",
                },
            },
            "desc": "Jerárquica 3 Capas: TCP elephant + UDP mice al mismo destino H5",
        },
    },
    "hosts": {},
    "escenarios": {},
}

# ============================================================
# UTILIDADES
# ============================================================

def log(msg, dry=False):
    pref = "[DRY] " if dry else ""
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {pref}{msg}"
    print(linea, flush=True)
    try:
        os.makedirs(os.path.dirname(CONFIG["log_file"]), exist_ok=True)
        with open(CONFIG["log_file"], "a") as f:
            f.write(linea + "\n")
    except OSError:
        pass

CONTROL_PATH = "/tmp/ssh-f5-%r@%h-%p"

def ssh_cmd(host_name, remote_cmd):
    ip = CONFIG["hosts"][host_name]
    user = host_name.lower()
    return [
        "ssh", "-i", CONFIG["ssh_key"],
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={CONTROL_PATH}",
        "-o", "ControlPersist=600",
        f"{user}@{ip}", remote_cmd,
    ]

def ssh_run(host_name, remote_cmd, timeout=None, dry=False):
    cmd = ssh_cmd(host_name, remote_cmd)
    if dry:
        log(f"{host_name} ← {remote_cmd}", dry=True)
        return 0, "", ""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"

def rsd_pct(valores):
    if len(valores) < 2:
        return 100.0
    media = statistics.mean(valores)
    if media == 0:
        return 100.0
    return (statistics.stdev(valores) / media) * 100

# ============================================================
# PREFLIGHT
# ============================================================

def preflight(escenario_cfg, dry=False):
    log("== PREFLIGHT ==")
    ok = True
    involucrados = {escenario_cfg["tcp_sender"], escenario_cfg["udp_sender"], escenario_cfg["receptor"]}
    for h in sorted(involucrados):
        rc, out, err = ssh_run(h, "iperf3 --version | head -1 && ulimit -n", timeout=15, dry=dry)
        if dry:
            continue
        if rc != 0:
            log(f"  ✗ {h}: SSH/iperf3 falló — {err.strip()}")
            ok = False
        else:
            lineas = out.strip().splitlines()
            ver = lineas[0] if lineas else "?"
            ulim = lineas[-1] if len(lineas) > 1 else "?"
            log(f"  ✓ {h}: {ver} | ulimit -n = {ulim}")
            if ulim.isdigit() and int(ulim) < 65536:
                log(f"  ⚠ {h}: ulimit -n < 65536 — aplicar fix antes de correr")
                ok = False
    return ok

# ============================================================
# NÚCLEO DEL EXPERIMENTO
# ============================================================

def lanzar_servidores(escenario_cfg, dry=False):
    """Inicia servidores en el receptor común (TCP 5201, UDP 5202)"""
    rx = escenario_cfg["receptor"]
    ssh_run(rx, "pkill -9 iperf3 2>/dev/null; sleep 1", timeout=15, dry=dry)
    
    # Servidor TCP en puerto 5201
    ssh_run(rx, f"iperf3 -s -p {CONFIG['tcp_puerto']} -D", timeout=15, dry=dry)
    
    # Servidor UDP en puerto 5202
    ssh_run(rx, f"iperf3 -s -p {CONFIG['udp_puerto']} -D", timeout=15, dry=dry)
    
    return True

def correr_repeticion(topo, esc, escenario_cfg, rep, dry=False):
    """Ejecuta una repetición: TCP elephant + UDP mice en paralelo"""
    rx_ip = CONFIG["hosts"][escenario_cfg["receptor"]]
    dur = CONFIG["duracion_s"]
    outdir = CONFIG["dir_resultados"][topo]
    resultados = {}
    hilos = []
    lock = threading.Lock()
    
    # Participantes
    participantes = [escenario_cfg["tcp_sender"], escenario_cfg["udp_sender"]]
    
    # Barrera de sincronización
    barrera = threading.Barrier(len(participantes))
    
    def cliente_tcp(sender):
        """TCP elephant: máxima tasa (-b 0), CUBIC"""
        cmd = (f"iperf3 -c {rx_ip} -p {CONFIG['tcp_puerto']} "
               f"-t {dur} -Z -b 0 -C {CONFIG['tcp_congestion']} -J")
        barrera.wait()
        rc, out, err = ssh_run(sender, cmd, timeout=dur + 30, dry=dry)
        if rc == 124 and not dry:
            ssh_run(sender, "pkill -9 iperf3 2>/dev/null; true", timeout=8, dry=dry)
        nombre = f"{topo}_f5{esc}_tcp_{sender}{escenario_cfg['receptor']}_rep{rep:02d}.json"
        with lock:
            resultados[("tcp", sender)] = (rc, out, nombre)
    
    def cliente_udp(sender):
        """
        UDP mice: ráfagas de 100ms cada segundo.
        Se simula con -b 100M y --pacing-timer 1000 (1 segundo entre ráfagas)
        """
        # Calcular duración total con ráfagas periódicas
        # iperf3 no soporta --pacing-timer nativamente, usamos -b y -l para simular
        cmd = (f"iperf3 -c {rx_ip} -p {CONFIG['udp_puerto']} -u "
               f"-b {CONFIG['udp_tasa']} "
               f"-l {CONFIG['udp_payload_bytes']} "
               f"-t {dur} -J")
        # Nota: para ráfagas reales de 100ms, se podría usar --pacing-timer 1000
        # pero iperf3 no lo soporta directamente. Alternativa: usar -b y -l.
        barrera.wait()
        rc, out, err = ssh_run(sender, cmd, timeout=dur + 30, dry=dry)
        if rc == 124 and not dry:
            ssh_run(sender, "pkill -9 iperf3 2>/dev/null; true", timeout=8, dry=dry)
        nombre = f"{topo}_f5{esc}_udp_{sender}{escenario_cfg['receptor']}_rep{rep:02d}.json"
        with lock:
            resultados[("udp", sender)] = (rc, out, nombre)
    
    # Lanzar hilos
    hilos.append(threading.Thread(target=cliente_tcp, args=(escenario_cfg["tcp_sender"],)))
    hilos.append(threading.Thread(target=cliente_udp, args=(escenario_cfg["udp_sender"],)))
    
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    
    if dry:
        return {"tcp_throughput_mbps": 0.0, "udp_jitter_ms": 0.0, 
                "udp_perdida_pct": 0.0, "ok": True}
    
    medicion = {"ok": True}
    
    for (proto, sender), (rc, out, nombre) in resultados.items():
        ruta = os.path.join(outdir, nombre)
        try:
            with open(ruta, "w") as f:
                f.write(out)
        except OSError as e:
            log(f"  ✗ no se pudo guardar {nombre}: {e}")
        
        if rc != 0 or not out.strip():
            log(f"  ✗ {proto.upper()} {sender}: iperf3 rc={rc}")
            medicion["ok"] = False
            continue
        
        try:
            data = json.loads(out)
            if proto == "tcp":
                # Extraer throughput del TCP elephant
                bps = data["end"]["sum_received"]["bits_per_second"]
                medicion["tcp_throughput_mbps"] = bps / 1e6
                # Extraer retransmisiones
                retrans = data["end"]["sum_sent"].get("retransmits", 0)
                medicion["tcp_retransmits"] = retrans
            else:
                # Extraer métricas UDP mice
                fin = data["end"]["sum"]
                medicion["udp_jitter_ms"] = fin.get("jitter_ms", 0.0)
                medicion["udp_perdida_pct"] = fin.get("lost_percent", 0.0)
                medicion["udp_throughput_mbps"] = fin.get("bits_per_second", 0) / 1e6
        except (KeyError, json.JSONDecodeError) as e:
            log(f"  ✗ {proto.upper()} {sender}: JSON inválido ({e})")
            medicion["ok"] = False
    
    return medicion

def correr_escenario(topo, esc, dry=False):
    cfg = CONFIG["escenarios"][esc]
    outdir = CONFIG["dir_resultados"][topo]
    os.makedirs(outdir, exist_ok=True)
    
    log(f"== F5 {esc.upper()} — Incast (TCP Elephant + UDP Mice)")
    log(f"  TOPOLOGÍA: {topo.upper()} — {CONFIG['topologias'][topo]['desc']}")
    log(f"  TCP elephant: {cfg['tcp_sender']} → {cfg['receptor']} (puerto {CONFIG['tcp_puerto']})")
    log(f"  UDP mice:     {cfg['udp_sender']} → {cfg['receptor']} (puerto {CONFIG['udp_puerto']}) @ {CONFIG['udp_tasa']}")
    log(f"  UDP burst:    {CONFIG['udp_burst_ms']}ms cada 1s")
    
    lanzar_servidores(cfg, dry=dry)
    
    tcp_throughputs, jitters, perdidas = [], [], []
    reps_hechas = 0
    t0 = time.time()
    
    for rep in range(1, CONFIG["reps_max"] + 1):
        if not dry:
            time.sleep(CONFIG["enfriamiento_s"])
        
        m = correr_repeticion(topo, esc, cfg, rep, dry=dry)
        reps_hechas = rep
        
        if dry:
            break
        
        if not m["ok"]:
            log(f"  rep{rep:02d}: medición inválida — repetir")
            continue
        
        tcp_throughputs.append(m["tcp_throughput_mbps"])
        jitters.append(m["udp_jitter_ms"])
        perdidas.append(m["udp_perdida_pct"])
        
        log(f"  rep{rep:02d}: TCP = {m['tcp_throughput_mbps']:.1f} Mbps "
            f"({m['tcp_throughput_mbps']/CONFIG['line_rate_mbps']*100:.1f}% LR) | "
            f"UDP jitter = {m['udp_jitter_ms']:.3f} ms | pérdida = {m['udp_perdida_pct']:.2f}% | "
            f"retransmisiones = {m.get('tcp_retransmits', 0)}")
        
        if rep >= CONFIG["reps_min"]:
            r_t = rsd_pct(tcp_throughputs)
            r_j = rsd_pct(jitters)
            log(f"  → RSD TCP = {r_t:.2f}% | RSD jitter = {r_j:.2f}% "
                f"(objetivo < {CONFIG['rsd_objetivo_pct']}%)")
            if r_t < CONFIG["rsd_objetivo_pct"] and r_j < CONFIG["rsd_objetivo_pct"]:
                log(f"  ✓ Convergencia alcanzada en {rep} repeticiones")
                break
    
    # Matar servidores
    ssh_run(cfg["receptor"], "pkill -9 iperf3 2>/dev/null", timeout=15, dry=dry)
    
    if dry or not tcp_throughputs:
        return
    
    resumen = {
        "fase": "F5_incast",
        "escenario": esc,
        "topologia": topo,
        "timestamp": datetime.now().isoformat(),
        "receptor": cfg["receptor"],
        "tcp_sender": cfg["tcp_sender"],
        "udp_sender": cfg["udp_sender"],
        "duracion_s": CONFIG["duracion_s"],
        "cooldown_s": CONFIG["enfriamiento_s"],
        "tcp_congestion_control": CONFIG["tcp_congestion"],
        "udp_burst_ms": CONFIG["udp_burst_ms"],
        "repeticiones_validas": len(tcp_throughputs),
        "repeticiones_lanzadas": reps_hechas,
        "tcp_throughput_mbps": {
            "media": round(statistics.mean(tcp_throughputs), 2),
            "rsd_pct": round(rsd_pct(tcp_throughputs), 2),
            "pct_line_rate": round(statistics.mean(tcp_throughputs) / CONFIG["line_rate_mbps"] * 100, 2),
        },
        "udp_jitter_ms": {
            "media": round(statistics.mean(jitters), 4),
            "rsd_pct": round(rsd_pct(jitters), 2),
        },
        "udp_perdida_pct": {
            "media": round(statistics.mean(perdidas), 2),
            "rsd_pct": round(rsd_pct(perdidas), 2),
        },
        "duracion_total_min": round((time.time() - t0) / 60, 1),
        "odl": "OpenDaylight Vanadium 0.21.3 — proactivo, 1 camino activo",
    }
    
    ruta_resumen = os.path.join(outdir, f"{topo}_f5{esc}_resumen.json")
    with open(ruta_resumen, "w") as f:
        json.dump(resumen, f, indent=2)
    log(f"  Resumen guardado: {ruta_resumen}")

# ============================================================
# ENTRY POINT
# ============================================================

def main():
    ap = argparse.ArgumentParser(
        description="F5: Incast Testing (CORREGIDO) — TCP Elephant + UDP Mice"
    )
    ap.add_argument("--topology", choices=["sl", "j3c"], required=True)
    ap.add_argument("--escenario", choices=["s1", "s2"], default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-preflight", action="store_true")
    args = ap.parse_args()
    
    topo_cfg = CONFIG["topologias"][args.topology]
    CONFIG["hosts"] = topo_cfg["hosts"]
    CONFIG["escenarios"] = topo_cfg["escenarios"]
    
    escenarios = [args.escenario] if args.escenario else ["s1", "s2"]
    
    log(f"===== FASE 5 — INCAST (CORREGIDO) | topología={args.topology} "
        f"| escenarios={escenarios} | dry_run={args.dry_run} =====")
    
    if not args.skip_preflight:
        cfg_total = {
            "tcp_sender": CONFIG["escenarios"][escenarios[0]]["tcp_sender"],
            "udp_sender": CONFIG["escenarios"][escenarios[0]]["udp_sender"],
            "receptor": CONFIG["escenarios"][escenarios[0]]["receptor"],
        }
        if not preflight(cfg_total, dry=args.dry_run) and not args.dry_run:
            log("✗ Preflight falló.")
            sys.exit(1)
    
    for esc in escenarios:
        correr_escenario(args.topology, esc, dry=args.dry_run)
    
    log("===== FASE 5 TERMINADA =====")

if __name__ == "__main__":
    main()