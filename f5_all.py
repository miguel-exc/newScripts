#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
f5_all.py — Fase 5: Incast Testing (Unificado) — VERSIÓN HOMOGENIZADA
RFC 8239 §6 — Testbed SDN Spine-Leaf vs Jerárquica 3 Capas — UASLP 2026

CORRECCIONES METODOLÓGICAS (2026-07-03):
  - Cooldown: 5s entre repeticiones (estandarizado)
  - TCP CUBIC forzado explícitamente (-C cubic)
  - _meta completo: cooldown_s, tcp_congestion_control
  - FIX: en "s1", H4 estaba en el mismo leaf que el receptor H5 (no
    cruzaba spine) -> reemplazado por H8. En "s2", el propio receptor
    H5 aparecía dentro de tcp_senders (auto-tráfico por loopback,
    nunca toca la red física e infla el goodput agregado) y además
    H4/H6 eran intra-leaf como senders TCP.
  - Emisor UDP reasignado de H2 a H4: como sender TCP hacia H5, H4 es
    intra-leaf (inválido), pero como emisor UDP hacia H7 sí cruza spine
    (L2->L3). Esto libera a H2 para ser el 4to sender TCP en "s2" sin
    que ningún host cumpla doble rol (emisor UDP + emisor TCP a la vez),
    evitando confundir el jitter medido con contención local del host.
    tcp_senders "s2" final: [H1,H2,H3,H8] -> H5; udp: H4 -> H7 (igual
    en s1 y s2, para aislar el efecto del fan-in TCP).
  - No se etiqueta S1/S2 explícitamente: el mapeo físico puerto-uplink
    -> spine (UPLINK_PORT) todavía no está confirmado.

Mide TCP Goodput (tráfico stateful) y latencia/jitter UDP (tráfico stateless)
de forma SIMULTÁNEA en un escenario many-to-one (incast).

Uso:
  python3 f5_all.py --topology sl                 # ambos escenarios (balanceado)
  python3 f5_all.py --topology sl --escenario s1  # solo s1
  python3 f5_all.py --topology j3c --dry-run      # ver plan sin ejecutar

Ejecución desatendida:
  nohup python3 f5_all.py --topology sl > ~/fase5/f5_nohup.out 2>&1 &
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
    
    # ── Parámetros RFC confirmados ──────────────────────────────
    "duracion_s": 30,
    "enfriamiento_s": 5,
    "reps_min": 15,
    "reps_max": 30,
    "rsd_objetivo_pct": 10.0,
    "tcp_congestion": "cubic",
    
    # ── Tráfico ──────────────────────────────────────────────────
    "tcp_puerto_base": 5201,
    "udp_puerto": 5400,
    "udp_tasa": "100M",
    "udp_payload_bytes": 1470,  # ≈ frame Ethernet 1518B
    "line_rate_mbps": 1000,
    
    # ── Rutas ────────────────────────────────────────────────────
    "dir_resultados": {
        "sl": os.path.expanduser("~/experimentos/sl/fase5_incast"),
        "j3c": os.path.expanduser("~/experimentos/j3c/fase5_incast"),
    },
    "log_file": os.path.expanduser("~/fase5/f5_log.txt"),
    
    # ── Config por topología (se fija en main()) ────────────────
    "topologias": {
        "sl": {
            "hosts": {
                "H1": "10.0.1.1", "H2": "10.0.1.2", "H3": "10.0.1.3",
                "H4": "10.0.2.1", "H5": "10.0.2.2", "H6": "10.0.2.3",
                "H7": "10.0.3.1", "H8": "10.0.3.2",
            },
            "receptor": {"tcp": "H5", "udp": "H7"},  # BALANCEADO: TCP a S1, UDP a S2
            "escenarios": {
                "s1": {
                    "tcp_senders": ["H1", "H8"],   # H1(L1) y H8(L3) -> H5(L2), ambos cruzan spine
                    "udp_sender": "H4",             # H4(L2) -> H7(L3), cruza spine, sin overlap de rol
                    "tcp_receptor": "H5",
                    "udp_receptor": "H7",
                },
                "s2": {
                    "tcp_senders": ["H1", "H2", "H3", "H8"],  # todos cruzan spine hacia H5(L2), sin overlap
                    "udp_sender": "H4",                       # mismo emisor UDP que en s1, para aislar el efecto del fan-in TCP
                    "tcp_receptor": "H5",
                    "udp_receptor": "H7",
                },
            },
            "desc": "Spine-Leaf: TCPs cruzando leaf hacia H5 + UDP H2->H7 cruzando leaf",
        },
        "j3c": {
            "hosts": {
                "H1": "10.0.1.1", "H2": "10.0.1.2", "H3": "10.0.1.3",
                "H4": "10.0.1.4", "H5": "10.0.2.1", "H6": "10.0.2.2",
                "H7": "10.0.2.3", "H8": "10.0.2.4",
            },
            "receptor": {"tcp": "H5", "udp": "H5"},
            "escenarios": {
                "s1": {
                    "tcp_senders": ["H1", "H4"],
                    "udp_sender": "H2",
                    "tcp_receptor": "H5",
                    "udp_receptor": "H5",
                },
                "s2": {
                    "tcp_senders": ["H1", "H4", "H3"],
                    "udp_sender": "H2",
                    "tcp_receptor": "H5",
                    "udp_receptor": "H5",
                },
            },
            "desc": "Jerárquica 3 Capas: Edge1=H1-H4, Edge2=H5-H8",
        },
    },
    # Placeholders (se sobrescriben en main())
    "hosts": {},
    "receptor": {},
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

def ssh_warmup(host_name, dry=False):
    """Abre (o reutiliza) la conexión SSH multiplexada de antemano.
    El handshake/autenticación se paga AQUÍ, fuera de la ventana
    cronometrada, para que el dispatch del comando real durante el test
    sea casi instantáneo — sin depender de que los relojes de los hosts
    estén sincronizados entre sí."""
    if dry:
        return
    ssh_run(host_name, "true", timeout=15, dry=False)

def ssh_close(host_name):
    """Cierra la conexión multiplexada de un host (limpieza al terminar)."""
    ip = CONFIG["hosts"][host_name]
    user = host_name.lower()
    subprocess.run(
        ["ssh", "-o", f"ControlPath={CONTROL_PATH}", "-O", "exit", f"{user}@{ip}"],
        capture_output=True, timeout=5,
    )

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
    involucrados = set(escenario_cfg["tcp_senders"]) | {escenario_cfg["udp_sender"], 
                                                         escenario_cfg["tcp_receptor"],
                                                         escenario_cfg["udp_receptor"]}
    for h in sorted(involucrados):
        rc, out, err = ssh_run(h, f"iperf3 --version | head -1 && ulimit -n", timeout=15, dry=dry)
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
    # Servidores TCP en el receptor TCP (H5 para SL balanceado)
    rx_tcp = escenario_cfg["tcp_receptor"]
    ssh_run(rx_tcp, "pkill -9 iperf3 2>/dev/null; sleep 1", timeout=15, dry=dry)
    puertos = [CONFIG["tcp_puerto_base"] + i for i in range(len(escenario_cfg["tcp_senders"]))]
    for p in puertos:
        ssh_run(rx_tcp, f"iperf3 -s -p {p} -D", timeout=15, dry=dry)
    
    # Servidor UDP en el receptor UDP (H7 para SL balanceado)
    rx_udp = escenario_cfg["udp_receptor"]
    ssh_run(rx_udp, f"iperf3 -s -p {CONFIG['udp_puerto']} -D", timeout=15, dry=dry)
    return puertos

def correr_repeticion(topo, esc, escenario_cfg, puertos, rep, dry=False):
    rx_tcp_ip = CONFIG["hosts"][escenario_cfg["tcp_receptor"]]
    rx_udp_ip = CONFIG["hosts"][escenario_cfg["udp_receptor"]]
    dur = CONFIG["duracion_s"]
    outdir = CONFIG["dir_resultados"][topo]
    resultados = {}
    hilos = []
    lock = threading.Lock()

    # Warm-up: abrir de antemano las conexiones SSH multiplexadas de todos
    # los hosts que van a mandar tráfico. Así el handshake/auth (que puede
    # variar cientos de ms a >1s entre hosts) ocurre ANTES del arranque
    # cronometrado, no durante él.
    participantes = list(escenario_cfg["tcp_senders"]) + [escenario_cfg["udp_sender"]]
    if not dry:
        warmup_threads = [threading.Thread(target=ssh_warmup, args=(h, dry)) for h in participantes]
        for t in warmup_threads:
            t.start()
        for t in warmup_threads:
            t.join()

    # Barrera local: todos los hilos esperan aquí hasta que TODOS estén
    # listos para disparar, y se liberan juntos. La sincronización la
    # coordina este proceso (Python), no el reloj de cada host — el único
    # tiempo no controlado que queda es la latencia de red para abrir el
    # canal sobre la conexión ya multiplexada (unos pocos ms en LAN).
    barrera = threading.Barrier(len(participantes))

    def cliente_tcp(sender, puerto):
        cmd = f"iperf3 -c {rx_tcp_ip} -p {puerto} -t {dur} -Z -C {CONFIG['tcp_congestion']} -J"
        if not dry:
            barrera.wait()
        rc, out, err = ssh_run(sender, cmd, timeout=dur + 30, dry=dry)
        if rc == 124 and not dry:
            # timeout local: el iperf3 remoto pudo quedar vivo, lo matamos
            ssh_run(sender, "pkill -9 iperf3 2>/dev/null; true", timeout=8, dry=dry)
        nombre = f"{topo}_f5{esc}_tcp_{sender}{escenario_cfg['tcp_receptor']}_rep{rep:02d}.json"
        with lock:
            resultados[("tcp", sender)] = (rc, out, nombre)

    def cliente_udp(sender):
        cmd = (f"iperf3 -c {rx_udp_ip} -p {CONFIG['udp_puerto']} -u "
               f"-b {CONFIG['udp_tasa']} -l {CONFIG['udp_payload_bytes']} -t {dur} -J")
        if not dry:
            barrera.wait()
        rc, out, err = ssh_run(sender, cmd, timeout=dur + 30, dry=dry)
        if rc == 124 and not dry:
            ssh_run(sender, "pkill -9 iperf3 2>/dev/null; true", timeout=8, dry=dry)
        nombre = f"{topo}_f5{esc}_udp_{sender}{escenario_cfg['udp_receptor']}_rep{rep:02d}.json"
        with lock:
            resultados[("udp", sender)] = (rc, out, nombre)

    for i, s in enumerate(escenario_cfg["tcp_senders"]):
        hilos.append(threading.Thread(target=cliente_tcp, args=(s, puertos[i])))
    hilos.append(threading.Thread(target=cliente_udp, args=(escenario_cfg["udp_sender"],)))

    for h in hilos:
        h.start()
    for h in hilos:
        h.join()

    if dry:
        return {"goodput_por_emisor": {}, "goodput_agregado_mbps": 0.0,
                "udp_jitter_ms": 0.0, "udp_perdida_pct": 0.0, "ok": True}

    medicion = {"goodput_por_emisor": {}, "ok": True}
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
                bps = data["end"]["sum_received"]["bits_per_second"]
                medicion["goodput_por_emisor"][sender] = bps / 1e6
                # Extraer retransmisiones
                retrans = data["end"]["sum_sent"].get("retransmits", 0)
                medicion.setdefault("retransmits", {})[sender] = retrans
            else:
                fin = data["end"]["sum"]
                medicion["udp_jitter_ms"] = fin.get("jitter_ms", 0.0)
                medicion["udp_perdida_pct"] = fin.get("lost_percent", 0.0)
        except (KeyError, json.JSONDecodeError) as e:
            log(f"  ✗ {proto.upper()} {sender}: JSON inválido ({e})")
            medicion["ok"] = False

    medicion["goodput_agregado_mbps"] = sum(medicion["goodput_por_emisor"].values())
    medicion.setdefault("udp_jitter_ms", 0.0)
    medicion.setdefault("udp_perdida_pct", 0.0)
    medicion.setdefault("retransmits", {})
    return medicion

def correr_escenario(topo, esc, dry=False):
    cfg = CONFIG["escenarios"][esc]
    outdir = CONFIG["dir_resultados"][topo]
    os.makedirs(outdir, exist_ok=True)

    log(f"== F5 {esc.upper()} — Incast {len(cfg['tcp_senders'])}:1 → {cfg['tcp_receptor']} (TCP) + {cfg['udp_receptor']} (UDP)")
    log(f"  TOPOLOGÍA: {topo.upper()} — {CONFIG['topologias'][topo]['desc']}")
    log(f"  TCP stateful: {', '.join(cfg['tcp_senders'])} → {cfg['tcp_receptor']}")
    log(f"  UDP stateless: {cfg['udp_sender']} @ {CONFIG['udp_tasa']} → {cfg['udp_receptor']}")

    puertos = lanzar_servidores(cfg, dry=dry)

    goodputs, jitters, retrans_totales = [], [], []
    reps_hechas = 0
    t0 = time.time()

    for rep in range(1, CONFIG["reps_max"] + 1):
        if not dry:
            time.sleep(CONFIG["enfriamiento_s"])
        m = correr_repeticion(topo, esc, cfg, puertos, rep, dry=dry)
        reps_hechas = rep
        if dry:
            break
        if not m["ok"]:
            log(f"  rep{rep:02d}: medición inválida — se repetirá el conteo sin esta rep")
            continue
        goodputs.append(m["goodput_agregado_mbps"])
        jitters.append(m["udp_jitter_ms"])
        retrans_total = sum(m.get("retransmits", {}).values())
        retrans_totales.append(retrans_total)
        log(f"  rep{rep:02d}: goodput = {m['goodput_agregado_mbps']:.1f} Mbps "
            f"({m['goodput_agregado_mbps']/CONFIG['line_rate_mbps']*100:.1f}% LR) | "
            f"jitter UDP = {m['udp_jitter_ms']:.3f} ms | pérdida = {m['udp_perdida_pct']:.2f}% | "
            f"retransmisiones = {retrans_total}")

        if rep >= CONFIG["reps_min"]:
            r_g, r_j = rsd_pct(goodputs), rsd_pct(jitters)
            log(f"  → RSD goodput = {r_g:.2f}% | RSD jitter = {r_j:.2f}% "
                f"(objetivo < {CONFIG['rsd_objetivo_pct']}%)")
            if r_g < CONFIG["rsd_objetivo_pct"] and r_j < CONFIG["rsd_objetivo_pct"]:
                log(f"  ✓ Convergencia alcanzada en {rep} repeticiones")
                break

    # Matar servidores
    ssh_run(cfg["tcp_receptor"], "pkill -9 iperf3 2>/dev/null", timeout=15, dry=dry)
    if cfg["udp_receptor"] != cfg["tcp_receptor"]:
        ssh_run(cfg["udp_receptor"], "pkill -9 iperf3 2>/dev/null", timeout=15, dry=dry)

    if dry or not goodputs:
        return

    resumen = {
        "fase": "F5_incast",
        "escenario": esc,
        "topologia": topo,
        "timestamp": datetime.now().isoformat(),
        "tcp_receptor": cfg["tcp_receptor"],
        "udp_receptor": cfg["udp_receptor"],
        "tcp_senders": cfg["tcp_senders"],
        "udp_sender": cfg["udp_sender"],
        "duracion_s": CONFIG["duracion_s"],
        "cooldown_s": CONFIG["enfriamiento_s"],
        "tcp_congestion_control": CONFIG["tcp_congestion"],
        "balanced": (topo == "sl"),
        "repeticiones_validas": len(goodputs),
        "repeticiones_lanzadas": reps_hechas,
        "goodput_agregado_mbps": {
            "media": round(statistics.mean(goodputs), 2),
            "rsd_pct": round(rsd_pct(goodputs), 2),
            "pct_line_rate": round(statistics.mean(goodputs) / CONFIG["line_rate_mbps"] * 100, 2),
        },
        "udp_jitter_ms": {
            "media": round(statistics.mean(jitters), 4),
            "rsd_pct": round(rsd_pct(jitters), 2),
        },
        "tcp_retransmits_total": {
            "media": round(statistics.mean(retrans_totales), 1),
            "rsd_pct": round(rsd_pct(retrans_totales), 2) if retrans_totales else None,
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
        description="F5: Incast Testing Unificado (RFC 8239 §6) — CON BALANCEO MANUAL"
    )
    ap.add_argument("--topology", choices=["sl", "j3c"], required=True,
                    help="sl = Spine-Leaf | j3c = Jerárquica 3 Capas")
    ap.add_argument("--escenario", choices=["s1", "s2"], default=None,
                    help="Correr solo un escenario (default: ambos)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-preflight", action="store_true")
    args = ap.parse_args()

    # Fijar configuración según topología
    topo_cfg = CONFIG["topologias"][args.topology]
    CONFIG["hosts"] = topo_cfg["hosts"]
    CONFIG["receptor"] = topo_cfg["receptor"]
    CONFIG["escenarios"] = topo_cfg["escenarios"]

    escenarios = [args.escenario] if args.escenario else ["s1", "s2"]

    log(f"===== FASE 5 — INCAST (RFC 8239 §6) | topología={args.topology} "
        f"| escenarios={escenarios} | dry_run={args.dry_run} =====")
    log(f"  {topo_cfg['desc']}")

    if not args.skip_preflight:
        # Usar el primer escenario para preflight
        cfg_total = {
            "tcp_senders": sorted({h for e in escenarios
                                   for h in CONFIG["escenarios"][e]["tcp_senders"]}),
            "udp_sender": CONFIG["escenarios"][escenarios[0]]["udp_sender"],
            "tcp_receptor": CONFIG["escenarios"][escenarios[0]]["tcp_receptor"],
            "udp_receptor": CONFIG["escenarios"][escenarios[0]]["udp_receptor"],
        }
        if not preflight(cfg_total, dry=args.dry_run) and not args.dry_run:
            log("✗ Preflight falló. Corrige o usa --skip-preflight bajo tu propio riesgo.")
            sys.exit(1)

    for esc in escenarios:
        correr_escenario(args.topology, esc, dry=args.dry_run)

    if not args.dry_run:
        for h in CONFIG["hosts"]:
            ssh_close(h)

    log("===== FASE 5 TERMINADA =====")

if __name__ == "__main__":
    main()
