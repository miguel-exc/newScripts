"""
check_spine_traffic.py

Lee los contadores de bytes por puerto (node-connector) de los switches
Spine (S1, S2) directamente del controlador ODL, para verificar
empíricamente por cuál spine está pasando el tráfico de un experimento.

No depende de UPLINK_PORT (que aún no está confirmado): en vez de asumir
qué puerto es el uplink, muestra TODOS los puertos físicos de S1 y S2 con
sus contadores rx/tx, para que compares antes/después de una corrida y
veas en cuál puerto/switch subió el tráfico.

Uso:
    # 1) Antes de correr el experimento:
    python3 check_spine_traffic.py --host <ODL_HOST> --save antes.json

    # 2) Corre tu experimento (f5_all.py, etc.)

    # 3) Justo después:
    python3 check_spine_traffic.py --host <ODL_HOST> --save despues.json

    # 4) Comparar automáticamente:
    python3 check_spine_traffic.py --diff antes.json despues.json
"""
import argparse
import json
import sys
import requests

# DPIDs de los spines según vars.py (NAMES: "1"=S1, "2"=S2)
SPINES = {
    "S1": "2977893393545632",
    "S2": "2977893393545536",
}


def fetch_node(host, dpid, auth=("admin", "admin"), port=8181):
    url = (f"http://{host}:{port}/rests/data/opendaylight-inventory:nodes/"
           f"node=openflow:{dpid}?content=nonconfig")
    r = requests.get(url, auth=auth, headers={"Accept": "application/json"}, timeout=10)
    r.raise_for_status()
    return r.json()


def extract_port_counters(node_json):
    """Devuelve {port_id: {rx_bytes, tx_bytes, rx_packets, tx_packets}} para
    cada node-connector físico (excluye el puerto LOCAL)."""
    out = {}
    try:
        node = node_json["opendaylight-inventory:node"][0]
    except (KeyError, IndexError):
        return out

    for nc in node.get("node-connector", []):
        port_id = nc.get("id", "")
        if port_id.endswith("LOCAL"):
            continue
        stats = nc.get("opendaylight-port-statistics:flow-capable-node-connector-statistics")
        if not stats:
            continue
        bytes_ = stats.get("bytes", {})
        packets_ = stats.get("packets", {})
        out[port_id] = {
            "rx_bytes": int(bytes_.get("received", 0)),
            "tx_bytes": int(bytes_.get("transmitted", 0)),
            "rx_packets": int(packets_.get("received", 0)),
            "tx_packets": int(packets_.get("transmitted", 0)),
        }
    return out


def snapshot(host):
    data = {}
    for name, dpid in SPINES.items():
        try:
            node_json = fetch_node(host, dpid)
            data[name] = extract_port_counters(node_json)
        except Exception as e:
            print(f"  ! Error consultando {name} (dpid {dpid}): {e}", file=sys.stderr)
            data[name] = {}
    return data


def print_snapshot(data):
    for spine, ports in data.items():
        print(f"\n== {spine} ==")
        if not ports:
            print("  (sin datos - revisa conectividad/DPID)")
            continue
        for port_id, c in sorted(ports.items()):
            print(f"  {port_id:<20} rx={c['rx_bytes']:>12} B  tx={c['tx_bytes']:>12} B  "
                  f"rx_pkts={c['rx_packets']:>8}  tx_pkts={c['tx_packets']:>8}")


def diff_snapshots(before, after):
    print("\n===== DELTA (después - antes) =====")
    any_traffic = {"S1": False, "S2": False}
    for spine in SPINES:
        print(f"\n== {spine} ==")
        b = before.get(spine, {})
        a = after.get(spine, {})
        ports = sorted(set(b) | set(a))
        if not ports:
            print("  (sin datos)")
            continue
        for port_id in ports:
            bb = b.get(port_id, {"rx_bytes": 0, "tx_bytes": 0})
            aa = a.get(port_id, {"rx_bytes": 0, "tx_bytes": 0})
            d_rx = aa["rx_bytes"] - bb["rx_bytes"]
            d_tx = aa["tx_bytes"] - bb["tx_bytes"]
            marker = "  <-- tráfico aquí" if (d_rx > 1000 or d_tx > 1000) else ""
            if marker:
                any_traffic[spine] = True
            print(f"  {port_id:<20} Δrx={d_rx:>10} B  Δtx={d_tx:>10} B{marker}")

    print("\n===== RESUMEN =====")
    for spine, used in any_traffic.items():
        estado = "SÍ vio tráfico" if used else "sin tráfico detectado"
        print(f"  {spine}: {estado}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", help="IP del controlador ODL (ej. 172.17.0.2)")
    p.add_argument("--save", help="Archivo donde guardar el snapshot actual (JSON)")
    p.add_argument("--diff", nargs=2, metavar=("ANTES", "DESPUES"),
                    help="Compara dos snapshots ya guardados")
    args = p.parse_args()

    if args.diff:
        with open(args.diff[0]) as f:
            before = json.load(f)
        with open(args.diff[1]) as f:
            after = json.load(f)
        diff_snapshots(before, after)
        sys.exit(0)

    if not args.host:
        p.error("--host es requerido si no usas --diff")

    data = snapshot(args.host)
    print_snapshot(data)

    if args.save:
        with open(args.save, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\n✔  Guardado en {args.save}")
