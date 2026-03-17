#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ABU prototype topology — multi-ISP with NAT + QoS + smoke tests

Added: a route-watcher / provisioner thread that watches /tmp/abu_provision_trigger
and ensures ISP transit routes/interfaces are installed (zero-touch). Fixed to
use correct host IPs and be idempotent.
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSBridge
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info, warn
import os, subprocess, time, threading, re

ENABLE_NAT = True
H1_PID_FILE = "/tmp/mn_h1.pid"
H1_READY = "/tmp/mn_ready"
NAT_PID_FILE = "/tmp/mn_nat.pid"
NAT_READY = "/tmp/mn_nat_ready"
PROVISION_TRIGGER = "/tmp/abu_provision_trigger"

def sh(cmd):
    return subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True)

def ensure_host_nat_stack():
    info("*** Host prep: loading NAT/conntrack kernel modules\n")
    for mod in ("nf_nat", "iptable_nat", "nf_conntrack"):
        sh("sudo modprobe " + mod + " || true")
    sh("sudo update-alternatives --set iptables /usr/sbin/iptables-legacy >/dev/null 2>&1 || true")
    sh("sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy >/dev/null 2>&1 || true")

def disable_rp_filter(*nodes):
    for n in nodes:
        n.cmd("sysctl -w net.ipv4.conf.all.rp_filter=0")
        n.cmd("sysctl -w net.ipv4.conf.default.rp_filter=0")

def config_nat_tables(nat):
    nat.cmd("sysctl -w net.ipv4.ip_forward=1")
    info("*** NAT: applying iptables rules (FORWARD + MASQUERADE)\n")
    nat.cmd("iptables -F; iptables -t nat -F")
    nat.cmd("iptables -P FORWARD ACCEPT")
    nat.cmd("iptables -A FORWARD -i nat-eth1 -o nat-eth0 -j ACCEPT")
    nat.cmd("iptables -A FORWARD -i nat-eth0 -o nat-eth1 -p icmp -j ACCEPT")
    nat.cmd("iptables -A FORWARD -i nat-eth0 -o nat-eth1 -m state --state RELATED,ESTABLISHED -j ACCEPT")
    rc = nat.cmd("iptables -t nat -A POSTROUTING -o nat-eth0 -j MASQUERADE; echo $?").strip()
    if rc == "0":
        info("*** NAT: iptables MASQUERADE installed\n")
        info("*** NAT sanity:\n")
        info(nat.cmd("iptables -S FORWARD"))
        info(nat.cmd("iptables -t nat -S | sed -n '1,10p'"))
        return True
    warn("*** NAT: MASQUERADE not detected — trying legacy explicitly\n")
    nat.cmd("iptables-legacy -t nat -A POSTROUTING -o nat-eth0 -j MASQUERADE || true")
    rc2 = nat.cmd("iptables-legacy -t nat -C POSTROUTING -o nat-eth0 -j MASQUERADE; echo $?").strip()
    if rc2 == "0":
        info("*** NAT: iptables-legacy MASQUERADE installed\n")
        info("*** NAT sanity:\n")
        info(nat.cmd("iptables-legacy -S FORWARD"))
        info(nat.cmd("iptables-legacy -t nat -S | sed -n '1,10p'"))
        return True
    return False

def config_qos_on_nat(nat):
    info("*** QoS: configuring tc slices on nat-eth0 (uplink)\n")
    nat.cmd("tc qdisc del dev nat-eth0 root 2>/dev/null || true")
    nat.cmd("tc qdisc add dev nat-eth0 root handle 1: htb default 30")
    nat.cmd("tc class add dev nat-eth0 parent 1: classid 1:1 htb rate 10Gbit")
    nat.cmd("tc class add dev nat-eth0 parent 1:1 classid 1:10 htb rate 7Gbit ceil 10Gbit")
    nat.cmd("tc class add dev nat-eth0 parent 1:1 classid 1:20 htb rate 3Gbit ceil 10Gbit")
    nat.cmd("tc filter add dev nat-eth0 protocol ip parent 1:0 prio 1 handle 1 fw flowid 1:10")
    nat.cmd("tc filter add dev nat-eth0 protocol ip parent 1:0 prio 1 handle 2 fw flowid 1:20")
    nat.cmd("iptables -t mangle -F")
    nat.cmd("iptables -t mangle -A PREROUTING -s 192.168.1.0/24 -j MARK --set-mark 1")
    nat.cmd("iptables -t mangle -A PREROUTING -s 192.168.2.0/24 -j MARK --set-mark 2")

def autostart_iperf_servers(inet):
    info("*** QoS: starting iperf3 servers on inet\n")
    inet.cmd("pkill iperf3; iperf3 -s -p 5001 -D")
    inet.cmd("iperf3 -s -p 5002 -D")

def write_pid_flag(node, pid_file, ready_flag):
    with open(pid_file, "w") as f:
        f.write(str(node.pid))
    open(ready_flag, "w").close()

def _smoke(net, h1, glo, mtn, air, nat, inet):
    info("\n[SMOKE] Hop-by-hop sanity\n")
    info(" - h1 → Glo gw 192.168.1.1\n"); info(h1.cmd("ping -n -c1 -W1 192.168.1.1"))
    info(" - h1 → MTN gw 192.168.2.1\n"); info(h1.cmd("ping -n -c1 -W1 192.168.2.1"))
    info(" - h1 → Airtel gw 192.168.3.1\n"); info(h1.cmd("ping -n -c1 -W1 192.168.3.1"))
    info(" - Glo → NAT 172.16.0.1\n"); info(glo.cmd("ping -n -c1 -W1 172.16.0.1"))
    info(" - NAT → INET 10.0.0.2\n"); info(nat.cmd("ping -n -c1 -W1 10.0.0.2"))
    info(" - INET → NAT 10.0.0.1\n"); info(inet.cmd("ping -n -c1 -W1 10.0.0.1"))

    _ = h1.cmd("arp -n >/dev/null 2>&1 || true")

    info("\n[SMOKE] NAT E2E to 10.0.0.2 via each ISP\n")
    tests = [
        ("Glo", "192.168.1.1", "h1-eth0", "192.168.1.10"),
        ("MTN", "192.168.2.1", "h1-eth1", "192.168.2.10"),
        ("Airtel", "192.168.3.1", "h1-eth2", "192.168.3.10"),
    ]
    for name, gw, dev, src in tests:
        h1.cmd("arping -c1 -w1 -I " + dev + " " + gw + " >/dev/null 2>&1 || true")
        h1.cmd("ip route del 10.0.0.2/32 2>/dev/null || true")
        h1.cmd("ip route replace 10.0.0.2/32 via " + gw + " dev " + dev + " src " + src + " metric 5")
        rc = h1.cmd("ping -n -c1 -W2 -I " + dev + " 10.0.0.2; echo $?").strip().splitlines()[-1]
        info("[SMOKE] {0:6s}: {1}\n".format(name, "PASS" if rc == "0" else "FAIL"))
        h1.cmd("ip route del 10.0.0.2/32 2>/dev/null || true")

    info("\n[i] NAT ready. After the controller sets default, try:\n"
         " mininet> h1 ping -c2 10.0.0.2\n"
         " mininet> h1 iperf3 -c 10.0.0.2 -p 5001 -t 10 -B 192.168.1.10\n")

def route_watcher_thread(net, hosts_by_name, stop_event):
    """
    Watch PROVISION_TRIGGER file and idempotently ensure ISP transit routes/interfaces exist.
    Format expected: lines like 'Glo,glo-eth1' or other free-form tokens; watcher extracts iface
    and host via heuristic+alias_map. After successful processing the file is removed.
    """
    info("*** route-watcher: started\n")

    # mapping controller ISP names or common tokens -> topology host names
    alias_map = {"airtel": "air", "mtn": "mtn", "glo": "glo", "inet": "inet", "nat": "nat"}

    # expected 172.16.x addresses per ISP host (host side of ISP transit)
    expected_isp_addr = {
        "glo": "172.16.0.2/24",
        "mtn": "172.16.0.3/24",
        "air": "172.16.0.4/24",
        "airtel": "172.16.0.4/24",
    }

    iface_re = re.compile(r"\b[\w\-]+-eth\d+\b")

    while not stop_event.is_set():
        try:
            if not os.path.exists(PROVISION_TRIGGER):
                time.sleep(0.25)
                continue

            info("*** route-watcher: detected trigger\n")
            try:
                with open(PROVISION_TRIGGER, "r") as f:
                    lines = [l.strip() for l in f.readlines() if l.strip()]
            except Exception as e:
                info(f"*** route-watcher: error reading trigger: {e}\n")
                lines = []

            if not lines:
                try:
                    os.remove(PROVISION_TRIGGER)
                except Exception:
                    pass
                time.sleep(0.25)
                continue

            # For convenience build a lowercase lookup for hosts_by_name
            hosts_lc = {k.lower(): v for k, v in hosts_by_name.items()}

            for line in lines:
                try:
                    # find an iface token like 'glo-eth1'
                    found_iface = None
                    for m in iface_re.finditer(line):
                        found_iface = m.group(0)
                        break

                    # try to find an isp/host token and map to topology host
                    tokens = re.split(r"[\s,->:]+", line)
                    found_host = None
                    isp_token = None
                    for t in tokens:
                        if not t:
                            continue
                        tl = t.lower()
                        if tl in hosts_lc:
                            found_host = hosts_lc[tl]
                            isp_token = tl
                            break
                        mapped = alias_map.get(tl)
                        if mapped and mapped in hosts_lc:
                            found_host = hosts_lc[mapped]
                            isp_token = tl
                            break
                        # also handle tokens like 'glo' or 'airtel' that map to hosts
                        if tl in alias_map and alias_map[tl] in hosts_lc:
                            found_host = hosts_lc[alias_map[tl]]
                            isp_token = tl
                            break

                    # As a last resort if no host token matched, try the first alias_map value present
                    if found_host is None and tokens:
                        for t in tokens:
                            tl = t.lower()
                            if tl in alias_map and alias_map[tl] in hosts_lc:
                                found_host = hosts_lc[alias_map[tl]]
                                isp_token = tl
                                break

                    if not found_iface or not found_host:
                        info(f"*** route-watcher: unable to parse trigger line: '{line}'\n")
                        continue

                    info(f"*** route-watcher: provisioning {isp_token or 'unknown'} -> {found_iface} on {found_host.name}\n")

                    # Idempotent provisioning: bring iface up, ensure expected IP present, install 10.0.0.0/24 via NAT, warm ARP
                    attempts = 5
                    success = False
                    host_key = found_host.name.lower()
                    expected = expected_isp_addr.get(isp_token, expected_isp_addr.get(host_key))

                    for attempt in range(attempts):
                        # bring iface up
                        found_host.cmd(f"ip link set {found_iface} up >/dev/null 2>&1 || true")
                        time.sleep(0.05)

                        # ensure expected IPv4 exists (only add if interface currently has no IPv4)
                        cur_addr = found_host.cmd(f"ip -o -4 addr show dev {found_iface} 2>/dev/null || true").strip()
                        if not cur_addr and expected:
                            found_host.cmd(f"ip addr add {expected} dev {found_iface} 2>/dev/null || true")
                            time.sleep(0.05)

                        # ensure return route to 10.0.0.0/24 via NAT
                        found_host.cmd(f"ip route replace 10.0.0.0/24 via 172.16.0.1 dev {found_iface} || true")

                        # warm ARP
                        found_host.cmd(f"arping -c1 -I {found_iface} 172.16.0.1 >/dev/null 2>&1 || true")

                        # sanity checks
                        out = found_host.cmd(f"ip route show 10.0.0.0/24 | grep -F 'via 172.16.0.1' || true")
                        link_state = found_host.cmd(f"ip -o link show dev {found_iface} 2>/dev/null || true")
                        if out and ("state UP" in link_state or "LOWER_UP" in link_state):
                            success = True
                            break

                        time.sleep(0.25)

                    if success:
                        info(f"*** route-watcher: provisioned {isp_token or found_host.name} ok\n")
                    else:
                        info(f"*** route-watcher: provision FAILED for {isp_token or found_host.name} (tried {attempts}x)\n")

                except Exception as e:
                    info(f"*** route-watcher: exception processing line '{line}': {e}\n")

            # cleanup trigger file
            try:
                os.remove(PROVISION_TRIGGER)
                info("*** route-watcher: removed trigger file\n")
            except Exception:
                pass

        except Exception as e:
            info(f"*** route-watcher: loop exception: {e}\n")
        time.sleep(0.25)

    info("*** route-watcher: stopped\n")

def build():
    if ENABLE_NAT:
        ensure_host_nat_stack()

    net = MinInet = Mininet(controller=None, link=TCLink, switch=OVSBridge, cleanup=True)

    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
    s1 = net.addSwitch('s1'); s2 = net.addSwitch('s2'); s3 = net.addSwitch('s3')
    s7 = net.addSwitch('s7'); s9 = net.addSwitch('s9')

    h1 = net.addHost('h1'); h2 = net.addHost('h2'); h3 = net.addHost('h3'); h4 = net.addHost('h4')
    glo = net.addHost('glo'); mtn = net.addHost('mtn'); air = net.addHost('air')
    nat = net.addHost('nat'); inet = net.addHost('inet')

    # wiring
    net.addLink(h1, s1); net.addLink(h1, s2); net.addLink(h1, s3)
    net.addLink(h2, s1); net.addLink(h3, s1); net.addLink(h4, s1)
    net.addLink(glo, s1); net.addLink(mtn, s2); net.addLink(air, s3)
    net.addLink(inet, s7)
    if ENABLE_NAT:
        net.addLink(nat, s7); net.addLink(nat, s9)
        net.addLink(glo, s9); net.addLink(mtn, s9); net.addLink(air, s9)

    net.build(); c0.start()
    for sw in (s1, s2, s3, s7, s9):
        sw.start([c0])

    info("*** Configuring hosts\n")
    h1.cmd("ip addr flush dev h1-eth0; ip addr flush dev h1-eth1; ip addr flush dev h1-eth2")
    h1.cmd("ip link set h1-eth0 up; ip link set h1-eth1 up; ip link set h1-eth2 up")
    h1.cmd("ip addr add 192.168.1.10/24 dev h1-eth0")
    h1.cmd("ip addr add 192.168.2.10/24 dev h1-eth1")
    h1.cmd("ip addr add 192.168.3.10/24 dev h1-eth2")

    glo.cmd("ip addr flush dev glo-eth0; ip link set glo-eth0 up; ip addr add 192.168.1.1/24 dev glo-eth0; sysctl -w net.ipv4.ip_forward=1")
    mtn.cmd("ip addr flush dev mtn-eth0; ip link set mtn-eth0 up; ip addr add 192.168.2.1/24 dev mtn-eth0; sysctl -w net.ipv4.ip_forward=1")
    air.cmd("ip addr flush dev air-eth0; ip link set air-eth0 up; ip addr add 192.168.3.1/24 dev air-eth0; sysctl -w net.ipv4.ip_forward=1")

    if ENABLE_NAT:
        glo.cmd("ip addr flush dev glo-eth1; ip link set glo-eth1 up; ip addr add 172.16.0.2/24 dev glo-eth1; ip route replace default via 172.16.0.1")
        glo.cmd("ip route replace 10.0.0.0/24 via 172.16.0.1 dev glo-eth1")
        glo.cmd("arping -c1 -I glo-eth1 172.16.0.1 >/dev/null 2>&1 || true")

        mtn.cmd("ip addr flush dev mtn-eth1; ip link set mtn-eth1 up; ip addr add 172.16.0.3/24 dev mtn-eth1; ip route replace default via 172.16.0.1")
        mtn.cmd("ip route replace 10.0.0.0/24 via 172.16.0.1 dev mtn-eth1")
        mtn.cmd("arping -c1 -I mtn-eth1 172.16.0.1 >/dev/null 2>&1 || true")

        air.cmd("ip addr flush dev air-eth1; ip link set air-eth1 up; ip addr add 172.16.0.4/24 dev air-eth1; ip route replace default via 172.16.0.1")
        air.cmd("ip route replace 10.0.0.0/24 via 172.16.0.1 dev air-eth1")
        air.cmd("arping -c1 -I air-eth1 172.16.0.1 >/dev/null 2>&1 || true")

        time.sleep(0.25)

        nat.cmd("ip addr flush dev nat-eth0; ip link set nat-eth0 up; ip addr add 10.0.0.1/24 dev nat-eth0")
        nat.cmd("ip addr flush dev nat-eth1; ip link set nat-eth1 up; ip addr add 172.16.0.1/24 dev nat-eth1")

        inet.cmd("ip addr flush dev inet-eth0; ip link set inet-eth0 up; ip addr add 10.0.0.2/24 dev inet-eth0")
        inet.cmd("ip route flush default; ip route add default via 10.0.0.1")

        disable_rp_filter(glo, mtn, air, nat, h1)
        if not config_nat_tables(nat):
            warn("\n[WARN] NAT MASQUERADE not detected. If issues persist, inside NAT try:\n"
                 "  iptables-legacy -t nat -A POSTROUTING -o nat-eth0 -j MASQUERADE\n\n")

        nat.cmd("ip route replace 192.168.1.0/24 via 172.16.0.2 dev nat-eth1")
        nat.cmd("ip route replace 192.168.2.0/24 via 172.16.0.3 dev nat-eth1")
        nat.cmd("ip route replace 192.168.3.0/24 via 172.16.0.4 dev nat-eth1")

        config_qos_on_nat(nat)
        autostart_iperf_servers(inet)

    # Export PID/ready flags (only after topology + warmup done)
    try:
        for f in (H1_PID_FILE, H1_READY, NAT_PID_FILE, NAT_READY):
            if os.path.exists(f): os.remove(f)
    except OSError:
        pass

    # Start route-watcher thread (daemon) that will process PROVISION_TRIGGERs
    stop_event = threading.Event()
    hosts_by_name = {n.name.lower(): n for n in (h1, h2, h3, h4, glo, mtn, air, nat, inet)}
    t = threading.Thread(target=route_watcher_thread, args=(net, hosts_by_name, stop_event), daemon=True)
    t.start()

    write_pid_flag(h1, H1_PID_FILE, H1_READY)
    write_pid_flag(nat, NAT_PID_FILE, NAT_READY)
    info("[+] Exported h1 long-lived PID={0}\n".format(h1.pid))
    info("[+] Exported nat long-lived PID={0}\n".format(nat.pid))

    info("[+] Base config done. Quick tests:\n")
    info(" - h1 -> Glo\n");    info(h1.cmd("ping -n -c1 -W1 192.168.1.1"))
    info(" - h1 -> MTN\n");    info(h1.cmd("ping -n -c1 -W1 192.168.2.1"))
    info(" - h1 -> Airtel\n"); info(h1.cmd("ping -n -c1 -W1 192.168.3.1"))

    _smoke(net, h1, glo, mtn, air, nat, inet)

    CLI(net)

    # Stop route-watcher
    stop_event.set()
    t.join(timeout=1.0)

    net.stop()

if __name__ == "__main__":
    setLogLevel('info')
    build()
