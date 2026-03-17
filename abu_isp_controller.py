#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ABU ISP Controller — bedrock only (health + EMA + RTT-based selection + failover)

Enhancement: Per-application (port-based) traffic steering with failover fallback.
 - Defines APPLICATIONS mapping (proto + dport/sport) -> preferred ISP index.
 - Installs iptables mangle rules in h1 to mark app packets.
 - Creates ip rule: fwmark -> lookup <table_id> (table per-ISP).
 - On ISP or global-route change the controller updates app rules so each app
   uses its preferred ISP when available, otherwise falls back to the active ISP.
All previous functionality (health probes, EMA, RTT selection, provision trigger)
is preserved.
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp
import os, csv, time, threading, subprocess, logging, shutil

# --------- Files exported by topology ----------
H1_PID_FILE   = "/tmp/mn_h1.pid"
H1_READY_FLAG = "/tmp/mn_ready"
LOG_FILE      = "abu_isp_log.csv"
PROVISION_TRIGGER = "/tmp/abu_provision_trigger"

# --------- ISPs (priority order: Glo first) ----------
ISPS = [
    {"name": "Glo",    "gw_ip": "192.168.1.1", "src_ip": "192.168.1.10", "src_dev": "h1-eth0", "table_id": 100},
    {"name": "MTN",    "gw_ip": "192.168.2.1", "src_ip": "192.168.2.10", "src_dev": "h1-eth1", "table_id": 101},
    {"name": "Airtel", "gw_ip": "192.168.3.1", "src_ip": "192.168.3.10", "src_dev": "h1-eth2", "table_id": 102},
]

# --------- Health + decision tunables ----------
PROBE_INTERVAL_S     = 2
PING_TIMEOUT_S       = "2"   # seconds (string for ping -W)
DEBOUNCE_UP_OK       = 1
DEBOUNCE_DOWN_FAIL   = 2
HOLDDOWN_SEC         = 6
REQUIRE_INET_OK      = True
INET_TARGET_IP       = "10.0.0.2"
EMA_ALPHA            = 0.5
RTT_SWITCH_MARGIN_MS = 5.0
PREFERRED_IDX = 0          # Glo
PREFER_SLACK_MS = 5.0      # how close Glo must be to the best to preempt back

# --------- Per-application steering configuration ----------
# Each application entry:
#   name: human name
#   proto: 'udp' or 'tcp'
#   dports: (lo, hi) inclusive tuple for destination port(s) OR integer for single port
#   preferred_idx: index into ISPS for the ISP this app prefers (0-based)
APPLICATIONS = [
    {"name": "VoIP-SIP",   "proto": "udp", "dports": 5060,                    "preferred_idx": 1},  # prefer MTN for SIP
    {"name": "VoIP-RTP",   "proto": "udp", "dports": (10000, 20000),         "preferred_idx": 1},  # prefer MTN for RTP
    {"name": "Video",      "proto": "udp", "dports": (5004, 5050),           "preferred_idx": 2},  # example: prefer Airtel
    {"name": "HTTP",       "proto": "tcp", "dports": 80,                     "preferred_idx": 0},  # prefer Glo for web
    {"name": "HTTPS",      "proto": "tcp", "dports": 443,                    "preferred_idx": 0},  # prefer Glo for TLS
]

# Assign each application a unique fwmark (non-zero)
# We'll programmatically assign marks starting at 1
# (note: ensure no collision with other system marks)
# (Internal: _app_marks[name] -> integer)
# ---------------------------------------------------------------------------------

def _fmt_ema(x):
    return ("%.1f" % x) if x is not None else "NA"


class ABU_ISP_Controller(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ABU_ISP_Controller, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.INFO)
        self._ensure_csv()
        self.log_event("INIT", "ABU ISP Controller (bedrock + per-app steering + provision trigger)")

        # OpenFlow bookkeeping
        self.datapaths = {}
        self.mac_to_port = {}

        # runtime state
        self.current_idx = None
        self.h1_pid = None
        self._mnexec = shutil.which("mnexec") or "/usr/bin/mnexec"

        self._raw = {i["name"]: {"gw_ok": False, "inet_ok": False, "rtt_ms": None} for i in ISPS}
        self._deb = {i["name"]: {"up_cnt": 0, "down_cnt": 0, "is_up": False} for i in ISPS}
        self._ema = {i["name"]: None for i in ISPS}
        self._last_switch_ts = 0

        # per-app mark mapping
        self._app_marks = {}
        for idx, app in enumerate(APPLICATIONS, start=1):
            self._app_marks[app["name"]] = idx

        # start monitor thread
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    # ---------- logging ----------
    def _ensure_csv(self):
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                csv.writer(f).writerow(["timestamp", "event", "details"])

    def log_event(self, event, details):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow([ts, event, details])
        self.logger.info("[" + event + "] " + details)

    # ---------- OF helpers ----------
    def add_flow(self, dp, priority, match, actions, table_id=0, idle_timeout=0, hard_timeout=0, cookie=0):
        ofp, p = dp.ofproto, dp.ofproto_parser
        inst = [p.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = p.OFPFlowMod(datapath=dp, priority=priority, match=match,
                           instructions=inst, idle_timeout=idle_timeout,
                           hard_timeout=hard_timeout, cookie=cookie)
        dp.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        ofp, p = dp.ofproto, dp.ofproto_parser

        # table-miss -> controller
        actions = [p.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, p.OFPMatch(), actions)
        self.datapaths[dp.id] = dp
        self.log_event("SWITCH", f"Switch {dp.id} connected (table-miss to controller)")

        # ARP flood
        match_arp = p.OFPMatch(eth_type=0x0806)
        actions_arp = [p.OFPActionOutput(ofp.OFPP_FLOOD)]
        self.add_flow(dp, 40000, match_arp, actions_arp, cookie=0xA00001)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg, dp = ev.msg, ev.msg.datapath
        ofp, p = dp.ofproto, dp.ofproto_parser
        in_port = msg.match.get('in_port', ofp.OFPP_CONTROLLER)

        pkt = packet.Packet(msg.data)
        ethp = pkt.get_protocol(ethernet.ethernet)
        if not ethp or ethp.ethertype == 0x88cc:
            return

        dpid, src, dst = dp.id, ethp.src, ethp.dst
        self.mac_to_port.setdefault(dpid, {})[src] = in_port
        out_port = self.mac_to_port[dpid].get(dst, ofp.OFPP_FLOOD)
        actions = [p.OFPActionOutput(out_port)]
        data = None if msg.buffer_id != ofp.OFP_NO_BUFFER else msg.data

        out = p.OFPPacketOut(datapath=dp, buffer_id=msg.buffer_id, in_port=in_port,
                             actions=actions, data=data)
        dp.send_msg(out)

        if pkt.get_protocol(arp.arp):
            a = pkt.get_protocol(arp.arp)
            self.log_event("ARP", f"{a.src_ip} -> {a.dst_ip}")

    # ---------- mnexec helpers ----------
    def _read_pid(self):
        if not (os.path.exists(H1_READY_FLAG) and os.path.exists(H1_PID_FILE)):
            return False
        new_pid = open(H1_PID_FILE).read().strip()
        if new_pid and new_pid != self.h1_pid:
            self.h1_pid = new_pid
            self.log_event("MONITOR", "Detected h1 PID=" + self.h1_pid)
            return True
        return False

    def _mn_in_h1(self, cmd, capture=True):
        if not self.h1_pid:
            return 1, ""
        c = ["sudo", "-n", (shutil.which("mnexec") or "/usr/bin/mnexec"),
             "-a", str(self.h1_pid), "bash", "-lc", cmd]
        try:
            if capture:
                out = subprocess.check_output(c, stderr=subprocess.STDOUT, text=True)
                return 0, out
            else:
                rc = subprocess.call(c)
                return rc, ""
        except subprocess.CalledProcessError as e:
            return e.returncode, e.output
        except Exception as e:
            return 1, str(e)

    def _validate_ns_once(self):
        rc, _ = self._mn_in_h1("ip -o link show dev h1-eth0 >/dev/null 2>&1")
        return rc == 0

    def _probe_pair(self, dev, gw, src_ip):
        """
        Probe a given device/gateway pair from h1 netns.

        Returns: (gw_ok: bool, inet_ok: bool, rtt_ms: float|None)
        """
        try:
            self._mn_in_h1(f"arping -c1 -w1 -I {dev} {gw} >/dev/null 2>&1 || true", capture=False)
        except Exception:
            pass

        # ping GW
        try:
            rc_gw, out_gw = self._mn_in_h1(
                f"ping -n -c1 -W{PING_TIMEOUT_S} -I {dev} {gw} | awk -F'[=/ ]' '/time=/{{print $(NF-1)}}'"
            )
        except Exception:
            rc_gw, out_gw = 1, ""

        gw_ok = (rc_gw == 0) and bool(out_gw.strip())
        gw_rtt = float(out_gw.strip()) if gw_ok else None

        # optional INET probe
        inet_ok = True
        inet_rtt = None
        if REQUIRE_INET_OK:
            try:
                self._mn_in_h1(f"ip route del {INET_TARGET_IP}/32 2>/dev/null || true", capture=False)
                self._mn_in_h1(f"ip route replace {INET_TARGET_IP}/32 via {gw} dev {dev} src {src_ip} metric 5", capture=False)

                rc_in, out_in = self._mn_in_h1(
                    f"ping -n -c1 -W{PING_TIMEOUT_S} -I {src_ip} {INET_TARGET_IP} | awk -F'[=/ ]' '/time=/{{print $(NF-1)}}'"
                )
            except Exception:
                rc_in, out_in = 1, ""
            finally:
                try:
                    self._mn_in_h1(f"ip route del {INET_TARGET_IP}/32 2>/dev/null || true", capture=False)
                except Exception:
                    pass

            inet_ok = (rc_in == 0) and bool(out_in.strip())
            inet_rtt = float(out_in.strip()) if inet_ok else None

        rtt = inet_rtt if (REQUIRE_INET_OK and inet_rtt is not None) else gw_rtt
        return gw_ok, inet_ok, rtt

    def _update_ema(self, name, sample_ms):
        if sample_ms is None:
            return
        cur = self._ema[name]
        self._ema[name] = sample_ms if cur is None else (EMA_ALPHA * sample_ms + (1.0 - EMA_ALPHA) * cur)

    # ---------- Per-application policy helpers ----------
    def _ensure_app_policies_installed(self):
        """
        Install iptables mangle rules inside h1 to mark outbound packets for each application,
        and ensure per-ISP routing table entries exist. Idempotent.
        """
        try:
            # for each app create the iptables OUTPUT mangle rule for marking
            for app in APPLICATIONS:
                name = app["name"]
                proto = app["proto"]
                dports = app["dports"]
                mark = self._app_marks[name]

                if isinstance(dports, tuple):
                    lo, hi = dports
                    rule = (f"iptables -t mangle -C OUTPUT -p {proto} --dport {lo}:{hi} -j MARK --set-mark {mark} "
                            f"|| iptables -t mangle -A OUTPUT -p {proto} --dport {lo}:{hi} -j MARK --set-mark {mark}")
                else:
                    rule = (f"iptables -t mangle -C OUTPUT -p {proto} --dport {dports} -j MARK --set-mark {mark} "
                            f"|| iptables -t mangle -A OUTPUT -p {proto} --dport {dports} -j MARK --set-mark {mark}")

                self._mn_in_h1(rule, capture=False)

            # remove any old ip rule for marks we manage, to avoid duplicates; we'll re-add below
            # (remove all marks we use)
            for mark in self._app_marks.values():
                self._mn_in_h1(f"ip rule del fwmark {mark} 2>/dev/null || true", capture=False)

            # Ensure per-ISP routing table entries exist (default + INET target)
            for isp in ISPS:
                tbl = isp["table_id"]
                gw = isp["gw_ip"]
                dev = isp["src_dev"]
                src = isp["src_ip"]
                self._mn_in_h1(f"ip route replace default via {gw} dev {dev} table {tbl} 2>/dev/null || true", capture=False)
                self._mn_in_h1(f"ip route replace {INET_TARGET_IP}/32 via {gw} dev {dev} src {src} table {tbl} 2>/dev/null || true", capture=False)

            self.log_event("APP_POL", "App iptables marks and per-ISP tables ensured (idempotent)")
        except Exception as e:
            self.log_event("ERROR", f"app policy install failed: {e}")

    def _update_app_rules(self):
        """
        For each application, set ip rule 'fwmark <mark> lookup <table>' where
        <table> is chosen as: preferred ISP table if preferred ISP is up,
        otherwise global current ISP's table. If neither available, skip.
        """
        try:
            # If global current isn't set, attempt to pick a live ISP for fallback policies
            fallback_idx = self.current_idx
            if fallback_idx is None:
                for i, isp in enumerate(ISPS):
                    if self._deb[isp["name"]]["is_up"]:
                        fallback_idx = i
                        break

            # Now for each app determine target table and set rule
            for app in APPLICATIONS:
                name = app["name"]
                preferred = app.get("preferred_idx", None)
                mark = self._app_marks[name]

                target_idx = None
                # prefer preferred ISP if it's up
                if isinstance(preferred, int) and 0 <= preferred < len(ISPS):
                    pref_name = ISPS[preferred]["name"]
                    if self._deb[pref_name]["is_up"]:
                        target_idx = preferred

                # else fallback to global chosen
                if target_idx is None:
                    target_idx = fallback_idx

                if target_idx is None:
                    # nothing to do
                    self.log_event("APP_POL", f"No available ISP for app {name}; skipping rule set")
                    continue

                table_id = ISPS[target_idx]["table_id"]

                # remove old rule(s) for this mark and add new one
                self._mn_in_h1(f"ip rule del fwmark {mark} 2>/dev/null || true", capture=False)
                self._mn_in_h1(f"ip rule add fwmark {mark} lookup {table_id}", capture=False)
                self.log_event("APP_POL", f"App {name} (mark={mark}) -> table {table_id} ({ISPS[target_idx]['name']})")
        except Exception as e:
            self.log_event("ERROR", f"update_app_rules failed: {e}")

    # ---------- monitor loop ----------
    def _monitor_loop(self):
        while not (os.path.exists(H1_READY_FLAG) and os.path.exists(H1_PID_FILE)):
            time.sleep(0.5)
        self._read_pid()
        self.log_event("MONITOR", "Health loop started.")

        # install app policy baseline once h1 ready
        try:
            self._ensure_app_policies_installed()
            # initial app rules (may be no current_idx yet; update_app_rules handles fallback)
            self._update_app_rules()
        except Exception as e:
            self.log_event("ERROR", f"App policy baseline failed: {e}")

        while True:
            try:
                self._read_pid()
                if not self._validate_ns_once():
                    self.log_event("WARN", "h1 netns not ready; skipping this cycle")
                    time.sleep(PROBE_INTERVAL_S)
                    continue

                # probes for each ISP
                for isp in ISPS:
                    name, gw, dev, src_ip = isp["name"], isp["gw_ip"], isp["src_dev"], isp["src_ip"]
                    gw_ok, inet_ok, rtt = self._probe_pair(dev, gw, src_ip)

                    self._raw[name]["gw_ok"] = gw_ok
                    self._raw[name]["inet_ok"] = inet_ok
                    self._raw[name]["rtt_ms"] = rtt
                    self._update_ema(name, rtt)

                    st = self._deb[name]
                    is_ok = gw_ok and (inet_ok if REQUIRE_INET_OK else True)
                    if is_ok:
                        st["up_cnt"] += 1
                        st["down_cnt"] = 0
                        if not st["is_up"] and st["up_cnt"] >= DEBOUNCE_UP_OK:
                            st["is_up"] = True
                            self.log_event("ISP_UP", f"{name} debounced UP (gw {gw})")
                    else:
                        st["down_cnt"] += 1
                        st["up_cnt"] = 0
                        if st["is_up"] and st["down_cnt"] >= DEBOUNCE_DOWN_FAIL:
                            st["is_up"] = False
                            self.log_event("ISP_DOWN", f"{name} debounced DOWN (gw {gw})")

                # health summary
                parts = []
                for isp in ISPS:
                    nm = isp["name"]
                    parts.append(f"{nm}:gw={self._raw[nm]['gw_ok']} inet={self._raw[nm]['inet_ok']} ema={_fmt_ema(self._ema[nm])}")
                self.log_event("HC", " | ".join(parts) + f" (prefer RTT=True INET={REQUIRE_INET_OK})")

                # choose best among UPs by min EMA (tie-break = list order)
                chosen_idx = self.current_idx
                up_isps = [i for i in ISPS if self._deb[i["name"]]["is_up"]]
                if up_isps:
                    best = None  # (idx, ema)
                    for i, isp in enumerate(ISPS):
                        nm = isp["name"]
                        if not self._deb[nm]["is_up"]:
                            continue
                        ema = self._ema[nm]
                        if ema is None:
                            if best is None:
                                best = (i, 1e9)
                            continue
                        if best is None or ema < best[1] - RTT_SWITCH_MARGIN_MS:
                            best = (i, ema)
                    if best is not None:
                        chosen_idx = best[0]

                # Bootstrap prefer Glo initially
                if self.current_idx is None and up_isps:
                    if self._deb[ISPS[0]["name"]]["is_up"] or self._raw[ISPS[0]["name"]]["gw_ok"]:
                        chosen_idx = 0
                    else:
                        for i in range(1, len(ISPS)):
                            nm = ISPS[i]["name"]
                            if self._deb[nm]["is_up"] or self._raw[nm]["gw_ok"]:
                                chosen_idx = i
                                break

                # Prefer Glo when within slack
                pref_up = self._deb[ISPS[PREFERRED_IDX]["name"]]["is_up"]
                pref_ema = self._ema[ISPS[PREFERRED_IDX]["name"]]
                if pref_up and (pref_ema is not None):
                    best_ema = None
                    for isp in ISPS:
                        nm = isp["name"]
                        if not self._deb[nm]["is_up"]:
                            continue
                        e = self._ema[nm]
                        if e is None:
                            continue
                        if best_ema is None or e < best_ema:
                            best_ema = e
                    if best_ema is not None and pref_ema <= best_ema + PREFER_SLACK_MS:
                        chosen_idx = PREFERRED_IDX

                # Hold-down timing
                now = time.time()
                hold_ok = (now - self._last_switch_ts) >= HOLDDOWN_SEC

                if self.current_idx is None and chosen_idx is not None:
                    self.log_event("FAILOVER", "None \u2192 " + ISPS[chosen_idx]['name'])
                    self.current_idx = chosen_idx
                    self._last_switch_ts = now
                    self._on_route_change()

                elif chosen_idx is not None and chosen_idx != self.current_idx:
                    if hold_ok:
                        prev = ISPS[self.current_idx]["name"] if self.current_idx is not None else "None"
                        prev_nm = ISPS[self.current_idx]["name"] if self.current_idx is not None else None
                        failover_reason = (prev_nm is not None) and (not self._deb[prev_nm]["is_up"])

                        if failover_reason:
                            self.log_event("FAILOVER", f"{prev} \u2192 {ISPS[chosen_idx]['name']}")
                        else:
                            self.log_event("RTT_SWITCH", f"{prev} \u2192 {ISPS[chosen_idx]['name']}")

                        self.current_idx = chosen_idx
                        self._last_switch_ts = now
                        self._on_route_change()
                    else:
                        cur = ISPS[self.current_idx]['name'] if self.current_idx is not None else "None"
                        nxt = ISPS[chosen_idx]['name']
                        self.log_event("DECISION(no-change)", f"current={cur} best={nxt} (hold-down)")
                else:
                    cur = ISPS[self.current_idx]['name'] if self.current_idx is not None else "None"
                    self.log_event("DECISION", f"current={cur} \u2192 chosen={cur} (prefer RTT=True; hold_ok={hold_ok})")

                # Periodically refresh app rules in case of transient changes (idempotent)
                self._update_app_rules()

                time.sleep(PROBE_INTERVAL_S)

            except Exception as e:
                self.log_event("ERROR", f"monitor loop exception: {e}")
                time.sleep(PROBE_INTERVAL_S)
                continue

    # ---------- route change on h1 ----------
    def _on_route_change(self):
        isp = ISPS[self.current_idx]
        gw, dev, src = isp["gw_ip"], isp["src_dev"], isp["src_ip"]
        cmds = [
            f"ip route replace default via {gw} dev {dev}",
            f"ip route replace {INET_TARGET_IP}/32 via {gw} dev {dev} src {src} metric 5",
            "ip route show",
        ]
        for c in cmds:
            rc, out = self._mn_in_h1(c)
            if c.startswith("ip route show") and rc == 0:
                for line in out.splitlines():
                    self.log_event("ROUTE_TABLE", line)
        self.log_event("ROUTE", f"default via {isp['name']} {gw} dev {dev}")

        # write provision trigger so topology's route-watcher persists ISP transit routes/interfaces
        entries = [("Glo", "glo-eth1"), ("MTN", "mtn-eth1"), ("Airtel", "air-eth1")]
        self._write_provision_trigger(entries)

        # ensure per-ISP tables are up-to-date and update per-app ip rules
        for s in ISPS:
            tbl = s["table_id"]
            gw2, dev2, src2 = s["gw_ip"], s["src_dev"], s["src_ip"]
            self._mn_in_h1(f"ip route replace default via {gw2} dev {dev2} table {tbl} 2>/dev/null || true", capture=False)
            self._mn_in_h1(f"ip route replace {INET_TARGET_IP}/32 via {gw2} dev {dev2} src {src2} table {tbl} 2>/dev/null || true", capture=False)

        # update app-specific rules so each app uses its preferred or fallback ISP table
        self._update_app_rules()

    # ---------- write provision trigger (zero-touch) ----------
    def _write_provision_trigger(self, entries):
        """
        entries: iterable of (name, iface) tuples like ("Glo", "glo-eth1")
        Writes PROVISION_TRIGGER atomically.
        """
        try:
            tmp = PROVISION_TRIGGER + ".tmp"
            with open(tmp, "w") as f:
                for name, iface in entries:
                    f.write(f"{name},{iface}\n")
            os.replace(tmp, PROVISION_TRIGGER)
            self.log_event("PROVISION_TRIGGER", f"Wrote {PROVISION_TRIGGER} for topology to process")
        except Exception as e:
            self.log_event("ERROR", f"failed writing provision trigger: {e}")
