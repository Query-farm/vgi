"""Linux process accounting. RSS sums double-count shared pages; use PSS too.

Worker CPU is a sampled lower bound: a process can exit between observations.
Host query CPU is measured precisely inside driver.cpp with getrusage instead.
"""
from __future__ import annotations

import os
from pathlib import Path


def parse_stat(text):
    # comm may contain spaces and parentheses. Fields below are relative to state.
    fields = text[text.rfind(")") + 2:].split()
    return {"ppid": int(fields[1]), "user_s": int(fields[11]) / os.sysconf("SC_CLK_TCK"),
            "system_s": int(fields[12]) / os.sysconf("SC_CLK_TCK"), "start_ticks": int(fields[19])}


def memory(pid):
    status = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in {"VmRSS", "VmHWM", "RssAnon", "VmSwap"}:
            status[key] = int(value.split()[0]) * 1024
    pss = private = None
    try:
        smaps = {}
        for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in {"Pss", "Private_Clean", "Private_Dirty", "Private_Hugetlb"}:
                smaps[key] = int(value.split()[0]) * 1024
        pss = smaps.get("Pss")
        private = sum(smaps.get(k, 0) for k in ("Private_Clean", "Private_Dirty", "Private_Hugetlb"))
    except (OSError, ValueError):
        pass
    return {"rss_bytes": status.get("VmRSS", 0), "hwm_bytes": status.get("VmHWM", 0),
            "anonymous_bytes": status.get("RssAnon", 0), "swap_bytes": status.get("VmSwap", 0),
            "pss_bytes": pss, "private_bytes": private}


class ProcessTree:
    def __init__(self, pid):
        self.pid = pid
        self.known = {}  # pid -> start time; follow children even after reparenting

    def snapshot(self):
        stats = {}
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit():
                try:
                    stats[int(entry.name)] = parse_stat((entry / "stat").read_text())
                except (OSError, ValueError, IndexError):
                    pass
        selected = {p for p, start in self.known.items() if p in stats and stats[p]["start_ticks"] == start}
        if self.pid in stats and (self.pid not in self.known or self.known[self.pid] == stats[self.pid]["start_ticks"]):
            selected.add(self.pid)
        while True:
            children = {p for p, stat in stats.items() if stat["ppid"] in selected}
            if children <= selected:
                break
            selected |= children
        result = []
        for pid in sorted(selected):
            try:
                item = {"pid": pid, "role": "host" if pid == self.pid else "worker", **stats[pid], **memory(pid)}
                # Reject a PID that got recycled while reading smaps.
                if parse_stat(Path(f"/proc/{pid}/stat").read_text())["start_ticks"] != item["start_ticks"]:
                    continue
                self.known[pid] = item["start_ticks"]
                result.append(item)
            except (OSError, ValueError, IndexError):
                pass
        return result


def totals(processes, role):
    items = [p for p in processes if role == "tree" or p["role"] == role]
    result = {"processes": len(items)}
    for key in ("rss_bytes", "pss_bytes", "private_bytes", "anonymous_bytes", "swap_bytes"):
        values = [p[key] for p in items]
        result[key] = sum(values) if all(v is not None for v in values) else None
    return result


def worker_cpu(samples):
    baseline = {(p["pid"], p["start_ticks"]): p["user_s"] + p["system_s"]
                for p in samples[0]["processes"] if p["role"] == "worker"}
    last = {}
    for sample in samples:
        for p in sample["processes"]:
            if p["role"] == "worker":
                key = p["pid"], p["start_ticks"]
                last[key] = max(last.get(key, 0), p["user_s"] + p["system_s"])
    return sum(max(0, cpu - baseline.get(key, 0)) for key, cpu in last.items())
