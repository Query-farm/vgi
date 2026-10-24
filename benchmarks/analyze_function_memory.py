#!/usr/bin/env python3
"""Combine completed function-memory runs into a median table and HTML artifact."""
import argparse
import csv
import html
import json
from pathlib import Path
import statistics

from bench import write_json


def analyze(runs, output):
    rows = []
    failures = []
    for run in runs:
        metadata = json.loads((run / "metadata.json").read_text())
        results = json.loads((run / "results.json").read_text())
        grouped = {}
        for result in results:
            name = result["case"]["name"]
            if result["status"] != "ok":
                failures.append(dict(run=run.name, case=name, trial=result["trial"], error=result.get("error")))
                continue
            grouped.setdefault(name, []).append(result)
        for name, trials in grouped.items():
            metrics = []
            for result in trials:
                events = result["events"]
                queries = [e for e in events if e["phase"].startswith("query:")]
                releases = [e for e in events if e["phase"].startswith("released:")]
                stage = {e["phase"]:e for e in events}
                process_peak = max(e.get("peak_worker_processes", e["after"]["worker"]["processes"]) for e in queries)
                if any("peak_worker_processes" not in e for e in queries):
                    with (run/f"{name}-{result['trial']}"/"samples.jsonl").open() as f:
                        for line in f:
                            sample = json.loads(line)
                            if sample["phase"].startswith("query:"):
                                process_peak = max(process_peak,sample["worker"]["processes"])
                # Query peak excludes setup, verification and cleanup. Lifetime
                # HWM complements sampling but includes those earlier phases.
                m = dict(host_peak_mib=max(e["peaks"]["host"]["rss_bytes"] for e in queries)/2**20,
                         worker_peak_mib=max(e["peaks"]["worker"]["rss_bytes"] for e in queries)/2**20,
                         host_lifetime_hwm_mib=max(e["host_lifetime_peak_rss_bytes"] for e in queries)/2**20,
                         worker_processes=process_peak,
                         wall_s=statistics.median(e["wall_s"] for e in queries),
                         host_cpu_s=statistics.median(e["host_cpu_s"] for e in queries),
                         worker_cpu_sampled_s=statistics.median(e["worker_cpu_sampled_s"] for e in queries))
                pss = [e["peaks"]["worker"]["pss_bytes"] for e in queries]
                m["worker_peak_pss_mib"] = max(pss)/2**20 if all(x is not None for x in pss) else None
                for label in ("before_queries","cache_cleared","workers_flushed","database_closed","after_arrow_release","after_glibc_trim"):
                    if label in stage:
                        event = stage[label]
                        m[label+"_host_mib"] = event["after"]["host"]["rss_bytes"]/2**20
                        m[label+"_worker_mib"] = event["after"]["worker"]["rss_bytes"]/2**20
                        m[label+"_arrow_live_mib"] = event["allocator"].get("arrow",{}).get("bytes_allocated",0)/2**20 if "arrow" in event["allocator"] else None
                for role in ("host","worker"):
                    m[f"released_{role}_growth_mib"] = (releases[-1]["after"][role]["rss_bytes"]-releases[0]["after"][role]["rss_bytes"])/2**20
                    # Late-run spread is more useful than attributing warmup
                    # allocation to a leak. No statistical leak claim is made.
                    late = [e["after"][role]["rss_bytes"] for e in releases[len(releases)//2:]]
                    m[f"released_{role}_late_spread_mib"] = (max(late)-min(late))/2**20
                arrow = releases[-1]["allocator"].get("arrow",{})
                m["released_arrow_live_mib"] = arrow.get("bytes_allocated",0)/2**20 if arrow else None
                m["arrow_high_water_mib"] = arrow.get("max_memory",0)/2**20 if arrow else None
                metrics.append(m)
            row = dict(run=run.name, case=name, family=trials[0]["case"]["family"],
                       worker="native" if trials[0]["case"].get("native") else metadata["arguments"]["worker"],
                       trials=len(trials), iterations=metadata["arguments"]["iterations"],
                       profile=metadata["arguments"]["profile"])
            for key in dict.fromkeys(k for m in metrics for k in m):
                values = [m.get(key) for m in metrics]
                row[key] = statistics.median(values) if all(x is not None for x in values) else None
            rows.append(row)
    output.mkdir(parents=True,exist_ok=True)
    write_json(output/"function-summary.json",rows)
    write_json(output/"function-failures.json",failures)
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with (output/"function-summary.csv").open("w") as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    columns=["run","case","worker","trials","host_peak_mib","worker_peak_mib","worker_peak_pss_mib","worker_processes","released_arrow_live_mib","database_closed_host_mib","after_arrow_release_host_mib","wall_s"]
    def value(v):
        return "—" if v is None else f"{v:.2f}" if isinstance(v,float) else html.escape(str(v))
    table="".join("<tr>"+"".join("<td>"+value(r.get(k))+"</td>" for k in columns)+"</tr>" for r in rows)
    errors="".join("<li>"+html.escape(f"{f['run']}/{f['case']}: {f['error']}")+"</li>" for f in failures)
    output.joinpath("function-overview.html").write_text("""<!doctype html><meta charset="utf-8"><title>VGI function memory research</title>
<style>body{font:14px system-ui;margin:24px;color:#192838}table{border-collapse:collapse}th{position:sticky;top:0;background:#eaf0f5}td,th{padding:7px;border-bottom:1px solid #ddd;text-align:right}td:nth-child(-n+3){text-align:left}input{padding:9px;width:420px}tr:hover{background:#f0f5f9}</style>
<h1>VGI function memory research</h1><p>Independent fresh-process trials; values are trial medians. Memory is MiB. Worker RSS sums shared pages; PSS apportions them. Query peaks exclude setup and cleanup. CPU/profile runs and different worker runtimes are not regression comparisons.</p>
<label>Filter cases <input id="filter" placeholder="window, scalar, python…"></label>
<table><thead><tr>"""+"".join("<th>"+html.escape(k)+"</th>" for k in columns)+"</tr></thead><tbody>"+table+"</tbody></table><h2>Incomplete cases</h2><ul>"+errors+"</ul><script>document.querySelector('#filter').addEventListener('input',e=>{for(const r of document.querySelectorAll('tbody tr'))r.hidden=!r.textContent.toLowerCase().includes(e.target.value.toLowerCase())})</script>")
    print(f"{len(rows)} summaries; {len(failures)} failed trials")


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs",nargs="+",type=Path)
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args()
    analyze(args.runs,args.out)
