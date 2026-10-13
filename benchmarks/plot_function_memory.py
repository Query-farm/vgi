#!/usr/bin/env python3
"""Plot the October 13 research artifacts; optional Matplotlib dependency."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main(root, out):
    rows=json.loads((root/"function-summary.json").read_text())
    lookup={(r["run"],r["case"]):r for r in rows}
    plt.rcParams.update({"font.size":10,"axes.spines.top":False,"axes.spines.right":False})
    fig, axes=plt.subplots(1,3,figsize=(14,4.5),layout="constrained")
    for run, label, color in (("retention-rust","Before fix","#c6423a"),("schema-fix-rust","After fix","#2574a9")):
        result=json.loads((root/"results"/run/"buffered_t4-0"/"result.json").read_text())
        before=next(e["allocator"]["arrow"]["bytes_allocated"] for e in result["events"] if e["phase"]=="before_queries")
        values=[(e["allocator"]["arrow"]["bytes_allocated"]-before)/1024 for e in result["events"] if e["phase"].startswith("released:")]
        axes[0].plot(range(1,len(values)+1),values,"o-",label=label,color=color)
    axes[0].set(title="Buffered schema leak",xlabel="Completed queries",ylabel="Extra live Arrow allocation (KiB)")
    axes[0].legend(frameon=False)
    columns=[1,4,16,32]
    values=[lookup["additional-python",f"scalar_connections_{n}"]["worker_peak_pss_mib"] for n in columns]
    axes[1].bar([str(n) for n in columns],values,color="#2574a9")
    axes[1].set(title="Python scalar worker growth",xlabel="Scalar expressions / active workers",ylabel="Combined worker peak PSS (MiB)")
    for i,value in enumerate(values):
        axes[1].text(i,value+35,f"{value:,.0f}",ha="center",fontsize=9)
    payloads=[8,32,128]
    native=[lookup["full-rust",f"window_strings_{n}_native"]["host_peak_mib"] for n in (128,512,2048)]
    vgi=[lookup[run,f"window_strings_{n}_vgi"]["host_peak_mib"] for run,n in (("crosscheck-python",128),("crosscheck-python",512),("large-window-python",2048))]
    axes[2].plot(payloads,native,"o-",label="Native DuckDB",color="#6f7882")
    axes[2].plot(payloads,vgi,"o-",label="VGI + Python",color="#2574a9")
    axes[2].set(title="String window materialization",xlabel="Partition string payload (MiB)",ylabel="Host peak RSS (MiB)")
    axes[2].legend(frameon=False)
    for ax in axes:
        ax.grid(axis="y",alpha=.18)
        ax.set_axisbelow(True)
    fig.suptitle("VGI memory investigation · EC2 ARM64 · 2026-10-13",fontsize=15)
    out.mkdir(parents=True,exist_ok=True)
    for suffix in ("png","svg"):
        fig.savefig(out/f"function-memory-findings.{suffix}",dpi=170,bbox_inches="tight")


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("root",type=Path)
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    main(args.root,args.out)
