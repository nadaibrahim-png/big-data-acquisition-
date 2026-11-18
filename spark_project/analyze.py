"""Summarise results/timings.csv into tables (markdown + csv) and charts in results/figures/.
Run on the host:  py analyze.py
"""
import re
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).parent
RES = ROOT / "results"
FIG = RES / "figures"
FIG.mkdir(exist_ok=True)

BLOCKS = {"A_w2": "A: baseline (2 workers, cache, broadcast)",
          "B_w1": "B: 1 worker",
          "C_nocache": "C: no cache",
          "D_smj": "D: sort-merge join"}
STAGES = ["raw_count", "load_clean_join", "q1_hourly_by_borough", "q2_top_routes",
          "q3_top_zones_window", "q4_moving_avg", "q5_tip_by_payment", "fit_LinearRegression"]

t = pd.read_csv(RES / "timings.csv")
t["block"] = t["tag"].str.replace(r"_r\d+$", "", regex=True)
t["rep"] = t["tag"].str.extract(r"_r(\d+)$")[0]

# disturbed runs (executor lost) are excluded from the statistics
disturbed = set()
for err in (RES / "logs").glob("*.log.err"):
    raw = err.read_bytes()
    # PowerShell 5.1 writes the redirected stderr as UTF-16
    txt = raw.decode("utf-16", errors="ignore") if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else raw.decode("utf-8", errors="ignore")
    if re.search(r"Lost executor|ExecutorLostFailure", txt):
        disturbed.add(err.name.replace(".log.err", ""))
disturbed |= set(t.loc[t["seconds"] < 0, "tag"])   # wall-clock jump inside a stage
bench = t[t["block"].isin(BLOCKS) & ~t["tag"].isin(disturbed)].copy()

# ---- per-run groups ----
def group_of(stage):
    if stage in ("raw_count", "load_clean_join"): return "load"
    if stage.startswith("q"): return "analytics"
    return "ml"
bench["group"] = bench["stage"].map(group_of)
per_run = bench.pivot_table(index=["block", "tag"], columns="group", values="seconds", aggfunc="sum")
per_run["total"] = per_run.sum(axis=1)
per_run = per_run.reset_index()

summary = per_run.groupby("block")[["load", "analytics", "ml", "total"]].agg(["mean", "std"]).round(1)
summary = summary.reindex([b for b in BLOCKS if b in summary.index])
runs = per_run.groupby("block").size().reindex(summary.index)

base = summary.loc["A_w2", ("total", "mean")] if "A_w2" in summary.index else None
lines = ["| Block | Runs | Load (s) | Analytics q1-q5 (s) | LR fit (s) | Total (s) | vs A |",
         "|---|---|---|---|---|---|---|"]
for b in summary.index:
    s = summary.loc[b]
    f = lambda g: f"{s[(g,'mean')]:.1f} ± {0 if pd.isna(s[(g,'std')]) else s[(g,'std')]:.1f}"
    ratio = f"{s[('total','mean')]/base:.2f}×" if base else "-"
    lines.append(f"| {BLOCKS[b]} | {runs[b]} | {f('load')} | {f('analytics')} | {f('ml')} | {f('total')} | {ratio} |")
block_table = "\n".join(lines)

stage_mean = bench.groupby(["stage", "block"])["seconds"].mean().unstack().reindex(STAGES)
stage_mean = stage_mean[[b for b in BLOCKS if b in stage_mean.columns]].round(1)
st_lines = ["| Stage | " + " | ".join(stage_mean.columns) + " |", "|---" * (len(stage_mean.columns) + 1) + "|"]
for st, row in stage_mean.iterrows():
    st_lines.append(f"| {st} | " + " | ".join(f"{v:.1f}" for v in row) + " |")
stage_table = "\n".join(st_lines)

# ---- model metrics ----
metrics = []
for m in (RES / "logs").glob("*_metrics.txt"):
    for line in m.read_text().splitlines():
        p = line.split(",")
        if len(p) == 4:
            metrics.append({"tag": m.name.replace("_metrics.txt", ""), "model": p[1],
                            "rmse": float(p[2].split("=")[1]), "r2": float(p[3].split("=")[1])})
metrics = pd.DataFrame(metrics)
fit = t[t["stage"].str.startswith("fit_")]
model_lines = ["| Model | RMSE (min) | R² | Fit time (s) |", "|---|---|---|---|"]
if not metrics.empty:
    for model, g in metrics.groupby("model"):
        ft = fit[(fit["stage"] == f"fit_{model}") & (fit["tag"] == "F_full_gbt")]["seconds"]  # same run for both models
        model_lines.append(f"| {model} | {g['rmse'].iloc[0]:.3f} | {g['r2'].iloc[0]:.4f} | {ft.mean():.1f} |")
model_table = "\n".join(model_lines)

# ---- charts ----
colors = {"load": "#4C72B0", "analytics": "#DD8452", "ml": "#55A868"}
means = summary.xs("mean", axis=1, level=1)
fig, ax = plt.subplots(figsize=(8, 4.5))
bottom = pd.Series(0.0, index=means.index)
for g, label in [("load", "Load + clean + join"), ("analytics", "Analytics q1–q5"), ("ml", "LinearRegression fit")]:
    ax.bar([BLOCKS[b].split(":")[0] + ":" + BLOCKS[b].split(":")[1].split("(")[0] for b in means.index],
           means[g], bottom=bottom, label=label, color=colors[g])
    bottom += means[g]
errs = summary.xs("std", axis=1, level=1)["total"].fillna(0)
ax.errorbar(range(len(means)), means["total"], yerr=errs, fmt="none", ecolor="black", capsize=4)
ax.set_ylabel("seconds (mean of runs)")
ax.set_title("End-to-end time per configuration")
ax.legend(frameon=False)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); fig.savefig(FIG / "total_by_block.png", dpi=150); plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 4.5))
stage_mean.plot.bar(ax=ax, width=0.8)
ax.set_ylabel("seconds (mean)"); ax.set_xlabel("")
ax.set_title("Time per stage and configuration")
ax.set_xticklabels([s.replace("_", "\n", 1) for s in stage_mean.index], rotation=0, fontsize=8)
ax.legend([BLOCKS[c].split(":")[0] for c in stage_mean.columns], title="Block", frameon=False)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); fig.savefig(FIG / "stages_by_block.png", dpi=150); plt.close(fig)

per_run.to_csv(RES / "summary_per_run.csv", index=False)
(RES / "summary.md").write_text(
    "## Per-block summary (mean ± std over runs)\n\n" + block_table +
    "\n\n## Mean seconds per stage\n\n" + stage_table +
    "\n\n## Model quality\n\n" + model_table +
    f"\n\nExcluded (executor lost or clock jump): {sorted(disturbed) or 'none'}\n", encoding="utf-8")
print((RES / "summary.md").read_text(encoding="utf-8"))
