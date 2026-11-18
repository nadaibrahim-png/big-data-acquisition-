"""Build report/Spark_Taxi_Report.docx from the benchmark results.
Run on the host after run_all.ps1:  py build_report.py
"""
from pathlib import Path
from docx import Document
from docx.shared import Cm, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

import analyze as A          # computes tables + figures from results/timings.csv
from report_text import TEXT  # narrative paragraphs (edit there)

ROOT = Path(__file__).parent
OUT = ROOT / "report"; OUT.mkdir(exist_ok=True)
SHOTS = ROOT / "screenshots"

doc = Document()
doc.styles["Normal"].font.name = "Calibri"
doc.styles["Normal"].font.size = Pt(11)

def para(text, style=None):
    for chunk in text.strip().split("\n\n"):
        doc.add_paragraph(" ".join(chunk.split()), style=style)

def bullets(items):
    for it in items:
        doc.add_paragraph(it, style="List Bullet")

def table(header, rows):
    t = doc.add_table(rows=1, cols=len(header)); t.style = "Light Grid Accent 1"
    for c, h in zip(t.rows[0].cells, header): c.text = str(h)
    for r in rows:
        cells = t.add_row().cells
        for c, v in zip(cells, r): c.text = str(v)
    doc.add_paragraph()

def figure(path, caption, width=15):
    if Path(path).exists():
        doc.add_picture(str(path), width=Cm(width))
        doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
        cap = doc.add_paragraph(caption); cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
        cap.runs[0].italic = True

def code(text):
    p = doc.add_paragraph(); r = p.add_run(text.strip()); r.font.name = "Consolas"; r.font.size = Pt(8.5)

def cache_table():
    """Peak cache memory/disk per run: cache_stats.csv samples matched to run windows in run_all_progress.log."""
    import re, pandas as pd
    cs_path, prog = ROOT / "results" / "cache_stats.csv", ROOT / "results" / "run_all_progress.log"
    files = [p for p in [cs_path, ROOT / "results" / "archive_unsynced_ABC" / "cache_stats_first.csv"] if p.exists()]
    if not files: return []
    cs = pd.concat([pd.read_csv(p) for p in files])
    windows, start = {}, {}
    for line in prog.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"(\d\d:\d\d:\d\d) (start|done)\s+(\S+)", line)
        if m:
            tm, kind, tag = m.groups()
            if kind == "start": start[tag] = tm
            elif tag in start: windows[tag] = (start[tag], tm)   # last attempt wins
    rows = []
    for tag in sorted(windows):
        if not re.match(r"(A_w2|B_w1|D_smj)_r\d", tag): continue   # cached blocks only
        a, b = windows[tag]
        w = cs[(cs["time"] >= a) & (cs["time"] <= b) & (cs["total_parts"] > 0)]
        if len(w):
            rows.append([tag, int(w["n_exec"].max()), f"{int(w['cached_parts'].max())}/{int(w['total_parts'].max())}",
                         f"{int(w['n_exec'].max()) * 281}", int(w["disk_MB"].max())])
    return rows

# ---------------- title ----------------
doc.add_heading(TEXT["title"], 0)
para(TEXT["subtitle"])

doc.add_heading("1. Objective", 1);                 para(TEXT["objective"])
doc.add_heading("2. Cluster architecture", 1);      para(TEXT["architecture"])
table(["Component", "Container", "Resources"], TEXT["cluster_rows"])
figure(SHOTS / "Spark_Master.png", "Figure 1 – Spark master UI with the two registered workers")

doc.add_heading("3. Dataset", 1);                   para(TEXT["dataset"])
doc.add_heading("4. Pipeline", 1);                  para(TEXT["pipeline_intro"])
bullets(TEXT["pipeline_steps"])
figure(SHOTS / "spark_job.png", "Figure 2 – Jobs of one pipeline run in the Spark application UI")
figure(SHOTS / "SQL_plan.png", "Figure 3 – SQL physical plan (BroadcastHashJoin of trips with zone lookup)")

doc.add_heading("5. Experimental design", 1);       para(TEXT["design"])
table(["Block", "Workers", "Cache", "Join", "Question"], TEXT["design_rows"])
para(TEXT["fixed_settings"])

# ---------------- results ----------------
doc.add_heading("6. Results", 1)
para("All ratios in this section come from the final batch, in which the blocks ran in sequence. Section 7 explains why they are approximate.")
doc.add_heading("6.1 End-to-end time per configuration", 2)
s = A.summary
fmt = lambda b, g: f"{s.loc[b, (g, 'mean')]:.1f} ± {0 if A.pd.isna(s.loc[b, (g, 'std')]) else s.loc[b, (g, 'std')]:.1f}"
base = s.loc["A_w2", ("total", "mean")]
table(["Block", "Runs", "Load+clean+join (s)", "Analytics q1–q5 (s)", "LR fit (s)", "Total (s)", "Relative to A"],
      [[A.BLOCKS[b], int(A.runs[b]), fmt(b, "load"), fmt(b, "analytics"), fmt(b, "ml"), fmt(b, "total"),
        f"{s.loc[b, ('total', 'mean')] / base:.2f}×"] for b in s.index])
figure(A.FIG / "total_by_block.png", "Figure 4 – Mean end-to-end time per block, split by pipeline phase (error bar = std of total)")

doc.add_heading("6.2 Time per stage", 2)
sm = A.stage_mean
table(["Stage"] + [c.split("_")[0] for c in sm.columns],
      [[st] + [f"{v:.1f}" for v in row] for st, row in sm.iterrows()])
figure(A.FIG / "stages_by_block.png", "Figure 5 – Mean seconds per stage for each block")

doc.add_heading("6.3 Join strategy in the physical plan", 2); para(TEXT["plans_intro"]); code(TEXT["plans"])

doc.add_heading("6.4 Where the cached data lives", 2); para(TEXT["cache_intro"])
cache_rows = cache_table()
if cache_rows:
    table(["Run", "Executors", "Cached partitions", "Storage memory available (MB, approx.)", "Peak disk reported by sampler (MB)"], cache_rows)
figure(SHOTS / "Spark_Storage.png", "Figure 6 – Storage tab: the cached joined DataFrame (MEMORY_AND_DISK)")
para(TEXT["cache_discussion"])

doc.add_heading("6.5 Analytics output", 2);         para(TEXT["analytics_results"])
doc.add_heading("6.6 Machine learning", 2);        para(TEXT["ml_intro"])
if not A.metrics.empty:
    fit = A.t[A.t["tag"] == "F_full_gbt"]
    rows = []
    for model in ["LinearRegression", "GBT"]:
        m = A.metrics[(A.metrics["model"] == model) & (A.metrics["tag"] == "F_full_gbt")]
        if m.empty: continue
        ft = fit[fit["stage"] == f"fit_{model}"]["seconds"]
        rows.append([model, f"{m['rmse'].iloc[0]:.3f}", f"{m['r2'].iloc[0]:.4f}", f"{ft.iloc[0]:.1f}" if len(ft) else "-"])
    table(["Model", "RMSE (minutes)", "R²", "Fit time (s)"], rows)
para(TEXT["ml_discussion"])

doc.add_heading("7. Discussion", 1)
para(TEXT["discussion_intro"])
table(["Ratio", "Nov 2 batch", "Nov 6, before clock fix", "Final batch"], TEXT["batch_rows"])
for head, body in TEXT["discussion"]:
    doc.add_heading(head, 2); para(body)

doc.add_heading("8. Limitations", 1);               bullets(TEXT["limitations"])
doc.add_heading("9. Conclusion", 1);                para(TEXT["conclusion"])
doc.add_heading("Appendix A – How to reproduce", 1); code(TEXT["reproduce"])
if A.disturbed:
    para("Runs excluded because an executor was lost: " + ", ".join(sorted(A.disturbed)))

out = OUT / "Spark_Taxi_Report.docx"
doc.save(out)
print("written", out)
