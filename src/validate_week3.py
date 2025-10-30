# src/validate_week3.py
# -*- coding: utf-8 -*-
import os, pathlib, glob, sys
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

def latest(path, pattern):
    files = sorted(glob.glob(str(path / pattern)))
    return files[-1] if files else None

def ok(flag): return "PASS" if flag else "FAIL"

def main():
    # 1) Artefactos
    csv_latest  = latest(REPORTS, "report_daily_*.csv")
    html_latest = latest(REPORTS, "report_daily_*.html")
    png_avg     = latest(REPORTS, "fig_avg_*.png")
    png_box     = latest(REPORTS, "fig_box_*.png")

    print("=== Semana 3 • Validación ===")
    print(f"- CSV encontrado : {bool(csv_latest)} -> {csv_latest}")
    print(f"- HTML encontrado: {bool(html_latest)} -> {html_latest}")
    print(f"- PNG avg        : {bool(png_avg)} -> {png_avg}")
    print(f"- PNG box        : {bool(png_box)} -> {png_box}")

    art_ok = all([csv_latest, html_latest, png_avg, png_box])

    # 2) Secciones y checks de contenido (si hay CSV)
    sections_ok = False
    avg_ok = corr_ok = pct_ok = rank_ok = False
    subjects_ok = False
    monotonic_ok = False

    if csv_latest:
        df = pd.read_csv(csv_latest)
        if "section" not in df.columns:
            print("FAIL: CSV no trae columna 'section'.")
            sys.exit(1)

        needed = {
            "avg_g3_by_school_subject",
            "corr_overall",
            "corr_by_subject",
            "percentiles_g3",
            "top10_g3_by_subject",
        }
        present = set(df["section"].unique())
        sections_ok = needed.issubset(present)
        print(f"- Secciones en CSV: {present}  -> {ok(sections_ok)}")

        # avg
        try:
            avg_df = df[df["section"]=="avg_g3_by_school_subject"][["school","subject","avg_g3","n"]]
            avg_ok = (len(avg_df)>0) and avg_df["n"].gt(0).all() and avg_df["avg_g3"].notna().all()
            # subjects
            subjects_ok = set(avg_df["subject"].unique()) >= {"Math","Portuguese"}
        except Exception:
            avg_ok = False

        # corr
        try:
            corr_overall = df[df["section"]=="corr_overall"]["corr_g1_g3_overall"].astype(float)
            corr_by_subj = df[df["section"]=="corr_by_subject"][["subject","corr_g1_g3"]].dropna()
            corr_ok = (
                (len(corr_overall)==1) and
                corr_overall.between(-1,1).all() and
                (len(corr_by_subj)>=2) and
                corr_by_subj["corr_g1_g3"].astype(float).between(-1,1).all()
            )
        except Exception:
            corr_ok = False

        # percentiles
        try:
            pct_df = df[df["section"]=="percentiles_g3"][["subject","p10","p25","p50","p75","p90"]].dropna()
            def mono(row): return row["p10"]<=row["p25"]<=row["p50"]<=row["p75"]<=row["p90"]
            monotonic_ok = pct_df.apply(mono, axis=1).all() and (len(pct_df)>=2)
            pct_ok = monotonic_ok
        except Exception:
            pct_ok = False

        # ranking
        try:
            rank_df = df[df["section"]=="top10_g3_by_subject"].copy()
            rank_ok = (len(rank_df)>0) and rank_df["rk"].between(1,10).all()
        except Exception:
            rank_ok = False

    # Resumen
    print(f"- Artefactos (CSV/HTML/PNGs): {ok(art_ok)}")
    print(f"- Secciones requeridas       : {ok(sections_ok)}")
    print(f"- AVG G3                     : {ok(avg_ok)}")
    print(f"- Subjects (Math/Portuguese) : {ok(subjects_ok)}")
    print(f"- CORR (global/subject)      : {ok(corr_ok)}")
    print(f"- Percentiles monótonos      : {ok(pct_ok)}")
    print(f"- Ranking top10 (rk 1..10)   : {ok(rank_ok)}")

    all_ok = all([art_ok, sections_ok, avg_ok, subjects_ok, corr_ok, pct_ok, rank_ok])
    print("\n>>> Semana 3:", "COMPLETA ✅" if all_ok else "INCOMPLETA ❌")
    sys.exit(0 if all_ok else 2)

if __name__ == "__main__":
    main()
