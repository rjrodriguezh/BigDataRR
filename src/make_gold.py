# -*- coding: utf-8 -*-
import os, zipfile, pathlib
import pandas as pd
from glob import glob

BASE = "."
RAW_DIR  = os.path.join(BASE, "data", "raw")
GOLD_DIR = os.path.join(BASE, "data", "gold")
ZIP_PATH = os.path.join(RAW_DIR, "student_performance.zip")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

UCI_URLS = {
    "Math": "https://archive.ics.uci.edu/ml/machine-learning-databases/00320/student-mat.csv",
    "Portuguese": "https://archive.ics.uci.edu/ml/machine-learning-databases/00320/student-por.csv",
}

def list_found_csvs(root):
    csvs = [p for p in glob(os.path.join(root, "**", "*.csv"), recursive=True)]
    print(" CSVs encontrados (recursivo):")
    if not csvs:
        print("  (ninguno)")
    else:
        for p in csvs:
            try:
                size = os.path.getsize(p)
            except Exception:
                size = -1
            print(f"  - {p}  ({size} bytes)")
    return csvs

def pick_candidates(csv_paths):
    patterns_math = ("mat", "math", "matematic")
    patterns_por  = ("por", "portug")

    math = None
    por  = None
    for p in csv_paths:
        low = os.path.basename(p).lower()
        if any(k in low for k in patterns_math) and math is None:
            math = p
        if any(k in low for k in patterns_por) and por is None:
            por = p

    if math and por:
        return math, por

    if len(csv_paths) >= 2:
        csv_paths_sorted = sorted(csv_paths, key=lambda x: os.path.getsize(x), reverse=True)
        cand1, cand2 = csv_paths_sorted[:2]
        print(" No se detectó por nombre. Fallback (2 CSV más grandes):")
        print("   ", cand1)
        print("   ", cand2)
        def guess_subject(path):
            low = os.path.basename(path).lower()
            if any(k in low for k in patterns_math): return "Math"
            if any(k in low for k in patterns_por):  return "Portuguese"
            return None
        s1, s2 = guess_subject(cand1), guess_subject(cand2)
        return (cand1, cand2, s1, s2)

    raise FileNotFoundError("No se encontraron CSV en data\\raw.")

def read_uci_csv(path_or_url):
    try:
        return pd.read_csv(path_or_url, sep=";")
    except UnicodeDecodeError:
        return pd.read_csv(path_or_url, sep=";", encoding="latin-1")

def build_gold():
    csvs = []
    if os.path.exists(ZIP_PATH):
        print(f" Asegurando extracción desde ZIP en: {RAW_DIR}")
        try:
            with zipfile.ZipFile(ZIP_PATH, "r") as zf:
                zf.extractall(RAW_DIR)
        except zipfile.BadZipFile:
            raise RuntimeError("El ZIP está corrupto. Bórralo y vuelve a descargarlo.")
        csvs = list_found_csvs(RAW_DIR)

    # Si no hay ZIP o no hay CSV tras extraer, traemos directo desde UCI
    if not csvs:
        print(" No hay ZIP o no se hallaron CSV. Leyendo directo desde UCI…")
        frames = []
        for subj, url in UCI_URLS.items():
            df = read_uci_csv(url)
            df["subject"] = subj
            # guardamos copia RAW para trazabilidad
            raw_out = os.path.join(RAW_DIR, f"student_{subj.lower()}.csv")
            df.to_csv(raw_out, index=False, encoding="utf-8")
            frames.append(df)
    else:
        picked = pick_candidates(csvs)
        subj_map = {}
        if len(picked) == 2:
            math_csv, por_csv = picked
            subj_map = {math_csv: "Math", por_csv: "Portuguese"}
        else:
            cand1, cand2, s1, s2 = picked
            subj_map[cand1] = s1 if s1 else "Math"
            subj_map[cand2] = s2 if s2 else "Portuguese"
            if not s1 or not s2:
                print(" Asignación por defecto de subjects (revisa si es necesario cambiar):")
                for k, v in subj_map.items():
                    print(f"   {os.path.basename(k)} -> {v}")

        frames = []
        for path, subj in subj_map.items():
            df = read_uci_csv(path)
            df["subject"] = subj
            frames.append(df)

    all_df = pd.concat(frames, ignore_index=True)

    # Tipos numéricos clave
    for c in ["G1","G2","G3","absences","age"]:
        if c in all_df.columns:
            all_df[c] = pd.to_numeric(all_df[c], errors="coerce")

    out_path = os.path.join(GOLD_DIR, "student_all.parquet")
    all_df.to_parquet(out_path, index=False)
    print(f" Gold listo: {out_path} ({len(all_df)} filas, {len(all_df.columns)} columnas)")

if __name__ == "__main__":
    build_gold()
