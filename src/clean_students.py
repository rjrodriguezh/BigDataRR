# src/clean_students.py
import os
import pandas as pd

RAW = "data/student_all.csv"
CLEAN = "data/student_clean.csv"

assert os.path.exists(RAW), f"No existe {RAW}. Corre primero src/load_students.py"

df = pd.read_csv(RAW)

# --- 1) Tipos y valores básicos ---
# Quita duplicados exactos (por si existen)
before = len(df)
df = df.drop_duplicates().reset_index(drop=True)
dups = before - len(df)

# Asegura enteros en notas y ausencias
for col in ["G1","G2","G3","absences","age"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

# Sanitiza strings en minúscula consistente
for col in ["school","sex","address","famsize","Pstatus","Mjob","Fjob","reason",
            "guardian","schoolsup","famsup","paid","activities","nursery","higher",
            "internet","romantic","subject"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# --- 2) Reglas de calidad simples ---
# Notas dentro de 0–20
mask_range = df[["G1","G2","G3"]].apply(lambda s: s.between(0,20)).all(axis=1)
bad_rows = (~mask_range).sum()
df = df[mask_range].copy()

# Edad razonable 15–22 (dataset original)
mask_age = df["age"].between(15,22)
bad_age = (~mask_age).sum()
df = df[mask_age].copy()

# Ausencias negativas no permitidas
df = df[df["absences"].fillna(0) >= 0].copy()

# --- 3) Features básicos ---
df["G_avg"] = df[["G1","G2","G3"]].mean(axis=1).round(2)
df["passed"] = (df["G3"] >= 10).map({True:1, False:0})  # 10 como aprobado

# Intensidad de estudio (proxy simple)
df["study_load"] = pd.Categorical(df["studytime"], ordered=True)
# Riesgo por alcohol (heurístico)
df["alc_heavy"] = ((df["Dalc"]>=3) | (df["Walc"]>=3)).astype(int)

# Bucket de edad
df["age_bin"] = pd.cut(df["age"], bins=[14,16,18,22], labels=["15-16","17-18","19-22"], include_lowest=True)

# --- 4) Guardar y pequeño resumen ---
os.makedirs("data", exist_ok=True)
df.to_csv(CLEAN, index=False)

print("✅ Limpieza completa")
print(f" - Filas originales: {before}")
print(f" - Duplicados removidos: {dups}")
print(f" - Filas fuera de rango de notas: {bad_rows}")
print(f" - Filas fuera de rango de edad:  {bad_age}")
print(f" - Filas finales: {len(df)}")
print(f"Archivo guardado: {CLEAN}")
