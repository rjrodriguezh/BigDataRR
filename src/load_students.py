# src/load_students.py
import os, io, re, zipfile, urllib.request
import pandas as pd

# 1) Legacy (ya 404 en muchos casos)
LEGACY_MAT = "https://archive.ics.uci.edu/ml/machine-learning-databases/00320/student-mat.csv"
LEGACY_POR = "https://archive.ics.uci.edu/ml/machine-learning-databases/00320/student-por.csv"

# 2) ZIP oficial actual en UCI (página del dataset lo expone)
UCI_ZIP = "https://archive.ics.uci.edu/static/public/320/student%2Bperformance.zip"

# 3) Fallback espejo (mismo contenido, hospedado en GitHub raw)
MIRROR_MAT = "https://raw.githubusercontent.com/arunk13/MSDA-Assignments/master/IS607Fall2015/Assignment3/student-mat.csv"
MIRROR_POR = "https://raw.githubusercontent.com/arunk13/MSDA-Assignments/master/IS607Fall2015/Assignment3/student-por.csv"

os.makedirs("data", exist_ok=True)

def try_read_csv(url, sep=';'):
    return pd.read_csv(url, sep=sep)

def read_legacy():
    df_mat = try_read_csv(LEGACY_MAT)
    df_por = try_read_csv(LEGACY_POR)
    return df_mat, df_por

def read_from_zip():
    with urllib.request.urlopen(UCI_ZIP) as resp:
        zbytes = resp.read()
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        names = zf.namelist()

        # helper: busca un CSV que contenga palabras clave
        def find_member(keywords):
            cands = [n for n in names if n.lower().endswith(".csv")]
            # prioriza rutas cortas y que incluyan todas las keywords
            cands = [n for n in cands if all(k in n.lower() for k in keywords)]
            if not cands:
                return None
            return sorted(cands, key=len)[0]

        # intenta patrones razonables
        mat_name = find_member(["student", "mat"]) or find_member(["mat"])
        por_name = find_member(["student", "por"]) or find_member(["por"])

        if not mat_name or not por_name:
            # imprime nombres para depuración si algo cambia
            print("⚠️ No se hallaron nombres esperados en el ZIP. Contenido:")
            for n in names:
                print(" -", n)
            raise FileNotFoundError("No se encontraron los CSV 'mat'/'por' dentro del ZIP.")

        with zf.open(mat_name) as fmat:
            df_mat = pd.read_csv(fmat, sep=';')
        with zf.open(por_name) as fpor:
            df_por = pd.read_csv(fpor, sep=';')
    return df_mat, df_por

def read_from_mirror():
    df_mat = try_read_csv(MIRROR_MAT)
    df_por = try_read_csv(MIRROR_POR)
    return df_mat, df_por

# Orquestación robusta
last_err = None
for loader in (read_legacy, read_from_zip, read_from_mirror):
    try:
        df_mat, df_por = loader()
        print(f"✔️ Loader OK: {loader.__name__}")
        break
    except Exception as e:
        print(f"❌ Loader falló: {loader.__name__} -> {e}")
        last_err = e
else:
    raise SystemExit(f"No se pudo obtener el dataset desde ninguna fuente. Último error: {last_err}")

# Etiqueta asignatura, concatena y guarda
df_mat["subject"] = "Math"
df_por["subject"] = "Portuguese"
df = pd.concat([df_mat, df_por], ignore_index=True)

out_path = "data/student_all.csv"
df.to_csv(out_path, index=False)

print(f"✅ Dataset unificado con {len(df)} filas. Guardado en {out_path}")
print(df.head(3).to_string(index=False))