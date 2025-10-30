# ANTES (solo Colab):
# BASE = '/content/drive/MyDrive/edu-data-platform'

# DESPUÉS (funciona en Colab y en GitHub):
import os
BASE = os.environ.get("EDP_BASE", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
PATH_RAW    = f"{BASE}/data/raw"
PATH_SILVER = f"{BASE}/data/silver"
PATH_GOLD   = f"{BASE}/data/gold"
PATH_REP    = f"{BASE}/reportes"
