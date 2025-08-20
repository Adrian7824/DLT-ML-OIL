# 01_generate_synthetic_data.py — Generador de datos sintéticos (Trial / Community)

from datetime import date, timedelta
import random
import pandas as pd
from pathlib import Path

# ---- Parámetros principales ----
MODE = "trial"   # "trial" o "community"
NUM_WELLS = 10
MONTHS_BACK = 18
SEED = 42

# Rutas base según modo
if MODE == "trial":
    BASE = Path("/Volumes/oil/dev/raw")
    # En Trial escribimos en CARPETAS, como espera Auto Loader (cloudFiles)
    PATH_WELL   = BASE / "sample_well_daily_production"
    PATH_COST   = BASE / "sample_opex_capex"
    PATH_PRICE  = BASE / "sample_price_brent"
else:
    BASE = Path("/databricks/driver/data")
    # En Community escribimos CSV planos en una carpeta
    PATH_WELL   = BASE
    PATH_COST   = BASE
    PATH_PRICE  = BASE

random.seed(SEED)

# ---- Fechas ----
today = date.today().replace(day=1)  # inicio de mes actual
start_month = (today - timedelta(days=MONTHS_BACK * 30)).replace(day=1)
end_day = date.today()

def daterange(start, end, step_days=1):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=step_days)

def month_starts(start, end):
    m = start
    acc = []
    while m <= end:
        acc.append(m)
        # siguiente mes
        if m.month == 12:
            m = date(m.year + 1, 1, 1)
        else:
            m = date(m.year, m.month + 1, 1)
    return acc

months = month_starts(start_month, end_day)

# ---- 1) Producción diaria por pozo ----
rows_prod = []
for w in range(1, NUM_WELLS + 1):
    base_oil = random.uniform(90, 220)
    decline = random.uniform(0.0006, 0.0016)
    water_cut = random.uniform(0.12, 0.38)
    gas_oil_ratio = random.uniform(900, 1600)
    start_day = start_month
    for i, d in enumerate(daterange(start_day, end_day)):
        oil = max(0, base_oil * (1 - decline) ** i + random.gauss(0, 3))
        water = max(0, oil * water_cut + random.gauss(0, 1))
        gas = max(0, oil * gas_oil_ratio + random.gauss(0, 50))
        rows_prod.append({
            "pozo_id": f"W{w:02d}",
            "fecha": d.isoformat(),
            "aceite_bbl": round(oil, 2),
            "gas_m3": round(gas, 2),
            "agua_bbl": round(water, 2),
        })
df_prod = pd.DataFrame(rows_prod)

# ---- 2) Opex/Capex mensual expandido a diario ----
rows_cost_m = []
for w in range(1, NUM_WELLS + 1):
    for m in months:
        opex = random.uniform(2200, 6500)
        capex = random.choice([0, 0, 0, random.uniform(25000, 90000)])
        rows_cost_m.append({
            "pozo_id": f"W{w:02d}",
            "fecha": m.isoformat(),
            "opex_usd": round(opex, 2),
            "capex_usd": round(capex, 2)
        })
df_cost_m = pd.DataFrame(rows_cost_m)

# expandir a diario (forward fill por mes)
rows_cost_d = []
for _, r in df_cost_m.iterrows():
    m = date.fromisoformat(r["fecha"])
    # último día del mes
    if m.month == 12:
        next_m = date(m.year + 1, 1, 1)
    else:
        next_m = date(m.year, m.month + 1, 1)
    last_day = min(next_m - timedelta(days=1), end_day)
    d = m
    while d <= last_day:
        rows_cost_d.append({
            "pozo_id": r["pozo_id"],
            "fecha": d.isoformat(),
            "opex_usd": r["opex_usd"],
            "capex_usd": r["capex_usd"]
        })
        d += timedelta(days=1)
df_cost = pd.DataFrame(rows_cost_d)

# ---- 3) Precio Brent mensual ----
price_rows = []
price = 78.0
for m in months:
    price += random.gauss(0, 2.8)
    price_rows.append({
        "fecha": m.isoformat(),
        "precio_usd_bbl": round(max(40, price), 2)
    })
df_price = pd.DataFrame(price_rows)

# ---- Guardado ----
if MODE == "trial":
    # aseguramos carpetas
    for p in [PATH_WELL, PATH_COST, PATH_PRICE]:
        p.mkdir(parents=True, exist_ok=True)
    # un archivo CSV por dataset dentro de su carpeta
    df_prod.to_csv(PATH_WELL / "sample_well_daily_production.csv", index=False)
    df_cost.to_csv(PATH_COST / "sample_opex_capex.csv", index=False)
    df_price.to_csv(PATH_PRICE / "sample_price_brent.csv", index=False)
    print(f"Datos escritos en {BASE}")
else:
    BASE.mkdir(parents=True, exist_ok=True)
    df_prod.to_csv(PATH_WELL / "sample_well_daily_production.csv", index=False)
    df_cost.to_csv(PATH_COST / "sample_opex_capex.csv", index=False)
    df_price.to_csv(PATH_PRICE / "sample_price_brent.csv", index=False)
    print(f"Datos escritos en {BASE}")

print("✔ Generación completa:")
print(f"- Producción: {len(df_prod):,} filas")
print(f"- Opex/Capex (diario): {len(df_cost):,} filas")
print(f"- Precio Brent (mensual): {len(df_price):,} filas")
