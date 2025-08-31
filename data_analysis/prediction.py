import joblib
import pandas as pd
import numpy as np

# =========================
# 1) CARGA MODELO
# =========================
bundle = joblib.load("data_analysis/models/rascacielos.joblib")
pipe = bundle["pipeline"]
cat_cols = bundle["cat_cols"]
num_cols = bundle["num_cols"]

# =========================
# 2) LOOKUP: promedio por (dow, month, hora) de los últimos N años
# =========================
HIST_CSV = "data/by_game/rascacielos.csv"

def preparar_historico_largo(hist_df: pd.DataFrame) -> pd.DataFrame:
    df = hist_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dow"]   = df["date"].dt.dayofweek

    horas = sorted({c.split()[0] for c in df.columns if c.endswith("asistencia") or c.endswith("ciclos")})
    registros = []
    for h in horas:
        col_a = f"{h} asistencia"
        col_c = f"{h} ciclos"
        if (col_a not in df.columns) and (col_c not in df.columns):
            continue
        tmp = df[["date","year","month","dow"]].copy()
        tmp["hora"] = h
        tmp["asistencia_h"] = df[col_a] if col_a in df.columns else np.nan
        tmp["ciclos_h"]     = df[col_c] if col_c in df.columns else np.nan
        registros.append(tmp)
    if not registros:
        raise ValueError("No se encontraron columnas 'HH:00 asistencia/ciclos' en el histórico.")
    return pd.concat(registros, ignore_index=True)

def construir_lookup(hist_largo: pd.DataFrame, lookback_years: int = 3):
    # toma los últimos N años disponibles en el histórico
    max_year = int(hist_largo["year"].max())
    years = list(range(max_year - lookback_years, max_year + 1))  # incluye max_year
    sub = hist_largo[hist_largo["year"].isin(years)].copy()

    # promedio por (dow, month, hora)
    grp = sub.groupby(["dow","month","hora"])[["asistencia_h","ciclos_h"]].mean()

    # fallback
    grp_dow_h = sub.groupby(["dow","hora"])[["asistencia_h","ciclos_h"]].mean()
    grp_h     = sub.groupby(["hora"])[["asistencia_h","ciclos_h"]].mean()
    glob      = sub[["asistencia_h","ciclos_h"]].mean()

    return {"dow_month_hora": grp, "dow_hora": grp_dow_h, "hora": grp_h, "global": glob}

# Cargar histórico y construir lookup una sola vez
try:
    hist_raw = pd.read_csv(HIST_CSV)
    HIST_LARGO = preparar_historico_largo(hist_raw)
    LOOKUP = construir_lookup(HIST_LARGO, lookback_years=3)
except Exception as e:
    print(f"⚠️ No se pudo preparar el lookup histórico ({e}). Se usarán NaNs para asistencia/ciclos.")
    HIST_LARGO = None
    LOOKUP = None

def rellenar_expecteds(df_nuevo: pd.DataFrame) -> pd.DataFrame:
    """Rellena asistencia_h y ciclos_h con promedio de últimos 3 años por (dow, month, hora)."""
    if LOOKUP is None:
        # sin lookup, no hacemos nada
        return df_nuevo

    out = df_nuevo.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out["_month_auto"] = out["date"].dt.month
        out["_dow_auto"]   = out["date"].dt.dayofweek
        out["month"] = out["month"].fillna(out["_month_auto"]) if "month" in out.columns else out["_month_auto"]
        if "day_of_week" not in out.columns:
            out["day_of_week"] = out["date"].dt.day_name()

    dow_map = {"Monday":0,"Tuesday":1,"Wednesday":2,"Thursday":3,"Friday":4,"Saturday":5,"Sunday":6}
    if "day_of_week" in out.columns and "date" not in out.columns:
        out["_dow_auto"] = out["day_of_week"].map(dow_map)

    out["month"] = out["month"].astype(int)
    out["_dow_auto"] = out["_dow_auto"].astype(int)

    if "asistencia_h" not in out.columns: out["asistencia_h"] = np.nan
    if "ciclos_h"     not in out.columns: out["ciclos_h"]     = np.nan

    # lookup
    vals_a, vals_c = [], []
    for _, r in out.iterrows():
        key = (int(r["_dow_auto"]), int(r["month"]), str(r["hora"]))
        val = None
        # 1) (dow, month, hora)
        if LOOKUP["dow_month_hora"].index.isin([key]).any():
            val = LOOKUP["dow_month_hora"].loc[key]
        # 2) (dow, hora)
        if val is None or (isinstance(val, pd.Series) and val.isna().all()):
            key2 = (int(r["_dow_auto"]), str(r["hora"]))
            if LOOKUP["dow_hora"].index.isin([key2]).any():
                val = LOOKUP["dow_hora"].loc[key2]
        # 3) (hora)
        if val is None or (isinstance(val, pd.Series) and val.isna().all()):
            if str(r["hora"]) in LOOKUP["hora"].index:
                val = LOOKUP["hora"].loc[str(r["hora"])]
        # 4) global
        if val is None or (isinstance(val, pd.Series) and val.isna().all()):
            val = LOOKUP["global"]

        print(f"Promedio usado para dow={r['_dow_auto']}, mes={r['month']}, hora={r['hora']}:")
        print(val.round(0))
        vals_a.append(float(val.get("asistencia_h", np.nan).round(0)))
        vals_c.append(float(val.get("ciclos_h", np.nan).round(0)))

    out["asistencia_h"] = out["asistencia_h"].fillna(pd.Series(vals_a, index=out.index))
    out["ciclos_h"]     = out["ciclos_h"].fillna(pd.Series(vals_c, index=out.index))

    return out.drop(columns=["_month_auto","_dow_auto"], errors="ignore")

# =========================
# 3) PREDICCIÓN
# =========================
def predecir(df_nuevo: pd.DataFrame) -> pd.Series:
    df_in = rellenar_expecteds(df_nuevo)

    # si faltara alguna columna esperada, la creamos para no romper el pipeline
    for c in cat_cols:
        if c not in df_in.columns:
            df_in[c] = pd.NA
    for c in num_cols:
        if c not in df_in.columns:
            df_in[c] = np.nan

    X_nuevo = df_in[cat_cols + num_cols]
    pred = pipe.predict(X_nuevo)
    return pd.Series(np.round(pred, 2), index=df_nuevo.index, name="prediccion")

# =========================
# 4) EJEMPLO DE USO
# =========================
df_nuevo = pd.DataFrame({
    "day_of_week": ["Sunday"],
    "hora": ["13:00"],
    "es_festivo": [False],
    "condiciones_cielo": [""],
    "nombre_festivo": [""],
    "month": [8],
    "day": [31],
    "temperatura_max": [25.0],
    "temporada_alta": [0],
})

predicciones = predecir(df_nuevo)
print(predicciones)