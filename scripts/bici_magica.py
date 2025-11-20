import os, joblib, pandas as pd, numpy as np, requests
from datetime import datetime, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pb_helpers import set_time_by_name


# =========================
# 0) CONFIG 
# =========================
TZ = ZoneInfo("America/Guatemala")

# Feriados Guatemala (MM-DD -> nombre)
FERIADOS_GT = {
    "01-01": "Año Nuevo",
    "01-15": "Día del Cristo Negro",
    "05-01": "Día del Trabajo",
    "06-30": "Día del Ejército",
    "09-15": "Día de la Independencia",
    "10-20": "Revolución de 1944",
    "11-01": "Día de Todos los Santos",
    "12-25": "Navidad",
}

load_dotenv()
WWO_KEY = os.getenv("CLIMATE_API_KEY")  # World Weather Online

# =========================
# 1) CARGA MODELO
# =========================
bundle = joblib.load(os.path.expanduser("data_analysis/models/bici_magica.joblib"))
pipe = bundle["pipeline"]
cat_cols = bundle["cat_cols"]
num_cols = bundle["num_cols"]

# =========================
# 2) LOOKUP HISTÓRICO
# =========================
HIST_CSV = os.path.expanduser("data/by_game/bici_magica.csv")

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
    max_year = int(hist_largo["year"].max())
    years = list(range(max_year - lookback_years, max_year + 1))
    sub = hist_largo[hist_largo["year"].isin(years)].copy()

    grp = sub.groupby(["dow","month","hora"])[["asistencia_h","ciclos_h"]].mean()
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

# =========================
# 2.1) CLIMA (WWO)
# =========================
def obtener_clima_wwo(api_key, fecha_dt: date, lugar="Petapa,Guatemala"):
    """World Weather Online - past-weather (tp=24) para la fecha dada."""
    if not api_key:
        return {"temperatura_max": None, "temperatura_min": None, "condiciones_cielo": None, "prob_precipitacion": None}

    url = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"
    params = {
        "key": api_key,
        "q": lugar,
        "format": "json",
        "date": fecha_dt.strftime("%Y-%m-%d"),
        "tp": 24
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        clima_dia = data["data"]["weather"][0]
        detalle = clima_dia["hourly"][0]
        return {
            "temperatura_max": float(clima_dia.get("maxtempC")) if clima_dia.get("maxtempC") is not None else None,
            "temperatura_min": float(clima_dia.get("mintempC")) if clima_dia.get("mintempC") is not None else None,
            "condiciones_cielo": detalle["weatherDesc"][0]["value"] if detalle.get("weatherDesc") else None,
            "prob_precipitacion": int(detalle.get("chanceofrain", 0)) if detalle.get("chanceofrain") is not None else None
        }
    except Exception as e:
        print(f"❌ Error al obtener clima: {e}")
        return {"temperatura_max": None, "temperatura_min": None, "condiciones_cielo": None, "prob_precipitacion": None}

# =========================
# 2.2) RELLENAR 
# =========================
def rellenar_expecteds(df_nuevo: pd.DataFrame) -> pd.DataFrame:
    if LOOKUP is None:
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

    vals_a, vals_c = [], []
    for _, r in out.iterrows():
        key = (int(r["_dow_auto"]), int(r["month"]), str(r["hora"]))
        val = None
        if LOOKUP["dow_month_hora"].index.isin([key]).any():
            val = LOOKUP["dow_month_hora"].loc[key]
        if val is None or (isinstance(val, pd.Series) and val.isna().all()):
            key2 = (int(r["_dow_auto"]), str(r["hora"]))
            if LOOKUP["dow_hora"].index.isin([key2]).any():
                val = LOOKUP["dow_hora"].loc[key2]
        if val is None or (isinstance(val, pd.Series) and val.isna().all()):
            if str(r["hora"]) in LOOKUP["hora"].index:
                val = LOOKUP["hora"].loc[str(r["hora"])]
        if val is None or (isinstance(val, pd.Series) and val.isna().all()):
            val = LOOKUP["global"]

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
# 4) ENTRADA DINÁMICA
# =========================
def construir_fila_actual():
    now = datetime.now(TZ)
    fecha_hoy = now.date()  # date
    mes_dia = now.strftime("%m-%d")
    festivo = FERIADOS_GT.get(mes_dia)

    # Clima del día
    clima = obtener_clima_wwo(WWO_KEY, fecha_hoy, lugar="Petapa,Guatemala")

    # day_of_week 
    day_of_week = now.strftime("%A")
    hora_hh = now.strftime("%H:00")

    fila = {
        "day_of_week": [day_of_week],
        "hora": [hora_hh],
        "es_festivo": [festivo is not None],
        "condiciones_cielo": [clima.get("condiciones_cielo")],
        "nombre_festivo": [festivo if festivo else ""],
        "month": [now.month],
        "day": [now.day],
        "temperatura_max": [clima.get("temperatura_max")],
        "temporada_alta": [0],
        "prob_precipitacion": [clima.get("prob_precipitacion")]
    }
    return pd.DataFrame(fila)

if __name__ == "__main__":
    GAME_NAME = "Bici Mágica"

    df_nuevo = construir_fila_actual()
    predicciones = predecir(df_nuevo)
    pred = float(predicciones.iloc[0])

    info = {
        "timestamp": datetime.now(TZ).isoformat(),
        "input_row": df_nuevo.to_dict(orient="records")[0],
        "prediccion": pred,
    }
    print(info)

    # Actualizar PocketBase
    try:
        resp = set_time_by_name(GAME_NAME, pred)
        print("Actualizado en PocketBase. Nuevo time:", resp.get("time"))
    except Exception as e:
        print("Error actualizando PocketBase:", e)
