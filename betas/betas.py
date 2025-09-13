import argparse
import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path

CAPACIDADES_JUEGOS = {
    "Balloon Wheel": 24,
    "Samba Balloon": 32,
    "Loco Bus": 24,
    "Dragon": 20,
    "Comanche": 24,
    "Ratón Loroco": 4,
    "Remolino": 6,
    "Rascacielos": 18,
    "Tronco Splash": 4,
    "Bumperazo": 60,
    "Brincanguro": 24,
    "Moto Bala": 12,
    "Convoy": 20,
    "Bumpercitos": 28,
    "Bici Magica": 12,
    "Polo Norte": 12,
    "Sol de Mi Barrio": 108,
    "Tifón": 16,
    "Carrusel": 52,
    "Trencito": 74,
    "El Revoloteo": 24,
    "Guerra Pirata": 24,
    "Faro Saltarín": 16,
    "El Relámpago": 9,
    "Casichoco": 40
}

def detect_hour_pairs(columns):
    """Devuelve lista de (hora, col_asistencia, col_ciclos) presentes en el DataFrame."""
    hour_re = re.compile(r"^(\d{1,2}:\d{2})\s+")
    asist = [c for c in columns if re.search(r"^\d{1,2}:\d{2}\s+asistencia$", c)]
    cicl  = [c for c in columns if re.search(r"^\d{1,2}:\d{2}\s+ciclos$", c)]
    hours = sorted({hour_re.match(c).group(1) for c in asist + cicl if hour_re.match(c)})
    return [(h, f"{h} asistencia", f"{h} ciclos") for h in hours
            if f"{h} asistencia" in columns and f"{h} ciclos" in columns]

def build_long(df, caps):
    """Expande a formato largo con lambda, ciclos, mu_nom y beta_req."""
    rows = []
    pairs = detect_hour_pairs(df.columns)
    for _, row in df.iterrows():
        juego = row.get("juego", None)
        if juego not in caps:
            continue
        cap_nom = caps[juego]
        for h, a_col, c_col in pairs:
            lam_h = row.get(a_col, np.nan)
            ciclos = row.get(c_col, np.nan)
            if pd.isna(lam_h) or pd.isna(ciclos):
                continue
            try:
                lam_h = float(lam_h)
                ciclos = float(ciclos)
            except Exception:
                continue
            mu_nom = ciclos * cap_nom
            if mu_nom <= 0:
                beta_req = np.nan if lam_h == 0 else np.inf
            else:
                beta_req = lam_h / mu_nom
            rows.append({
                "juego": juego,
                "hour": h,
                "lambda_pax_h": lam_h,
                "ciclos_h": ciclos,
                "cap_nom": cap_nom,
                "mu_nom_pax_h": mu_nom,
                "beta_req": beta_req
            })
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(description="Calcula percentiles de beta_req por juego (p50, p75, p80, p90, p95).")
    ap.add_argument("--in",  dest="inp",  default="all_data.csv", help="CSV de entrada")
    ap.add_argument("--out", dest="outp", default="betas_por_juego.csv", help="CSV de salida")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.outp)
    if not inp.exists():
        print(f"[ERROR] No encuentro {inp.resolve()}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(inp)
    long = build_long(df, CAPACIDADES_JUEGOS)

    valid = long.replace([np.inf, -np.inf], np.nan).dropna(subset=["beta_req"]).copy()
    if valid.empty:
        cols = ["juego","p50","p75","p80","p90","p95"]
        pd.DataFrame(columns=cols).to_csv(outp, index=False)
        print(f"[WARN] No hay observaciones válidas. Archivo vacío escrito en {outp}")
        return

    stats = (valid.groupby("juego")["beta_req"]
                   .quantile([0.50, 0.75, 0.80, 0.90, 0.95])
                   .unstack()
                   .rename(columns={0.50:"p50", 0.75:"p75", 0.80:"p80", 0.90:"p90", 0.95:"p95"})
                   .reset_index())

    cols = ["juego","p50","p75","p80","p90","p95"]
    stats[cols].sort_values("juego").to_csv(outp, index=False)
    print(f"Betas guardados en {outp}")

if __name__ == "__main__":
    main()