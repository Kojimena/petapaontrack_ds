import pandas as pd
import os
import re
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Juegos de la lista a excluir de los reportes
JUEGOS_EXCLUIR = [
    "Carros Gnomos", "Aviones", "Carroussel Antiguo", "Tren Antiguo",
    "Delta", "Los Voladores", "Frisbi"
]

# Días festivos en Guatemala
FERIADOS_GT = {
    "01-01": "Año Nuevo",
    "01-15": "Día del Cristo Negro",
    "05-01": "Día del Trabajo",
    "06-30": "Día del Ejército",
    "09-15": "Día de la Independencia",
    "10-20": "Revolución de 1944",
    "11-01": "Día de Todos los Santos",
    "12-25": "Navidad"
}

# Carpeta de entrada y salida
input_folder = "./data/2024/sucio"
output_folder = "./data/2024/limpio"
Path(output_folder).mkdir(parents=True, exist_ok=True)

# =======================
# FUNCIONES
# =======================
def extraer_fecha(df):
    """Extrae la fecha del reporte de las hojas de Excel.

    Args:
        df (dict): DataFrame de las hojas de Excel.

    Returns:
        datetime: Fecha extraída o None si no se encuentra.
    """
    pattern = re.compile(r"\d{2}/\d{2}/\d{4}")
    for sheet in df.values():
        for fila in sheet.itertuples(index=False):
            for celda in fila:
                if isinstance(celda, str):
                    match = pattern.search(celda)
                    if match:
                        try:
                            return datetime.strptime(match.group(), "%d/%m/%Y")
                        except ValueError:
                            continue
    return None


def obtener_clima_wwo(api_key, fecha, lugar="Petapa,Guatemala"):
    """Obtiene el clima de World Weather Online para una fecha y lugar específicos.
    """

    url = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"
    params = {
        "key": api_key,
        "q": lugar,
        "format": "json",
        "date": fecha.strftime("%Y-%m-%d"),
        "tp": 24
    }

    try:
        response = requests.get(url, params=params)
        data = response.json()
        clima_dia = data["data"]["weather"][0]
        detalle = clima_dia["hourly"][0]

        return {
            "temperatura_max": float(clima_dia["maxtempC"]),
            "temperatura_min": float(clima_dia["mintempC"]),
            "condiciones_cielo": detalle["weatherDesc"][0]["value"],
            "prob_precipitacion": int(detalle.get("chanceofrain", 0))
        }
    except Exception as e:
        print(f"❌ Error al obtener clima: {e}")
        return {
            "temperatura_max": None,
            "temperatura_min": None,
            "condiciones_cielo": None,
            "prob_precipitacion": None
        }

def hacer_nombres_unicos(cols):
    """Genera nombres únicos para las columnas, agregando un sufijo si es necesario.
    Args:
        cols (list): Lista de nombres de columnas.
    Returns:
        list: Lista de nombres únicos.
    """
    seen = {}
    result = []
    for col in cols:
        col_str = str(col)
        if col_str not in seen:
            seen[col_str] = 1
            result.append(col_str)
        else:
            seen[col_str] += 1
            result.append(f"{col_str}_{seen[col_str]}")
    return result

# =======================
# PROCESAMIENTO
# =======================

# Recorrer los archivos en la carpeta de entrada
for filename in os.listdir(input_folder):
    # Verificar si el archivo es un Excel
    if not filename.endswith(".xls"):
        continue

    # Leer el archivo Excel
    file_path = os.path.join(input_folder, filename)
    print(f"\n📄 Procesando archivo: {filename}")
    df_sheets = pd.read_excel(file_path, sheet_name=None, header=None)

    # Procesar cada hoja del archivo
    for name, sheet in df_sheets.items():
        try:
            # Buscar la fila con 'ciclos' para identificar el inicio de los datos
            inicio_datos_idx = sheet[sheet.astype(str).apply(
                lambda row: row.str.contains("ciclos", case=False).any(), axis=1
            )].index[0]

            # Encabezados y datos, asegurando que los nombres de columnas sean únicos
            column_headers = sheet.iloc[inicio_datos_idx + 1]
            df = sheet.iloc[inicio_datos_idx + 2:].copy()
            df.columns = hacer_nombres_unicos(column_headers)
            df = df.rename(columns={df.columns[0]: "juego"})

            # Filtros
            # Excluir juegos no deseados y eliminar filas de arcadas
            df = df[~df["juego"].astype(str).isin(JUEGOS_EXCLUIR)]
            arcadas = df[df["juego"].astype(str).str.contains("Arcada", case=False)].index
            if len(arcadas) > 0:
                df = df.loc[:arcadas[0] - 1]
            df = df.dropna(axis=1, how="all")
            df = df[~df["juego"].astype(str).str.contains("Mecánico", case=False, na=False)]

            # Reordenar columnas
            ciclos_total_col = [col for col in df.columns if "ciclos total" in str(col).lower()]
            asistencia_total_col = [col for col in df.columns if "asistencia total" in str(col).lower()]
            cols_por_hora = [col for col in df.columns if col not in ciclos_total_col + asistencia_total_col + ["juego"]]

            n_pares = len(cols_por_hora) // 2
            horas = [f"{h}:00" for h in range(9, 9 + n_pares)]
            nombres_columnas = ["juego"]
            for hora in horas:
                nombres_columnas.extend([f"{hora} ciclos", f"{hora} asistencia"])

            nombres_columnas = nombres_columnas[:len(df.columns) - 2] + ["ciclos total", "asistencia total"]
            df = df[["juego"] + cols_por_hora + ciclos_total_col + asistencia_total_col]
            df.columns = nombres_columnas

            # Eliminar filas con juegos mecánicos o totales
            df = df[~df["juego"].astype(str).str.contains("Mec·nico|Total", case=False, na=False)]

            # Fecha
            # Extraer la fecha del reporte y agregar información de clima y festivos
            fecha_reporte = extraer_fecha(df_sheets)
            if fecha_reporte:
                mes_dia = fecha_reporte.strftime("%m-%d")
                festivo = FERIADOS_GT.get(mes_dia)
                df["fecha"] = fecha_reporte.date()
                df["es_festivo"] = festivo is not None
                df["nombre_festivo"] = festivo if festivo else "Ninguno"
                api_key = os.getenv('CLIMATE_API_KEY')
                info_clima = obtener_clima_wwo(api_key, fecha_reporte)
                for k, v in info_clima.items():
                    df[k] = v
            else:
                print("⚠️ No se pudo extraer la fecha del archivo.")

            # Estadísticas
            df["promedio_asistencia_por_hora"] = df.filter(like="asistencia").mean(axis=1)
            df["promedio_ciclos_por_hora"] = df.filter(like="ciclos").mean(axis=1)

            # Guardar con mismo nombre en carpeta de salida
            nombre_salida = os.path.splitext(filename)[0] + "_limpio.csv"
            output_path = os.path.join(output_folder, nombre_salida)
            df.to_csv(output_path, index=False)
            print(f"✅ Guardado en: {output_path}")

        except Exception as e:
            print(f"❌ Error procesando hoja '{name}' en {filename}: {e}")
