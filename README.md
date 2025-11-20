# PetapaOnTrack DS

Repositorio para el análisis de datos del proyecto: pipelines de limpieza, notebooks de modelado y scripts que empujan resultados en vivo hacia PocketBase.

---

## Arquitectura del repo

```
petapaontrack_ds/
├── data/
│   ├── by_game/            # Históricos separados por juego listos para inferencia
│   └── <año>/{sucio,limpio}# Descargas crudas y su versión limpia por año
├── data_analysis/
│   ├── eda/                # Notebooks (uno por juego) con feature engineering y métricas
│   └── models/             # Bundles .joblib {'pipeline','cat_cols','num_cols'}
├── scripts/
│   ├── <juego>.py          # Script de predicción + push a PocketBase
│   ├── prep_data.py        # Une históricos limpios → all_data.csv
│   └── load_info.py        # Seed de juegos en PocketBase usando games.json
├── games.json              # Catálogo maestro de juegos
├── reader.py               # Limpia hojas XLS y agrega clima/festivos
├── requirements.txt / environment.yml
└── all_data.csv            # Dataset consolidado para exploración y entrenamiento
```

---

## Flujo de datos y  modelos
1. **Ingesta y limpieza**  
   - Coloca los XLS diarios en `data/<año>/sucio/`.  
   - Ejecuta `python reader.py` para generar CSV limpios (enriquecidos con clima, feriados y promedios).
2. **Consolidación histórica**  
   - Corre `python scripts/prep_data.py` y obtén `all_data.csv` con columnas de fecha/temporada.
3. **EDA y entrenamiento**  
   - Usa los notebooks en `data_analysis/eda/*.ipynb` como plantillas de exploración, selección de features y entrenamiento.  
   - Exporta el pipeline final a `data_analysis/models/<juego>.joblib` (incluye columnas categóricas/numéricas).
4. **Preparación para inferencia**  
   - Genera `data/by_game/<juego>.csv` con el histórico pivotado.
5. **Predicción en vivo**  
   - `python scripts/<juego>.py` arma la fila del momento (hora actual, clima del día, feriados), predice la espera y llama a `set_time_by_name` (PocketBase) para actualizar el dashboard.

---

##  Correr localmente
1. **Instala dependencias**
   ```bash
   conda env create -f environment.yml   # o python -m venv .venv && pip install -r requirements.txt
   ```
2. **Variables de entorno (`.env`)**
   ```bash
   CLIMATE_API_KEY=key_de_world_weather_online
   PB_URL=https://petapaontrack.kojimena.com
   PB_ADMIN_EMAIL=...
   PB_ADMIN_PASSWORD=...
   ```
   > Los scripts usan `python-dotenv`; basta con dejar el `.env` en la raíz.

3. **Probar un juego**
   ```bash
   source .venv/bin/activate
   python scripts/trencito.py
   ```
   Deberías ver un JSON con el input y la predicción y, si `pb_helpers.set_time_by_name` está configurado, el update en PocketBase.

---

## Servidor de predicciones (Ubuntu @ AWS)
```
ssh -i "/Users/jime/10mo semestre/graduacion/petapaontrackv2.pem" ubuntu@3.20.88.111
cd ~/predicciones
source venv/bin/activate
```

Estructura principal:
```
/home/ubuntu/predicciones
├── data/
│   └── by_game/
├── data_analysis/
│   └── models/
├── scripts/
│   ├── pb_helpers.py       # funciones REST → PocketBase
│   └── <juegos>.py
└── venv/                   # Entorno de producción (Python 3.10+)
```

### Acciones en el server
- **Actualizar artefactos** (modelos y CSV) via `scp`:
  ```bash
  scp -i "/Users/jime/10mo semestre/graduacion/petapaontrackv2.pem" data_analysis/models/*.joblib ubuntu@3.20.88.111:~/predicciones/data_analysis/models/
  scp -i "/Users/jime/10mo semestre/graduacion/petapaontrackv2.pem" data/by_game/*.csv ubuntu@3.20.88.111:~/predicciones/data/by_game/
  scp -i "/Users/jime/10mo semestre/graduacion/petapaontrackv2.pem" scripts/ballon_wheel.py ubuntu@3.20.88.111:~/predicciones/scripts/ballon_wheel.py
  ```
- **Ejecutar predicciones**: `python scripts/<juego>.py`.
- **Logs rápidos**: cada script imprime un dict con timestamp + input + predicción, así puedes monitorear desde `tail -f`.

---
