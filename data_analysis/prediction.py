import joblib
import pandas as pd

bundle = joblib.load("data_analysis/models/rascacielos_rf_pipeline.joblib")
pipe = bundle["pipeline"]
cat_cols = bundle["cat_cols"]
num_cols = bundle["num_cols"]

# df_nuevo debe tener estas columnas (las que existan de cat_cols + num_cols)
def predecir(df_nuevo: pd.DataFrame) -> pd.Series:
    # Asegurarse de que df_nuevo tenga las columnas necesarias
    for col in cat_cols + num_cols:
        if col not in df_nuevo.columns:
            raise ValueError(f"Falta la columna requerida: {col}")

    # Seleccionar solo las columnas necesarias
    X_nuevo = df_nuevo[cat_cols + num_cols]

    # Realizar la predicción
    predicciones = pipe.predict(X_nuevo)

    return pd.Series(predicciones, index=df_nuevo.index)

df_nuevo = pd.DataFrame({
    "day_of_week": ["Sunday"],
    "hora": ["10:00"],
    "es_festivo": [False],
    "condiciones_cielo": ["Patchy rain possible"],
    "nombre_festivo": ["Ninguno"],
    "month": [8],
    "day": [31],
    "temperatura_max": [25.0],
    "temporada_alta": [0],
    "asistencia_h": [180],
    "ciclos_h": [7]
})

predicciones = predecir(df_nuevo)
print(predicciones)
