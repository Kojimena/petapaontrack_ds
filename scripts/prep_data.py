import pandas as pd
import numpy as np
import glob
import os

def cargar_con_origen(file):
    df = pd.read_csv(file)
    df['source_file'] = os.path.basename(file)  
    return df

file_2022 = sorted(glob.glob("./data/2022/limpio/*.csv"))
file_2023 = sorted(glob.glob("./data/2023/limpio/*.csv"))
file_2024 = sorted(glob.glob("./data/2024/limpio/*.csv"))

all_data = pd.concat([cargar_con_origen(file) for file in file_2022 + file_2023 + file_2024], ignore_index=True)

# %%
# put date in first column
all_data.insert(0, 'date', all_data['fecha'])
# remove 'fecha' column
all_data.drop(columns=['fecha'], inplace=True)

# %%
all_data['date'] = pd.to_datetime(all_data['date'], format='%Y-%m-%d')

all_data['day'] = all_data['date'].dt.day
all_data['month'] = all_data['date'].dt.month
all_data['year'] = all_data['date'].dt.year
all_data['day_of_week'] = all_data['date'].dt.day_name()

# %%
def es_temporada_alta(fecha):
    """
    Determina si una fecha está en temporada alta
    1 = Temporada alta, 0 = Temporada normal
    """
    mes = fecha.month
    dia = fecha.day
    
    # Temporada Navideña (Octubre, Noviembre, Diciembre)
    if mes in [11, 12]:
        return 1
    
    # Semana Santa ( marzo-abril)
    if mes == 3 and dia >= 15:  # Segunda quincena de marzo
        return 1
    if mes == 4 and dia <= 15:  # Primera quincena de abril
        return 1
    
    # Vacaciones de medio año
    if mes == 6 and dia >= 15:  # Segunda quincena de junio
        return 1
    
    return 0

all_data['temporada_alta'] = all_data['date'].apply(es_temporada_alta)

print("Distribución de temporada alta:")
print(all_data['temporada_alta'].value_counts())
print(f"\nPorcentaje temporada alta: {(all_data['temporada_alta'].sum() / len(all_data) * 100):.1f}%")

# %%
all_data

# %%
#export to csv
all_data.to_csv('all_data.csv', index=False)