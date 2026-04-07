"""
This is a boilerplate pipeline 'data_cleaning'
generated using Kedro 1.3.0
"""
import pandas as pd
import numpy as np

# ==========================================
# 1. FUNCIONES AUXILIARES (Reutilizables)
# ==========================================

def eliminar_duplicados(df):
    n_antes = len(df)
    df_limpio = df.drop_duplicates().copy()
    print(f"Borrados {n_antes - len(df_limpio)} duplicados.")
    return df_limpio

def limpiar_textos(df, columnas):
    for col in columnas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            df[col] = df[col].replace('Nan', np.nan)
            df[col] = df[col].replace('Sos', 'SOS')
    return df

def estandarizar_fechas(df, columnas):
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='mixed', dayfirst=True, errors='coerce')
    return df

def limpiar_telefonos(df, col='telefono'):
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)
        df[col] = df[col].replace('nan', np.nan)
        df[col] = df[col].apply(lambda x: "+" + x if pd.notnull(x) and str(x).startswith("56") else x)
    return df

def imputar_nulos(df):
    num_cols = df.select_dtypes(include=[np.number]).columns
    cat_cols = df.select_dtypes(include=['object']).columns
    
    for col in num_cols:
        df[col] = df[col].fillna(df[col].median())
        
    for col in cat_cols:
        if not df[col].mode().empty:
            df[col] = df[col].fillna(df[col].mode()[0])
            
    return df

# ==========================================
# 2. NODOS DE KEDRO
# ==========================================

def limpiar_pacientes(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica la limpieza al dataset de pacientes."""
    df = eliminar_duplicados(df)
    df = limpiar_textos(df, ['nombre', 'genero', 'prevision', 'comuna'])
    df = estandarizar_fechas(df, ['fecha_nacimiento'])
    df = limpiar_telefonos(df, 'telefono')
    df = imputar_nulos(df)
    return df

def limpiar_consultas(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica la limpieza al dataset de consultas."""
    df = eliminar_duplicados(df)
    
    # Arreglo especial para consultas: quitar el símbolo '$' del costo y convertir a número
    if 'costo' in df.columns:
        df['costo'] = df['costo'].astype(str).str.replace('$', '', regex=False)
        # Volvemos a convertirlo en float para que imputar_nulos lo trate como número
        df['costo'] = pd.to_numeric(df['costo'], errors='coerce') 

    df = limpiar_textos(df, ['especialidad', 'diagnostico_principal', 'diagnostico_secundario'])
    df = estandarizar_fechas(df, ['fecha'])
    df = imputar_nulos(df)
    
    return df

