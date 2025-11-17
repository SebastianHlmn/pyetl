"""
step_3_build_atlas.py

Paso 3: Carga los datos procesados del Paso 2 y los enriquece
con todas las referencias (Delitos, Actuaciones, UNISA, etc.)
para crear la base analítica "Atlas" (Mixto + Acusatorio).
"""
import pandas as pd
import numpy as np
import os
import time
import sys
import psutil
import gc
import json 
import traceback 
from datetime import datetime

# --- Constantes ---
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "step_3.log")
PID_FILE = os.path.join(LOG_DIR, "step_3.pid")
PAUSE_FILE = os.path.join(LOG_DIR, "step_3.pause")
RUNNING_FLAG = os.path.join(LOG_DIR, "step_3.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_3_metrics.json")
CONFIG_FILE = 'config.json'

# --- Funciones de Control ---
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f: f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f: f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 3] Iniciando...\n")

def log_message(msg):
    print(msg); sys.stdout.flush()
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"); f.flush(); os.fsync(f.fileno())
    except: pass

def check_pause():
    if os.path.exists(PAUSE_FILE):
        log_message("[Pausa] Esperando...")
        while os.path.exists(PAUSE_FILE): time.sleep(1)
        log_message("[Reanudar] Continuando...")

def log_memory_usage():
    mem = psutil.virtual_memory().percent
    log_message(f"  [MEM] Sistema: {mem}%")

def load_paths():
    if not os.path.exists(CONFIG_FILE): return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f: return json.load(f).get('paths')

def save_metrics(df, output_path, inputs):
    preview = df.head(5).astype(str).to_dict(orient='records')
    metrics = {
        "rows": len(df), "columns": len(df.columns),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        "columns_list": list(df.columns),
        "output_file": output_path, "input_files": inputs,
        "preview": preview, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(METRICS_FILE, 'w', encoding='utf-8') as f: json.dump(metrics, f, indent=2)

def safe_load(path, log_error=True):
    if not os.path.exists(path):
        if log_error: log_message(f"ERROR: No encontrado {path}")
        return None
    try: return pd.read_parquet(path)
    except Exception as e:
        log_message(f"ERROR leyendo {path}: {e}"); return None

def optimize_memory(df):
    log_message("  ⚡ Optimizando memoria...")
    for col in df.select_dtypes(include=['object']).columns:
        if col in df.columns and df[col].dtype == 'object':
            num_unique = len(df[col].unique())
            num_total = len(df)
            if num_unique > 0 and num_unique / num_total < 0.5:
                df[col] = df[col].astype('category')
    return df

# --- Main ---
def run_step_3_main():
    start_time_total = time.time()
    setup_logging()
    log_message("[Paso 3] Construcción Base Atlas (Mixto+Acusatorio)...")
    
    paths = load_paths()
    processed_dir = paths.get('intermediate_processed')
    loaded_dir = paths.get('intermediate_loaded')
    raw_dir = paths.get('raw_data') # Para los Excel referenciales
    analytical_dir = paths.get('intermediate_analytical')
    os.makedirs(analytical_dir, exist_ok=True)
    
    input_files_used = []

    # 1. CARGA BASE (Paso 2)
    check_pause()
    log_message("  1/7 Cargando datos base (Paso 2)...")
    df = safe_load(os.path.join(processed_dir, 'data_casos_processed.parquet'))
    if df is None: 
        log_message("ERROR CRITICO: Falta 'data_casos_processed.parquet'. Ejecute el Paso 2.")
        return
    input_files_used.append("data_casos_processed.parquet")

    # 2. CARGA REFERENCIAS
    log_message("  2/7 Cargando Referencias...")
    df_delitos = safe_load(os.path.join(loaded_dir, 'df_delitos.parquet'), log_error=False)
    df_ult_act = safe_load(os.path.join(loaded_dir, 'CasosUltimaActuacionEstado.parquet'), log_error=False)
    df_personas_full = safe_load(os.path.join(loaded_dir, 'df_persona_actuacion_delito.parquet'), log_error=False)
    df_victimas = safe_load(os.path.join(loaded_dir, 'victimas_imputados.parquet'), log_error=False)
    
    excel_path = os.path.join(raw_dir, "TipoActuacionAcusatorioUNISA_Relacionales.xlsx")
    try:
        df_estados_unisa = pd.read_excel(excel_path, sheet_name="Relacional")
        df_orden_unisa = pd.read_excel(excel_path, sheet_name="OrdenUnisa")
        input_files_used.append("TipoActuacionAcusatorioUNISA_Relacionales.xlsx")
    except Exception as e:
        log_message(f"ERROR CRITICO: No se pudo leer {excel_path}. {e}")
        return

    # 3. MERGE DELITOS
    check_pause()
    log_message("  3/7 Cruzando Delitos...")
    if df_delitos is not None:
        df = pd.merge(df, df_delitos, on='IdCaso', how='left')
        input_files_used.append("df_delitos.parquet")
        del df_delitos; gc.collect()
    else:
        log_message("  -> ADVERTENCIA: 'df_delitos.parquet' no encontrado. Se continúa sin él.")
    
    # 4. MERGE ULTIMA ACTUACION
    check_pause()
    log_message("  4/7 Cruzando Última Actuación...")
    if df_ult_act is not None:
        df = pd.merge(df, df_ult_act, on='IdCaso', how='left')
        input_files_used.append("CasosUltimaActuacionEstado.parquet")
        del df_ult_act; gc.collect()
    else:
         log_message("  -> ADVERTENCIA: 'CasosUltimaActuacionEstado.parquet' no encontrado.")

    # 5. MERGE IMPUTADOS y VICTIMAS
    check_pause()
    log_message("  5/7 Contando Imputados y Víctimas...")
    if df_personas_full is not None:
        tImputadosporCaso = df_personas_full.groupby('IdCaso')['IdPersona'].nunique().reset_index().rename(columns={'IdPersona':'imputados'})
        df = pd.merge(df, tImputadosporCaso, on='IdCaso', how='left')
        df['imputados_complejo'] = np.where(df['imputados'] >= 3, '3 o más imputados', 'Menos de 3 imputados')
        del tImputadosporCaso, df_personas_full
        gc.collect()
    else:
        log_message("  -> ADVERTENCIA: 'df_persona_actuacion_delito.parquet' no encontrado.")
    
    if df_victimas is not None:
        tVictimasporCaso = df_victimas[df_victimas['rol_persona_descripcion'] == "Víctima"][['idcaso', 'cantidad']].drop_duplicates()
        tVictimasporCaso = tVictimasporCaso.rename(columns={'idcaso':'IdCaso', 'cantidad':'Victimas'})
        df = pd.merge(df, tVictimasporCaso, on='IdCaso', how='left')
        df['victimas_complejo'] = np.where(df['Victimas'] >= 3, '3 o más victimas', 'Menos de 3 victimas')
        del tVictimasporCaso, df_victimas
        gc.collect()
    else:
        log_message("  -> ADVERTENCIA: 'victimas_imputados.parquet' no encontrado.")

    # 6. ESTADO UNISA (Jerarquía)
    check_pause()
    log_message("  6/7 Calculando Estado UNISA...")
    df_finaliza = pd.merge(df_estados_unisa, df_orden_unisa, left_on="EstadoUNISA", right_on="EstadoUnisaOrden", how="left")
    df_finaliza['OrdenUnisa'] = pd.to_numeric(df_finaliza['OrdenUnisa'], errors='coerce').fillna(0)
    
    col_estado = 'actuacion_estadodelcaso' if 'actuacion_estadodelcaso' in df.columns else 'IdEstadoActuacion'
    df_unisa = pd.merge(df, df_finaliza, left_on=col_estado, right_on="EstadoCoiron", how='left')
    df_unisa['OrdenUnisa'] = df_unisa['OrdenUnisa'].fillna(0)
    
    df_unisa['ordenultimoestado'] = df_unisa.groupby('IdCaso')['OrdenUnisa'].transform('max')
    
    df_max = df_unisa[df_unisa['OrdenUnisa'] == df_unisa['ordenultimoestado']].copy()
    df_max['IdActuacionUltimoEstadoUNISA'] = df_max.groupby('IdCaso')['IdActuacion'].transform('min')
    
    df_final_info = df_max[df_max['IdActuacion'] == df_max['IdActuacionUltimoEstadoUNISA']][
        ['IdCaso', 'IdActuacionUltimoEstadoUNISA', 'ordenultimoestado', 'EstadoUNISA', 'fechaactuacion']
    ].rename(columns={'fechaactuacion': 'EstadoUnisaFecha'}).drop_duplicates(subset=['IdCaso'])
    
    df = pd.merge(df, df_final_info, on='IdCaso', how='left')
    df['EstadoUNISA'] = df['EstadoUNISA'].fillna("Sin Salidas")
    
    del df_unisa, df_max, df_finaliza, df_final_info; gc.collect()

    # 7. VARIABLES FINALES (Implementación, Tiempos, Audiencias)
    check_pause()
    log_message("  7/7 Calculando Variables Finales...")
    
    conds = [
        (df['jurisdiccion_ingreso'] == 'Rosario') | (df['jurisdiccion_actual'] == 'Rosario'),
        (df['jurisdiccion_ingreso'] == 'Mendoza') | (df['jurisdiccion_actual'] == 'Mendoza'),
        (df['jurisdiccion_ingreso'] == 'Salta') | (df['jurisdiccion_actual'] == 'Salta'),
        (df['jurisdiccion_ingreso'] == 'General Roca'), (df['jurisdiccion_ingreso'] == 'Comodoro Rivadavia'),
        (df['jurisdiccion_ingreso'] == 'Mar del Plata')
    ]
    choices = ['Rosario', 'Mendoza', 'Salta', 'General Roca', 'Comodoro Rivadavia', 'Mar del Plata']
    df['jurisdiccion_para_implementacion'] = np.select(conds, choices, default='Otras')
    
    for col in ['FechaIngreso', 'fechaactuacion', 'fecha_hecho', 'EstadoUnisaFecha']:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce')
        
    if 'fecha_hecho' in df.columns:
        df['tiempo_hecho_Ingreso'] = (df['FechaIngreso'] - df['fecha_hecho']).dt.days
        df['tiempo_hecho_ESTADOUNISA'] = (df['EstadoUnisaFecha'] - df['fecha_hecho']).dt.days
    
    df['descripcion_sistemaprocesal'] = np.where(df['IdSistemaProcesal'] == 2, "Acusatorio", "Mixto")
    
    df = df.drop_duplicates()
    
    df = optimize_memory(df)
    output_path = os.path.join(analytical_dir, 'data_final_comparativo.parquet')
    df.to_parquet(output_path, index=False, engine='pyarrow')
    save_metrics(df, output_path, input_files_used)
    
    log_message(f"  ¡Éxito! Atlas generado: {output_path} ({len(df.columns)} columnas)")
    log_message(f"--- [Paso 3] FINALIZADO ({time.time() - start_time_total:.2f}s) ---")

if __name__ == "__main__":
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_3_main()
    except Exception as e:
        try:
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG): os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE): os.remove(PID_FILE)