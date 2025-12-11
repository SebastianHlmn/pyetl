# step_2_process_casos.py
"""
step_2_process_casos.py

Paso 2: Carga los casos de '1_intermedio_cargado', los procesa, unifica y 
crea variables base. Guarda el resultado en '2_intermedio_procesado'.
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
LOG_FILE = os.path.join(LOG_DIR, "step_2.log")
PID_FILE = os.path.join(LOG_DIR, "step_2.pid")      
PAUSE_FILE = os.path.join(LOG_DIR, "step_2.pause")  
RUNNING_FLAG = os.path.join(LOG_DIR, "step_2.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_2_metrics.json") 
CONFIG_FILE = 'config.json'

# --- Funciones de Control ---

def check_pause(log_key="[Paso 2]"):
    """Revisa si existe el archivo de pausa y espera hasta que se borre."""
    if os.path.exists(PAUSE_FILE):
        log_message(f"{log_key} ⏸️ PAUSADO por el usuario. Esperando...")
        while os.path.exists(PAUSE_FILE):
            time.sleep(1)
        log_message(f"{log_key} ▶️ REANUDANDO ejecución...")

def save_metrics(df, output_path, input_files_list):
    """Guarda un resumen procesado + lista de inputs para el Inspector."""
    preview = df.head(10).astype(str).to_dict(orient='records')
    
    metrics = {
        "rows": len(df),
        "columns": len(df.columns),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        "columns_list": list(df.columns),
        "output_file": output_path,
        "input_files": input_files_list, 
        "preview": preview,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(METRICS_FILE, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 2] Iniciando proceso (PID: {os.getpid()})...\n")

def log_message(new_message):
    print(new_message) 
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {new_message}\n")
            f.flush() 
            os.fsync(f.fileno())
    except Exception as e:
        print(f"Error log: {e}")

def log_memory_usage():
    mem_usage = psutil.virtual_memory().percent
    log_message(f"  [MEM] Uso de RAM del Sistema: {mem_usage}%")
    
def safe_load(path, log_error=True):
    if not os.path.exists(path):
        if log_error: log_message(f"  -> ERROR: Archivo no encontrado: {path}")
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        if log_error: log_message(f"  -> ERROR Leyendo {path}: {e}")
        return None

def load_config():
    if not os.path.exists(CONFIG_FILE): 
        log_message(f"ERROR: No se encontró {CONFIG_FILE}")
        return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def optimize_memory(df):
    """Convierte columnas object a category para ahorrar RAM antes de guardar."""
    log_message("  ⚡ Optimizando uso de memoria (Convirtiendo a Categorías)...")
    initial_mem = df.memory_usage(deep=True).sum() / 1024**2
    
    cols_to_category = [
        'organismo', 'IdOficinaAlta', 'IdOficinaActual', 'IdSistemaProcesal',
        'IdEstadoActuacion', 'IdCasoDivision', 'Activa', 'origen', 'tipodecaso',
        'jurisdiccion_ingreso', 'jurisdiccion_actual', 'jurisdiccion_actuacion',
        'fiscalia_ingreso', 'fiscalia_actual', 'fiscalia_actuacion',
        'oficina_ingreso', 'oficina_actual', 'oficina_actuacion',
        'unidadfiscal_ingreso', 'unidadfiscal_actual', 'unidadfiscal_actuacion',
        'territorio_acusatorio_ingreso', 'territorio_acusatorio_actual', 'territorio_acusatorio_actuacion',
        'caso_incidente', 'ActuacionCopiada', 'estadocaso'
    ]
    
    for col in cols_to_category:
        if col in df.columns and df[col].dtype == 'object':
            if df[col].nunique() > 0:
                if df[col].nunique() / len(df) < 0.5: 
                    df[col] = df[col].astype('category')

    final_mem = df.memory_usage(deep=True).sum() / 1024**2
    log_message(f"  -> Reducción de RAM: {initial_mem:.2f} MB -> {final_mem:.2f} MB")
    return df

# --- LÓGICA PRINCIPAL ---

def run_step_2_main():
    start_time_total = time.time()
    setup_logging()
    
    log_message("[Paso 2] Motor de Procesamiento Iniciado.")
    log_memory_usage()
    
    config = load_config()
    if not config:
        log_message("ERROR FATAL: Configuración inválida.")
        return

    paths_config = config.get('paths', {})
    filters_config = config.get('filters', {})

    loaded_dir = paths_config.get('intermediate_loaded')
    processed_dir = paths_config.get('intermediate_processed')
    os.makedirs(processed_dir, exist_ok=True)
    
    input_files_used = []

    # 1. CARGA DE DATOS
    check_pause()
    log_message("  1/5 Cargando archivos Parquet...")
    
    f1 = os.path.join(loaded_dir, 'CasosActuacionesInquisitivo.parquet')
    f2 = os.path.join(loaded_dir, 'CasosActuacionesAcusatorio.parquet')
    f3 = os.path.join(loaded_dir, 'fechadelhecho.parquet')
    
    df_inquisitivo = safe_load(f1)
    df_acusatorio = safe_load(f2)
    df_fechadelhecho = safe_load(f3, log_error=False) 
    
    if df_inquisitivo is None or df_acusatorio is None:
        log_message("ERROR CRITICO: Faltan archivos de casos (Inquisitivo o Acusatorio).")
        return
    
    input_files_used.extend(["CasosActuacionesInquisitivo.parquet", "CasosActuacionesAcusatorio.parquet"])
    if df_fechadelhecho is not None:
        input_files_used.append("fechadelhecho.parquet")
    else:
        log_message("[Paso 2] ADVERTENCIA: No se encontró 'fechadelhecho.parquet'. Se continuará sin él.")
        
    # 2. UNIFICACIÓN
    check_pause()
    log_message("  2/5 Unificando datasets...")
    
    rename_map = {
        'idcaso': 'IdCaso', 'fechaingreso': 'FechaIngreso', 'idtipoestadisticaprocesal': 'IdTipoEstadisticaProcesal',
        'organismo': 'Organismo', 'idoficinaalta': 'IdOficinaAlta', 'idoficinactual': 'IdOficinaActual',
        'idsistemaprocesal': 'IdSistemaProcesal', 'autoresignorados': 'AutoresIgnorados', 'idprocedimiento': 'IdProcedimiento',
        'idactuacion': 'IdActuacion', 'idestadoactuacion': 'IdEstadoActuacion', 'idcasodivision': 'IdCasoDivision',
        'activa': 'Activa', 'idprocedimiento_1': 'IdProcedimiento_1', 'personadelito': 'PersonaDelito'
    }
    df_acusatorio = df_acusatorio.rename(columns=rename_map)
    
    df_casos = pd.concat([df_inquisitivo, df_acusatorio], ignore_index=True)
    del df_inquisitivo, df_acusatorio
    gc.collect()
    
    log_message(f"  -> Total filas brutas: {len(df_casos):,}")
    df_casos = df_casos[df_casos['estadocaso'] != 'Anulado']
    log_message("  -> Filtro de anulados aplicado.")
    log_memory_usage()

    # 3. PROCESAMIENTO
    check_pause()
    log_message("  3/5 Procesando lógica de negocio (IDs, Fechas)...")

    df_casos['numero'] = df_casos['numero'].astype(str)
    # Lógica de caso original
    casos_originales = df_casos[~df_casos['numero'].str.contains('/INC', na=False, case=False)][['numero', 'FechaIngreso']]
    casos_originales = casos_originales.drop_duplicates(subset=['numero'])
    casos_originales = casos_originales.rename(columns={'FechaIngreso': 'FechaIngresoCasoIncidente'})
    
    df_casos['IdCasoOriginal'] = df_casos['numero'].str.extract(r"(\d+/\d{4})")
    
    df_casos = pd.merge(
        df_casos, casos_originales, 
        left_on='IdCasoOriginal', right_on='numero', 
        how='left', suffixes=('', '_orig')
    )
    if 'numero_orig' in df_casos.columns:
        df_casos = df_casos.drop(columns=['numero_orig'])
    df_casos['FechaIngresoOriginal'] = pd.to_datetime(df_casos['FechaIngreso'], errors='coerce')
    del casos_originales
    gc.collect()

    for col in ['FechaIngreso', 'fechaactuacion', 'fechaaltaactuacion']:
        df_casos[col] = pd.to_datetime(df_casos[col], errors='coerce')

    # --- CORRECCIÓN: FILTROS DE FECHA DESDE CONFIG ---
    fecha_start_str = filters_config.get("date_start", "2018-01-01")
    fecha_end_str = filters_config.get("date_end", "2025-12-31")
    
    fechaA = pd.to_datetime(fecha_start_str)
    fechaB = pd.to_datetime(fecha_end_str)
    
    log_message(f"  -> Aplicando filtro de fechas: {fechaA.date()} a {fechaB.date()}")

    df_casos = df_casos[df_casos['FechaIngreso'].between(fechaA, fechaB)]
    mask_fecha_act = (df_casos['fechaactuacion'].between(fechaA, fechaB)) | (df_casos['IdActuacion'].isna())
    df_casos = df_casos[mask_fecha_act]
    
    df_casos['oficina_ingreso'] = df_casos['oficina_ingreso'].fillna(df_casos['oficina_actual'])
    df_casos['fiscalia_ingreso'] = df_casos['fiscalia_ingreso'].fillna(df_casos['fiscalia_actual'])
    df_casos['jurisdiccion_ingreso'] = df_casos['jurisdiccion_ingreso'].fillna(df_casos['jurisdiccion_actual'])
    
    df_casos['fechaprimeractuacion'] = df_casos.groupby('IdCaso')['fechaaltaactuacion'].transform('min')
    df_casos['caso_incidente'] = np.where(df_casos['origen'] == 'Incidente de Caso', 'Incidente', 'Caso')
    df_casos['ActuacionCopiada'] = np.where(df_casos['IdCasoDivision'].notna(), "ActuacionCopiada", "NoCopiada")

    # 4. CRUCE CON FECHA DEL HECHO
    check_pause()
    if df_fechadelhecho is not None:
        log_message("  4/5 Uniendo con 'fechadelhecho'...")
        df_fechadelhecho = df_fechadelhecho.rename(columns={'idcaso': 'idcaso_hecho'})
        df_casos = pd.merge(
            df_casos, df_fechadelhecho, 
            left_on='IdCaso', right_on='idcaso_hecho', 
            how='left'
        )
        if 'idcaso_hecho' in df_casos.columns:
            df_casos = df_casos.drop(columns=['idcaso_hecho'])
        del df_fechadelhecho
        gc.collect()
    else:
        df_casos['fecha_hecho'] = pd.NaT
        df_casos['fechahechosk'] = np.nan

    # 5. LÓGICA DE UNIDADES FISCALES
    log_message("  5/5 Aplicando lógica de Unidades Fiscales...")
    for col in ['fiscalia_ingreso', 'fiscalia_actual', 'fiscalia_actuacion']:
        df_casos[col] = df_casos[col].astype(str).fillna('Sin datos')

    df_casos['unidadfiscal_ingreso'] = df_casos['fiscalia_ingreso']
    df_casos['unidadfiscal_actual'] = df_casos['fiscalia_actual']
    df_casos['unidadfiscal_actuacion'] = df_casos['fiscalia_actuacion']

    # Optimización usando np.select en lugar de case_when anidado
    conds_ing = [df_casos['oficina_ingreso'] == 'Subsede ORAN', df_casos['oficina_ingreso'] == 'Subsede TARTAGAL']
    choices_ing = ['Subsede ORAN', 'Subsede TARTAGAL']
    df_casos['unidadfiscal_ingreso'] = np.select(conds_ing, choices_ing, default=df_casos['unidadfiscal_ingreso'])

    conds_act = [df_casos['oficina_actual'] == 'Subsede ORAN', df_casos['oficina_actual'] == 'Subsede TARTAGAL']
    choices_act = ['Subsede ORAN', 'Subsede TARTAGAL']
    df_casos['unidadfiscal_actual'] = np.select(conds_act, choices_act, default=df_casos['unidadfiscal_actual'])
    
    conds_actu = [df_casos['oficina_actuacion'] == 'Subsede ORAN', df_casos['oficina_actuacion'] == 'Subsede TARTAGAL']
    choices_actu = ['Subsede ORAN', 'Subsede TARTAGAL']
    df_casos['unidadfiscal_actuacion'] = np.select(conds_actu, choices_actu, default=df_casos['unidadfiscal_actuacion'])
    
    conds_terr_ing = [(df_casos['unidadfiscal_ingreso'] == 'Subsede ORAN') | (df_casos['unidadfiscal_ingreso'] == 'Subsede TARTAGAL')]
    df_casos['territorio_acusatorio_ingreso'] = np.select(conds_terr_ing, ['Orán Tartagal'], default=df_casos['unidadfiscal_ingreso'])
    
    conds_terr_act = [(df_casos['unidadfiscal_actual'] == 'Subsede ORAN') | (df_casos['unidadfiscal_actual'] == 'Subsede TARTAGAL')]
    df_casos['territorio_acusatorio_actual'] = np.select(conds_terr_act, ['Orán Tartagal'], default=df_casos['unidadfiscal_actual'])
    
    conds_terr_actu = [(df_casos['unidadfiscal_actuacion'] == 'Subsede ORAN') | (df_casos['unidadfiscal_actuacion'] == 'Subsede TARTAGAL')]
    df_casos['territorio_acusatorio_actuacion'] = np.select(conds_terr_actu, ['Orán Tartagal'], default=df_casos['unidadfiscal_actuacion'])

    # --- OPTIMIZACIÓN ---
    check_pause()
    df_casos = optimize_memory(df_casos)
    gc.collect()

    # 6. GUARDADO FINAL
    log_message("  -> Guardando resultado final...")
    output_path = os.path.join(processed_dir, 'data_casos_processed.parquet')
    
    try:
        df_casos.to_parquet(output_path, index=False, engine='pyarrow')
        log_message(f"  ¡Éxito! Archivo guardado: {output_path}")
        save_metrics(df_casos, output_path, input_files_used)
    except Exception as e:
        log_message(f"  ERROR FATAL al guardar: {e}")
        return False
        
    end_time_total = time.time()
    log_message(f"--- [Paso 2] FINALIZADO CORRECTAMENTE ({end_time_total - start_time_total:.2f}s) ---")
    log_memory_usage()
    return True

if __name__ == "__main__":
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_2_main()
    except Exception as e:
        try:
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] Excepción no controlada: {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG): os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE): os.remove(PID_FILE)
# step_2_process_casos.py