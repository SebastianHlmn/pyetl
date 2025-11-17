"""
step_1_load_raw_data.py

Paso 1: Carga todos los archivos CSV y Excel de la carpeta 'raw_data',
y los guarda en formato Parquet en '1_intermedio_cargado'.
"""
import os
import pandas as pd
import time
import sys
import psutil
import gc
import json 
import traceback 
from datetime import datetime

# --- Constantes ---
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "step_1.log")
PID_FILE = os.path.join(LOG_DIR, "step_1.pid")      
PAUSE_FILE = os.path.join(LOG_DIR, "step_1.pause")  
RUNNING_FLAG = os.path.join(LOG_DIR, "step_1.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_1_metrics.json") 
CONFIG_FILE = 'config.json'

# --- Funciones de Control ---

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 1] Iniciando proceso (PID: {os.getpid()})...\n")

def log_message(new_message):
    print(new_message) 
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {new_message}\n")
            f.flush() 
            os.fsync(f.fileno())
    except Exception as e:
        print(f"Error log: {e}")

def check_pause(log_key="[Paso 1]"):
    """Revisa si existe el archivo de pausa y espera hasta que se borre."""
    if os.path.exists(PAUSE_FILE):
        log_message(f"{log_key} ⏸️ PAUSADO por el usuario. Esperando...")
        while os.path.exists(PAUSE_FILE):
            time.sleep(1)
        log_message(f"{log_key} ▶️ REANUDANDO ejecución...")

def load_paths():
    if not os.path.exists(CONFIG_FILE): 
        log_message(f"ERROR: No se encontró {CONFIG_FILE}")
        return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f).get('paths')

def save_metrics(processed_files_dict, output_dir, total_rows):
    """Guarda un resumen de los archivos cargados."""
    global raw_dir
    metrics = {
        "rows": total_rows, 
        "columns": None, 
        "memory_mb": None, 
        "files_processed_count": len(processed_files_dict),
        "created_files": list(processed_files_dict.keys()), 
        "output_file": output_dir,
        "input_files": [f"Directorio: {raw_dir}"], 
        "files_summary": processed_files_dict, 
        "preview": [], 
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(METRICS_FILE, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)

# --- LÓGICA PRINCIPAL ---
raw_dir = "" # Variable global para el log de métricas

def run_step_1_main():
    global raw_dir
    start_time_total = time.time()
    setup_logging()
    
    log_message("[Paso 1] Iniciando. Buscando archivos en 'raw_data'...")
    
    paths_config = load_paths()
    if not paths_config:
        log_message("ERROR FATAL: Configuración de rutas inválida.")
        return

    raw_dir = paths_config.get('raw_data')
    loaded_dir = paths_config.get('intermediate_loaded')
    
    if not raw_dir or not loaded_dir:
        log_message("[Paso 1] ERROR: Rutas 'raw_data' or 'intermediate_loaded' no encontradas en config.")
        return
        
    os.makedirs(loaded_dir, exist_ok=True)
    
    try:
        files_to_process = [f for f in os.listdir(raw_dir) if f.endswith(('.csv', '.xlsx', '.xls'))]
        if not files_to_process:
            log_message(f"[Paso 1] ADVERTENCIA: No se encontraron archivos .csv o .xlsx en {raw_dir}.")
            return
            
    except FileNotFoundError:
        log_message(f"[Paso 1] ERROR: El directorio 'raw_data' no existe en: {raw_dir}")
        return
    
    total_files = len(files_to_process)
    success_count = 0
    processed_files_dict = {}
    total_rows_loaded = 0
    log_message(f"[Paso 1] Se encontraron {total_files} archivos para procesar.")

    for i, filename in enumerate(files_to_process):
        check_pause() 
        
        file_path = os.path.join(raw_dir, filename)
        start_time_file = time.time()
        
        try:
            if filename.endswith('.csv'):
                log_message(f"  ({i+1}/{total_files}) Cargando CSV: {filename}...")
                df = pd.read_csv(
                    file_path, 
                    sep=';', 
                    encoding='latin-1', 
                    low_memory=False,
                    skip_blank_lines=True
                )
                output_filename = os.path.splitext(filename)[0] + '.parquet'
                output_path = os.path.join(loaded_dir, output_filename)
                log_message(f"  -> {df.shape[0]:,} filas, {df.shape[1]} columnas.")
                df.to_parquet(output_path, index=False, engine='pyarrow')
                processed_files_dict[output_filename] = len(df)
                total_rows_loaded += len(df)
            
            elif filename.endswith(('.xlsx', '.xls')):
                log_message(f"  ({i+1}/{total_files}) Cargando Excel: {filename}...")
                xls = pd.ExcelFile(file_path)
                # Si es un Excel "normal", carga solo la primera hoja
                if not any(keyword in filename for keyword in ["dim_tiempo", "RRHH_MPF_Periodos", "tablas_relacional_PJN-MPF_Ok", "RelacionalRRHHConsolidada"]):
                     df = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
                     output_filename = os.path.splitext(filename)[0] + '.parquet'
                     output_path = os.path.join(loaded_dir, output_filename)
                     log_message(f"  -> Hoja '{xls.sheet_names[0]}' cargada. {df.shape[0]:,} filas, {df.shape[1]} columnas.")
                     df.to_parquet(output_path, index=False, engine='pyarrow')
                     processed_files_dict[output_filename] = len(df)
                     total_rows_loaded += len(df)
                else:
                    # Si es un referencial, carga todas las hojas
                    log_message(f"    -> Detectado Excel con múltiples hojas. Cargando todas...")
                    for sheet_name in xls.sheet_names:
                        sheet_df = pd.read_excel(xls, sheet_name=sheet_name)
                        sheet_output_filename = f"{os.path.splitext(filename)[0]}_{sheet_name}.parquet"
                        sheet_output_path = os.path.join(loaded_dir, sheet_output_filename)
                        sheet_df.to_parquet(sheet_output_path, index=False, engine='pyarrow')
                        log_message(f"    -> Hoja '{sheet_name}' guardada en {sheet_output_path} ({len(sheet_df)} filas)")
                        processed_files_dict[sheet_output_filename] = len(sheet_df)
                        total_rows_loaded += len(sheet_df)

            end_time_file = time.time()
            log_message(f"  -> ¡Éxito! Procesado en ({end_time_file - start_time_file:.2f}s)")
            success_count += 1
            
        except Exception as e:
            log_message(f"  -> ERROR al procesar {filename}: {e}")
            pass 
            
    end_time_total = time.time()
    log_message(f"--- [Paso 1] Completado ---")
    log_message(f"Archivos procesados exitosamente: {success_count}/{total_files}")
    log_message(f"Tiempo total del Paso 1: {end_time_total - start_time_total:.2f}s")
    
    save_metrics(processed_files_dict, loaded_dir, total_rows_loaded)

if __name__ == "__main__":
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_1_main()
    except Exception as e:
        try:
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG): os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE): os.remove(PID_FILE)