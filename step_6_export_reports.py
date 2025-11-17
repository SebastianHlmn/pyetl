"""
step_6_export_reports.py

Paso 6: Carga la base "Atlas" del Paso 3 y genera los reportes
finales en CSV y Excel para el usuario.
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
LOG_FILE = os.path.join(LOG_DIR, "step_6.log")
PID_FILE = os.path.join(LOG_DIR, "step_6.pid")
PAUSE_FILE = os.path.join(LOG_DIR, "step_6.pause")
RUNNING_FLAG = os.path.join(LOG_DIR, "step_6.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_6_metrics.json")
CONFIG_FILE = 'config.json'

# --- Funciones de Control ---
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f: f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f: 
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 6] Iniciando Generación de Reportes...\n")

def log_message(msg):
    print(msg); sys.stdout.flush()
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"); f.flush(); os.fsync(f.fileno())
    except: pass

def check_pause():
    if os.path.exists(PAUSE_FILE):
        log_message("[Paso 6] ⏸️ PAUSADO.")
        while os.path.exists(PAUSE_FILE): time.sleep(1)
        log_message("[Paso 6] ▶️ REANUDANDO...")

def load_paths():
    if not os.path.exists(CONFIG_FILE): return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f: return json.load(f).get('paths')

def save_metrics(df, main_output, created_files):
    """Guarda métricas y la lista de todos los archivos creados."""
    preview = df.head(5).astype(str).to_dict(orient='records')
    metrics = {
        "rows": len(df),
        "columns": len(df.columns),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        "columns_list": list(df.columns),
        "output_file": main_output, 
        "created_files": created_files, 
        "preview": preview,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(METRICS_FILE, 'w', encoding='utf-8') as f: json.dump(metrics, f, indent=2)

def safe_load(path, log_error=True):
    if not os.path.exists(path):
        if log_error: log_message(f"ERROR: No encontrado {path}")
        return None
    try: return pd.read_parquet(path)
    except Exception as e:
        log_message(f"ERROR leyendo {path}: {e}"); return None

# --- Lógica Principal ---
def run_step_6_main():
    start_time_total = time.time()
    setup_logging()
    log_message("[Paso 6] Generando Reportes Finales (Excel/CSV)...")
    
    paths = load_paths()
    analytical_dir = paths.get('intermediate_analytical')
    output_dir = paths.get('output_reports')
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. CARGA
    check_pause()
    log_message("  1/4 Cargando Base Analítica (Atlas)...")
    input_file_path = os.path.join(analytical_dir, 'data_final_comparativo.parquet')
    df = safe_load(input_file_path)
    if df is None: 
        log_message("ERROR CRITICO: Falta data_final_comparativo.parquet (Ejecutar Paso 3 primero)")
        return

    created_files = []
    input_files_used = ["data_final_comparativo.parquet"]

    # 2. GENERACIÓN DE BASE MAESTRA (CSV)
    check_pause()
    log_message("  2/4 Exportando Base Maestra (baseUnisaMixtoAcusatorio.csv)...")
    
    master_csv_name = "baseUnisaMixtoAcusatorio.csv"
    master_csv_path = os.path.join(output_dir, master_csv_name)
    
    try:
        # Convertimos 'category' a 'object' (string) para evitar errores de codificación
        for col in df.select_dtypes(include=['category']).columns:
            df[col] = df[col].astype(str)
            
        df.to_csv(master_csv_path, index=False, sep=';', encoding='latin-1', errors='replace')
        created_files.append(master_csv_name)
        log_message(f"    -> ¡Éxito! {master_csv_name}")
    except Exception as e:
        log_message(f"    ERROR guardando Base Maestra: {e}")

    # 3. REPORTES JURISDICCIONALES
    check_pause()
    log_message("  3/4 Generando Reportes Excel por Jurisdicción...")

    if 'jurisdiccion_para_implementacion' not in df.columns:
        log_message("WARN: Columna 'jurisdiccion_para_implementacion' faltante. Saltando recortes.")
    else:
        reportes = {
            "Rosario": df[
                (df['jurisdiccion_para_implementacion'] == "Rosario") & 
                (df['oficina_ingreso'] != "Noreste") # Lógica específica de R
            ],
            "Mendoza": df[df['jurisdiccion_para_implementacion'] == "Mendoza"],
            "Salta": df[df['jurisdiccion_para_implementacion'] == "Salta"],
            "GeneralRoca": df[df['jurisdiccion_para_implementacion'] == "General Roca"],
            "ComodoroRivadavia": df[df['jurisdiccion_para_implementacion'] == "Comodoro Rivadavia"],
        }

        for nombre, data in reportes.items():
            if not data.empty:
                fname = f"ParaReporte{nombre}.xlsx"
                fpath = os.path.join(output_dir, fname)
                log_message(f"    -> Generando {fname} ({len(data)} filas)...")
                try:
                    data.to_excel(fpath, index=False)
                    created_files.append(fname)
                except Exception as e:
                    log_message(f"    ERROR guardando {fname}: {e}")
            else:
                log_message(f"    -> Saltando {nombre} (Sin datos).")

    # 4. REPORTE CONSOLIDADO
    log_message("  4/4 Generando Reporte Consolidado (Implementadas)...")
    
    jurisdicciones_impl = ["Rosario", "Mendoza", "Salta", "General Roca", "Comodoro Rivadavia", "Mar del Plata"]
    if 'jurisdiccion_para_implementacion' in df.columns:
        df_impl = df[df['jurisdiccion_para_implementacion'].isin(jurisdicciones_impl)]
    else:
        df_impl = pd.DataFrame() # Dataframe vacío
    
    fname_impl = "ParaReporteImplementadas.xlsx"
    fpath_impl = os.path.join(output_dir, fname_impl)
    
    if not df_impl.empty:
        log_message(f"    -> Guardando Consolidado ({len(df_impl)} filas)...")
        try:
            df_impl.to_excel(fpath_impl, index=False)
            created_files.append(fname_impl)
        except Exception as e:
            log_message(f"    ERROR guardando consolidado: {e}")
    else:
        log_message("    -> Saltando Consolidado (Sin datos).")
    
    # FINALIZACIÓN
    log_message(f"--- [Paso 6] FINALIZADO. {len(created_files)} archivos generados. ---")
    
    save_metrics(df, master_csv_path, input_files_used)

if __name__ == "__main__":
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_6_main()
    except Exception as e:
        try:
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG):
            os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)