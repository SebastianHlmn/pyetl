# step_6_export_reports.py
import pandas as pd
import numpy as np
import os
import time
import sys
import psutil
import gc
import json 
import math 
import traceback # AGREGADO
from datetime import datetime # AGREGADO

# --- Constantes ---
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "step_6.log")
PID_FILE = os.path.join(LOG_DIR, "step_6.pid")
PAUSE_FILE = os.path.join(LOG_DIR, "step_6.pause")
RUNNING_FLAG = os.path.join(LOG_DIR, "step_6.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_6_metrics.json")
CONFIG_FILE = 'config.json'
CHUNK_SIZE = 50000 

# --- Funciones de Control ---
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f: f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f: 
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 6] Iniciando...\n")

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

def load_paths():
    if not os.path.exists(CONFIG_FILE): return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f: return json.load(f).get('paths')

def save_metrics(df, main_output, created_files):
    preview = df.head(5).astype(str).to_dict(orient='records')
    metrics = {
        "rows": len(df), "columns": len(df.columns),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        "columns_list": list(df.columns),
        "output_file": main_output,
        "created_files": created_files,
        "preview": preview,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(METRICS_FILE, 'w', encoding='utf-8') as f: json.dump(metrics, f, indent=2)

def safe_load(path):
    if not os.path.exists(path):
        log_message(f"ERROR: No encontrado {path}"); return None
    try: return pd.read_parquet(path)
    except Exception as e:
        log_message(f"ERROR leyendo {path}: {e}"); return None

def log_memory():
    mem = psutil.virtual_memory().percent
    return f"(RAM: {mem}%)"

# --- FUNCIÓN DE ESCRITURA POR LOTES (Master CSV) ---
def save_csv_chunked(df, path, sep=';', encoding='windows-1252'):
    """Guarda un CSV grande reportando progreso para no parecer colgado."""
    total_rows = len(df)
    chunks = math.ceil(total_rows / CHUNK_SIZE)
    
    log_message(f"    -> Iniciando exportación de {total_rows:,} filas en {chunks} lotes...")
    
    # 1. Escribir cabecera (Modo 'w')
    with open(path, 'w', encoding=encoding, newline='') as f:
        df.head(0).to_csv(f, sep=sep, index=False)
    
    # 2. Escribir datos por lotes (Modo 'a')
    for i in range(chunks):
        check_pause() 
        start_idx = i * CHUNK_SIZE
        end_idx = min((i + 1) * CHUNK_SIZE, total_rows)
        
        chunk = df.iloc[start_idx:end_idx].copy()
        
        chunk.to_csv(path, sep=sep, encoding=encoding, index=False, header=False, mode='a', errors='replace')
        
        del chunk
        gc.collect() # Liberar memoria entre lotes
        
        percent = int((end_idx / total_rows) * 100)
        log_message(f"      [{percent}%] Guardado lote {i+1}/{chunks} (filas {start_idx}-{end_idx}) {log_memory()}")

# --- Lógica Principal ---
def run_step_6_main():
    setup_logging()
    log_message("[Paso 6] Generando Reportes...")
    
    paths = load_paths()
    analytical_dir = paths.get('intermediate_analytical')
    output_dir = paths.get('output_reports')
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. CARGA
    check_pause()
    log_message(f"  1/4 Cargando Atlas... {log_memory()}")
    df = safe_load(os.path.join(analytical_dir, 'data_final_comparativo.parquet'))
    if df is None: 
        log_message("ERROR CRITICO: Falta data_final_comparativo.parquet")
        return

    created_files = []

    # --- PREPARACIÓN DE TIPOS Y MEMORIA ANTES DEL BLOQUEO CRÍTICO ---
    log_message("  -> Normalizando tipos de datos a String (Prevención de Buffer Crash)...")
    for col in df.select_dtypes(include=['category']).columns:
        df[col] = df[col].astype(str)
    gc.collect()
    
    # 2. GENERACIÓN DE BASE MAESTRA (CSV)
    check_pause()
    log_message(f"  2/4 Exportando CSV Maestro (baseUnisaMixtoAcusatorio.csv)... {log_memory()}")
    
    master_name = "baseUnisaMixtoAcusatorio.csv"
    master_path = os.path.join(output_dir, master_name)
    
    try:
        save_csv_chunked(df, master_path, sep=';', encoding='windows-1252')
        created_files.append(master_name)
        log_message(f"    ✅ ¡CSV Maestro Terminado!")
    except Exception as e:
        log_message(f"    ❌ ERROR CRITICO guardando CSV: {e}")
        log_message("    [DIAGNÓSTICO] Falló el buffer de conversión (MemoryError).")
        
    # 3. REPORTES EXCEL
    check_pause()
    log_message(f"  3/4 Generando Excels Jurisdiccionales... {log_memory()}")

    if 'jurisdiccion_para_implementacion' not in df.columns:
        df['jurisdiccion_para_implementacion'] = df['jurisdiccion_ingreso'] if 'jurisdiccion_ingreso' in df.columns else 'Desconocido'

    reportes = {
        "Rosario": df[(df['jurisdiccion_para_implementacion'] == "Rosario") & (df['oficina_ingreso'] != "Noreste")],
        "Mendoza": df[df['jurisdiccion_para_implementacion'] == "Mendoza"],
        "Salta": df[df['jurisdiccion_para_implementacion'] == "Salta"],
    }

    df_impl = pd.DataFrame()

    for nombre, data in reportes.items():
        if not data.empty:
            fname = f"ParaReporte{nombre}.xlsx"
            fpath = os.path.join(output_dir, fname)
            log_message(f"    -> Generando {fname} ({len(data)} filas)...")
            try:
                data.to_excel(fpath, index=False)
                created_files.append(fname)
                df_impl = pd.concat([df_impl, data], ignore_index=True)
            except Exception as e:
                log_message(f"    ❌ ERROR guardando {fname}: {e}")
        else:
            log_message(f"    -> . {nombre} (Vacío)")

    # 4. CONSOLIDADO
    log_message(f"  4/4 Generando Consolidado Implementadas... {log_memory()}")
    targets = ["Rosario", "Mendoza", "Salta", "General Roca", "Comodoro Rivadavia", "Mar del Plata"]
    df_impl = df[df['jurisdiccion_para_implementacion'].isin(targets)]
    
    if not df_impl.empty:
        fname_impl = "ParaReporteImplementadas.xlsx"
        fpath_impl = os.path.join(output_dir, fname_impl)
        try:
            df_impl.to_excel(fpath_impl, index=False)
            created_files.append(fname_impl)
        except Exception as e:
            log_message(f"    ❌ ERROR guardando consolidado: {e}")
    
    log_message(f"--- [Paso 6] FINALIZADO. {len(created_files)} archivos generados. ---")
    
    save_metrics(df_impl if not df_impl.empty else df, master_path, created_files)

if __name__ == "__main__":
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_6_main()
    except Exception as e:
        # SINTAXIS CORREGIDA
        try: 
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG): os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE): os.remove(PID_FILE)