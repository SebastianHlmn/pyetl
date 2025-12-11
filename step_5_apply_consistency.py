# step_5_apply_consistency.py
import pandas as pd
import numpy as np
import os
import time
import sys
import psutil
import gc
import json 
from datetime import datetime
import traceback
import hashlib # Necesario para IdTrinomio (digest)

# --- Constantes ---
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "step_5.log")
PID_FILE = os.path.join(LOG_DIR, "step_5.pid")
PAUSE_FILE = os.path.join(LOG_DIR, "step_5.pause")
RUNNING_FLAG = os.path.join(LOG_DIR, "step_5.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_5_metrics.json")
CONFIG_FILE = 'config.json'

# --- Jerarquía ---
ORDEN_RESOLUCIONES = {
    "Archivo": 0, "Archivo - Aplicación": 0, "Archivo - Firme": 1,
    "Desestimación": 0, "Desestimación - Aplicación": 0, "Desestimación - Firme": 1,
    "Derivado a organismo externo": 0, "Incompetencia": 0, "Cese de intervención del MPF": 0,
    "Criterio Oportunidad - Aplicación": 2, "Criterio Oportunidad - Firme": 3,
    "Sobreseimiento por Criterio de oportunidad": 4, "Conciliación": 5,
    "Sobreseimiento por Conciliación": 6, "Reparación Integral": 5, "Sobreseimiento por Reparación": 6,
    "Suspensión de proceso a prueba": 5, "Sobreseimiento por suspensión proceso a prueba": 6,
    "Suspensión acción penal por cuestión prejudicial": 5, "Sobreseimiento": 7, "Expulsión": 7,
    "SentenciaAbsolutoriaJuicio": 8, "SentenciaAbsolutoriaAcuerdoPleno": 8,
    "SentenciaCondenatoriaJuicio": 9, "SentenciaCondenatoriaAcuerdoPleno": 9,
    "criteriodeoportunidadrevisión": 3
}

# --- Funciones Control ---
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f: f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f: f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 5] Iniciando...\n")

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

def safe_load(path):
    if not os.path.exists(path):
        log_message(f"ERROR: No encontrado {path}"); return None
    try: return pd.read_parquet(path)
    except Exception as e:
        log_message(f"ERROR leyendo {path}: {e}"); return None

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

def optimize_memory(df):
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5: df[col] = df[col].astype('category')
    return df

# --- Main ---
def run_step_5_main():
    start_time_total = time.time()
    setup_logging()
    log_message("[Paso 5] Consistencia y Jerarquías (Acusatorio)...")
    
    paths = load_paths()
    processed_dir = paths.get('intermediate_processed')
    output_dir = paths.get('output_reports')
    os.makedirs(output_dir, exist_ok=True)
    
    check_pause()
    log_message("  1/5 Cargando datos del Paso 4...")
    input_file = os.path.join(processed_dir, 'df_casos_personas_final.parquet')
    df = safe_load(input_file)
    if df is None: return

    # Inicializar columnas
    df['EstadoInformeConsistencia'] = df['EstadoInforme']
    df['InconsistenciaTipo'] = ""
    df['InconsistenciaDetalle'] = ""

    # Separar Copiadas
    if 'ActuacionCopiada' not in df.columns: df['ActuacionCopiada'] = "NoCopiada"
        
    df_nocop = df[df['ActuacionCopiada'] == 'NoCopiada'].copy()
    df_copiadas = df[df['ActuacionCopiada'] != 'NoCopiada'].copy()
    del df; gc.collect()

    # 2. CONFLICTO SENTENCIAS
    check_pause()
    log_message("  2/5 Detectando conflictos de sentencias...")
    sentencias = ["SentenciaCondenatoriaJuicio", "SentenciaCondenatoriaAcuerdoPleno"]
    
    if 'fuente_datos_actuacion' in df_nocop.columns:
        mask_con_persona = df_nocop['fuente_datos_actuacion'] == "con_persona"
    else:
        mask_con_persona = df_nocop['IdPersona'].notna()
        
    mask_sent = mask_con_persona & (df_nocop['EstadoInforme'].isin(sentencias))
    df_sent = df_nocop[mask_sent]
    
    if not df_sent.empty:
        grupos = df_sent.groupby(['IdCasoOriginal', 'IdPersona', 'IdDelito'])['EstadoInforme'].nunique()
        conflicto = grupos[grupos > 1].index
        if not conflicto.empty:
            df_nocop.set_index(['IdCasoOriginal', 'IdPersona', 'IdDelito'], inplace=True, drop=False)
            df_nocop.loc[conflicto, 'InconsistenciaTipo'] = "JuicioYAcuerdoPleno"
            mask = df_nocop.index.isin(conflicto) & df_nocop['EstadoInforme'].isin(sentencias)
            df_nocop.loc[mask, 'EstadoInformeConsistencia'] = "SentenciaCondenatoriaConflicto"
            df_nocop.reset_index(drop=True, inplace=True)

    # 3. JERARQUÍA Y HITOS
    check_pause()
    log_message("  3/5 Calculando Hitos y Jerarquía...")
    df_final = pd.concat([df_nocop, df_copiadas], ignore_index=True)
    del df_nocop, df_copiadas; gc.collect()

    df_final['orden_jerarquia'] = df_final['EstadoInforme'].map(ORDEN_RESOLUCIONES).fillna(-1).astype(int)
    
    df_final['HitoMasAvanzado_Caso'] = df_final.groupby('IdCasoOriginal')['orden_jerarquia'].transform('max')
    df_final['HitoMasAvanzado_Persona'] = df_final.groupby('IdPersona')['orden_jerarquia'].transform('max')

    df_final['IdPersona'] = df_final['IdPersona'].fillna(-1)
    
    # 4. EXPORTAR BASE ACUSATORIO COMPLETA (DfCasosPersonasFinal_3)
    check_pause()
    log_message("  4/5 Exportando CSV Acusatorio (baseUnisaAcusatorio.csv)...")
    
    acusatorio_csv_path = os.path.join(output_dir, 'baseUnisaAcusatorio.csv')
    
    try:
        # 1. Crear ID único (Hash) para Hitos
        df_final['IdTrinomio'] = df_final['IdPersona'].astype(str) + '_' + df_final['IdCasoOriginal'].astype(str) + '_' + df_final['IdDelito'].astype(str)
        df_final['idhitoprocesal'] = df_final['IdTrinomio'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest()[:16] if pd.notna(x) else np.nan)
        
        # 2. Convertir a string para exportación (WINDOWS-1252)
        df_export = df_final.copy()
        for col in df_export.select_dtypes(include=['category']).columns:
            df_export[col] = df_export[col].astype(str)

        # 3. Guardar CSV (Formato R: ; y WINDOWS-1252)
        df_export.to_csv(acusatorio_csv_path, index=False, sep=';', encoding='windows-1252', errors='replace')
        log_message(f"    ✅ Exportado CSV Acusatorio en: {acusatorio_csv_path}")

        del df_export; gc.collect()
    except Exception as e:
        log_message(f"    ❌ ERROR guardando CSV Acusatorio: {e}")

    # 5. GUARDADO PARQUET FINAL
    check_pause()
    log_message("  5/5 Guardando Parquet Procesal Unificado...")
    df_final = optimize_memory(df_final)
    
    output_path = os.path.join(processed_dir, 'df_procesal_unificado.parquet')
    df_final.to_parquet(output_path, index=False, engine='pyarrow')
    save_metrics(df_final, output_path, [input_file])
    
    log_message(f"--- [Paso 5] FINALIZADO ---")

if __name__ == "__main__":
    setup_logging()
    try:
        run_step_5_main()
    except Exception as e:
        try:
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except:
            pass