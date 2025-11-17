"""
step_5_apply_consistency.py

(Este era tu 'step_5_build_analytical_base.py' y 'step_4_apply_consistency.py')
Aplica lógica de consistencia y calcula hitos finales sobre la base Acusatorio.
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
    "Suspensión de proceso a prueba": 5, "Sobreseimiento por suspensión del proceso a prueba": 6,
    "Suspensión acción penal por cuestión prejudicial": 5, "Sobreseimiento": 7, "Expulsión": 7,
    "SentenciaAbsolutoriaJuicio": 8, "SentenciaAbsolutoriaAcuerdoPleno": 8,
    "SentenciaCondenatoriaJuicio": 9, "SentenciaCondenatoriaAcuerdoPleno": 9,
    "criteriodeoportunidadrevisión": 3,
    "Otros Estados": -1
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

def safe_load(path, log_error=True):
    if not os.path.exists(path):
        if log_error: log_message(f"ERROR: No encontrado {path}"); 
        return None
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
    log_message("  ⚡ Optimizando memoria...")
    for col in df.select_dtypes(include=['object']).columns:
        if col in df.columns and df[col].dtype == 'object':
            num_unique = len(df[col].unique())
            num_total = len(df)
            if num_unique > 0 and num_unique / num_total < 0.5:
                df[col] = df[col].astype('category')
    return df

# --- Main ---
def run_step_5_main():
    start_time_total = time.time()
    setup_logging()
    log_message("[Paso 5] Consistencia y Jerarquías (Acusatorio)...")
    
    paths = load_paths()
    processed_dir = paths.get('intermediate_processed')
    analytical_dir = paths.get('intermediate_analytical')
    
    check_pause()
    log_message("  1/4 Cargando datos del Paso 4 (Personas) y Atlas (Paso 3)...")
    input_file = os.path.join(processed_dir, 'df_casos_personas_final.parquet')
    df_nocop = safe_load(input_file) # Este ya es solo Acusatorio y 'con_persona'
    if df_nocop is None: 
        log_message("ERROR CRITICO: Falta df_casos_personas_final.parquet (Ejecutar Paso 4 primero)")
        return
    
    input_file_atlas = os.path.join(analytical_dir, 'data_final_comparativo.parquet')
    df_atlas = safe_load(input_file_atlas)
    if df_atlas is None:
        log_message("ERROR CRITICO: Falta data_final_comparativo.parquet (Ejecutar Paso 3 primero)")
        return
        
    # --- CORRECCIÓN DEL BUG ---
    # Si df_nocop está vacío (0 filas por filtros), la lógica de 'df_resto' falla.
    if df_nocop.empty:
        log_message("ADVERTENCIA: No hay datos 'con_persona' para procesar (Paso 4 no generó filas).")
        # Filtramos el atlas para obtener 'sin_persona' y 'copiados'
        df_final = df_atlas[df_atlas['descripcion_sistemaprocesal'] == 'Acusatorio'].copy()
        # Aseguramos que las columnas existan, aunque estén vacías
        df_final['EstadoInformeConsistencia'] = df_final.get('EstadoInforme', "Otros Estados") # Usamos .get() por seguridad
        df_final['InconsistenciaTipo'] = ""
        df_final['InconsistenciaDetalle'] = ""
    
    else:
        # Extraer los 'copiados' y 'sin_persona' del Atlas (que no están en df_nocop)
        df_nocop_ids = df_nocop['IdActuacion'].unique()
        df_resto = df_atlas[
            (~df_atlas['IdActuacion'].isin(df_nocop_ids)) &
            (df_atlas['descripcion_sistemaprocesal'] == 'Acusatorio')
        ].copy()
        
        # Inicializar columnas
        df_nocop['EstadoInformeConsistencia'] = df_nocop['EstadoInforme']
        df_nocop['InconsistenciaTipo'] = ""
        df_nocop['InconsistenciaDetalle'] = ""
        
        df_resto['EstadoInformeConsistencia'] = df_resto.get('EstadoInforme', "Otros Estados")
        df_resto['InconsistenciaTipo'] = ""
        df_resto['InconsistenciaDetalle'] = ""

        log_message(f"  -> {len(df_nocop)} filas 'con_persona', {len(df_resto)} filas 'sin_persona' o 'copiadas'.")

        # 2. CONFLICTO SENTENCIAS (Solo en df_nocop)
        check_pause()
        log_message("  2/4 Detectando conflictos de sentencias...")
        sentencias = ["SentenciaCondenatoriaJuicio", "SentenciaCondenatoriaAcuerdoPleno"]
        mask_sent = df_nocop['EstadoInforme'].isin(sentencias)
        
        if mask_sent.any():
            df_sent = df_nocop[mask_sent]
            grupos = df_sent.groupby(['IdCasoOriginal', 'IdPersona', 'IdDelito'])['EstadoInforme'].nunique()
            conflicto = grupos[grupos > 1].index
            
            if not conflicto.empty:
                log_message(f"  -> Encontrados {len(conflicto)} trinomios en conflicto.")
                conflicto_set = set(conflicto.to_list())
                
                df_nocop_index = pd.MultiIndex.from_frame(df_nocop[['IdCasoOriginal', 'IdPersona', 'IdDelito']])
                mask_conflicto = df_nocop_index.isin(conflicto_set)

                df_nocop.loc[mask_conflicto, 'InconsistenciaTipo'] = "JuicioYAcuerdoPleno"
                mask_sent_conflicto = mask_conflicto & mask_sent
                df_nocop.loc[mask_sent_conflicto, 'EstadoInformeConsistencia'] = "SentenciaCondenatoriaConflicto"
            else:
                log_message("  -> No se encontraron conflictos de sentencias.")
        else:
            log_message("  -> No hay sentencias para analizar conflictos.")

        # 3. JERARQUÍA Y HITOS
        check_pause()
        log_message("  3/4 Calculando Hitos y Jerarquía...")
        df_final = pd.concat([df_nocop, df_resto], ignore_index=True)
        del df_nocop, df_resto; gc.collect()
    # --- FIN CORRECCIÓN BUG ---

    df_final['orden_jerarquia'] = df_final['EstadoInforme'].map(ORDEN_RESOLUCIONES).fillna(-1).astype(int)
    
    log_message("    -> Calculando HitoMasAvanzado_Caso...")
    df_final['HitoMasAvanzado_Caso'] = df_final.groupby('IdCasoOriginal')['orden_jerarquia'].transform('max')
    
    df_final['IdPersona'] = df_final['IdPersona'].fillna(-1)
    
    log_message("    -> Calculando HitoMasAvanzado_Persona...")
    df_final['HitoMasAvanzado_Persona'] = df_final.groupby('IdPersona')['orden_jerarquia'].transform('max')

    log_message("    -> Creando IdTrinomio...")
    df_final['IdTrinomio'] = df_final['IdPersona'].astype(str) + '_' + \
                             df_final['IdCasoOriginal'].astype(str) + '_' + \
                             df_final['IdDelito'].astype(str)
    
    log_message("    -> Agrupando por IdTrinomio para banderas de hitos...")
    hitos_series = df_final.groupby('IdTrinomio')['EstadoInforme'].apply(set)
    
    def check_hitos(estados_set):
        tiene_form = any(s in {"Formalización", "Acusación Fiscal"} or str(s).startswith("Sentencia") for s in estados_set)
        tiene_acus = any(s == "Acusación Fiscal" or str(s).startswith("Sentencia") for s in estados_set)
        tiene_sent = any(str(s).startswith("Sentencia") for s in estados_set)
        return (tiene_form, tiene_acus, tiene_sent)
        
    hitos_calc = hitos_series.apply(check_hitos)
    hitos_df = pd.DataFrame(hitos_calc.to_list(), index=hitos_series.index, columns=['tiene_formalizacion', 'tiene_acusacion', 'tiene_sentencia'])
    
    log_message("    -> Uniendo banderas de hitos al dataframe...")
    df_final = pd.merge(df_final, hitos_df, left_on='IdTrinomio', right_index=True, how='left')
    
    del hitos_series, hitos_df, hitos_calc
    gc.collect()
    
    # 4. GUARDADO
    check_pause()
    log_message("  4/4 Guardando...")
    df_final = optimize_memory(df_final)
    
    output_path = os.path.join(processed_dir, 'df_procesal_unificado.parquet')
    df_final.to_parquet(output_path, index=False, engine='pyarrow')
    save_metrics(df_final, output_path, [input_file, input_file_atlas])
    
    log_message(f"  ¡Éxito! Columnas finales: {len(df_final.columns)}")
    log_message(f"--- [Paso 5] FINALIZADO ({time.time() - start_time_total:.2f}s) ---")

if __name__ == "__main__":
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_5_main()
    except Exception as e:
        try:
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG): os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE): os.remove(PID_FILE)