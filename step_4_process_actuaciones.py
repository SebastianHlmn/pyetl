"""
step_4_process_actuaciones.py

(Este era tu 'step_3_process_actuaciones.py')
Filtra la base Atlas por Acusatorio, aplica filtros de fecha,
cruza con personas (INNER JOIN) y calcula el 'EstadoInforme'.
"""
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

# --- Constantes ---
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "step_4.log")
PID_FILE = os.path.join(LOG_DIR, "step_4.pid")
PAUSE_FILE = os.path.join(LOG_DIR, "step_4.pause")
RUNNING_FLAG = os.path.join(LOG_DIR, "step_4.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_4_metrics.json")
CONFIG_FILE = 'config.json'

# --- Mapeo de Estados (COPIADO DE TU SCRIPT) ---
ESTADOS_MAP = {
    "SentenciaCondenatoriaJuicio": ["Sentencia Condenatoria -juicio oral- (art. 305, CPPF)", "Sentencia Condenatoria Firme -juicio oral- (art. 305, CPPF)", "Sentencia Condenatoria (art. 305, CPPF)", "Sentencia Condenatoria Firme (art. 305, CPPF)"],
    "SentenciaCondenatoriaAcuerdoPleno": ["Sentencia Condenatoria -acuerdo pleno- (art. 325, CPPF)", "Sentencia Condenatoria Firme -acuerdo pleno- (art. 325, CPPF)", "Sentencia Condenatoria (art. 325, CPPF)", "Sentencia Condenatoria Firme (art. 325, CPPF)", "Acuerdo Pleno (Art. 323, CPPF)"],
    "SentenciaAbsolutoriaJuicio": ["Sentencia Absolutoria -juicio oral- (art. 305, CPPF)", "Sentencia Absolutoria Firme -juicio oral- (art. 305, CPPF)", "Sentencia Absolutoria (art. 305, CPPF)", "Sentencia Absolutoria Firme (art. 305, CPPF)"],
    "SentenciaAbsolutoriaAcuerdoPleno": ["Sentencia Absolutoria -acuerdo pleno- (art. 325, CPPF)", "Sentencia Absolutoria Firme -acuerdo pleno- (art. 325, CPPF)", "Sentencia Absolutoria (art. 325, CPPF)", "Sentencia Absolutoria Firme (art. 325, CPPF)"],
    "Formalización": ["Audiencia de Formalización de la investigación preparatoria (Art. 258, CPPF)"],
    "Acusación Fiscal": ["Acusación Fiscal (Art. 274, CPPF)", "Cierre de la investigación preparatoria por Acusación Fiscal (Arts. 268 y 274, CPPF)", "Audiencia de control de la Acusación (Art. 279, CPPF)", "Cierre de la IPP por Acusación Fiscal junto a Querella (Art. 268, CPPF)", "Remisión de Acusación Fiscal a Oficina Judicial (art. 276, últ., párr., CPPF) "],
    "Archivo - Aplicación": ["Archivo (Art. 250, CPPF)", "Archivo en caso con autores ignorados (Art. 250, CPPF)"],
    "Archivo - Firme": ["Fiscal revisor confirma decisión de archivo (art. 251, 3ero. Párr., CPPF)", "Confirmación de archivo (art. 251, 3ero. Párr., CPPF)", "Vencimiento de plazo de la víctima para revisión de archivo (Art 252, 2do párr., CPPF)"],
    "Desestimación - Aplicación": ["Desestimación por inexistencia de delito (Art. 249, CPPF)"],
    "Desestimación - Firme": ["Fiscal revisor confirma decisión de desestimación (art. 251, 3ero. Párr., CPPF)", "Confirmación de desestimación  (art. 251, 3ero. Párr., CPPF)", "Vencimiento de plazo de la víctima para revisión de desestimación (Art 252, 2do párr., CPPF)"],
    "Criterio Oportunidad - Aplicación": ["Aplicación de Criterio de Oportunidad por insignificancia (art. 31, inc. a, CPPF)", "Aplicación de Criterio de Oportunidad por insignificancia (art. 31, inc a, CPPF)", "Aplicación de Criterio de Oportunidad por insignificancia con autores ignorados (art. 31, inc. a)", "Aplicación de Criterio de Oportunidad por menor relevancia (art. 31, inc. b, CPPF)", "Aplicación de Criterio de Oportunidad por pena natural (art. 31, inc. c, CPPF)", "Aplicación de Criterio de Oportunidad por pena que carece de importancia (art. 31, inc. d, CPPF)"],
    "Criterio Oportunidad - Firme": ["Confirmación de aplicación de criterio de oportunidad (art. 251, 3ero. Párr., CPPF)", "Fiscal revisor confirma criterio de oportunidad por insignificancia (art. 251, 3er párr, CPPF)", "Fiscal revisor confirma criterio de oportunidad por menor relevancia (art. 251, 3er párr., CPPF)", "Fiscal revisor confirma criterio de oportunidad por pena natural (art. 251, 3er párr., CPPF)", "Fiscal revisor confirma Criterio de Oportunidad por pena que carece importancia (art. 251, 3er párr., CPPF)", "Vencimiento de plazo de la víctima para revisión criterio de oportunidad (Art 252, 2do párr., CPPF)"],
    "Criterio Oportunidad- Rechazado por el Fiscal Revisor": ["Decisión que rechaza revisión de víctima por aplicación de crit. de oport. (252, 4to. párr., CPPF)"],
    "Sobreseimiento": ["Resolución de Sobreseimiento por extinción de la acción penal (Arts. 269, inc. F, y 273, CPPF)", "Resolución de Sobreseimiento por falta de pruebas para juicio (Arts. 269, inc. E, y 273, CPPF)", "Resolución de Sobreseimiento por no ser el autor o partícipe (Arts. 269, inc. C y 273, CPPF)", "Resolución que hace lugar a sobreseimiento (Arts. 272 y 279 inc. c., CPPF)", "Resolución de Sobreseimiento por Hecho Atípico (art. 336, inc. 3, CPPN)", "Sobreseimiento por agotamiento de la investigación (Arts. 269, inc. E, y 273, CPPF)", "Sobreseimiento por extinción de la acción penal (Arts. 269, inc. F, y 273, CPPF)", "Sobreseimiento por hecho no cometido (Arts. 269, inc. A, y 273, CPPF)", "Sobreseimiento por justificación, inculpabilidad o ausencia punibilidad (Arts. 269, inc. D, y 273, CPPF)", "Sobreseimiento por no adecuarse a figura legal (Arts. 269, inc. B, y 273, CPPF)", "Sobreseimiento por no tomar parte en el hecho (Arts. 269, inc. C y 273, CPPF)"],
    "Sobreseimiento por Criterio de oportunidad": ["Sobreseimiento por aplicación de criterio de oportunidad (Arts. 269, inc. G, y 273, CPPF)"],
    "Conciliación": ["Resolución que homologa conciliación (Arts. 34 y 279 inc. d, CPPF)", "Resolución que homologa conciliación (art. 34, CPPF)"],
    "Sobreseimiento por Conciliación": ["Resolución de sobreseimiento por acuerdo conciliatorio", "Sobreseimiento por acuerdo conciliatorio (Arts. 269, inc. G, y 273, CPPF)"],
    "Suspensión de proceso a prueba": ["Acuerdo de suspensión del proceso a prueba (art. 35, 4to. párr, CPPF)", "Suspensión del proceso a prueba en plazo de cumplimiento"],
    "Sobreseimiento por suspensión proceso a prueba": ["Sobreseimiento por suspensión del proceso a prueba (Arts. 269, inc. G, y 273, CPPF)"],
    "Reparación Integral": ["Resolución que hace lugar a reparación integral", "Resolución de Homologación de Reparación"],
    "Sobreseimiento por Reparación": ["Sobreseimiento por acuerdo reparatorio (Arts. 269, inc. G, y 273, CPPF)"],
    "Incompetencia": ["Resolución de incompetencia (Art. 48)", "Resolución de incompetencia con autores ignorados (Art. 48)", "Derivado a justicia provincial/local"],
    "Derivado_a_organismo_externo": ["Derivado a organismo externo"],
    "Cese de intervención del MPF": ["Cese de intervención del MPF"],
    "Expulsión": ["Resolución que hace lugar a expulsión (art. 35, CPPF)"],
    "Rebeldía": ["Resolución que declara rebeldía (Art. 69, 2do párr., CPPF)"],
    "Con detención": ["Acta de detención (art. 215, CPPF)"]
}

# --- Funciones de Soporte ---
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FILE, 'w') as f: f.write(str(os.getpid()))
    with open(LOG_FILE, 'w', encoding='utf-8') as f: f.write(f"[{datetime.now().strftime('%H:%M:%S')}] [Paso 4] Iniciando...\n")

def log_message(msg):
    print(msg)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"); f.flush(); os.fsync(f.fileno())
    except: pass

def log_memory_usage():
    mem = psutil.virtual_memory().percent
    log_message(f"  [MEM] Sistema: {mem}%")

def check_pause():
    if os.path.exists(PAUSE_FILE):
        log_message("[PAUSA] Esperando...")
        while os.path.exists(PAUSE_FILE): time.sleep(1)
        log_message("[RESUME] Continuando...")

def load_config():
    if not os.path.exists(CONFIG_FILE): return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f: return json.load(f)

def safe_load(path, log_error=True):
    if not os.path.exists(path):
        if log_error: log_message(f"ERROR: No encontrado {path}")
        return None
    try: return pd.read_parquet(path)
    except Exception as e:
        log_message(f"ERROR leyendo {path}: {e}")
        return None

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

# --- LOGICA PRINCIPAL ---
def run_step_4_main():
    start_time_total = time.time()
    setup_logging()
    log_message("[Paso 4] Procesando Actuaciones (SOLO ACUSATORIO)...")
    log_memory_usage()
    
    config = load_config()
    paths = config.get('paths', {})
    filters = config.get('filters', {})
    processed_dir = paths.get('intermediate_processed')
    analytical_dir = paths.get('intermediate_analytical')
    loaded_dir = paths.get('intermediate_loaded')
    
    input_files_used = []

    # 1. Cargar Atlas (Paso 3) y Filtros
    check_pause()
    log_message("  1/5 Cargando Atlas (Paso 3) y Aplicando Filtros...")
    
    df_atlas = safe_load(os.path.join(analytical_dir, 'data_final_comparativo.parquet'))
    if df_atlas is None:
        log_message("ERROR CRITICO: Falta data_final_comparativo.parquet (Ejecutar Paso 3 primero)")
        return
    input_files_used.append("data_final_comparativo.parquet")

    # --- FILTRO ACUSATORIO ---
    initial_rows = len(df_atlas)
    df_casos = df_atlas[df_atlas['descripcion_sistemaprocesal'] == 'Acusatorio'].copy()
    log_message(f"  -> Filtro Acusatorio: {initial_rows:,} -> {len(df_casos):,} filas")
    
    # --- FILTRO FECHAS ---
    if 'date_start' in filters and 'date_end' in filters:
        d_start = pd.to_datetime(filters['date_start'])
        d_end = pd.to_datetime(filters['date_end'])
        df_casos['FechaIngreso'] = pd.to_datetime(df_casos['FechaIngreso'], errors='coerce')
        df_casos = df_casos[(df_casos['FechaIngreso'] >= d_start) & (df_casos['FechaIngreso'] <= d_end)]
        log_message(f"  -> Filtro Fechas ({d_start.date()} - {d_end.date()}): {len(df_casos):,} filas restantes")

    if df_casos.empty:
        log_message("⚠️ ADVERTENCIA: El dataset quedó vacío tras los filtros. Creando archivo vacío.")
        # --- CORRECCIÓN DEL BUG ---
        # Creamos un DF vacío pero con las columnas que el Paso 5 espera, para evitar el KeyError
        columnas_esperadas = list(df_atlas.columns) + ['IdPersona', 'IdDelito', 'fuente_datos_actuacion', 'EstadoInforme']
        # Nos aseguramos de que no haya columnas duplicadas si ya existían
        columnas_finales_unicas = list(dict.fromkeys(columnas_esperadas)) 
        df_final = pd.DataFrame(columns=columnas_finales_unicas)
        del df_atlas
        # --- FIN CORRECCIÓN ---
    else:
        del df_atlas; gc.collect()

        # 2. Cargar Personas (para el INNER JOIN)
        check_pause()
        log_message("  2/5 Cargando Personas...")
        df_personas = safe_load(os.path.join(loaded_dir, 'df_persona_actuacion_delito.parquet'), log_error=False)
        if df_personas is None:
            log_message("ERROR CRITICO: Sin archivo de personas.")
            return
        input_files_used.append("df_persona_actuacion_delito.parquet")
        
        # 3. Join (INNER JOIN de R)
        check_pause()
        log_message("  3/5 Cruzando Casos Acusatorio con Personas (INNER)...")
        
        df_final = pd.merge(
            df_casos, 
            df_personas, 
            on='IdActuacion', 
            how='inner',
            suffixes=('', '_per')
        )
        
        # Limpiar columnas duplicadas del merge
        cols_to_drop = [c for c in df_final.columns if c.endswith('_per')]
        df_final.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        if 'IdPersona_x' in df_final.columns:
            df_final['IdPersona'] = df_final['IdPersona_y'].fillna(df_final['IdPersona_x'])
            df_final.drop(columns=['IdPersona_x', 'IdPersona_y'], inplace=True, errors='ignore')
        
        if 'IdDelito_x' in df_final.columns:
            df_final['IdDelito'] = df_final['IdDelito_y'].fillna(df_final['IdDelito_x'])
            df_final.drop(columns=['IdDelito_x', 'IdDelito_y'], inplace=True, errors='ignore')

        df_final['fuente_datos_actuacion'] = "con_persona"
        
        del df_personas, df_casos
        gc.collect()
        log_message(f"  -> Dataset filtrado: {len(df_final):,} filas")
        log_memory_usage()

        # 4. Clasificación (EstadoInforme)
        check_pause()
        log_message("  4/5 Aplicando Clasificación de Estados...")
        
        # Invertir el mapeo para búsqueda rápida
        desc_to_estado_map = {}
        for estado, descripciones in ESTADOS_MAP.items():
            for desc in descripciones:
                desc_to_estado_map[desc] = estado
        
        df_final['descripcionactuacion'] = df_final['descripcionactuacion'].astype(str)
        df_final['EstadoInforme'] = df_final['descripcionactuacion'].map(desc_to_estado_map).fillna("Otros Estados")
        
        mask_rechazo = df_final['descripcionactuacion'] == "Decisión que rechaza revisión de víctima por aplicación de crit. de oport. (252, 4to. párr., CPPF)"
        
        if mask_rechazo.any():
            df_final.loc[mask_rechazo, 'EstadoInforme'] = "Criterio Oportunidad- Rechazado por el Fiscal Revisor"

    # 5. Guardar
    check_pause()
    log_message("  5/5 Guardando...")
    df_final = optimize_memory(df_final)
    
    output_path = os.path.join(processed_dir, 'df_casos_personas_final.parquet')
    df_final.to_parquet(output_path, index=False, engine='pyarrow')
    save_metrics(df_final, output_path, input_files_used)
    
    log_message(f"  ¡Éxito! Guardado en {output_path}")
    log_message(f"--- [Paso 4] FINALIZADO ({time.time() - start_time_total:.2f}s) ---")

if __name__ == "__main__":
    with open(RUNNING_FLAG, 'w') as f: f.write("running")
    try:
        run_step_4_main()
    except Exception as e:
        try: 
            with open(LOG_FILE, 'a') as f: f.write(f"\n[CRASH] {e}\n{traceback.format_exc()}\n")
        except: pass
        print(f"[CRASH] {e}")
    finally:
        if os.path.exists(RUNNING_FLAG): os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE): os.remove(PID_FILE)