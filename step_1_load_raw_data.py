# step_1_load_raw_data.py
import pandas as pd
import os
import json
import time
import sys

# --- Configuración y Constantes ---
CONFIG_FILE = 'config.json'
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "step_1.log")
RUNNING_FLAG = os.path.join(LOG_DIR, "step_1.running")
METRICS_FILE = os.path.join(LOG_DIR, "step_1_metrics.json")
PID_FILE = os.path.join(LOG_DIR, "step_1.pid")

# --- Funciones de Logging y Control ---

def setup_logging():
    """Prepara los directorios y archivos de log."""
    os.makedirs(LOG_DIR, exist_ok=True)
    # Guardamos el PID para poder detener el proceso desde la App
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    # Iniciamos el log limpio
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"[Paso 1] Iniciando Carga de Datos (PID: {os.getpid()})...\n")

def log_message(msg):
    """Escribe en consola y en el archivo de log en tiempo real."""
    print(msg)
    sys.stdout.flush() # Forzar salida en consola
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{msg}\n")
            f.flush()
            os.fsync(f.fileno()) # Forzar escritura en disco para que la App lo lea al instante
    except:
        pass

def load_config():
    """Carga la configuración de rutas."""
    if not os.path.exists(CONFIG_FILE):
        log_message(f"ERROR: No se encontró {CONFIG_FILE}")
        return None
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_metrics(last_df, output_dir, file_count):
    """Guarda métricas para el Inspector de la App."""
    # Usamos el último DF cargado para mostrar una previsualización de ejemplo
    preview = []
    cols = []
    if last_df is not None:
        preview = last_df.head(5).astype(str).to_dict(orient='records')
        cols = list(last_df.columns)

    metrics = {
        "rows": f"Total archivos: {file_count}", # En este paso, rows es menos relevante que files
        "columns": len(cols),
        "memory_mb": 0, # No relevante en carga batch
        "columns_list": cols,
        "output_file": output_dir, # Mostramos la carpeta de destino
        "input_files": [f"Origen: {file_count} archivos procesados"],
        "preview": preview,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(METRICS_FILE, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)

# --- Lógica Principal del ETL ---

def run_step_1(paths_config, log_key_dummy=None):
    """Función principal de carga."""
    
    # 1. Obtener rutas
    raw_dir = paths_config.get('raw_data')
    loaded_dir = paths_config.get('intermediate_loaded')
    
    if not raw_dir or not loaded_dir:
        log_message("ERROR: Rutas 'raw_data' o 'intermediate_loaded' no definidas.")
        return False
        
    # Crear carpeta de destino si no existe
    os.makedirs(loaded_dir, exist_ok=True)
    log_message(f"📂 Buscando archivos en: {raw_dir}")
    
    # 2. Listar archivos
    try:
        all_files = os.listdir(raw_dir)
        files_to_process = [f for f in all_files if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
    except Exception as e:
        log_message(f"ERROR leyendo directorio: {e}")
        return False
    
    if not files_to_process:
        log_message("⚠️ No se encontraron archivos .csv o .xlsx.")
        return True
    
    total_files = len(files_to_process)
    log_message(f"🚀 Se encontraron {total_files} archivos para procesar.")
    
    success_count = 0
    last_processed_df = None
    start_time_total = time.time()

    # 3. Bucle de Procesamiento
    for i, filename in enumerate(files_to_process):
        file_path = os.path.join(raw_dir, filename)
        # Cambio de extensión a .parquet
        output_filename = os.path.splitext(filename)[0] + '.parquet'
        output_path = os.path.join(loaded_dir, output_filename)
        
        log_message(f"  ({i+1}/{total_files}) Leyendo: {filename}...")
        start_t = time.time()
        
        try:
            df = None
            # Carga CSV (Configuración compatible con R: Latin-1 y punto y coma)
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(
                    file_path, 
                    sep=';', 
                    encoding='latin-1', 
                    low_memory=False,
                    on_bad_lines='skip' # Evita crash por líneas malformadas
                )
            
            # Carga Excel
            elif filename.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            
            # Guardado a Parquet
            if df is not None:
                # Normalizar nombres de columnas (opcional pero recomendado: minúsculas y sin espacios)
                # df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
                
                df.to_parquet(output_path, index=False)
                elapsed = time.time() - start_t
                log_message(f"    -> OK. Guardado como Parquet ({len(df)} filas) [{elapsed:.2f}s]")
                
                success_count += 1
                last_processed_df = df # Guardamos referencia para la preview
                
        except Exception as e:
            log_message(f"    ❌ ERROR procesando archivo: {e}")
            # No detenemos el loop, intentamos con el siguiente
            continue

    # 4. Finalización
    total_time = time.time() - start_time_total
    log_message("-" * 30)
    log_message(f"✅ PROCESO COMPLETADO")
    log_message(f"   Archivos exitosos: {success_count} / {total_files}")
    log_message(f"   Tiempo total: {total_time:.2f}s")
    
    # Generar métricas para la UI
    save_metrics(last_processed_df, loaded_dir, success_count)
    
    return True

# --- Bloque de Ejecución (Punto de Entrada) ---

if __name__ == "__main__":
    # 1. Configurar logs
    setup_logging()
    
    # 2. Crear bandera de "Corriendo"
    with open(RUNNING_FLAG, 'w') as f:
        f.write("running")
        
    try:
        # 3. Cargar config y ejecutar
        config = load_config()
        if config:
            run_step_1(config.get('paths', {}))
        else:
            log_message("ERROR CRITICO: No se pudo cargar config.json")
            
    except Exception as e:
        # Captura de error global
        log_message(f"ERROR FATAL EN MAIN: {e}")
        
    finally:
        # 4. Limpieza de banderas (siempre se ejecuta)
        if os.path.exists(RUNNING_FLAG):
            os.remove(RUNNING_FLAG)
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)