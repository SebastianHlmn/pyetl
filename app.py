"""
app.py

Orquestador principal de Streamlit.
Lanza y monitorea los scripts de pasos (step_*.py) como subprocesos.
"""
import streamlit as st
import json
import os
import sys
import time
import psutil 
import subprocess
import pandas as pd
import io 
from datetime import date, datetime

# --- Configuración Inicial ---
sys.path.append(os.getcwd()) 

CONFIG_FILE = 'config.json'
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# --- DEFINICIÓN DE PASOS (Sincronizada con los nombres de archivo) ---
STEP_CTRLS = {
    1: { 
        "log": os.path.join(LOG_DIR, "step_1.log"), "pid": os.path.join(LOG_DIR, "step_1.pid"),
        "pause": os.path.join(LOG_DIR, "step_1.pause"), "running": os.path.join(LOG_DIR, "step_1.running"),
        "metrics": os.path.join(LOG_DIR, "step_1_metrics.json"), "script": "step_1_load_raw_data.py",
        "title": "Carga de Datos Crudos", "desc": "Carga CSV/Excel desde 'raw_data' a Parquet en 'loaded'."
    },
    2: {
        "log": os.path.join(LOG_DIR, "step_2.log"), "pid": os.path.join(LOG_DIR, "step_2.pid"),
        "pause": os.path.join(LOG_DIR, "step_2.pause"), "running": os.path.join(LOG_DIR, "step_2.running"),
        "metrics": os.path.join(LOG_DIR, "step_2_metrics.json"), "script": "step_2_process_casos.py",
        "title": "Procesar Casos (Mixto+Acus.)", "desc": "Unifica, limpia y filtra Casos/Actuaciones. Crea 'data_casos_processed'."
    },
    3: {
        "log": os.path.join(LOG_DIR, "step_3.log"), "pid": os.path.join(LOG_DIR, "step_3.pid"),
        "pause": os.path.join(LOG_DIR, "step_3.pause"), "running": os.path.join(LOG_DIR, "step_3.running"),
        "metrics": os.path.join(LOG_DIR, "step_3_metrics.json"), "script": "step_3_build_atlas.py",
        "title": "Construir Atlas (Base General)", "desc": "Enriquece con delitos, víctimas, etc. Crea 'data_final_comparativo'."
    },
    4. {
        "log": os.path.join(LOG_DIR, "step_4.log"), "pid": os.path.join(LOG_DIR, "step_4.pid"),
        "pause": os.path.join(LOG_DIR, "step_4.pause"), "running": os.path.join(LOG_DIR, "step_4.running"),
        "metrics": os.path.join(LOG_DIR, "step_4_metrics.json"), "script": "step_4_process_actuaciones.py",
        "title": "Procesar Actuaciones (Acusatorio)", "desc": "Filtra Acusatorio y aplica mapeo de 'EstadoInforme'. Crea 'df_casos_personas_final'."
    },
    5: {
        "log": os.path.join(LOG_DIR, "step_5.log"), "pid": os.path.join(LOG_DIR, "step_5.pid"),
        "pause": os.path.join(LOG_DIR, "step_5.pause"), "running": os.path.join(LOG_DIR, "step_5.running"),
        "metrics": os.path.join(LOG_DIR, "step_5_metrics.json"), "script": "step_5_apply_consistency.py",
        "title": "Consistencia y Hitos (Acusatorio)", "desc": "Calcula conflictos de sentencias e hitos. Crea 'df_procesal_unificado'."
    },
    6: {
        "log": os.path.join(LOG_DIR, "step_6.log"), "pid": os.path.join(LOG_DIR, "step_6.pid"),
        "pause": os.path.join(LOG_DIR, "step_6.pause"), "running": os.path.join(LOG_DIR, "step_6.running"),
        "metrics": os.path.join(LOG_DIR, "step_6_metrics.json"), "script": "step_6_export_reports.py",
        "title": "Exportar Reportes", "desc": "Genera los CSV y Excel finales desde el Atlas."
    }
}

# --- Funciones de Utilidad ---
def load_config(config_filename=CONFIG_FILE):
    if os.path.exists(config_filename):
        with open(config_filename, 'r', encoding='utf-8') as f: return json.load(f)
    # Si no existe, creamos uno por defecto
    default_config = {
        "paths": {
            "raw_data": "C:\\__DATASETS_COIRON_PERIODICOS",
            "intermediate_loaded": "C:\\__DATASETS_COIRON\\1_loaded",
            "intermediate_processed": "C:\\__DATASETS_COIRON\\2_processed",
            "intermediate_analytical": "C:\\__DATASETS_COIRON\\3_analytical",
            "output_reports": "C:\\__DATASETS_COIRON\\4_output"
        },
        "filters": {
            "date_start": "2018-01-01T00:00:00",
            "date_end": "2025-12-31T00:00:00"
        }
    }
    save_config(default_config, config_filename)
    return default_config


def save_config(config_data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f: json.dump(config_data, f, indent=2)
        return True
    except: return False

def get_available_sessions():
    return [f for f in os.listdir('.') if f.endswith('.json') and 'step_' not in f and f != 'config.json']

def check_status(paths):
    status = {}
    
    # Definimos los archivos de "salida" clave para cada paso
    outputs = {
        1: os.path.join(paths.get('intermediate_loaded',''), 'CasosActuacionesAcusatorio.parquet'),
        2: os.path.join(paths.get('intermediate_processed',''), 'data_casos_processed.parquet'),
        3: os.path.join(paths.get('intermediate_analytical',''), 'data_final_comparativo.parquet'),
        4: os.path.join(paths.get('intermediate_processed',''), 'df_casos_personas_final.parquet'),
        5: os.path.join(paths.get('intermediate_processed',''), 'df_procesal_unificado.parquet'),
        6: os.path.join(paths.get('output_reports',''), 'baseUnisaMixtoAcusatorio.csv')
    }
    
    for i in range(1, 7):
        status[f'step_{i}_done'] = os.path.exists(outputs.get(i, ''))
        status[f'step_{i}_running'] = os.path.exists(STEP_CTRLS[i]['running'])
        
    return status

def read_log_tail(file_path, n=30):
    if not os.path.exists(file_path): return "Esperando log..."
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            return "".join(lines[-n:])
    except: return "Leyendo..."

def stop_process(pid_file, running_flag):
    try:
        if os.path.exists(pid_file):
            with open(pid_file, 'r') as f: pid = int(f.read().strip())
            p = psutil.Process(pid)
            p.terminate(); time.sleep(1)
            if p.is_running(): p.kill()
    except Exception as e:
        print(f"Error al detener {pid_file}: {e}")
    if os.path.exists(pid_file): os.remove(pid_file)
    if os.path.exists(running_flag): os.remove(running_flag)

def toggle_pause(pause_file):
    if os.path.exists(pause_file): os.remove(pause_file)
    else:
        with open(pause_file, 'w') as f: f.write("pause")

def check_log_for_errors(log_file):
    if os.path.exists(log_file):
        tail = read_log_tail(log_file, n=10)
        if "ERROR" in tail or "CRASH" in tail or "Traceback" in tail: return tail
    return None

def get_completion_timestamp(metrics_file):
    if not os.path.exists(metrics_file): return ""
    try:
        with open(metrics_file, 'r') as f: m = json.load(f)
        return m.get('timestamp', "")
    except: return ""

def render_inspector(step_name, metrics_file):
    st.markdown(f"### 🧐 Inspector: {step_name}")
    if not os.path.exists(metrics_file):
        st.info("No hay resultados de métricas disponibles.")
        return
    try:
        with open(metrics_file, 'r') as f: m = json.load(f)
        c1, c2, c3 = st.columns(3)
        c1.metric("Filas", f"{m.get('rows',0):,}")
        c2.metric("Columnas", m.get('columns',0))
        c3.metric("RAM (MB)", f"{m.get('memory_mb',0)}")
        st.caption(f"Generado: {m.get('timestamp', 'N/A')}")
        
        if 'created_files' in m:
            with st.expander("📂 Archivos Generados", expanded=True):
                for f in m['created_files']: st.success(f"📄 {f}")
        
        with st.expander("🛠️ Inputs Técnicos", expanded=False):
            st.markdown(f"**Archivo de Salida:** `{m.get('output_file')}`")
            st.markdown("**Archivos de Entrada:**")
            for f in m.get('input_files', []): st.markdown(f"- `{f}`")

        st.subheader("Previsualización")
        st.dataframe(pd.DataFrame(m.get('preview', [])), use_container_width=True) # Mantenemos por compatibilidad
        
        st.divider()
        st.subheader("📥 Descargar")
        output_file = m.get('output_file')
        if output_file and os.path.exists(output_file):
            if output_file.endswith(('.xlsx', '.csv')):
                with open(output_file, "rb") as f:
                    st.download_button("⬇️ Descargar Archivo", f, os.path.basename(output_file))
            else:
                fmt = st.radio("Formato:", ["Parquet", "CSV", "Excel"], horizontal=True, key=f"fmt_{step_name}")
                if fmt == "Parquet":
                    with open(output_file, "rb") as f:
                        st.download_button("⬇️ Descargar Parquet", f, os.path.basename(output_file))
                elif fmt == "CSV":
                    if st.button("Generar CSV", key=f"c_{step_name}"):
                        with st.spinner("Convirtiendo..."):
                            df = pd.read_parquet(output_file)
                            st.download_button("⬇️ Bajar CSV", df.to_csv(index=False).encode('utf-8'), "data.csv", "text/csv")
                elif fmt == "Excel":
                    if m.get('rows', 0) > 1000000: st.error("Muy grande para Excel.")
                    elif st.button("Generar Excel", key=f"x_{step_name}"):
                        with st.spinner("Generando..."):
                            buf = io.BytesIO()
                            pd.read_parquet(output_file).to_excel(buf, index=False)
                            st.download_button("⬇️ Bajar Excel", buf.getvalue(), "data.xlsx")
        else: st.warning(f"Archivo de salida no encontrado: {output_file}")
    except Exception as e: st.error(f"Error inspector: {e}")

# --- INTERFAZ ---
st.set_page_config(page_title="ETL UNISA", layout="wide", initial_sidebar_state="expanded")

# Carga de la configuración de la sesión
if 'current_session_file' not in st.session_state: 
    st.session_state.current_session_file = 'config_default.json'
if 'config' not in st.session_state: 
    st.session_state.config = load_config(st.session_state.current_session_file)

# Inicialización de estados de la UI
if 'inspector_step' not in st.session_state: st.session_state.inspector_step = None
if 'pipeline_active' not in st.session_state: st.session_state.pipeline_active = False
if 'pipeline_queue' not in st.session_state: st.session_state.pipeline_queue = []
if 'current_pipeline_step' not in st.session_state: st.session_state.current_pipeline_step = None

# Sincronizar config.json si no existe
if not os.path.exists(CONFIG_FILE):
    save_config(st.session_state.config, CONFIG_FILE)

status = check_status(st.session_state.config.get('paths', {}))

# Sincronizar el inspector si un paso se está ejecutando
for i in range(1, 7):
    if status.get(f'step_{i}_running'): 
        st.session_state.inspector_step = i

# --- ORQUESTADOR ---
if st.session_state.pipeline_active:
    if st.session_state.pipeline_queue:
        next_step = st.session_state.pipeline_queue[0]
        
        if status.get(f'step_{next_step}_running'):
            pass 
        else:
            log_file = STEP_CTRLS[next_step]['log']
            err = check_log_for_errors(log_file)
            
            if err and os.path.exists(log_file): 
                st.session_state.pipeline_active = False
                st.session_state.pipeline_queue = []
                st.error(f"❌ Pipeline detenido. Error en Paso {next_step}.")
            
            elif status.get(f'step_{next_step}_done') and st.session_state.current_pipeline_step == next_step:
                st.session_state.pipeline_queue.pop(0)
                st.session_state.current_pipeline_step = None
                st.toast(f"✅ Paso {next_step} Finalizado.")
                if not st.session_state.pipeline_queue:
                    st.session_state.pipeline_active = False
                    st.success("🎉 ¡Pipeline Completo!")
                    st.balloons()
                else:
                    st.rerun() 
            
            elif st.session_state.current_pipeline_step != next_step:
                script = STEP_CTRLS[next_step]['script']
                if not os.path.exists(script):
                    st.error(f"❌ ERROR: El script '{script}' no existe. Deteniendo pipeline.")
                    st.session_state.pipeline_active = False
                    st.session_state.pipeline_queue = []
                else:
                    if os.path.exists(log_file): os.remove(log_file)
                    with open(STEP_CTRLS[next_step]['running'], 'w') as f: f.write('running')
                    subprocess.Popen([sys.executable, script])
                    st.session_state.current_pipeline_step = next_step
                    st.toast(f"🚀 Iniciando Paso {next_step}...")
                    time.sleep(1) 
                    st.rerun()
    
    time.sleep(2) 
    st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.header("🎛️ Control")
    mem = psutil.virtual_memory().percent
    st.metric("RAM", f"{mem}%", delta_color="inverse" if mem > 85 else "normal")
    st.progress(mem/100)
    st.divider()
    
    st.subheader("🚀 Ejecución por Lotes")
    steps_to_run = st.multiselect("Pasos a ejecutar:", [1, 2, 3, 4, 5, 6], default=[1, 2, 3, 4, 5, 6])
    
    if st.session_state.pipeline_active:
        if st.button("🛑 DETENER PIPELINE", type="primary"):
            st.session_state.pipeline_active = False
            st.session_state.pipeline_queue = []
            if st.session_state.current_pipeline_step:
                ctrl = STEP_CTRLS[st.session_state.current_pipeline_step]
                stop_process(ctrl['pid'], ctrl['running'])
            st.warning("Pipeline detenido.")
            st.rerun()
    else:
        if st.button("▶️ Iniciar Pipeline"):
            st.session_state.pipeline_queue = sorted(steps_to_run)
            st.session_state.pipeline_active = True
            st.rerun()

    st.divider()
    
    st.subheader("📅 Filtros")
    with st.form("filters"):
        curr = st.session_state.config.get('filters', {})
        ds = st.date_input("Desde", datetime.fromisoformat(curr.get("date_start", "2018-01-01T00:00:00")))
        de = st.date_input("Hasta", datetime.fromisoformat(curr.get("date_end", "2025-12-31T00:00:00")))
        if st.form_submit_button("Aplicar"):
            st.session_state.config['filters'] = {"date_start": ds.isoformat(), "date_end": de.isoformat()}
            save_config(st.session_state.config, st.session_state.current_session_file)
            save_config(st.session_state.config, CONFIG_FILE) # Guardar también en config.json
            st.rerun()
    
    st.divider()
    monitor_mode = st.toggle("Modo Inspector", value=True)
    
    st.subheader("💾 Sesiones de Configuración")
    av = get_available_sessions()
    if av:
        sel = st.selectbox("Cargar sesión", av, index=0, key="session_select")
        if st.button("Cargar"):
            st.session_state.config = load_config(sel)
            st.session_state.current_session_file = sel
            save_config(st.session_state.config, CONFIG_FILE)
            st.rerun()
    
    nn = st.text_input("Guardar sesión como", "config_proyecto.json")
    if st.button("Guardar"):
        save_config(st.session_state.config, nn if nn.endswith('.json') else nn+".json")
        st.success("Guardado")
        
    with st.expander("✏️ Editar Rutas (config.json)"):
        with st.form("edit_paths"):
            paths = st.session_state.config.get('paths', {})
            p_raw = st.text_input("Raw Data (CSV, Excel)", paths.get('raw_data', 'C:/__DATASETS_COIRON_PERIODICOS'))
            p_loaded = st.text_input("Paso 1: Loaded (Parquet)", paths.get('intermediate_loaded', 'C:/__DATASETS_COIRON/1_loaded'))
            p_processed = st.text_input("Pasos 2, 4, 5: Processed (Parquet)", paths.get('intermediate_processed', 'C:/__DATASETS_COIRON/2_processed'))
            p_analytical = st.text_input("Paso 3: Analytical (Parquet)", paths.get('intermediate_analytical', 'C:/__DATASETS_COIRON/3_analytical'))
            p_output = st.text_input("Paso 6: Output (CSV, Excel)", paths.get('output_reports', 'C:/__DATASETS_COIRON/4_output'))
            if st.form_submit_button("Actualizar"):
                new_paths = {
                    "raw_data": p_raw, 
                    "intermediate_loaded": p_loaded, 
                    "intermediate_processed": p_processed, 
                    "intermediate_analytical": p_analytical, 
                    "output_reports": p_output
                }
                st.session_state.config['paths'] = new_paths
                save_config(st.session_state.config, st.session_state.current_session_file)
                save_config(st.session_state.config, CONFIG_FILE)
                st.rerun()

# --- MAIN ---
st.title(f"Pipeline de Datos UNISA")
st.caption(f"Sesión: **{st.session_state.current_session_file}**")

if st.session_state.pipeline_active:
    st.success(f"🏃‍♂️ **Pipeline en ejecución...** (Paso {st.session_state.current_pipeline_step})")

col_pipe, col_insp = st.columns([0.6, 0.4])

with col_pipe:
    
    # --- FUNCION GENERADORA DE TARJETAS ---
    def render_step_card(step_num, prev_done):
        ctrl = STEP_CTRLS[step_num]
        title = ctrl['title']
        desc = ctrl['desc']
        script_name = ctrl['script']
        
        k_run = f'step_{step_num}_running'
        k_done = f'step_{step_num}_done'
        
        with st.expander(f"**PASO {step_num}: {title}**", expanded=not status[k_done] or status[k_run]):
            st.caption(desc)
            err = check_log_for_errors(ctrl['log'])

            if status[k_done] or status[k_run] or err:
                if st.button(f"🔍 Detalles P{step_num}", key=f"i{step_num}"): 
                    st.session_state.inspector_step = step_num; st.rerun()

            if status[k_run]:
                st.info("⚙️ Procesando...")
                c1, c2 = st.columns(2)
                if c1.button("⏯️ Pausa", key=f"p{step_num}"): toggle_pause(ctrl['pause'])
                if c2.button("⏹️ STOP", key=f"s{step_num}"): stop_process(ctrl['pid'], ctrl['running']); st.rerun()
            
            elif status[k_done]:
                ts = get_completion_timestamp(ctrl['metrics'])
                st.success(f"✅ Completado {ts}")
                if st.button(f"Re-ejecutar P{step_num}", key=f"r{step_num}"): 
                    if os.path.exists(ctrl['log']): os.remove(ctrl['log'])
                    with open(ctrl['running'], 'w') as f: f.write('running')
                    subprocess.Popen([sys.executable, script_name]); st.rerun()
            
            elif err:
                st.error(f"❌ Error en último intento. (Ver log)")
                if st.button(f"Re-intentar P{step_num}", disabled=not prev_done, key=f"r{step_num}"): 
                    if os.path.exists(ctrl['log']): os.remove(ctrl['log'])
                    with open(ctrl['running'], 'w') as f: f.write('running')
                    subprocess.Popen([sys.executable, script_name]); st.rerun()
            
            else:
                st.warning("⚪ Pendiente")
                if st.button(f"Ejecutar P{step_num}", disabled=not prev_done, type="primary", key=f"r{step_num}"): 
                    if not os.path.exists(script_name):
                         st.error(f"❌ ERROR: El script '{script_name}' no existe.")
                    else:
                        if os.path.exists(ctrl['log']): os.remove(ctrl['log'])
                        with open(ctrl['running'], 'w') as f: f.write('running')
                        subprocess.Popen([sys.executable, script_name])
                        st.rerun()
            
            if os.path.exists(ctrl['log']):
                st.markdown("**Log de Ejecución:**")
                st.code(read_log_tail(ctrl['log'], n=15), language="log")

    # --- Renderizar Pasos 1 a 6 ---
    render_step_card(1, True) # El paso 1 no tiene dependencias
    render_step_card(2, status['step_1_done'])
    render_step_card(3, status['step_2_done'])
    render_step_card(4, status['step_3_done'])
    render_step_card(5, status['step_4_done'])
    render_step_card(6, status['step_3_done']) # El paso 6 depende del 3 (Atlas)

with col_insp:
    if monitor_mode and st.session_state.inspector_step:
        ctrls = {1: STEP_CTRLS[1], 2: STEP_CTRLS[2], 3: STEP_CTRLS[3], 4: STEP_CTRLS[4], 5: STEP_CTRLS[5], 6: STEP_CTRLS[6]}
        if st.session_state.inspector_step in ctrls:
            render_inspector(f"Paso {st.session_state.inspector_step}", ctrls[st.session_state.inspector_step]['metrics'])
    elif monitor_mode:
        st.info("👈 Selecciona un paso para inspeccionar sus resultados.")