# app.py
import streamlit as st
import json
import os
import sys
import time
import psutil 
import subprocess
import pandas as pd
import io 
from datetime import date

# --- Configuración Inicial ---
sys.path.append(os.getcwd()) 
try:
    import step_1_load_raw_data
except ImportError: pass

CONFIG_FILE = 'config.json'
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# --- DEFINICIÓN MAESTRA DE ARCHIVOS DE CONTROL ---
STEP_CTRLS = {
    1: { 
        "log": os.path.join(LOG_DIR, "step_1.log"), "pid": os.path.join(LOG_DIR, "step_1.pid"),
        "pause": os.path.join(LOG_DIR, "step_1.pause"), "running": os.path.join(LOG_DIR, "step_1.running"),
        "metrics": os.path.join(LOG_DIR, "step_1_metrics.json"), "script": "step_1_load_raw_data.py",
        "title": "Carga de Datos", "desc": "Carga CSV/Excel a Parquet"
    },
    2: {
        "log": os.path.join(LOG_DIR, "step_2.log"), "pid": os.path.join(LOG_DIR, "step_2.pid"),
        "pause": os.path.join(LOG_DIR, "step_2.pause"), "running": os.path.join(LOG_DIR, "step_2.running"),
        "metrics": os.path.join(LOG_DIR, "step_2_metrics.json"), "script": "step_2_process_casos.py",
        "title": "Procesar Casos", "desc": "Unifica y limpia Casos"
    },
    3: {
        "log": os.path.join(LOG_DIR, "step_3.log"), "pid": os.path.join(LOG_DIR, "step_3.pid"),
        "pause": os.path.join(LOG_DIR, "step_3.pause"), "running": os.path.join(LOG_DIR, "step_3.running"),
        "metrics": os.path.join(LOG_DIR, "step_3_metrics.json"), "script": "step_3_build_atlas.py",
        "title": "Construir Atlas", "desc": "Base Analítica General"
    },
    4: {
        "log": os.path.join(LOG_DIR, "step_4.log"), "pid": os.path.join(LOG_DIR, "step_4.pid"),
        "pause": os.path.join(LOG_DIR, "step_4.pause"), "running": os.path.join(LOG_DIR, "step_4.running"),
        "metrics": os.path.join(LOG_DIR, "step_4_metrics.json"), "script": "step_4_process_actuaciones.py",
        "title": "Actuaciones (Acusatorio)", "desc": "Filtra y clasifica estados"
    },
    5: {
        "log": os.path.join(LOG_DIR, "step_5.log"), "pid": os.path.join(LOG_DIR, "step_5.pid"),
        "pause": os.path.join(LOG_DIR, "step_5.pause"), "running": os.path.join(LOG_DIR, "step_5.running"),
        "metrics": os.path.join(LOG_DIR, "step_5_metrics.json"), "script": "step_5_apply_consistency.py",
        "title": "Consistencia y Hitos", "desc": "Calcula conflictos y jerarquías"
    },
    6: {
        "log": os.path.join(LOG_DIR, "step_6.log"), "pid": os.path.join(LOG_DIR, "step_6.pid"),
        "pause": os.path.join(LOG_DIR, "step_6.pause"), "running": os.path.join(LOG_DIR, "step_6.running"),
        "metrics": os.path.join(LOG_DIR, "step_6_metrics.json"), "script": "step_6_export_reports.py",
        "title": "Exportar Reportes", "desc": "Genera Excel y CSV final"
    }
}

# --- Funciones de Utilidad ---
def load_config(config_filename=CONFIG_FILE):
    if os.path.exists(config_filename):
        with open(config_filename, 'r', encoding='utf-8') as f: return json.load(f)
    return {"paths": {}, "filters": {}}

def save_config(config_data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f: json.dump(config_data, f, indent=2)
        return True
    except: return False

def get_available_sessions():
    return [f for f in os.listdir('.') if f.endswith('.json') and 'step_' not in f]

def check_status(paths):
    outputs = {
        1: os.path.join(paths.get('intermediate_loaded',''), 'CasosActuacionesInquisitivo.parquet'),
        2: os.path.join(paths.get('intermediate_processed',''), 'data_casos_processed.parquet'),
        3: os.path.join(paths.get('intermediate_analytical',''), 'data_final_comparativo.parquet'),
        4: os.path.join(paths.get('intermediate_processed',''), 'df_casos_personas_final.parquet'),
        5: os.path.join(paths.get('intermediate_processed',''), 'df_procesal_unificado.parquet'),
        6: os.path.join(paths.get('output_reports',''), 'baseUnisaMixtoAcusatorio.csv')
    }
    status = {}
    for i in range(1, 7):
        status[f'step_{i}_done'] = os.path.exists(outputs.get(i, ''))
        status[f'step_{i}_running'] = os.path.exists(STEP_CTRLS[i]['running'])
    return status

def read_log_tail(file_path, n=50):
    if not os.path.exists(file_path): return "Esperando log..."
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return "".join(f.readlines()[-n:])
    except: return "Leyendo..."

def stop_process(pid_file, running_flag):
    try:
        if os.path.exists(pid_file):
            with open(pid_file, 'r') as f: pid = int(f.read().strip())
            p = psutil.Process(pid)
            p.terminate(); time.sleep(1)
            if p.is_running(): p.kill()
    except: pass
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
        with open(metrics_file, 'r') as f: return json.load(f).get('timestamp', "")
    except: return ""

# --- INSPECTOR DERECHO ---
def render_inspector(step_num, metrics_file):
    ctrl = STEP_CTRLS[step_num]
    st.markdown(f"### 🧐 Inspector: {ctrl['title']}")
    
    if not os.path.exists(metrics_file):
        st.info("No hay resultados de métricas disponibles.")
        return
    try:
        with open(metrics_file, 'r') as f: m = json.load(f)
        c1, c2, c3 = st.columns(3)
        c1.metric("Filas", f"{m.get('rows',0):,}")
        c2.metric("Cols", m.get('columns',0))
        c3.metric("RAM (MB)", f"{m.get('memory_mb',0)}")
        st.caption(f"Generado: {m.get('timestamp', 'N/A')}")
        
        if 'created_files' in m:
            with st.expander("📂 Archivos Generados", expanded=True):
                for f in m['created_files']: st.success(f"📄 {f}")
        
        with st.expander("🛠️ Inputs Técnicos", expanded=False):
            st.markdown(f"**Salida:** `{os.path.basename(m.get('output_file', ''))}`")
            st.markdown("**Entradas:**")
            for f in m.get('input_files', []): st.markdown(f"- `{f}`")

        st.subheader("Previsualización")
        # --- FIX: Corrección de width para evitar advertencias ---
        st.dataframe(pd.DataFrame(m.get('preview', [])), width=None) 
        
        st.divider()
        st.subheader("📥 Descargar")
        output_file = m.get('output_file')
        
        if output_file and os.path.exists(output_file):
            if output_file.endswith(('.xlsx', '.csv')):
                with open(output_file, "rb") as f:
                    st.download_button("⬇️ Descargar Archivo", f, os.path.basename(output_file))
            else:
                fmt = st.radio("Formato:", ["Parquet", "CSV", "Excel"], horizontal=True, key=f"fmt_{step_num}")
                if fmt == "Parquet":
                    with open(output_file, "rb") as f:
                        st.download_button("⬇️ Descargar Parquet", f, os.path.basename(output_file))
                elif fmt == "CSV":
                    if st.button("Generar CSV", key=f"c_{step_num}"):
                        with st.spinner("Convirtiendo..."):
                            df = pd.read_parquet(output_file)
                            st.download_button("⬇️ Bajar CSV", df.to_csv(index=False).encode('utf-8'), "data.csv", "text/csv")
                elif fmt == "Excel":
                    if m.get('rows', 0) > 1000000: st.error("Muy grande para Excel.")
                    elif st.button("Generar Excel", key=f"x_{step_num}"):
                        with st.spinner("Generando..."):
                            buf = io.BytesIO()
                            pd.read_parquet(output_file).to_excel(buf, index=False)
                            st.download_button("⬇️ Bajar Excel", buf.getvalue(), "data.xlsx")
        else: st.warning(f"Archivo no encontrado: {output_file}")
    except Exception as e: st.error(f"Error inspector: {e}")

# --- INTERFAZ PRINCIPAL ---
st.set_page_config(page_title="ETL UNISA", layout="wide", initial_sidebar_state="expanded")

# Init State
if 'current_session_file' not in st.session_state: st.session_state.current_session_file = CONFIG_FILE
if 'config' not in st.session_state: st.session_state.config = load_config(st.session_state.current_session_file)
if 'inspector_step' not in st.session_state: st.session_state.inspector_step = None
if 'pipeline_active' not in st.session_state: st.session_state.pipeline_active = False
if 'pipeline_queue' not in st.session_state: st.session_state.pipeline_queue = []
if 'current_pipeline_step' not in st.session_state: st.session_state.current_pipeline_step = None

status = check_status(st.session_state.config.get('paths', {}))

# Auto-select inspector
for i in range(1, 7):
    if status.get(f'step_{i}_running'): st.session_state.inspector_step = i

# --- ORQUESTADOR (LÓGICA DE LOTE) ---
if st.session_state.pipeline_active:
    if st.session_state.pipeline_queue:
        next_step = st.session_state.pipeline_queue[0]
        
        if status.get(f'step_{next_step}_running'):
            pass 
        else:
            ctrl = STEP_CTRLS[next_step]
            log_file = ctrl['log']
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
            
            else:
                script = ctrl['script']
                if os.path.exists(log_file): os.remove(log_file)
                
                with open(ctrl['running'], 'w') as f: f.write('running')
                subprocess.Popen([sys.executable, script])
                
                st.session_state.current_pipeline_step = next_step
                st.toast(f"🚀 Iniciando Paso {next_step}...")
                time.sleep(1); st.rerun()

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
            if st.session_state.current_pipeline_step: stop_process(STEP_CTRLS[st.session_state.current_pipeline_step]['pid'], STEP_CTRLS[st.session_state.current_pipeline_step]['running'])
            st.warning("Pipeline detenido."); st.rerun()
    else:
        if st.button("▶️ Iniciar Pipeline"):
            st.session_state.pipeline_queue = sorted(steps_to_run)
            st.session_state.pipeline_active = True
            st.rerun()

    st.divider()
    st.subheader("📅 Filtros Globales")
    with st.form("filters"):
        curr = st.session_state.config.get('filters', {})
        try: ds = date.fromisoformat(curr.get("date_start", "2018-01-01"))
        except: ds = date(2018, 1, 1)
        try: de = date.fromisoformat(curr.get("date_end", "2025-12-31"))
        except: de = date(2025, 12, 31)
        d_start = st.date_input("Desde", ds)
        d_end = st.date_input("Hasta", de)
        if st.form_submit_button("Aplicar"):
            st.session_state.config['filters'] = {"date_start": d_start.isoformat(), "date_end": d_end.isoformat()}
            save_config(st.session_state.config, st.session_state.current_session_file)
            save_config(st.session_state.config, CONFIG_FILE)
            st.toast("Filtros guardados"); st.rerun()
    
    st.divider()
    monitor_mode = st.toggle("Modo Monitor", value=True)
    
    st.subheader("💾 Sesiones")
    av = get_available_sessions()
    if av:
        sel = st.selectbox("Archivo", av)
        if st.button("Cargar"):
            st.session_state.config = load_config(sel)
            st.session_state.current_session_file = sel
            save_config(st.session_state.config, CONFIG_FILE)
            st.rerun()
    nn = st.text_input("Nombre", "proy.json")
    if st.button("Guardar"):
        save_config(st.session_state.config, nn if nn.endswith('.json') else nn+".json")
        st.success("Guardado")
        
    with st.expander("✏️ Editar Rutas"):
        with st.form("edit_paths"):
            paths = st.session_state.config.get('paths', {})
            new_p = {}
            for k in ['raw_data', 'intermediate_loaded', 'intermediate_processed', 'intermediate_analytical', 'output_reports']:
                new_p[k] = st.text_input(k, paths.get(k, ''))
            if st.form_submit_button("Actualizar"):
                st.session_state.config['paths'] = new_p
                save_config(st.session_state.config, st.session_state.current_session_file)
                save_config(st.session_state.config, CONFIG_FILE)
                st.rerun()

# --- MAIN ---
st.title(f"Pipeline de Datos UNISA")
st.caption(f"Sesión: **{st.session_state.current_session_file}**")

col_pipe, col_insp = st.columns([0.6, 0.4])

with col_pipe:
    
    # FUNCION GENERADORA DE TARJETAS
    def render_step_card(step_num, prev_done):
        ctrl = STEP_CTRLS[step_num]
        k_run = f'step_{step_num}_running'
        k_done = f'step_{step_num}_done'
        
        with st.expander(f"**PASO {step_num}: {ctrl['title']}**", expanded=True):
            st.caption(ctrl['desc'])
            err = check_log_for_errors(ctrl['log'])

            if status[k_done] or status[k_run] or err:
                if st.button(f"🔍 Detalles", key=f"i{step_num}"): 
                    st.session_state.inspector_step = step_num; st.rerun()

            if status[k_run]:
                st.info("⚙️ Procesando...")
                c1, c2 = st.columns(2)
                if c1.button("⏯️ Pausa", key=f"p{step_num}"): toggle_pause(ctrl['pause'])
                if c2.button("⏹️ STOP", key=f"s{step_num}"): stop_process(ctrl['pid'], ctrl['running']); st.rerun()
            
            elif status[k_done]:
                ts = get_completion_timestamp(ctrl['metrics'])
                st.success(f"✅ Completado {ts}")
                if st.button(f"Re-ejecutar", key=f"re{step_num}"): 
                    if os.path.exists(ctrl['log']): os.remove(ctrl['log'])
                    with open(ctrl['running'], 'w') as f: f.write('running')
                    subprocess.Popen([sys.executable, ctrl['script']]); st.rerun()
            
            elif err:
                st.error("❌ Error")
                st.code(err, language="log")
                if st.button(f"Re-intentar", disabled=not prev_done, key=f"rt{step_num}"): 
                    if os.path.exists(ctrl['log']): os.remove(ctrl['log'])
                    with open(ctrl['running'], 'w') as f: f.write('running')
                    subprocess.Popen([sys.executable, ctrl['script']]); st.rerun()
            
            else:
                st.warning("⚪ Pendiente")
                if st.button(f"Ejecutar", disabled=not prev_done, type="primary", key=f"e{step_num}"): 
                    if os.path.exists(ctrl['log']): os.remove(ctrl['log'])
                    with open(ctrl['running'], 'w') as f: f.write('running')
                    subprocess.Popen([sys.executable, ctrl['script']])
                    st.rerun()

            if os.path.exists(ctrl['log']):
                st.markdown("**Log de Ejecución:**")
                st.code(read_log_tail(ctrl['log'], n=50), language="log")

    # Renderizar Pasos
    render_step_card(1, True)
    render_step_card(2, status['step_1_done'])
    render_step_card(3, status['step_2_done'])
    render_step_card(4, status['step_3_done'])
    render_step_card(5, status['step_4_done'])
    render_step_card(6, status['step_5_done'])

with col_insp:
    if monitor_mode and st.session_state.inspector_step:
        ctrls = {1: STEP_CTRLS[1], 2: STEP_CTRLS[2], 3: STEP_CTRLS[3], 4: STEP_CTRLS[4], 5: STEP_CTRLS[5], 6: STEP_CTRLS[6]}
        if st.session_state.inspector_step in ctrls:
            render_inspector(st.session_state.inspector_step, ctrls[st.session_state.inspector_step]['metrics'])
    elif monitor_mode:
        st.info("👈 Selecciona 'Detalles' para ver resultados.")