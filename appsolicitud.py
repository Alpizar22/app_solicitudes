import os
import json
import time, random
from uuid import uuid4
from datetime import datetime, timedelta
import re
from pathlib import Path

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from google.cloud import storage # GCS
import yagmail
from zoneinfo import ZoneInfo

# --- LIBRERÍAS IA ---
import openai
import numpy as np
try:
    import faiss
    from pypdf import PdfReader
except ImportError:
    st.warning("⚠️ Faltan librerías de IA. Ejecuta: pip install openai faiss-cpu pypdf")

# =========================
# Límites de subida
# =========================
MAX_IMAGE_MB = 10
MAX_VIDEO_MB = 50
_MB = 1024 * 1024
MAX_IMAGE_BYTES = MAX_IMAGE_MB * _MB
MAX_VIDEO_BYTES = MAX_VIDEO_MB * _MB

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.wmv', '.mkv', '.webm', '.ogg'}

def _guess_is_image_or_video(file_name: str, mime: str | None):
    ext = Path(file_name).suffix.lower()
    if mime:
        if mime.startswith("image/"): return "image", ext
        if mime.startswith("video/"): return "video", ext
    if ext in IMAGE_EXTS: return "image", ext
    if ext in VIDEO_EXTS: return "video", ext
    return None, ext

def validate_upload_limits(uploaded_file) -> tuple[bool, str]:
    if uploaded_file is None: return True, ""
    kind, _ext = _guess_is_image_or_video(uploaded_file.name, getattr(uploaded_file, "type", None))
    size = getattr(uploaded_file, "size", 0)
    size_mb = size / _MB

    if kind == "image":
        if size > MAX_IMAGE_BYTES:
            return False, f"❌ La imagen pesa {size_mb:.2f} MB y el límite es {MAX_IMAGE_MB} MB."
    elif kind == "video":
        if size > MAX_VIDEO_BYTES:
            return False, f"❌ El video pesa {size_mb:.2f} MB y el límite es {MAX_VIDEO_MB} MB."
    else:
        return False, "❌ Solo se permiten imágenes o videos."
    return True, ""

# =========================
# Utils
# =========================
EMAIL_RE = re.compile(r'([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})', re.I)
def _email_norm(s: str) -> str:
    if s is None: return ""
    m = EMAIL_RE.search(str(s))
    return m.group(1).strip().lower() if m else str(s).strip().lower()

def _norm(x): return str(x).strip().lower() if pd.notna(x) else ""
def _is_unrated(val: str) -> bool: return _norm(val) in ("", "pendiente", "na", "n/a", "sin calificacion", "-")

def with_backoff(fn, *args, **kwargs):
    for i in range(5):
        try: return fn(*args, **kwargs)
        except Exception: time.sleep(min(1*(2**i) + random.random(), 16))
    raise Exception("API Failed")

def load_json_safe(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f: return json.load(f)
    except: return {}

TZ_MX = ZoneInfo("America/Mexico_City")
def now_mx_str() -> str: return datetime.now(TZ_MX).strftime("%d/%m/%Y %H:%M:%S")

# =========================
# Config / Secrets
# =========================
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")
APP_MODE = st.secrets.get("mode", "dev")
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))
SHEET_ID = (st.secrets.get("sheets", {}).get("prod_id") if APP_MODE == "prod" else st.secrets.get("sheets", {}).get("dev_id"))

@st.cache_resource(ttl=3600)
def get_gspread_client():
    creds = Credentials.from_service_account_info(st.secrets["google_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds)

book = with_backoff(get_gspread_client().open_by_key, SHEET_ID)

GCS_BUCKET_NAME = st.secrets.get("google_cloud_storage", {}).get("bucket_name", "")
@st.cache_resource(ttl=3600)
def get_gcs_client():
    creds = Credentials.from_service_account_info(st.secrets["google_service_account"], scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return storage.Client(project=st.secrets["google_service_account"]["project_id"], credentials=creds)

def upload_to_gcs(file_buffer, filename_in_bucket, content_type):
    client = get_gcs_client()
    if not client or not GCS_BUCKET_NAME: return None
    try:
        blob = client.bucket(GCS_BUCKET_NAME).blob(filename_in_bucket)
        file_buffer.seek(0)
        with_backoff(blob.upload_from_file, file_buffer, content_type=content_type, rewind=True)
        return blob.generate_signed_url(
            version="v4", 
            expiration=timedelta(days=15),  # <--- 15 Días activo
            method="GET"
        )
    except: return None

# =========================
# 🧠 CEREBRO IA (PORTERO + RAG)
# =========================
@st.cache_resource
def get_openai_client():
    key = st.secrets.get("openai", {}).get("api_key")
    return openai.OpenAI(api_key=key) if key else None

def get_embedding(text, client):
    return client.embeddings.create(input=[text.replace("\n", " ")], model="text-embedding-3-small").data[0].embedding

@st.cache_data
def cargar_manual_pdf(ruta="manual.pdf"):
    chunks = []
    if os.path.exists(ruta):
        try:
            reader = PdfReader(ruta)
            texto = "".join([p.extract_text() for p in reader.pages])
            for i in range(0, len(texto), 1000):
                chunks.append(f"[MANUAL]: {texto[i:i+1000]}")
        except Exception as e: print(f"Error PDF: {e}")
    return chunks

# --- 1. EL PORTERO (VALIDACIÓN MANUAL 1.0 - VERSIÓN ROBUSTA) ---
def validar_incidencia_con_ia(asunto, descripcion, categoria, link, tiene_adjunto):
    client = get_openai_client()
    if not client: return True, "" 
    
    manual = cargar_manual_pdf("manual.pdf")
    contexto = "\n".join(manual[:6]) if manual else ""
    
    # Usamos términos muy claros para que la IA no se confunda
    estado_archivo = "CON_ARCHIVO" if tiene_adjunto else "SIN_ARCHIVO"

    prompt = f"""
    Eres el Validador de Calidad de Zoho CRM. Tu misión es aprobar o rechazar tickets basándote ESTRICTAMENTE en los datos siguientes.

    DATOS DEL TICKET:
    - Categoría: {categoria}
    - Asunto: {asunto}
    - Descripción: {descripcion}
    - Link: {link}
    - Estado del Adjunto: {estado_archivo}

    REGLAS DE VALIDACIÓN (CHECKLIST):

    1. SI CATEGORÍA ES 'Reactivación':
       - ¿Menciona estatus "Descartado"? (Busca en descripción o confirma si el usuario ya lo validó).
       - ¿Tiene Link? (El campo Link no debe estar vacío).
       - NO IMPORTA SI NO TIENE ARCHIVO. (Ignora el estado del adjunto).

    2. SI CATEGORÍA ES 'Desfase':
       - ¿Tiene Link? (Obligatorio).
       - ¿Tiene ID UAG? (Busca cualquier número de 7 u 8 dígitos dentro de la descripción o asunto. Ejemplo: 4659189). Si encuentras un número, márcalo como CUMPLIDO.
       - ¿Tiene Evidencia Visual? (Revisa el 'Estado del Adjunto'. Si es 'CON_ARCHIVO' -> CUMPLE. Si es 'SIN_ARCHIVO' -> RECHAZA).

    3. SI CATEGORÍA ES 'Equivalencia':
       - ¿Tiene Link? (Obligatorio).
       - ¿Menciona ID o Correo? (Busca en descripción).
       - NO requiere archivo obligatorio.

    4. SI CATEGORÍA ES 'Llamadas':
       - ¿Tiene Evidencia? (Si 'Estado del Adjunto' es 'SIN_ARCHIVO' -> RECHAZA).

    TAREA:
    Evalúa los puntos arriba.
    Si todo cumple, responde {{"valido": true, "razon_corta": ""}}.
    Si algo falla, responde {{"valido": false, "razon_corta": "Indica exactamente qué faltó (ej. Falta adjuntar la evidencia de imagen)."}}.
    """
    
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}, temperature=0.0
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get("valido", True), data.get("razon_corta", "")
    except: return True, ""
    
# --- 2. EL COPILOTO (RAG ADMIN) ---
def generar_respuesta_ia(asunto, descripcion, df_historico):
    client = get_openai_client()
    if not client: return "Error OpenAI."
    
    fuentes = cargar_manual_pdf("manual.pdf")
    cols = ["EstadoI", "RespuestadeSolicitudI", "Asunto"]
    if all(c in df_historico.columns for c in cols):
        df_ok = df_historico[
            (df_historico["EstadoI"].isin(["Atendido","Cerrado"])) & 
            (df_historico["RespuestadeSolicitudI"].str.len() > 15) & 
            (~df_historico["RespuestadeSolicitudI"].str.lower().isin(["listo", "quedo"]))
        ]
        for _, r in df_ok.iterrows():
            fuentes.append(f"[HISTORIAL]: Problema: {r['Asunto']} | Solución: {r['RespuestadeSolicitudI']}")
    
    contexto_str = ""
    if fuentes:
        try:
            vecs = [get_embedding(t, client) for t in fuentes]
            idx = faiss.IndexFlatL2(len(vecs[0]))
            idx.add(np.array(vecs).astype('float32'))
            q_vec = np.array([get_embedding(f"{asunto} {descripcion}", client)]).astype('float32')
            D, I = idx.search(q_vec, k=3)
            contexto_str = "\n".join([fuentes[i] for i in I[0] if i < len(fuentes)])
        except: pass

    prompt = f"""
    Actúa como soporte técnico experto. Redacta la respuesta al usuario final.
    CONTEXTO (Manual y Casos): {contexto_str}
    CASO NUEVO: {asunto} - {descripcion}
    """
    try:
        return client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}]).choices[0].message.content
    except: return "Error generando."

# =========================
# Datos
# =========================
sheets = {k: book.worksheet(k) for k in ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]}
sheet_solicitudes, sheet_incidencias, sheet_quejas, sheet_usuarios = sheets["Sheet1"], sheets["Incidencias"], sheets["Quejas"], sheets["Usuarios"]

def get_records_simple(_ws) -> pd.DataFrame:
    try:
        v = with_backoff(_ws.get_all_values)
        if not v: return pd.DataFrame()
        h, d = v[0], v[1:]
        return pd.DataFrame([r + [""]*(len(h)-len(r)) for r in d], columns=h)
    except: return pd.DataFrame()

data_folder = Path("data")
estructura_roles = load_json_safe(data_folder / "estructura_roles.json")
numeros_por_rol  = load_json_safe(data_folder / "numeros_por_rol.json")
horarios_dict    = load_json_safe(data_folder / "horarios.json")

udf = get_records_simple(sheet_usuarios)
usuarios_dict = {str(p).strip(): _email_norm(c) for p, c in zip(udf.get("Contraseña",[]), udf.get("Correo",[])) if str(p).strip()}

def enviar_correo(asunto, cuerpo, copia_a):
    if not SEND_EMAILS: return
    try:
        yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
        to = ["luis.alpizar@edu.uag.mx"]
        if copia_a and "@" in str(copia_a): to.append(copia_a)
        yag.send(to=to, subject=asunto, contents=[cuerpo])
    except: pass

def do_login(m): st.session_state.update({"usuario_logueado": _email_norm(m), "session_id": str(uuid4())}); st.rerun()
def do_logout(): st.session_state.clear(); st.rerun()
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None

# =========================
# NAVEGACIÓN Y VISTAS
# =========================
nav = ["🔍 Ver el estado de mis solicitudes", "🌟 Solicitudes CRM", "🛠️ Incidencias CRM", "📝 Mejoras y sugerencias", "🔐 Zona Admin"]
if 'nav_index' not in st.session_state: st.session_state.nav_index = 0
idx = st.sidebar.radio("Menú", range(len(nav)), format_func=lambda i: nav[i], index=st.session_state.nav_index)
st.session_state.nav_index = idx
seccion = nav[idx]

# --- 1. ESTADO ---
if seccion == "🔍 Ver el estado de mis solicitudes":
    st.markdown("## 🔍 Mis Trámites")
    if not st.session_state.usuario_logueado:
        with st.form("log"):
            pw = st.text_input("Contraseña", type="password")
            if st.form_submit_button("Entrar"):
                if pw.strip() in usuarios_dict: do_login(usuarios_dict[pw.strip()])
                else: st.error("Error")
    else:
        st.info(f"Usuario: **{st.session_state.usuario_logueado}**")
        if st.button("Salir"): do_logout()
        
        st.subheader("🛠️ Mis Incidencias")
        dfi = get_records_simple(sheet_incidencias)
        if not dfi.empty and "CorreoI" in dfi.columns:
            dfmi = dfi[dfi["CorreoI"].map(_email_norm) == st.session_state.usuario_logueado]
            for i, r in dfmi.iterrows():
                with st.expander(f"{r.get('Asunto')} ({r.get('EstadoI')})"):
                    st.write(r.get("DescripcionI"))
                    if r.get("RespuestadeSolicitudI"): st.info(f"Respuesta: {r.get('RespuestadeSolicitudI')}")

# --- 2. SOLICITUDES (ALTA/BAJA) ---
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Nueva Solicitud")
    st.info("Formulario de Altas y Bajas (Copia aquí tu lógica original si la necesitas completa)")

# ===================== SECCIÓN: INCIDENCIAS CRM =====================
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias (IA)")
    
    # --- SELECTOR FUERA DEL FORM ---
    c_cat1, c_cat2 = st.columns([1, 2])
    with c_cat1:
        st.write("") 
    with c_cat2:
        cat = st.selectbox("📂 Selecciona la Categoría:", ["Desfase", "Reactivación", "Equivalencia", "Llamadas", "Zoho", "Otro"])
    
    # --- AVISOS DINÁMICOS ---
    check_texto = "Confirmo que la información es correcta."
    
    if cat == "Reactivación":
        st.warning("⚠️ **REGLA DE ORO:** Solo procede si el estatus actual del Lead es **'Descartado'**.")
        check_texto = "✅ Confirmo que ya revisé en Zoho y el estatus es 'Descartado'."
        
    elif cat == "Desfase":
        st.info("ℹ️ **REQUISITO:** Para desfases, es obligatorio adjuntar la evidencia (Captura de pantalla PING vs Zoho).")
        check_texto = "✅ Confirmo que adjuntaré la evidencia visual de PING."

    elif cat == "Llamadas":
        st.info("ℹ️ Valida que el número tenga formato +521... antes de reportar.")
        
    st.divider() 
    
    with st.form("fi", clear_on_submit=False): 
        c1, c2 = st.columns(2)
        mail = c1.text_input("Tu Correo (*)")
        asunto = st.text_input("Asunto (*)")
        
        link = st.text_input("Link del registro afectado (Zoho) (*)")
        descripcion = st.text_area("Descripción detallada (*)", height=150)
        file = st.file_uploader("Adjuntar Imagen/Video (Evidencia)")
        
        confirmacion = st.checkbox(check_texto)
        
        enviar = st.form_submit_button("Enviar Incidencia")

    if enviar:
        if not mail or not asunto or not descripcion:
            st.warning("⚠️ Faltan campos obligatorios.")
            
        elif not confirmacion:
            st.error("🛑 Debes marcar la casilla de confirmación para proceder.")
            
        elif cat in ["Reactivación", "Desfase", "Equivalencia"] and ("zoho.com" not in link.lower() or len(link) < 15):
            st.error("🛑 **Link Inválido:** El link proporcionado no parece ser de Zoho CRM. Por favor copia y pega la URL completa.")
            
        else:
            tiene_archivo = file is not None

            with st.spinner("🤖 Validando reglas del Manual..."):
                desc_completa = f"{descripcion}. [Usuario confirmó: {confirmacion}]"
                es_valido, motivo = validar_incidencia_con_ia(asunto, desc_completa, cat, link, tiene_archivo)
            
            if not es_valido:
                st.error("✋ Solicitud rechazada por el sistema")
                st.info(f"💡 **Motivo:** {motivo}")
            else:
                valid_f, msg = validate_upload_limits(file)
                if not valid_f: st.error(msg)
                else:
                    url = ""
                    if file: url = upload_to_gcs(file, f"{uuid4()}_{file.name}", file.type) or ""
                    
                    row = [now_mx_str(), _email_norm(mail), asunto, cat, descripcion, link, "Pendiente", "", "", "", "", str(uuid4()), url]
                    with_backoff(sheet_incidencias.append_row, row)
                    
                    enviar_correo(f"Incidencia Aceptada: {asunto}", f"Recibido:\n{descripcion}", mail)
                    st.success("✅ Incidencia registrada."); st.balloons(); time.sleep(2); st.rerun()

# --- 4. MEJORAS ---
elif seccion == "📝 Mejoras y sugerencias":
    st.markdown("## 📝 Mejoras")
    with st.form("fq"):
        m, t, d = st.text_input("Correo"), st.selectbox("Tipo", ["Mejora","Queja"]), st.text_area("Detalle")
        if st.form_submit_button("Enviar"):
            with_backoff(sheet_quejas.append_row, [now_mx_str(), m, t, d, "", "", "Pendiente"])
            st.success("✅ Enviado"); time.sleep(1); st.rerun()

# --- 5. ADMIN (CON RAG) ---
elif seccion == "🔐 Zona Admin":
    pwd = st.text_input("Admin Password", type="password")
    if pwd == st.secrets.get("admin", {}).get("password") or st.session_state.get("is_admin"):
        st.session_state.is_admin = True
        
        dfi = get_records_simple(sheet_incidencias)
        st.dataframe(dfi)
        idi = dfi["IDI"].tolist() if "IDI" in dfi.columns else []
        if idi:
            sel_i = st.selectbox("ID Incidencia", idi)
            ri = dfi[dfi["IDI"]==sel_i].iloc[0]
            
            if st.button("✨ Sugerir Respuesta (IA)"):
                with st.spinner("Leyendo manual y casos previos..."):
                    st.session_state.rag = generar_respuesta_ia(ri.get("Asunto"), ri.get("DescripcionI"), dfi)
            
            resp = st.text_area("Respuesta", value=st.session_state.get("rag", ri.get("RespuestadeSolicitudI","")))
            
            if st.button("Actualizar"):
                c = with_backoff(sheet_incidencias.find, sel_i)
                if c:
                    hi = sheet_incidencias.row_values(1)
                    cells = [gspread.Cell(c.row, hi.index("EstadoI")+1, "Atendido"), gspread.Cell(c.row, hi.index("RespuestadeSolicitudI")+1, resp)]
                    with_backoff(sheet_incidencias.update_cells, cells, value_input_option='USER_ENTERED')
                    st.success("Actualizado"); time.sleep(1); st.rerun()

st.sidebar.divider()
if st.sidebar.button("Recargar"): st.rerun()