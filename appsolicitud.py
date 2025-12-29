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
try:
    from pypdf import PdfReader
except ImportError:
    st.warning("⚠️ Faltan librerías de IA. Ejecuta: pip install openai pypdf")

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
# Utils & Config
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
        # --- LINK DE 15 DÍAS ---
        return blob.generate_signed_url(version="v4", expiration=timedelta(days=15), method="GET")
    except: return None

# =========================
# 🧠 CEREBRO IA (PORTERO V3.2 - Checklist)
# =========================
@st.cache_resource
def get_openai_client():
    key = st.secrets.get("openai", {}).get("api_key")
    return openai.OpenAI(api_key=key) if key else None

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

def validar_incidencia_con_ia(asunto, descripcion, categoria, link, tiene_adjunto):
    client = get_openai_client()
    if not client: return True, "" 
    
    manual = cargar_manual_pdf("manual.pdf")
    contexto = "\n".join(manual[:6]) if manual else ""
    adjunto_str = "CON_ARCHIVO" if tiene_adjunto else "SIN_ARCHIVO"

    prompt = f"""
    Eres el Validador de Calidad de Zoho CRM. Tu misión es aprobar o rechazar tickets basándote ESTRICTAMENTE en los datos siguientes.

    DATOS DEL TICKET:
    - Categoría: {categoria}
    - Asunto: {asunto}
    - Descripción: {descripcion}
    - Link: {link}
    - Estado del Adjunto: {adjunto_str}

    REGLAS DE VALIDACIÓN (CHECKLIST):
    1. SI CATEGORÍA ES 'Reactivación':
       - ¿Menciona estatus "Descartado"? (Busca en descripción o confirma si el usuario ya lo validó).
       - ¿Tiene Link? (Obligatorio).
       - NO IMPORTA SI NO TIENE ARCHIVO. (Ignora el estado del adjunto).

    2. SI CATEGORÍA ES 'Desfase':
       - ¿Tiene Link? (Obligatorio).
       - ¿Tiene ID UAG? (Busca cualquier número de 7 u 8 dígitos dentro de la descripción o asunto). Si encuentras un número, márcalo como CUMPLIDO.
       - ¿Tiene Evidencia Visual? (Si es 'CON_ARCHIVO' -> CUMPLE. Si es 'SIN_ARCHIVO' -> RECHAZA).

    3. SI CATEGORÍA ES 'Equivalencia':
       - ¿Tiene Link? (Obligatorio).
       - ¿Menciona ID o Correo? (Busca en descripción).

    4. SI CATEGORÍA ES 'Llamadas':
       - ¿Tiene Evidencia? (Si 'SIN_ARCHIVO' -> RECHAZA).

    TAREA:
    Evalúa los puntos arriba.
    Si todo cumple, responde {{"valido": true, "razon_corta": ""}}.
    Si algo falla, responde {{"valido": false, "razon_corta": "Indica exactamente qué faltó."}}.
    """
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}, temperature=0.0
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get("valido", True), data.get("razon_corta", "")
    except: return True, ""

# =========================
# Datos y Funciones Aux
# =========================
sheets = {k: book.worksheet(k) for k in ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]}
sheet_solicitudes, sheet_incidencias, sheet_quejas, sheet_usuarios, sheet_accesos = sheets["Sheet1"], sheets["Incidencias"], sheets["Quejas"], sheets["Usuarios"], sheets["Accesos"]

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

def enviar_correo(asunto, cuerpo_detalle, para):
    if not SEND_EMAILS: return
    try:
        # Obtenemos el usuario y password de los secrets
        user_email = st.secrets["email"]["user"]
        password = st.secrets["email"]["password"]
        
        yag = yagmail.SMTP(user=user_email, password=password)
        
        # Copia oculta al equipo (opcional, puedes quitarlo si prefieres)
        to = [para]
        
        # --- AQUÍ ESTÁ EL TRUCO DEL NOMBRE ---
        # Esto hace que llegue como "Equipo CRM" en lugar de "Luis Alpizar"
        headers = {"From": f"Equipo CRM <{user_email}>"}
        
        # --- NUEVO MENSAJE DE "ACUSE DE RECIBO" ---
        mensaje_html = f"""
        <div style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #004B93;">Confirmación de Recepción</h2>
            <p>Hola,</p>
            <p>Hemos recibido tu solicitud con el asunto: <strong>{asunto}</strong>.</p>
            <p>Se ha notificado al equipo de CRM y tu caso ha entrado en la cola de gestión. 
            Será atendido en su momento conforme a la carga de trabajo.</p>
            <p><strong>No es necesario que respondas a este correo.</strong> 
            Te notificaremos nuevamente por este medio en cuanto haya una actualización o resolución.</p>
            <hr>
            <p style="font-size: 12px; color: #666;">Detalle recibido:<br>{cuerpo_detalle}</p>
            <br>
            <p>Atentamente,<br><strong>Equipo de Gestión CRM</strong></p>
        </div>
        """
        
        yag.send(to=to, subject=f"Recibido: {asunto}", contents=[mensaje_html], headers=headers)
    except Exception as e: 
        print(f"Error enviando correo: {e}")

def do_login(m): st.session_state.update({"usuario_logueado": _email_norm(m), "session_id": str(uuid4())}); st.rerun()
def do_logout(): st.session_state.clear(); st.rerun()
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None

# =========================
# NAVEGACIÓN
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

# --- 2. SOLICITUDES (Tu lógica completa) ---
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")
    ss = st.session_state
    defaults = {"sol_tipo": "Selecciona...", "sol_area": "Selecciona...", "sol_perfil": "Selecciona...", "sol_rol": "Selecciona...", "sol_horario": "Selecciona...", "sol_turno": ""}
    for k, v in defaults.items():
        if k not in ss: ss[k] = v

    def on_change_area(): ss.sol_perfil = "Selecciona..."; ss.sol_rol = "Selecciona..."; ss.sol_horario = "Selecciona..."; ss.sol_turno = ""
    def on_change_perfil(): ss.sol_rol = "Selecciona..."; ss.sol_horario = "Selecciona..."; ss.sol_turno = ""
    def on_change_horario(): ss.sol_turno = horarios_dict.get(ss.sol_horario, "") if ss.sol_horario != "Selecciona..." else ""

    st.markdown("### 1) Tipo de Solicitud")
    st.selectbox("Tipo de Solicitud en Zoho (*)", ["Selecciona...", "Alta", "Modificación", "Baja"], key="sol_tipo")

    if ss.sol_tipo == "Baja":
        st.markdown("### 2) Datos del Usuario a dar de baja")
        with st.form("solicitud_form_baja", clear_on_submit=True):
            nombre = st.text_input("Nombre Completo de Usuario (*)")
            correo_user = st.text_input("Correo institucional del usuario (*)")
            correo_solicitante = st.text_input("Correo de quien lo solicita (*)")
            if st.form_submit_button("✔️ Enviar Baja"):
                if not nombre or not correo_user or not correo_solicitante: st.warning("⚠️ Faltan campos.")
                else:
                    try:
                        fila_sol = [now_mx_str(), "Baja", nombre.strip(), correo_user.strip(), "N/A", "N/A", "N/A", "", "", "", "", _email_norm(correo_solicitante), "Pendiente", "", "", str(uuid4()), "", ""]
                        with_backoff(sheet_solicitudes.append_row, fila_sol)
                        st.success("✅ Baja registrada."); st.balloons()
                        ss.sol_tipo = "Selecciona..."
                    except Exception as e: st.error(f"Error: {e}")
                    
    elif ss.sol_tipo in ["Alta", "Modificación"]:
        st.markdown("### 2) Definición del Puesto")
        areas = ["Selecciona..."] + list(estructura_roles.keys())
        st.selectbox("Área (*)", areas, index=areas.index(ss.sol_area) if ss.sol_area in areas else 0, key="sol_area", on_change=on_change_area)
        
        perfiles_disp = ["Selecciona..."]
        if ss.sol_area in estructura_roles: perfiles_disp += list(estructura_roles[ss.sol_area].keys())
        st.selectbox("Perfil (*)", perfiles_disp, index=perfiles_disp.index(ss.sol_perfil) if ss.sol_perfil in perfiles_disp else 0, key="sol_perfil", on_change=on_change_perfil)
        
        roles_disp = ["Selecciona..."]
        if ss.sol_area in estructura_roles and ss.sol_perfil in estructura_roles[ss.sol_area]: roles_disp += estructura_roles[ss.sol_area][ss.sol_perfil]
        st.selectbox("Rol (*)", roles_disp, index=roles_disp.index(ss.sol_rol) if ss.sol_rol in roles_disp else 0, key="sol_rol")

        requiere_horario = ss.sol_perfil in {"Agente de Call Center", "Ejecutivo AC"}
        if requiere_horario:
            st.selectbox("Horario", ["Selecciona..."] + list(horarios_dict.keys()), key="sol_horario", on_change=on_change_horario)
            st.text_input("Turno", value=ss.sol_turno, disabled=True)

        with st.form("solicitud_form_alta_mod", clear_on_submit=True):
            st.markdown("### 4) Datos")
            c1, c2 = st.columns(2)
            nombre = c1.text_input("Nombre Usuario (*)")
            correo = c2.text_input("Correo Usuario (*)")
            
            nums_cfg = numeros_por_rol.get(ss.sol_rol, {})
            num_in = st.selectbox("Num IN", ["No aplica"] + nums_cfg.get("Numero_IN", [])) if ss.sol_rol in numeros_por_rol else "No aplica"
            num_out = st.selectbox("Num Out", ["No aplica"] + nums_cfg.get("Numero_Saliente", [])) if ss.sol_rol in numeros_por_rol else "No aplica"
            
            correo_sol = st.text_input("Correo Solicitante (*)")
            
            if st.form_submit_button("✔️ Enviar"):
                if not nombre or not correo or not correo_sol: st.warning("⚠️ Faltan campos.")
                elif ss.sol_area == "Selecciona...": st.warning("⚠️ Faltan área/rol.")
                else:
                    try:
                        fila = [now_mx_str(), ss.sol_tipo, nombre, correo, ss.sol_area, ss.sol_perfil, ss.sol_rol, str(num_in), str(num_out), ss.sol_horario, ss.sol_turno, _email_norm(correo_sol), "Pendiente", "", "", str(uuid4()), "", ""]
                        with_backoff(sheet_solicitudes.append_row, fila)
                        st.success("✅ Solicitud registrada."); st.balloons()
                        for k in defaults: 
                            if k != "sol_tipo": ss[k] = defaults[k]
                    except Exception as e: st.error(f"Error: {e}")

# --- 3. INCIDENCIAS CRM (V3.2 + Correo Nuevo) ---
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias (IA)")
    
    # Selector FUERA del form
    c_cat1, c_cat2 = st.columns([1, 2])
    with c_cat1: st.write("") 
    with c_cat2:
        cat = st.selectbox("📂 Categoría:", ["Desfase", "Reactivación", "Equivalencia", "Llamadas", "Zoho", "Otro"])
    
    check_texto = "Confirmo que la información es correcta."
    if cat == "Reactivación":
        st.warning("⚠️ **REGLA DE ORO:** Solo procede si el estatus actual del Lead es **'Descartado'**.")
        check_texto = "✅ Confirmo que ya revisé en Zoho y el estatus es 'Descartado'."
    elif cat == "Desfase":
        st.info("ℹ️ **REQUISITO:** Obligatorio adjuntar evidencia (PING vs Zoho).")
        check_texto = "✅ Confirmo que adjuntaré la evidencia visual de PING."

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
            st.error("🛑 Debes marcar la casilla de confirmación.")
        elif cat in ["Reactivación", "Desfase", "Equivalencia"] and ("zoho.com" not in link.lower() or len(link) < 15):
            st.error("🛑 **Link Inválido:** Debe ser un enlace de Zoho CRM.")
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
                    enviar_correo(f"Incidencia Recibida: {asunto}", descripcion, mail)
                    st.success("✅ Incidencia registrada."); st.balloons(); time.sleep(2); st.rerun()

# --- 4. MEJORAS ---
elif seccion == "📝 Mejoras y sugerencias":
    st.markdown("## 📝 Mejoras")
    with st.form("fq"):
        m, t, d = st.text_input("Correo"), st.selectbox("Tipo", ["Mejora","Queja"]), st.text_area("Detalle")
        if st.form_submit_button("Enviar"):
            with_backoff(sheet_quejas.append_row, [now_mx_str(), m, t, d, "", "", "Pendiente"])
            st.success("✅ Enviado"); time.sleep(1); st.rerun()

# --- 5. ADMIN (COMPLETO Y RESTAURADO) ---
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")
    
    # 1. Lógica de Login Admin
    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    admin_ok = st.session_state.get("is_admin", False)

    if not admin_ok:
        with st.form("admin_login"):
            pwd = st.text_input("Contraseña Admin", type="password")
            if st.form_submit_button("Entrar"):
                if pwd == ADMIN_PASS: 
                    st.session_state.is_admin = True
                    st.rerun()
                else: st.error("❌ Contraseña incorrecta")
    else:
        if st.button("Salir de Admin"):
            del st.session_state.is_admin
            st.rerun()
        
        # 2. Tabs de Gestión
        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Quejas"])
        
# --- 5. ADMIN (CON NOTIFICACIONES DE RESPUESTA) ---
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")
    
    # 1. Login Admin
    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    admin_ok = st.session_state.get("is_admin", False)

    if not admin_ok:
        with st.form("admin_login"):
            pwd = st.text_input("Contraseña Admin", type="password")
            if st.form_submit_button("Entrar"):
                if pwd == ADMIN_PASS: 
                    st.session_state.is_admin = True
                    st.rerun()
                else: st.error("❌ Contraseña incorrecta")
    else:
        if st.button("Salir de Admin"):
            del st.session_state.is_admin
            st.rerun()
        
        # 2. Tabs de Gestión
        tab1, tab2, tab3 = st.tabs(["Solicitudes (Altas/Bajas)", "Incidencias", "Quejas"])
        
        # ================= TAB 1: SOLICITUDES =================
        with tab1:
            st.subheader("Gestión de Solicitudes")
            df = get_records_simple(sheet_solicitudes)
            st.dataframe(df, use_container_width=True)
            
            if not df.empty and "IDS" in df.columns:
                ids = df[df["IDS"] != ""]["IDS"].unique().tolist()
                if ids:
                    st.divider()
                    c_sel, c_st = st.columns(2)
                    sel_id = c_sel.selectbox("Seleccionar ID para gestionar", ids, key="sel_sol")
                    
                    # Recuperamos datos de la fila seleccionada para saber a quién enviar el correo
                    row_s = df[df["IDS"] == sel_id].iloc[0]
                    correo_solicitante = row_s.get("CorreoSolicitanteS", "") # Asegúrate que esta columna exista o ajusta el nombre
                    if not correo_solicitante and "Solicitante" in row_s: correo_solicitante = row_s["Solicitante"] # Intento de fallback
                    
                    st.caption(f"Gestionando solicitud de: **{row_s.get('NombreS')}** | Solicitado por: **{correo_solicitante}**")

                    nuevo_estado = c_st.selectbox("Nuevo Estado", ["Pendiente", "En proceso", "Atendido"], key="st_sol")
                    
                    # --- CAMPO PARA CREDENCIALES O MENSAJE ---
                    mensaje_respuesta = st.text_area("Mensaje de Resolución / Credenciales (Se enviará por correo si es 'Atendido')", 
                                                   placeholder="Ej: Hola, el usuario ha sido creado. Usuario: x, Contraseña: y...")
                    
                    c1, c2 = st.columns(2)
                    if c1.button("💾 Actualizar y Notificar"):
                        cell = with_backoff(sheet_solicitudes.find, sel_id)
                        if cell:
                            header = sheet_solicitudes.row_values(1)
                            # Actualizamos Estado
                            col_idx = header.index("EstadoS") + 1
                            with_backoff(sheet_solicitudes.update_cell, cell.row, col_idx, nuevo_estado)
                            
                            # --- ENVÍO DE CORREO DE RESOLUCIÓN ---
                            if nuevo_estado == "Atendido" and mensaje_respuesta and correo_solicitante:
                                try:
                                    yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
                                    headers = {"From": f"Equipo CRM <{st.secrets['email']['user']}>"}
                                    html_resp = f"""
                                    <h3 style="color: green;">¡Solicitud Atendida!</h3>
                                    <p>Tu solicitud <strong>{row_s.get('TipoS')} - {row_s.get('NombreS')}</strong> ha sido completada.</p>
                                    <p><strong>Detalle / Credenciales:</strong></p>
                                    <pre style="background: #f4f4f4; padding: 10px;">{mensaje_respuesta}</pre>
                                    <p>Saludos,<br>Equipo CRM</p>
                                    """
                                    yag.send(to=correo_solicitante, subject=f"✅ Finalizado: {row_s.get('TipoS')} - {row_s.get('NombreS')}", contents=[html_resp], headers=headers)
                                    st.toast("📧 Correo de credenciales enviado exitosamente.")
                                except Exception as e: st.error(f"Se guardó pero falló el correo: {e}")

                            st.success("✅ Estado actualizado en sistema."); time.sleep(2); st.rerun()
                            
                    if c2.button("🗑️ Eliminar Solicitud"):
                        cell = with_backoff(sheet_solicitudes.find, sel_id)
                        if cell:
                            with_backoff(sheet_solicitudes.delete_rows, cell.row)
                            st.warning("Eliminado"); time.sleep(1); st.rerun()

        # ================= TAB 2: INCIDENCIAS =================
        with tab2:
            st.subheader("Gestión de Incidencias")
            dfi = get_records_simple(sheet_incidencias)
            st.dataframe(dfi, use_container_width=True)
            
            if not dfi.empty and "IDI" in dfi.columns:
                ids_i = dfi[dfi["IDI"] != ""]["IDI"].unique().tolist()
                if ids_i:
                    st.divider()
                    sel_idi = st.selectbox("Seleccionar ID Incidencia", ids_i, key="sel_inc")
                    row_i = dfi[dfi["IDI"] == sel_idi].iloc[0]
                    correo_usuario_i = row_i.get("CorreoI")
                    
                    st.info(f"Asunto: **{row_i.get('Asunto')}** | Usuario: {correo_usuario_i}")
                    
                    c_st_i, c_at_i = st.columns(2)
                    nuevo_estado_i = c_st_i.selectbox("Estado", ["Pendiente", "En proceso", "Atendido"], index=["Pendiente", "En proceso", "Atendido"].index(row_i.get("EstadoI", "Pendiente")), key="st_inc")
                    
                    respuesta = st.text_area("Respuesta Técnica al Usuario (Se enviará por correo)", value=row_i.get("RespuestadeSolicitudI", ""))
                    
                    c1, c2 = st.columns(2)
                    if c1.button("💾 Guardar y Responder"):
                        cell = with_backoff(sheet_incidencias.find, sel_idi)
                        if cell:
                            header = sheet_incidencias.row_values(1)
                            col_st = header.index("EstadoI") + 1
                            col_resp = header.index("RespuestadeSolicitudI") + 1
                            
                            # Actualizamos Google Sheets
                            sheet_incidencias.update_cell(cell.row, col_st, nuevo_estado_i)
                            sheet_incidencias.update_cell(cell.row, col_resp, respuesta)
                            
                            # --- ENVÍO DE CORREO DE RESOLUCIÓN ---
                            if nuevo_estado_i == "Atendido" and respuesta and correo_usuario_i:
                                try:
                                    yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
                                    headers = {"From": f"Equipo CRM <{st.secrets['email']['user']}>"}
                                    html_resp = f"""
                                    <h3 style="color: green;">Incidencia Resuelta</h3>
                                    <p>Hola,</p>
                                    <p>Tu reporte con asunto: <strong>{row_i.get('Asunto')}</strong> ha sido marcado como <strong>Atendido</strong>.</p>
                                    <hr>
                                    <p><strong>Respuesta del equipo técnico:</strong></p>
                                    <p style="background: #e8f4fd; padding: 15px; border-left: 4px solid #004B93;">{respuesta}</p>
                                    <hr>
                                    <p>Si consideras que esto no resuelve tu problema, por favor responde a este correo.</p>
                                    <p>Atentamente,<br>Soporte CRM</p>
                                    """
                                    yag.send(to=correo_usuario_i, subject=f"✅ Resuelto: {row_i.get('Asunto')}", contents=[html_resp], headers=headers)
                                    st.toast("📧 Notificación enviada al usuario.")
                                except Exception as e: st.error(f"Se guardó el dato pero falló el correo: {e}")

                            st.success("Incidencia actualizada y usuario notificado."); time.sleep(2); st.rerun()
                            
                    if c2.button("🗑️ Eliminar Incidencia"):
                        cell = with_backoff(sheet_incidencias.find, sel_idi)
                        if cell:
                            with_backoff(sheet_incidencias.delete_rows, cell.row)
                            st.warning("Eliminado"); time.sleep(1); st.rerun()

        # TAB 3: QUEJAS
        with tab3:
            st.subheader("Gestión de Quejas")
            dfq = get_records_simple(sheet_quejas)
            st.dataframe(dfq, use_container_width=True)

st.sidebar.divider()
if st.sidebar.button("Recargar"): st.rerun()