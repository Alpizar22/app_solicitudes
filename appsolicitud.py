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

def upload_to_gcs(file_buffer, filename_in_bucket, content_type, expires_minutes=720):
    """
    Sube a GCS y devuelve URL firmada temporal (compatible con UBLA/PAP).
    """
    client = get_gcs_client()
    if not client:
        st.error("❌ No se puede subir a GCS: cliente no disponible.")
        return None
    if not GCS_BUCKET_NAME:
        st.error("❌ No se puede subir a GCS: falta google_cloud_storage.bucket_name en secrets.")
        return None
    try:
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(filename_in_bucket)
        
        file_buffer.seek(0)
        with_backoff(blob.upload_from_file, file_buffer, content_type=content_type, rewind=True)
        
        # --- CORRECCIÓN: 7 DÍAS (Límite máximo de Google) ---
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(days=7),  # <--- CAMBIADO A 7 DÍAS
            method="GET",
        )
        
        st.toast("☁️ Archivo subido (Link válido por 7 días).", icon="☁️")
        return signed_url
    except Exception as e:
        st.error(f"❌ Error al subir archivo a GCS: {e}")
        return None
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
sheets = {k: book.worksheet(k) for k in ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios", "Historial_IA"]}

# Creamos las variables existentes
sheet_solicitudes = sheets["Sheet1"]
sheet_incidencias = sheets["Incidencias"]
sheet_quejas = sheets["Quejas"]
sheet_usuarios = sheets["Usuarios"]
sheet_historial = sheets["Historial_IA"]

# --- AGREGA ESTO NUEVO PARA CONECTAR EL CEREBRO ---
try:
    sheet_cerebro = book.worksheet("Cerebro")
except:
    # Si no la encuentra (por si se te olvidó crearla), evitamos que truene
    sheet_cerebro = None
    print("⚠️ Advertencia: No se encontró la hoja 'Cerebro' en Google Sheets.")

@st.cache_data(ttl=60, show_spinner=False)
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
        
        # --- LISTA DE COPIAS (CC) ---
        # Aquí pones los correos de los jefes/supervisores.
        # Al ponerlos aquí, se aplicará para TODOS los envíos del sistema.
        cc_list = [
            "luis.alpizar@edu.uag.mx", 
            "carlos.sotelo@edu.uag.mx", 
            "esther.diaz@edu.uag.mx"
        ]

        to = [para]
        headers = {"From": f"Equipo CRM <{user_email}>"}
        
        # --- TU DISEÑO HTML (INTACTO) ---
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
        
        # --- EL ENVÍO CON CC ---
        yag.send(
            to=to, 
            cc=cc_list,  # <--- AQUÍ SE AGREGAN LAS COPIAS
            subject=f"Recibido: {asunto}", 
            contents=[mensaje_html], 
            headers=headers
        )
        print(f"Correo enviado a {to} con copia a {cc_list}")

    except Exception as e: 
        print(f"Error enviando correo: {e}")

def do_login(m): st.session_state.update({"usuario_logueado": _email_norm(m), "session_id": str(uuid4())}); st.rerun()
def do_logout(): st.session_state.clear(); st.rerun()
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None


# ---------------------------------------------------------
# BLOQUE DE NAVEGACIÓN (Este debe ir ANTES de cualquier 'if seccion')
# ---------------------------------------------------------

# OCULTO - pendiente FAQ: para reactivar, descomenta la línea de abajo y comenta la siguiente
# nav = ["🏠 Asistente IA 24/7", "🔍 Ver el estado de mis solicitudes", "🌟 Solicitudes CRM", "🛠️ Incidencias CRM", "🔑 Accesos y Buzón", "🔐 Zona Admin"]
nav = ["🔍 Ver el estado de mis solicitudes", "🌟 Solicitudes CRM", "🛠️ Incidencias CRM", "🔑 Accesos y Buzón", "🔐 Zona Admin"]

if 'nav_index' not in st.session_state: st.session_state.nav_index = 0
idx = st.sidebar.radio("Menú", range(len(nav)), format_func=lambda i: nav[i], index=st.session_state.nav_index)
st.session_state.nav_index = idx

# ¡AQUÍ NACE LA VARIABLE!
seccion = nav[idx]

# =========================
# 1. FUNCIÓN DE CARGA DE CONOCIMIENTO (AGREGAR AL INICIO CON TUS OTRAS FUNCIONES)
# =========================

@st.cache_data(show_spinner=False)
def cargar_conocimiento():
    if sheet_cerebro is None: return ""
    
    try:
        # Leemos la celda A1 donde vive todo el texto
        # Usamos value_render_option='UNFORMATTED_VALUE' para evitar que Google le de formato raro
        val = with_backoff(sheet_cerebro.acell, 'A1').value
        
        if not val: return "No hay conocimiento base aún."
        return val
    except Exception as e:
        print(f"Error leyendo cerebro: {e}")
        return ""

# ===================== SECCIÓN: ASISTENTE IA (PORTERO EXPERTO) =====================


if seccion == "🏠 Asistente IA 24/7":
    st.markdown("## 🤖 Sistema de Canalización MKT")
    st.info("👋 **Hola.** Soy tu respaldo técnico. Conozco los procesos exactos de Altas, Bajas e Incidencias.")

    # Historial visual (Chat en pantalla)
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Escribe tu duda (ej: 'Tengo un desfase', 'Quiero una baja')..."):
        # 1. Mostrar mensaje del usuario
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 2. Generar respuesta IA
        with st.chat_message("assistant"):
            with st.spinner("Consultando manuales..."):
                try:
                    client_ai = get_openai_client()
                    if client_ai:
                        base_conocimiento = cargar_conocimiento()
                        
                        contexto = f"""
                        ERES EL SOPORTE TÉCNICO EXPERTO DEL CRM UAG.
                        TU FUENTE DE VERDAD: {base_conocimiento}
                        TU MISIÓN: Responde basándote en la fuente.
                        REGLAS:
                        1. Si es INCIDENCIA -> Ve a '🛠️ Incidencias CRM'.
                        2. Si es SOLICITUD -> Ve a '🌟 Solicitudes CRM'.
                        3. Sé breve y profesional.
                        4. Prohibido ser Grosero
                        """
                        
                        resp = client_ai.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[
                                {"role": "system", "content": contexto},
                                {"role": "user", "content": prompt}
                            ],
                            temperature=0.0
                        )
                        reply = resp.choices[0].message.content
                        
                        # --- 🔴 AQUÍ GUARDAMOS EN EXCEL (AUDITORÍA) ---
                        # Obtenemos usuario (si está logueado) o ponemos "Anónimo"
                        usuario_actual = st.session_state.usuario_logueado if st.session_state.usuario_logueado else "Anónimo/Invitado"
                        
                        # Fila: [Fecha, Usuario, Pregunta, Respuesta]
                        log_row = [now_mx_str(), usuario_actual, prompt, reply]
                        
                        # Guardamos en segundo plano (para no alentar el chat)
                        try:
                            with_backoff(sheet_historial.append_row, log_row)
                        except Exception as e:
                            print(f"No se pudo guardar historial: {e}")
                        # ---------------------------------------------

                    else:
                        reply = "La IA no está conectada."
                except Exception as e:
                    reply = f"Error: {e}"
                
                st.markdown(reply)
                st.session_state.messages.append({"role": "assistant", "content": reply})


# --- 1. ESTADO (CORREGIDO: AHORA MUESTRA SOLICITUDES E INCIDENCIAS) ---
if seccion == "🔍 Ver el estado de mis solicitudes":
    st.markdown("## 🔍 Mis Tickets")
    if not st.session_state.usuario_logueado:
        with st.form("log"):
            pw = st.text_input("Contraseña", type="password")
            if st.form_submit_button("Entrar"):
                if pw.strip() in usuarios_dict: do_login(usuarios_dict[pw.strip()])
                else: st.error("Contraseña incorrecta")
    else:
        st.info(f"Usuario: **{st.session_state.usuario_logueado}**")
        if st.button("Salir"): do_logout()
        
        # --- BLOQUE A: MIS SOLICITUDES (ALTAS/BAJAS) ---
        st.subheader("🌟 Mis Solicitudes (Altas/Bajas)")
        dfs = get_records_simple(sheet_solicitudes)
        
        # Verificamos si existe la columna "SolicitanteS" y filtramos
        if not dfs.empty and "SolicitanteS" in dfs.columns:
            # Filtramos donde el solicitante sea el usuario logueado
            dfms = dfs[dfs["SolicitanteS"].map(_email_norm) == st.session_state.usuario_logueado]
            
            if dfms.empty:
                st.caption("No tienes solicitudes registradas.")
            else:
                for i, r in dfms.iterrows():
                    color = "orange" if r.get('EstadoS') == "Pendiente" else "green"
                    with st.expander(f"{r.get('TipoS')} - {r.get('NombreS')} (:{color}[{r.get('EstadoS')}])"):
                        st.write(f"**Fecha:** {r.get('FechaS')}")
                        st.write(f"**Area/Rol:** {r.get('AreaS')} - {r.get('RolS')}")
                        if r.get("CredencialesZohoS"):
                            st.success(f"**Resolución:** {r.get('CredencialesZohoS')}")
        else:
            st.caption("No se encontraron datos de solicitudes.")

        st.divider()

        # --- BLOQUE B: MIS INCIDENCIAS (SOPORTE) ---
        st.subheader("🛠️ Mis Incidencias (Soporte)")
        dfi = get_records_simple(sheet_incidencias)
        if not dfi.empty and "CorreoI" in dfi.columns:
            dfmi = dfi[dfi["CorreoI"].map(_email_norm) == st.session_state.usuario_logueado]
            
            if dfmi.empty:
                st.caption("No tienes incidencias registradas.")
            else:
                for i, r in dfmi.iterrows():
                    color = "orange" if r.get('EstadoI') == "Pendiente" else "green"
                    with st.expander(f"{r.get('Asunto')} (:{color}[{r.get('EstadoI')}])"):
                        st.write(f"**Descripción:** {r.get('DescripcionI')}")
                        if r.get("RespuestadeSolicitudI"): 
                            st.info(f"**Respuesta Técnica:** {r.get('RespuestadeSolicitudI')}")

# ===================== SECCIÓN: SOLICITUDES CRM =====================
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")

    # --- Estado de sesión para los dropdowns ---
    ss = st.session_state
    defaults = {
        "sol_tipo": "Selecciona...", "sol_area": "Selecciona...",
        "sol_perfil": "Selecciona...", "sol_rol": "Selecciona...",
        "sol_horario": "Selecciona...", "sol_turno": "",
        "sol_num_in": "No aplica", "sol_num_out": "No aplica",
    }

    # --- 🟢 LÓGICA DE RESETEO (SOLUCIÓN AL ERROR) ---
    # Esto se ejecuta AL PRINCIPIO de la recarga, antes de dibujar los widgets.
    if ss.get("reset_solicitud_flag"):
        for k in defaults:
            if k != "sol_tipo": ss[k] = defaults[k]
        del ss["reset_solicitud_flag"] # Apagamos la bandera
        st.success("✅ Solicitud registrada y enviada correctamente."); st.balloons()
    # ----------------------------------------------------

    for k, v in defaults.items():
        if k not in ss: ss[k] = v

    # --- Callbacks para resetear dropdowns en cascada ---
    def on_change_area():
        ss.sol_perfil = "Selecciona..."
        ss.sol_rol = "Selecciona..."
        ss.sol_horario = "Selecciona..."
        ss.sol_turno = ""
    
    def on_change_perfil():
        ss.sol_rol = "Selecciona..."
        ss.sol_horario = "Selecciona..."
        ss.sol_turno = ""
        
    def on_change_horario():
        ss.sol_turno = horarios_dict.get(ss.sol_horario, "") if ss.sol_horario != "Selecciona..." else ""

    # -----------------------------------------------------------------
    # --- PASO 1: SELECCIONAR TIPO ---
    # -----------------------------------------------------------------
    st.markdown("### 1) Tipo de Solicitud")
    st.selectbox(
        "Tipo de Solicitud en Zoho (*)",
        ["Selecciona...", "Alta", "Modificación", "Baja"],
        key="sol_tipo"
    )

    # -----------------------------------------------------------------
    # --- FORMULARIO 1: BAJA ---
    # -----------------------------------------------------------------
    if ss.sol_tipo == "Baja":
        st.markdown("### 2) Datos del Usuario a dar de baja")
        with st.form("solicitud_form_baja", clear_on_submit=True):
            nombre = st.text_input("Nombre Completo de Usuario (*)")
            correo_user = st.text_input("Correo institucional del usuario (*)")
            correo_solicitante = st.text_input("Correo de quien lo solicita (*)")
            
            st.caption("(*) Campos obligatorios")
            submitted_baja = st.form_submit_button("✔️ Enviar Baja", use_container_width=True)

            if submitted_baja:
                if not nombre or not correo_user or not correo_solicitante:
                    st.warning("⚠️ Faltan campos obligatorios.")
                else:
                    try:
                        fila_sol = [
                            now_mx_str(), "Baja", nombre.strip(), correo_user.strip(), 
                            "N/A", "N/A", "N/A", "", "", "", "", _email_norm(correo_solicitante),
                            "Pendiente", "", "", str(uuid4()), "", ""
                        ]
                        header_s = sheet_solicitudes.row_values(1); fila_sol = fila_sol[:len(header_s)]
                        with_backoff(sheet_solicitudes.append_row, fila_sol, value_input_option='USER_ENTERED')
                        
                        resumen_baja = f"Tipo: Baja<br>Nombre: {nombre}<br>Correo usuario: {correo_user}<br>Solicitante: {correo_solicitante}"
                        enviar_correo(f"Solicitud CRM: Baja - {nombre}", resumen_baja, correo_solicitante)
                        
                        # Activamos la bandera y recargamos
                        ss.reset_solicitud_flag = True
                        st.rerun()

                    except Exception as e: 
                        st.error(f"❌ Error al registrar baja: {e}")
        st.stop() 

    # -----------------------------------------------------------------
    # --- FORMULARIO 2: ALTA / MODIFICACIÓN ---
    # -----------------------------------------------------------------
    elif ss.sol_tipo in ["Alta", "Modificación"]:
        
        # --- CASCADA DE DROPDOWNS ---
        st.markdown("### 2) Definición del Puesto (cascada)")
        areas = ["Selecciona..."] + list(estructura_roles.keys())
        area_idx = areas.index(ss.sol_area) if ss.sol_area in areas else 0
        st.selectbox("Área (*)", areas, index=area_idx, key="sol_area", on_change=on_change_area)
        
        perfiles_disp = ["Selecciona..."]
        if ss.sol_area in estructura_roles:
            perfiles_disp += list(estructura_roles[ss.sol_area].keys())
        if ss.sol_perfil not in perfiles_disp: ss.sol_perfil = "Selecciona..."
        perfil_idx = perfiles_disp.index(ss.sol_perfil)
        st.selectbox("Perfil (*)", perfiles_disp, index=perfil_idx, key="sol_perfil", on_change=on_change_perfil)
        
        roles_disp = ["Selecciona..."]
        if ss.sol_area in estructura_roles and ss.sol_perfil in estructura_roles[ss.sol_area]:
            roles_disp += estructura_roles[ss.sol_area][ss.sol_perfil]
        if ss.sol_rol not in roles_disp: ss.sol_rol = "Selecciona..."
        rol_idx = roles_disp.index(ss.sol_rol)
        st.selectbox("Rol (*)", roles_disp, index=rol_idx, key="sol_rol")

        requiere_horario = ss.sol_perfil in {"Agente de Call Center", "Ejecutivo AC"}
        if requiere_horario:
            st.markdown("### 3) Horario de trabajo (*)")
            horarios_disp = ["Selecciona..."] + list(horarios_dict.keys())
            st.selectbox("Horario", horarios_disp, key="sol_horario", on_change=on_change_horario)
            st.text_input("Turno (Automático)", value=ss.sol_turno, disabled=True)
        
        # --- INICIO DEL FORMULARIO ---
        with st.form("solicitud_form_alta_mod", clear_on_submit=True):
            st.markdown("### 4) Datos del Usuario")
            c1, c2 = st.columns(2)
            with c1: nombre = st.text_input("Nombre Completo de Usuario (*)", key="sol_nombre_input_form")
            with c2: correo = st.text_input("Correo institucional del usuario (*)", key="sol_correo_input_form")

            show_numeros = ss.sol_rol in numeros_por_rol
            if show_numeros:
                st.markdown("### 5) Extensiones y Salida (*)")
                nums_cfg = numeros_por_rol.get(ss.sol_rol, {})
                lista_in_raw  = nums_cfg.get("Numero_IN", [])
                lista_out_raw = nums_cfg.get("Numero_Saliente", [])

                # Auto-selección si solo hay 1 número disponible
                if len(lista_in_raw) == 1:
                    num_in = lista_in_raw[0]
                    st.info(f"📞 Número IN asignado automáticamente: **{num_in}**")
                elif len(lista_in_raw) > 1:
                    nums_in_list = ["Selecciona número IN (*)"] + lista_in_raw
                    c3, _ = st.columns(2)
                    with c3: num_in = st.selectbox("Número IN (*)", nums_in_list, key="sol_num_in_form")
                else:
                    num_in = "No aplica"

                if len(lista_out_raw) == 1:
                    num_out = lista_out_raw[0]
                    st.info(f"📤 Número Saliente asignado automáticamente: **{num_out}**")
                elif len(lista_out_raw) > 1:
                    nums_out_list = ["Selecciona número Saliente (*)"] + lista_out_raw
                    _, c4 = st.columns(2)
                    with c4: num_out = st.selectbox("Número Saliente (*)", nums_out_list, key="sol_num_out_form")
                else:
                    num_out = "No aplica"
            else:
                num_in, num_out = "No aplica", "No aplica"

            st.markdown("### 6) Horario Especial")
            trabaja_sabado = st.checkbox("📅 ¿Trabajará en sábado?", key="sol_sabado_form")

            st.markdown("### 7) Quién Solicita")
            correo_solicitante_form = st.text_input("Correo de quien lo solicita (*)", key="sol_correo_sol_input_form")
            
            st.caption("(*) Campos obligatorios")
            submitted_sol = st.form_submit_button("✔️ Enviar Solicitud", use_container_width=True)

            if submitted_sol:
                tipo, area, perfil = ss.sol_tipo, ss.sol_area, ss.sol_perfil
                rol, horario, turno = ss.sol_rol, ss.sol_horario, ss.sol_turno
                
                if not nombre or not correo or not correo_solicitante_form:
                    st.warning("⚠️ Faltan campos básicos."); st.stop()
                if area == "Selecciona..." or perfil == "Selecciona..." or rol == "Selecciona...":
                    st.warning("⚠️ Faltan campos de Área/Perfil/Rol."); st.stop()
                if requiere_horario and horario == "Selecciona...":
                    st.warning("⚠️ Selecciona un horario válido."); st.stop()
                # Validar números obligatorios cuando hay más de una opción
                if show_numeros:
                    nums_cfg_check = numeros_por_rol.get(rol, {})
                    if len(nums_cfg_check.get("Numero_IN", [])) > 1 and num_in.startswith("Selecciona"):
                        st.warning("⚠️ Debes seleccionar un Número IN para este rol."); st.stop()
                    if len(nums_cfg_check.get("Numero_Saliente", [])) > 1 and num_out.startswith("Selecciona"):
                        st.warning("⚠️ Debes seleccionar un Número Saliente para este rol."); st.stop()

                try:
                    num_in_val  = "" if (not show_numeros or num_in == "No aplica") else str(num_in)
                    num_out_val = "" if (not show_numeros or num_out == "No aplica") else str(num_out)
                    horario_val = "" if (not requiere_horario or horario == "Selecciona...") else horario
                    turno_val   = "" if (not requiere_horario) else turno
                    check_sabado_val = "TRUE" if trabaja_sabado else "FALSE"  # columna S = CheckSS

                    fila_sol = [
                        now_mx_str(), tipo, nombre.strip(), correo.strip(),   # A B C D
                        area, perfil, rol,                                      # E F G
                        num_in_val, num_out_val, horario_val, turno_val,       # H I J K
                        _email_norm(correo_solicitante_form), "Pendiente",     # L M
                        "", "", str(uuid4()), "", "",                           # N O P Q R
                        check_sabado_val                                        # S = CheckSS
                    ]
                    header_s = sheet_solicitudes.row_values(1); fila_sol = fila_sol[:len(header_s)]
                    with_backoff(sheet_solicitudes.append_row, fila_sol, value_input_option='USER_ENTERED')
                    
                    sabado_str  = "Sí" if trabaja_sabado else "No"
                    in_str      = num_in_val  if num_in_val  else "No aplica"
                    out_str     = num_out_val if num_out_val else "No aplica"
                    resumen_email = (
                        f"Tipo: {tipo}<br>"
                        f"Nombre: {nombre}<br>"
                        f"Correo usuario: {correo}<br>"
                        f"Solicitante: {correo_solicitante_form}<br>"
                        f"Área: {area}<br>"
                        f"Perfil: {perfil}<br>"
                        f"Rol: {rol}<br>"
                        f"Número IN: {in_str}<br>"
                        f"Número Saliente: {out_str}<br>"
                        f"Trabaja sábado: {sabado_str}"
                    )
                    enviar_correo(f"Solicitud CRM: {tipo} - {nombre}", resumen_email, correo_solicitante_form)
                    
                    # 🟢 AQUÍ ESTÁ EL CAMBIO CLAVE: Activamos bandera y recargamos
                    ss.reset_solicitud_flag = True
                    st.rerun()
                    
                except Exception as e: 
                    st.error(f"❌ Error al registrar solicitud: {e}")

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
        st.warning("⚠️ **Favor de Revisar** Solo procede si el estatus actual del Lead es **'Descartado'**.")
        check_texto = "✅ Confirmo que ya revisé en Zoho y el estatus es 'Descartado'."
    elif cat == "Desfase":
        st.info("ℹ️ **REQUISITO:** Obligatorio adjuntar evidencia (PING vs Zoho) así como el ID UAG del aspirante en la descripción.")
        check_texto = "✅ Confirmo que adjuntaré la evidencia visual de PING así como ID UAG."

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
            with st.spinner("🤖 Validando ticket..."):
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

# ===================== SECCIÓN FUSIONADA: ACCESOS Y BUZÓN =====================
elif seccion == "🔑 Accesos y Buzón":
    st.markdown("## 🔑 Gestión de Accesos y Buzón de Ayuda")

    # Subtabs: uno para accesos/buzón general, otro para solicitar nuevo rol
    subtab_acc, subtab_rol = st.tabs(["📋 Accesos y Buzón", "🆕 Solicitar Nuevo Rol / Perfil"])

    # ---- SUBTAB 1: ACCESOS Y BUZÓN (flujo original) ----
    with subtab_acc:
        st.info("Aquí puedes solicitar permisos especiales, desbloqueo de cursos, o dejarnos tus quejas y sugerencias sobre el CRM.")

        tipos_solicitud_unificada = [
            "Selecciona...",
            "--- ACCESOS Y PERMISOS ---",
            "Acceso a Zoho Analytics",
            "Ajuste de Permisos (Faltan)",
            "Quitar Permisos (Seguridad)",
            "Capacitación: Curso Reprobado (Reactivar)",
            "Capacitación: Faltan Cursos Asignados",
            "--- BUZÓN DE OPINIÓN ---",
            "Queja del Servicio",
            "Sugerencia de Mejora",
            "Felicitación",
            "Otro tema"
        ]

        with st.form("form_accesos_buzon", clear_on_submit=True):
            c1, c2 = st.columns(2)
            correo_solicitante = c1.text_input("Tu Correo Institucional (*)")
            tipo_solicitud = c2.selectbox("¿Qué necesitas? (*)", tipos_solicitud_unificada)

            if "Curso" in tipo_solicitud:
                st.caption("ℹ️ Indica el nombre del curso y el usuario afectado.")
            elif "Analytics" in tipo_solicitud:
                st.caption("ℹ️ Indica para qué espacio de trabajo requieres el acceso.")
            elif "Queja" in tipo_solicitud:
                st.caption("ℹ️ Lamentamos el inconveniente. Por favor detalla qué sucedió para solucionarlo.")

            asunto_acc = st.text_input("Asunto Breve (*)")
            justificacion = st.text_area("Detalle / Justificación (*)", height=100, placeholder="Explica tu solicitud, queja o sugerencia aquí...")
            enviar_acc = st.form_submit_button("Enviar Solicitud")

        if enviar_acc:
            if not correo_solicitante or "Selecciona" in tipo_solicitud or "---" in tipo_solicitud or not asunto_acc or not justificacion:
                st.warning("⚠️ Por favor completa todos los campos y selecciona una opción válida.")
            else:
                try:
                    id_unico = str(uuid4())
                    row_unificado = [
                        now_mx_str(), _email_norm(correo_solicitante), tipo_solicitud,
                        asunto_acc, justificacion, "", "Pendiente", "", "", id_unico, ""
                    ]
                    with_backoff(sheet_quejas.append_row, row_unificado)
                    msg_exito = "✅ Solicitud enviada."
                    if "Queja" in tipo_solicitud: msg_exito = "✅ Reporte recibido."
                    elif "Sugerencia" in tipo_solicitud: msg_exito = "✅ Sugerencia recibida."
                    st.success(msg_exito)
                    resumen = f"Tipo: {tipo_solicitud}<br>Asunto: {asunto_acc}<br>Detalle: {justificacion}"
                    enviar_correo(f"CRM Solicitud: {tipo_solicitud}", resumen, correo_solicitante)
                    st.balloons(); time.sleep(2); st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")

    # ---- SUBTAB 2: SOLICITAR NUEVO ROL / PERFIL ----
    with subtab_rol:
        st.warning("⚠️ Usa este formulario **solo si el rol o área que necesitas NO existe** en el catálogo de Solicitudes CRM.")
        st.markdown("El equipo evaluará la solicitud y, si procede, lo agregará al catálogo antes de crear el acceso en Zoho.")

        with st.form("form_nuevo_rol", clear_on_submit=True):
            c1, c2 = st.columns(2)
            nr_correo     = c1.text_input("Tu Correo Institucional (*)")
            nr_correo_usr = c2.text_input("Correo del usuario que recibirá el acceso (*)")

            st.markdown("#### Datos del nuevo rol / área")
            c3, c4 = st.columns(2)
            nr_area   = c3.text_input("Área o Departamento nuevo (*)", placeholder="Ej: Posgrado Internacional")
            nr_perfil = c4.text_input("Perfil nuevo (*)", placeholder="Ej: Coordinador de Admisiones")
            nr_rol    = st.text_input("Rol o Nombre del puesto (*)", placeholder="Ej: Coordinador Senior")

            nr_justificacion = st.text_area(
                "Justificación (*)",
                height=120,
                placeholder="Explica por qué se necesita este rol, qué funciones tendrá en Zoho y por qué no existe en el catálogo actual."
            )

            st.caption("(*) Campos obligatorios")
            enviar_rol = st.form_submit_button("📨 Enviar Solicitud de Nuevo Rol", use_container_width=True)

        if enviar_rol:
            if not nr_correo or not nr_correo_usr or not nr_area or not nr_perfil or not nr_rol or not nr_justificacion:
                st.warning("⚠️ Por favor completa todos los campos obligatorios.")
            else:
                try:
                    id_nr = str(uuid4())
                    detalle_nr = (
                        f"ÁREA: {nr_area} | PERFIL: {nr_perfil} | ROL: {nr_rol} | "
                        f"USUARIO DESTINO: {nr_correo_usr} | JUSTIFICACIÓN: {nr_justificacion}"
                    )
                    row_nuevo_rol = [
                        now_mx_str(),                       # 1. Fecha
                        _email_norm(nr_correo),             # 2. Correo solicitante
                        "Nuevo Rol / Perfil (Creación)",    # 3. Tipo
                        f"Nuevo Rol: {nr_rol} - {nr_area}", # 4. Asunto
                        detalle_nr,                         # 5. Detalle
                        "Nuevo Rol",                        # 6. Categoría
                        "Pendiente",                        # 7. Estado
                        "",                                 # 8. Calificación
                        "",                                 # 9. Categoría 2
                        id_nr,                              # 10. ID
                        ""                                  # 11. Respuesta Admin
                    ]
                    with_backoff(sheet_quejas.append_row, row_nuevo_rol)

                    resumen_nr = (
                        f"Área: {nr_area}<br>Perfil: {nr_perfil}<br>Rol: {nr_rol}<br>"
                        f"Usuario destino: {nr_correo_usr}<br>Justificación: {nr_justificacion}"
                    )
                    enviar_correo(f"Solicitud Nuevo Rol: {nr_rol} ({nr_area})", resumen_nr, nr_correo)

                    st.success("✅ Solicitud de nuevo rol enviada. El equipo la revisará y te notificará.")
                    st.balloons(); time.sleep(2); st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")



# ===================== SECCIÓN 5: ADMIN (COMPLETA Y CORREGIDA) =====================
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")

    # 1. BOTÓN DE EMERGENCIA
    col_refresh, col_spacer = st.columns([1, 4])
    if col_refresh.button("🔄 Refrescar Conexión"):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.toast("♻️ Conexión reiniciada...")
        time.sleep(1)
        st.rerun()

    # Correos de Jefes para Copia (CC)
    lista_supervisores = [
        "luis.alpizar@edu.uag.mx", 
        "carlos.sotelo@edu.uag.mx", 
        "esther.diaz@edu.uag.mx"
    ]

    pwd = st.text_input("Contraseña Admin", type="password")
    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    
    if pwd == ADMIN_PASS or st.session_state.get("is_admin", False):
        st.session_state.is_admin = True
        
        tab1, tab2, tab3, tab4 = st.tabs(["Solicitudes", "Incidencias", "Quejas", "🧠 Cerebro IA"])

        # ================= TAB 4: ENTRENAMIENTO IA (PERSISTENTE) =================
        with tab4:
            st.subheader("🧠 Gestión del Conocimiento (Cerebro IA)")
            st.info("Este texto se guarda en la celda A1 de la hoja 'Cerebro' en Google Sheets. Es la memoria de tu Asistente.")

            # Leemos lo actual
            try:
                contenido_actual = sheet_cerebro.acell('A1').value or ""
            except:
                contenido_actual = ""

            col_edit, col_preview = st.columns([2, 1])
        
            with col_edit:
                st.markdown("### ✏️ Editor Maestro")
                nuevo_contenido = st.text_area("Base de Conocimiento", value=contenido_actual, height=500, key="txt_cerebro_sheet")
            
                if st.button("💾 Guardar en la Nube"):
                    with st.spinner("Guardando..."):
                        try:
                            with_backoff(sheet_cerebro.update_acell, 'A1', nuevo_contenido)
                            cargar_conocimiento.clear() # Limpiamos caché
                            st.success("✅ Guardado exitoso.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {e}")

            with col_preview:
                st.markdown("### 📥 Ingestar PDFs")
                pdfs = st.file_uploader("Subir PDFs", type="pdf", accept_multiple_files=True)
            
                if st.button("⚙️ Procesar y Anexar"):
                    if not pdfs:
                        st.warning("Sin archivos.")
                    else:
                        texto_pdf_total = ""
                        bar = st.progress(0)
                        for idx, pdf_file in enumerate(pdfs):
                            try:
                                reader = PdfReader(pdf_file)
                                texto_local = ""
                                for page in reader.pages:
                                    texto_local += page.extract_text() + "\n"
                                texto_pdf_total += f"\n\n--- FUENTE: {pdf_file.name} ---\n{texto_local}"
                                bar.progress((idx + 1) / len(pdfs))
                            except: pass
                    
                        contenido_final = nuevo_contenido + texto_pdf_total
                    
                        # Validar límite (aprox 50k caracteres por celda)
                        if len(contenido_final) > 49000:
                            st.warning(f"⚠️ ¡Ojo! El texto ({len(contenido_final)}) está cerca del límite de la celda (50k).")
                    
                        with_backoff(sheet_cerebro.update_acell, 'A1', contenido_final)
                        cargar_conocimiento.clear()
                        st.success("✅ PDFs procesados.")
                        time.sleep(2)
                        st.rerun()

        
            # ================= TAB 1: SOLICITUDES (CORREGIDO IDS y EMAILS) =================
        with tab1:
            st.subheader("Gestión de Solicitudes")
            with st.spinner("Cargando..."):
                dfs = get_records_simple(sheet_solicitudes)
            
            if dfs.empty:
                st.warning("⚠️ No hay datos o conexión lenta.")
            else:
                st.dataframe(dfs, use_container_width=True)
                
                # Buscamos la columna IDS (Clave única)
                col_id_name = "IDS" if "IDS" in dfs.columns else "ID"
                
                if col_id_name in dfs.columns:
                    ids = dfs[dfs[col_id_name] != ""][col_id_name].unique().tolist()
                    if ids:
                        st.divider()
                        # Selector en la ÚLTIMA solicitud
                        idx_def = len(ids)-1 if len(ids) > 0 else 0
                        sel_id = st.selectbox("ID Solicitud", ids, index=idx_def)
                        
                        row_s = dfs[dfs[col_id_name] == sel_id].iloc[0]
                        
                        st.info(f"**{row_s.get('TipoS')}** - {row_s.get('NombreS')} ({row_s.get('CorreoS')})")
                        st.caption(f"Solicitado por: {row_s.get('SolicitanteS')}")
                        
                        c_st, c_at = st.columns(2)
                        st_act = row_s.get("EstadoS", "Pendiente")
                        opts = ["Pendiente", "En proceso", "Atendido"]
                        idx_st = opts.index(st_act) if st_act in opts else 0
                        
                        nuevo_estado = c_st.selectbox("Estado", opts, index=idx_st, key="st_sol_main")
                        
                        # Guardamos en CredencialesZohoS
                        val_resp = row_s.get("CredencialesZohoS", "")
                        mensaje_respuesta = st.text_area("Resolución / Credenciales", value=val_resp, key="resp_sol_main")
                        
                        c1, c2 = st.columns(2)
                        if c1.button("💾 Actualizar Solicitud"):
                            cell = with_backoff(sheet_solicitudes.find, sel_id)
                            if cell:
                                header = sheet_solicitudes.row_values(1)
                                try:
                                    # Buscamos índices dinámicamente
                                    col_st = header.index("EstadoS") + 1
                                    col_cred = header.index("CredencialesZohoS") + 1
                                    
                                    sheet_solicitudes.update_cell(cell.row, col_st, nuevo_estado)
                                    sheet_solicitudes.update_cell(cell.row, col_cred, mensaje_respuesta)
                                    
                                    # Correo al SolicitanteS
                                    correo_sol = row_s.get("SolicitanteS")
                                    if nuevo_estado == "Atendido" and mensaje_respuesta and correo_sol:
                                        try:
                                            yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
                                            headers = {"From": f"Equipo CRM <{st.secrets['email']['user']}>"}
                                            html = f"""
                                            <div style="font-family: Arial;">
                                                <h3 style="color: green;">¡Solicitud Atendida!</h3>
                                                <p>Tu solicitud <strong>{row_s.get('TipoS')}</strong> para <strong>{row_s.get('NombreS')}</strong> ha sido completada.</p>
                                                <pre style="background:#f4f4f4;padding:10px;">{mensaje_respuesta}</pre>
                                                <p>Saludos,<br>CRM UAG</p>
                                            </div>
                                            """
                                            yag.send(to=correo_sol, cc=lista_supervisores, subject=f"✅ Finalizado: {row_s.get('TipoS')}", contents=[html], headers=headers)
                                            st.toast("📧 Enviado.")
                                        except Exception as e: st.error(f"Error correo: {e}")
                                    
                                    st.success("✅ Actualizado"); time.sleep(1); st.rerun()
                                except Exception as e: st.error(f"Error columnas Excel: {e}")

                        if c2.button("🗑️ Eliminar Solicitud"):
                            cell = with_backoff(sheet_solicitudes.find, sel_id)
                            if cell:
                                with_backoff(sheet_solicitudes.delete_rows, cell.row)
                                st.warning("Eliminado"); time.sleep(1); st.rerun()

        # ================= TAB 2: INCIDENCIAS (CON BOTÓN IA) =================
        with tab2:
            st.subheader("Gestión de Incidencias")
            with st.spinner("Cargando..."):
                dfi = get_records_simple(sheet_incidencias)
            
            if dfi.empty:
                st.warning("⚠️ No hay datos.")
            else:
                st.dataframe(dfi, use_container_width=True)
                if "IDI" in dfi.columns:
                    ids_i = dfi[dfi["IDI"] != ""]["IDI"].unique().tolist()
                    if ids_i:
                        st.divider()
                        idx_def_i = len(ids_i)-1 if len(ids_i) > 0 else 0
                        sel_idi = st.selectbox("ID Incidencia", ids_i, index=idx_def_i, key="sel_inc")
                        row_i = dfi[dfi["IDI"] == sel_idi].iloc[0]
                        
                        st.info(f"**{row_i.get('Asunto')}** | {row_i.get('CorreoI')}")
                        
                        # --- BOTÓN DE IA (RAG) ---
                        if st.button("✨ Sugerir Respuesta (IA)"):
                            # Nota: Asegúrate de tener la función 'generar_respuesta_ia' definida o importada
                            # Si no la tienes en este archivo, comenta estas líneas para evitar error.
                            try:
                                with st.spinner("Leyendo manual y casos previos..."):
                                    st.session_state.rag = generar_respuesta_ia(row_i.get("Asunto"), row_i.get("DescripcionI"), dfi)
                            except NameError:
                                st.warning("La función de IA RAG no está definida en este contexto.")

                        c_st_i, c_at_i = st.columns(2)
                        st_act_i = row_i.get("EstadoI", "Pendiente")
                        opts_i = ["Pendiente", "En proceso", "Atendido"]
                        idx_i = opts_i.index(st_act_i) if st_act_i in opts_i else 0
                        
                        nuevo_estado_i = c_st_i.selectbox("Estado", opts_i, index=idx_i, key="st_inc_main")
                        
                        val_rag = st.session_state.get("rag", row_i.get("RespuestadeSolicitudI",""))
                        respuesta = st.text_area("Respuesta Técnica", value=val_rag, key="resp_inc_main")
                        
                        c1, c2 = st.columns(2)
                        if c1.button("💾 Responder Incidencia"):
                            cell = with_backoff(sheet_incidencias.find, sel_idi)
                            if cell:
                                header = sheet_incidencias.row_values(1)
                                col_st = header.index("EstadoI") + 1
                                col_resp = header.index("RespuestadeSolicitudI") + 1
                                sheet_incidencias.update_cell(cell.row, col_st, nuevo_estado_i)
                                sheet_incidencias.update_cell(cell.row, col_resp, respuesta)
                                
                                correo_usu = row_i.get("CorreoI")
                                if nuevo_estado_i == "Atendido" and respuesta and correo_usu:
                                    try:
                                        yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
                                        headers = {"From": f"Equipo CRM <{st.secrets['email']['user']}>"}
                                        html = f"""<div style="font-family: Arial;"><h3 style="color: green;">Resuelto</h3><p>Asunto: <strong>{row_i.get('Asunto')}</strong></p><p style="background:#e8f4fd;padding:10px;">{respuesta}</p></div>"""
                                        yag.send(to=correo_usu, cc=lista_supervisores, subject=f"✅ Resuelto: {row_i.get('Asunto')}", contents=[html], headers=headers)
                                        st.toast("📧 Notificado.")
                                    except: pass
                                st.success("✅ Actualizado"); time.sleep(1); st.rerun()

                        if c2.button("🗑️ Eliminar Incidencia"):
                            cell = with_backoff(sheet_incidencias.find, sel_idi)
                            if cell:
                                with_backoff(sheet_incidencias.delete_rows, cell.row)
                                st.warning("Eliminado"); time.sleep(1); st.rerun()

  # ================= TAB 3: GESTIÓN UNIFICADA (En hoja Quejas) =================
        with tab3:
            st.subheader("Gestión de Accesos, Quejas y Sugerencias")
        
            # Leemos de QUEJAS
            dfq = get_records_simple(sheet_quejas)
        
            if dfq.empty:
                st.info("No hay registros pendientes.")
            else:
                st.dataframe(dfq, use_container_width=True)
            
                # Buscamos la columna ID (En tu hoja Quejas suele ser IDQ o ID)
                # Ajusta "IDQ" si así se llama en tu Excel, o "ID" si es genérico.
                col_id_target = "IDQ" if "IDQ" in dfq.columns else "ID"
            
                if col_id_target in dfq.columns:
                    ids_q = dfq[dfq[col_id_target] != ""][col_id_target].unique().tolist()
                
                    if ids_q:
                        st.divider()
                        # Selector inteligente
                        sel_id_q = st.selectbox("Seleccionar Registro", ids_q, format_func=lambda x: f"{x} - {dfq[dfq[col_id_target]==x].iloc[0].get('TipoQ', 'Registro')}")
                    
                        row_q = dfq[dfq[col_id_target] == sel_id_q].iloc[0]
                    
                        # Nombres de columnas basados en tu hoja Quejas (ajusta si difieren)
                        tipo_val = row_q.get('TipoQ') or row_q.get('Tipo')
                        correo_val = row_q.get('CorreoQ') or row_q.get('Correo')
                        desc_val = row_q.get('DescripciónQ') or row_q.get('Justificacion') or row_q.get('Detalle')
                        estado_val = row_q.get('EstadoQ') or row_q.get('Estado') or "Pendiente"
                        resp_val = row_q.get('RespuestaQ') or row_q.get('RespuestaAdmin') or ""
                    
                        st.markdown(f"**Tipo:** {tipo_val} | **Solicitante:** {correo_val}")
                        st.warning(f"**Detalle:** {desc_val}")
                    
                        c_st_q, c_dummy = st.columns(2)
                        opts_q = ["Pendiente", "Aprobado", "Rechazado", "En Revisión", "Atendido"]
                        idx_q = opts_q.index(estado_val) if estado_val in opts_q else 0
                    
                        nuevo_estado = c_st_q.selectbox("Estado", opts_q, index=idx_q, key="st_fusion_q")
                        nueva_resp = st.text_area("Respuesta Admin", value=resp_val, key="rsp_fusion_q")
                    
                        if st.button("💾 Guardar Cambios"):
                            cell = with_backoff(sheet_quejas.find, sel_id_q)
                            if cell:
                                # En hoja Quejas: Columna 7 es Estado, Columna 11 es Respuesta
                                sheet_quejas.update_cell(cell.row, 7, nuevo_estado)
                                sheet_quejas.update_cell(cell.row, 11, nueva_resp)
                            
                                # Notificar
                                if nuevo_estado in ["Aprobado", "Rechazado", "Atendido"]:
                                    asunto_mail = f"Actualización: {tipo_val}"
                                    body_mail = f"<p>Estado actualizado a: <strong>{nuevo_estado}</strong>.</p><p>Respuesta: {nueva_resp}</p>"
                                    try:
                                        yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
                                        yag.send(to=correo_val, subject=asunto_mail, contents=[body_mail])
                                        st.toast("📧 Notificación enviada.")
                                    except: pass
                            
                                st.success("Registro actualizado.")
                                time.sleep(1)
                                st.rerun()
