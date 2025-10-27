import os
import json
import time
import random
from uuid import uuid4
from datetime import datetime, timedelta # Necesario para GCS Signed URL si la usaras
import re
import unicodedata
from pathlib import Path
import io

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from google.cloud import storage # Para GCS
import yagmail
from zoneinfo import ZoneInfo

# --- Límites de tamaño de archivo en MB ---
MAX_IMAGE_MB = 10  # Límite para imágenes (ej. 10 MB)
MAX_VIDEO_MB = 50  # Límite para videos (ej. 50 MB para ~30 seg)
# Convertir MB a Bytes (1 MB = 1024 * 1024 Bytes)
MAX_IMAGE_BYTES = MAX_IMAGE_MB * 1024 * 1024
MAX_VIDEO_BYTES = MAX_VIDEO_MB * 1024 * 1024

# -------------------------
# Utilidades y normalizadores
# -------------------------
EMAIL_RE = re.compile(r'([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})', re.I)

def _email_norm(s: str) -> str:
    if s is None: return ""
    text = str(s)
    m = EMAIL_RE.search(text)
    if m: return m.group(1).strip().lower()
    return text.strip().lower()

def _norm(x):
     return str(x).strip().lower() if pd.notna(x) else ""

def _is_unrated(val: str) -> bool:
     v = _norm(val)
     return v in ("", "pendiente", "na", "n/a", "sin calificacion", "sin calificación", "none", "null", "-")

def with_backoff(fn, *args, **kwargs):
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) or ("403" in str(e) and "rateLimitExceeded" in str(e)):
                 wait = min(1*(2**i) + random.random(), 16)
                 print(f"API Error ({e}). Retrying in {wait:.2f} seconds...")
                 time.sleep(wait)
                 continue
            st.error(f"API Error no recuperable ({e}). Por favor, revisa los permisos o cuotas.")
            raise # Lanza el error si no es recuperable
        except Exception as e: # Capturar otros errores (ej. conexión)
            wait = min(1*(2**i) + random.random(), 16)
            print(f"Connection Error: {e}. Retrying in {wait:.2f} seconds...")
            time.sleep(wait)
            continue
    # Si todos los reintentos fallan
    st.error(f"Falló la operación '{fn.__name__}' después de varios reintentos.")
    raise Exception(f"Failed after multiple retries for {fn.__name__}")


def load_json_safe(path: str) -> dict:
    try:
        json_file = Path(path)
        if not json_file.is_file():
            print(f"Warning: JSON file not found at {path}")
            return {}
        with open(json_file, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load JSON file at {path}. Error: {e}")
        return {}

TZ_MX = ZoneInfo("America/Mexico_City")
def now_mx_str() -> str:
    return datetime.now(TZ_MX).strftime("%d/%m/%Y %H:%M:%S")

# -------------------------
# Config / secrets
# -------------------------
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")

APP_MODE    = st.secrets.get("mode", "dev")
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))
SHEET_ID    = (st.secrets.get("sheets", {}).get("prod_id")
             if APP_MODE == "prod"
             else st.secrets.get("sheets", {}).get("dev_id"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/cloud-platform"
]

GCS_BUCKET_NAME = st.secrets.get("google_cloud_storage", {}).get("bucket_name", "")
if not GCS_BUCKET_NAME:
    msg = "❗ Falta [google_cloud_storage] bucket_name en secrets.toml. Subida de archivos no funcionará."
    if APP_MODE == "prod": st.error(msg); st.stop()
    else: st.warning(msg)

if not SHEET_ID:
    st.error("❗ No se encontró SHEET_ID en [sheets] de secrets.toml")
    st.stop()

# -------------------------
# Conexiones Google (Sheets & GCS)
# -------------------------
@st.cache_resource(ttl=3600)
def get_google_credentials():
    creds_dict = st.secrets.get("google_service_account")
    if not creds_dict: st.error("❗ Falta [google_service_account] en secrets."); st.stop()
    try: return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except Exception as e: st.error(f"❌ Error credenciales: {e}"); st.stop()

@st.cache_resource(ttl=3600)
def get_gspread_client():
    try: return gspread.authorize(get_google_credentials())
    except Exception as e: st.error(f"❌ Error gspread client: {e}"); st.stop()

@st.cache_resource(ttl=3600)
def get_book():
    client = get_gspread_client()
    try: return with_backoff(client.open_by_key, SHEET_ID)
    except Exception as e: st.error(f"❌ Error abrir Sheet (ID: {SHEET_ID}): {e}"); st.stop()

book = get_book()

@st.cache_resource(ttl=3600)
def get_gcs_client():
    credentials = get_google_credentials()
    project_id = st.secrets.get("google_service_account",{}).get("project_id")
    try:
        client = storage.Client(project=project_id, credentials=credentials)
        # Verificar acceso al bucket al iniciar (solo si GCS_BUCKET_NAME está definido)
        if GCS_BUCKET_NAME: with_backoff(client.get_bucket, GCS_BUCKET_NAME)
        return client
    except Exception as e: st.error(f"❌ Error cliente GCS (Bucket: '{GCS_BUCKET_NAME}'): {e}"); return None

# --- Función para subir archivo a GCS ---
def upload_to_gcs(file_buffer, filename_in_bucket, content_type):
    client = get_gcs_client()
    if not client or not GCS_BUCKET_NAME:
        st.error("❌ Configuración GCS incompleta. No se puede subir.")
        return None
    try:
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(filename_in_bucket)
        file_buffer.seek(0)
        with_backoff(blob.upload_from_file, file_buffer, content_type=content_type, rewind=True)
        with_backoff(blob.make_public) # Asegura que sea público
        public_url = blob.public_url
        print(f"GCS Upload OK: {public_url}")
        st.toast("Archivo subido a GCS.", icon="☁️")
        return public_url
    except Exception as e:
        st.error(f"❌ Error al subir a GCS: {e}")
        return None

# --- Conexión a las pestañas ---
sheets = {}
required_sheets = ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]
all_sheets_found = True
for sheet_name in required_sheets:
    try: sheets[sheet_name] = book.worksheet(sheet_name)
    except gspread.WorksheetNotFound: st.error(f"❌ Hoja '{sheet_name}' no encontrada."); all_sheets_found = False
    except Exception as e: st.error(f"❌ Error obteniendo hoja '{sheet_name}': {e}"); all_sheets_found = False
if not all_sheets_found: st.stop()

# Asignar variables
sheet_solicitudes, sheet_incidencias, sheet_quejas, sheet_accesos, sheet_usuarios = (
    sheets["Sheet1"], sheets["Incidencias"], sheets["Quejas"], sheets["Accesos"], sheets["Usuarios"]
)

# --- Lector de datos (Sin caché) ---
#@st.cache_data(ttl=180)
def get_records_simple(_ws) -> pd.DataFrame:
    ws_title = _ws.title
    try:
        all_values = with_backoff(_ws.get_all_values)
        if not all_values: return pd.DataFrame()
        header = [str(h).strip() for h in all_values[0]] # Limpiar header
        data = all_values[1:]
        num_cols = len(header)
        # Asegurar ancho y convertir todo a string inicialmente
        data_fixed = [[str(cell) for cell in (row[:num_cols] + [""] * (num_cols - len(row)))] for row in data]
        df = pd.DataFrame(data_fixed, columns=header)
        # Intentar convertir tipos comunes después de la carga inicial
        for col in df.columns:
             # Intentar convertir a numérico si parece posible (opcional, puede fallar)
             # df[col] = pd.to_numeric(df[col], errors='ignore')
             pass # Por ahora, dejar todo como string es más seguro
        return df
    except Exception as e:
        st.error(f"Error al leer '{ws_title}': {e}")
        return pd.DataFrame() # Devolver DF vacío en caso de error

# -------------------------
# Datos locales (JSON) y Usuarios (desde GSheets)
# -------------------------
data_folder = Path("data") # Usar Path para rutas
estructura_roles = load_json_safe(data_folder / "estructura_roles.json")
numeros_por_rol  = load_json_safe(data_folder / "numeros_por_rol.json")
horarios_dict    = load_json_safe(data_folder / "horarios.json")

#@st.cache_data(ttl=300) # Sin caché
def cargar_usuarios_df():
    try:
        df = get_records_simple(sheet_usuarios)
        if "Contraseña" not in df.columns or "Correo" not in df.columns:
             st.error("❌ Hoja 'Usuarios' debe tener 'Contraseña' y 'Correo'.")
             return pd.DataFrame(columns=["Contraseña","Correo"])
        df['Contraseña'] = df['Contraseña'].astype(str).str.strip()
        df = df[df['Contraseña'] != ''] # Filtrar vacíos
        return df
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
usuarios_dict = {str(p): _email_norm(c)
                 for p, c in zip(usuarios_df.get("Contraseña", []), usuarios_df.get("Correo", []))
                 if str(p)} # Ignorar contraseñas vacías
if not usuarios_dict: st.warning("⚠️ No se cargaron usuarios/contraseñas. Login no funcionará.")

# -------------------------
# Email, Sesión, Login/Logout (Funciones sin cambios internos)
# -------------------------
def enviar_correo(asunto, mensaje_resumen, copia_a):
    if not SEND_EMAILS: print("Envío de correo deshabilitado."); return
    email_user = st.secrets.get("email", {}).get("user"); email_pass = st.secrets.get("email", {}).get("password")
    if not email_user or not email_pass: st.warning("⚠️ Faltan credenciales de email."); return
    try:
        yag = yagmail.SMTP(user=email_user, password=email_pass)
        cuerpo = f"""<p>Hola,</p><p>...</p><p><strong>Resumen:</strong><br>{mensaje_resumen}</p><p>Saludos,<br><b>Equipo CRM UAG</b></p>"""
        to_list = ["luis.alpizar@edu.uag.mx"]; cc_list = ["carlos.sotelo@edu.uag.mx", "esther.diaz@edu.uag.mx"]
        if copia_a and isinstance(copia_a, str) and '@' in copia_a: to_list.append(_email_norm(copia_a))
        yag.send(to=to_list, cc=cc_list, subject=asunto, contents=[cuerpo], headers={"From": f"CRM UAG <{email_user}>"})
        print(f"Correo enviado a: {to_list}, CC: {cc_list}")
    except Exception as e: st.warning(f"No se pudo enviar correo: {e}")

def log_event(usuario, evento, session_id, dur_min=""):
    try:
        fila = [now_mx_str(), usuario or "", evento, session_id or "", str(dur_min or "")]
        with_backoff(sheet_accesos.append_row, fila, value_input_option='USER_ENTERED')
    except Exception as e: st.warning(f"No se pudo registrar acceso: {e}")

def do_login(correo):
    st.session_state.usuario_logueado = _email_norm(correo)
    st.session_state.session_id = str(uuid4())
    st.session_state.login_time = datetime.now(TZ_MX)
    log_event(st.session_state.usuario_logueado, "login", st.session_state.session_id)

def do_logout():
    dur = ""; user = st.session_state.get("usuario_logueado"); sid = st.session_state.get("session_id")
    if st.session_state.get("login_time"):
        try: dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
        except TypeError: dur = ""
    log_event(user, "logout", sid, str(dur))
    keys_to_delete = ["usuario_logueado", "session_id", "login_time", "nav_index", "is_admin"]
    for key in keys_to_delete:
        if key in st.session_state: del st.session_state[key]
    st.success("Sesión cerrada.")

# Inicialización de estado de sesión
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None
if 'nav_index' not in st.session_state: st.session_state.nav_index = 0

# -------------------------
# Navegación Principal (Sidebar Radio)
# -------------------------
nav_options = ["🔍 Ver el estado de mis solicitudes",
               "🌟 Solicitudes CRM",
               "🛠️ Incidencias CRM",
               "📝 Sugerencias y Mejoras", # Cambiado de "Quejas"
               "🔐 Zona Admin"]
def format_nav(index): return nav_options[index]
current_nav_index = st.session_state.get('nav_index', 0)
nav_index = st.sidebar.radio(
    "Navegación", range(len(nav_options)), format_func=format_nav,
    index=current_nav_index, key="nav_radio_selector"
)
st.session_state.nav_index = nav_index
seccion = nav_options[nav_index]

# ===================== SECCIÓN: CONSULTA =====================
if seccion == "🔍 Ver el estado de mis solicitudes":
    st.markdown("## 🔍 Consulta de Estado")
    # --- Login ---
    if st.session_state.get("usuario_logueado") is None:
        with st.form("login_form"):
            clave = st.text_input("Ingresa tu contraseña", type="password")
            submitted = st.form_submit_button("Entrar")
            if submitted:
                clave_str = str(clave).strip()
                if clave_str in usuarios_dict:
                    do_login(usuarios_dict[clave_str])
                    st.success(f"Bienvenido!"); st.rerun()
                elif clave_str: st.error("❌ Contraseña incorrecta")
                else: st.warning("Ingresa tu contraseña.")
    # --- Contenido Logueado ---
    elif st.session_state.get("usuario_logueado"):
        st.info(f"Usuario: **{st.session_state.usuario_logueado}**")
        if st.button("Cerrar sesión"): do_logout(); st.rerun()

        # -------- Solicitudes --------
        st.subheader("📋 Solicitudes registradas")
        df_s = get_records_simple(sheet_solicitudes) # Carga aquí para usar después
        if not df_s.empty and "SolicitanteS" in df_s.columns:
            df_s['SolicitanteS'] = df_s['SolicitanteS'].astype(str)
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == st.session_state.usuario_logueado].copy()
            try: # Ordenar
                df_mias['FechaS_dt'] = pd.to_datetime(df_mias['FechaS'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                df_mias = df_mias.sort_values(by="FechaS_dt", ascending=False).drop(columns=['FechaS_dt'])
            except: pass
        else:
            if not df_s.empty: st.warning("⚠️ No se encontró 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()
        st.caption(f"{len(df_mias)} solicitudes encontradas.")

        if not df_mias.empty:
            header_s_list = list(df_s.columns) # Encabezados para buscar columnas de calificación
            for index, row in df_mias.iterrows():
                with st.container():
                    # ... (Tu código para mostrar detalles y calificación de solicitud, usando header_s_list.index(...) para cols)...
                    pass # Reemplaza con tu lógica de visualización y calificación

        st.divider()

        # -------- Incidencias --------
        st.subheader("🛠️ Incidencias reportadas")
        df_i = get_records_simple(sheet_incidencias) # Carga aquí
        if not df_i.empty and "CorreoI" in df_i.columns:
             df_i['CorreoI'] = df_i['CorreoI'].astype(str)
             df_mis_inc = df_i[df_i["CorreoI"].map(_email_norm) == st.session_state.usuario_logueado].copy()
             try: # Ordenar
                 df_mis_inc['FechaI_dt'] = pd.to_datetime(df_mis_inc['FechaI'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                 df_mis_inc = df_mis_inc.sort_values(by="FechaI_dt", ascending=False).drop(columns=['FechaI_dt'])
             except: pass
        else:
            if not df_i.empty: st.warning("⚠️ No se encontró 'CorreoI' en 'Incidencias'.")
            df_mis_inc = pd.DataFrame()
        st.caption(f"{len(df_mis_inc)} incidencias encontradas.")

        if not df_mis_inc.empty:
            header_i_list = list(df_i.columns) # Encabezados para buscar columnas de calificación
            for index_i, row_i in df_mis_inc.iterrows():
                 with st.container():
                    # ... (Código para obtener estado, sat, id, url) ...
                    media_url = str(row_i.get("MediaFilenameI", "")).strip()
                    id_unico_i = str(row_i.get("IDI", f"idx_i_{index_i}")).strip()

                    titulo = f"🛠️ {row_i.get('Asunto','?')} ({row_i.get('FechaI','?')}) — Estado: {row_i.get('EstadoI','?')}"
                    with st.expander(titulo):
                        # ... (Tu markdown para detalles) ...

                        # --- Mostrar GCS URL ---
                        if media_url and media_url.startswith("http"):
                              try: # Tu código para st.image/st.video
                                  pass
                              except: pass # Evitar error si falla
                        elif media_url: st.caption(f"Adjunto (Info): `{media_url}`")
                        # --- FIN Mostrar ---

                        st.markdown(f"**⭐ Satisfacción:** {row_i.get('SatisfaccionI','') or '(Sin calificar)'}")

                        # ... (Tu código para calificación de incidencia, usando header_i_list.index(...) para cols) ...
                        pass # Reemplaza con tu lógica

# ===================== SECCIÓN: SOLICITUDES CRM =====================
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")
    with st.form("solicitud_form", clear_on_submit=True):
        # ... (Tu formulario completo de solicitudes) ...
        submitted_sol = st.form_submit_button("✔️ Enviar Solicitud")
        if submitted_sol:
             # ... (Tus validaciones) ...
             # else:
                 try: # Tu lógica para armar fila_sol y append_row
                    pass
                 except Exception as e: st.error(f"Error: {e}")

# ===================== SECCIÓN: INCIDENCIAS CRM =====================
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias")
    with st.form("form_incidencia", clear_on_submit=True):
        # ... (Inputs: correo_i, categoria, asunto, link, descripcion) ...
        correo_i = st.text_input("Correo de quien solicita (*)")
        categoria = st.selectbox("Categoría", ["Desfase", "Reactivación", "Equivalencia", "Llamadas IVR", "Funcionalidad Zoho", "Mensajes", "Otros", "Cambio de Periodo", "Cursos Zoho", "Asignación"]) # Opciones actualizadas
        asunto = st.text_input("Asunto o título (*)")
        link = st.text_input("Link del registro afectado (Zoho)")
        descripcion = st.text_area("Descripción breve (*)", height=100)

        uploaded_file = st.file_uploader(
            f"Adjuntar Imagen (máx {MAX_IMAGE_MB}MB) o Video (máx {MAX_VIDEO_MB}MB)",
            type=['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp',
                  'mp4', 'mov', 'avi', 'wmv', 'mkv', 'webm'],
            accept_multiple_files=False
        )
        st.caption("(*) Campos obligatorios")
        enviado = st.form_submit_button("✔️ Enviar Incidencia")

    if enviado:
        if not correo_i or not asunto or not descripcion:
            st.warning("⚠️ Completa correo, asunto y descripción.")
        else:
            google_cloud_storage_url = ""
            proceed_with_save = True # Flag

            if uploaded_file is not None:
                # --- Verificación de tamaño ---
                file_size = uploaded_file.size
                file_type = uploaded_file.type or "application/octet-stream"
                is_image = file_type.startswith("image/")
                is_video = file_type.startswith("video/")

                if is_image and file_size > MAX_IMAGE_BYTES:
                    st.error(f"❌ Imagen excede {MAX_IMAGE_MB}MB."); proceed_with_save = False
                elif is_video and file_size > MAX_VIDEO_BYTES:
                    st.error(f"❌ Video excede {MAX_VIDEO_MB}MB."); proceed_with_save = False
                # --- FIN Verificación ---

                if proceed_with_save:
                    st.info("Subiendo archivo a GCS...", icon="⏳")
                    file_extension = Path(uploaded_file.name).suffix.lower()
                    unique_filename = f"{uuid4()}{file_extension}"
                    public_url = upload_to_gcs(uploaded_file, unique_filename, file_type)
                    if public_url: google_cloud_storage_url = public_url
                    else: proceed_with_save = False # Detener si falla la subida

            # --- Guardar en Sheets si procede ---
            if proceed_with_save:
                try:
                    header_i = sheet_incidencias.row_values(1)
                    # Asegurar que MediaFilenameI esté en header_i o ajustar fila
                    if "MediaFilenameI" not in header_i:
                         st.warning("Falta columna 'MediaFilenameI' en hoja 'Incidencias'. Adjunto no se guardará.")
                         # Crear fila sin la última columna si falta
                         fila_inc = [now_mx_str(), _email_norm(correo_i), asunto.strip(), categoria, descripcion.strip(), link.strip(), "Pendiente", "", "", "", "", str(uuid4())]
                    else:
                         # Crear fila completa
                         fila_inc = [now_mx_str(), _email_norm(correo_i), asunto.strip(), categoria, descripcion.strip(), link.strip(), "Pendiente", "", "", "", "", str(uuid4()), google_cloud_storage_url]

                    # Truncar fila al ancho del header por seguridad
                    fila_inc = fila_inc[:len(header_i)]

                    with_backoff(sheet_incidencias.append_row, fila_inc, value_input_option='USER_ENTERED')
                    st.success("✅ Incidencia registrada.")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Error al registrar en Sheets: {e}")
# ===================== SECCIÓN: Sugerencias y Mejoras =====================
elif seccion == "📝 Sugerencias y Mejoras": # Nombre actualizado
    st.markdown("## 📝 Sugerencias y Mejoras")
    with st.form("queja_form", clear_on_submit=True):
        q_correo = st.text_input("Tu correo institucional (*)")
        # Quitar "Queja" si ahora es solo sugerencias/mejoras
        # q_tipo = st.selectbox("Tipo", ["Sugerencia", "Mejora"])
        q_asunto = st.text_input("Asunto (*)")
        q_categoria = st.selectbox("Categoría", ["Uso de CRM","Datos","Reportes","IVR","Mensajería","Soporte","Otro"])
        q_desc = st.text_area("Descripción (*)")
        q_calif = st.slider("Prioridad (opcional, 1=Baja, 5=Alta)", 1, 5, 3) # Cambiar etiqueta

        st.caption("(*) Campos obligatorios")
        submitted_q = st.form_submit_button("✔️ Enviar Sugerencia/Mejora")

        if submitted_q:
            if not q_correo or not q_asunto or not q_desc:
                st.warning("Completa correo, asunto y descripción.")
            else:
                try:
                    # Ajustar fila según las columnas reales en "Quejas"
                    header_q = sheet_quejas.row_values(1)
                    fila_q = [
                        now_mx_str(), _email_norm(q_correo), "Sugerencia/Mejora", # Tipo fijo?
                        q_asunto, q_desc, q_categoria, "Pendiente", q_calif
                    ]
                    # Manejar posible columna duplicada CategoriaQ o faltante
                    if len(header_q) > 8 and header_q[8].strip().lower() == 'categoriaq':
                         fila_q.append(q_categoria)

                    fila_q = fila_q[:len(header_q)] # Truncar

                    with_backoff(sheet_quejas.append_row, fila_q, value_input_option='USER_ENTERED')
                    st.success("✅ Gracias por tu feedback.")
                    st.balloons()
                except Exception as e:
                    st.error(f"Error al registrar sugerencia: {e}")

# ===================== SECCIÓN: ADMIN =====================
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")
    # --- Login Admin ---
    # ... (Tu código de login admin, sin cambios) ...
    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    # ... (resto del login admin) ...
    admin_ok = st.session_state.get("is_admin", False) # Simplificar chequeo
    if not admin_ok:
        with st.form("admin_login_form"):
             admin_pass_input = st.text_input("Contraseña admin", type="password")
             admin_login_submitted = st.form_submit_button("Entrar Admin")
             if admin_login_submitted:
                 if admin_pass_input == ADMIN_PASS:
                     st.session_state.is_admin = True; st.rerun()
                 else: st.error("❌ Contraseña incorrecta")
        # Acceso por lista blanca (solo si no entró por contraseña)
        elif st.session_state.get("usuario_logueado"):
             raw_emails = st.secrets.get("admin", {}).get("emails", [])
             ADMIN_EMAILS = {raw_emails.strip().lower()} if isinstance(raw_emails, str) else {e.strip().lower() for e in raw_emails}
             if ADMIN_EMAILS and _email_norm(st.session_state["usuario_logueado"]) in ADMIN_EMAILS:
                  st.session_state.is_admin = True; st.rerun()

    # --- Contenido Admin ---
    if admin_ok:
        st.success("🔑 Acceso Admin OK.")
        if st.button("Salir Admin"): del st.session_state.is_admin; st.rerun()

        st.info("Cargando datos...", icon="⏳")
        try: df_s = get_records_simple(sheet_solicitudes)
        except Exception: df_s = pd.DataFrame(); st.error("Error Solicitudes")
        try: df_i = get_records_simple(sheet_incidencias)
        except Exception: df_i = pd.DataFrame(); st.error("Error Incidencias")
        try: df_q = get_records_simple(sheet_quejas)
        except Exception: df_q = pd.DataFrame(); st.error("Error Quejas")
        # Ordenar si es posible
        try: df_s = df_s.sort_values(by="FechaS", ascending=False)
        except: pass
        try: df_i = df_i.sort_values(by="FechaI", ascending=False)
        except: pass
        try: df_q = df_q.sort_values(by="FechaQ", ascending=False)
        except: pass
        st.success("Datos Cargados.")

        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Sugerencias"]) # Tab renombrado

        with tab1: # Solicitudes Admin
             # ... (Tu código admin para Solicitudes usando ID) ...
             pass
        with tab2: # Incidencias Admin
             # ... (Tu código admin para Incidencias usando IDI) ...
             pass
        with tab3: # Quejas/Sugerencias Admin
             # ... (Tu código admin para Quejas usando índice DF) ...
             pass

    elif not st.session_state.get("usuario_logueado"): # Si no es admin Y no está logueado
         st.info("🔒 Ingresa tu contraseña de usuario o la de administrador.")


# --- ELEMENTOS FINALES DE LA BARRA LATERAL ---
st.sidebar.divider()
if st.sidebar.button("♻️ Recargar Página"): st.rerun()
# Info de Entorno (sin ID en PROD)
env_id_info = f"· `{SHEET_ID}`" if APP_MODE == "dev" else ""
env_icon = "🧪" if APP_MODE == "dev" else "🚀"
st.sidebar.caption(f"{env_icon} ENTORNO: {APP_MODE.upper()} {env_id_info}")

# FIN DEL ARCHIVO