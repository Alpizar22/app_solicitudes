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
    if s is None:
        return ""
    text = str(s)
    m = EMAIL_RE.search(text)
    if m:
        return m.group(1).strip().lower()
    return text.strip().lower()

def _norm(x):
     return str(x).strip().lower() if pd.notna(x) else ""

def _is_unrated(val: str) -> bool:
     v = _norm(val)
     return v in ("", "pendiente", "na", "n/a", "sin calificacion", "sin calificación", "none", "null", "-")

def with_backoff(fn, *args, **kwargs):
    # ... (Tu función with_backoff sin cambios)...
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) or ("403" in str(e) and "rateLimitExceeded" in str(e)):
                 wait = min(1*(2**i) + random.random(), 16)
                 print(f"API Error ({e}). Retrying in {wait:.2f} seconds...")
                 time.sleep(wait)
                 continue
            raise
        except Exception as e:
            print(f"Connection Error: {e}. Retrying...")
            time.sleep(min(1*(2**i) + random.random(), 16))
            continue
    raise Exception(f"Failed after multiple retries for {fn.__name__}")


def load_json_safe(path: str) -> dict:
    # ... (Tu función load_json_safe sin cambios)...
    try:
        # Usar Path para compatibilidad entre OS
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

# Scopes necesarios (Sheets + Drive/Cloud Platform para credenciales unificadas)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive", # Necesario para from_service_account_info con GCS
    "https://www.googleapis.com/auth/cloud-platform" # Opcional pero recomendado para GCS
]

# --- Nombre del bucket GCS ---
GCS_BUCKET_NAME = st.secrets.get("google_cloud_storage", {}).get("bucket_name", "")
if not GCS_BUCKET_NAME:
    # Solo mostrar error si estamos en prod, en dev puede ser opcional
    if APP_MODE == "prod":
        st.error("❗ Falta [google_cloud_storage] bucket_name en secrets.toml")
        st.stop()
    else: # En dev, solo advertir
        st.warning("⚠️ No se encontró google_cloud_storage.bucket_name en secrets. La subida de archivos GCS no funcionará.")


if not SHEET_ID:
    st.error("❗ No se encontró SHEET_ID en [sheets] de secrets.toml")
    st.stop()

# -------------------------
# Conexiones Google (Sheets & GCS)
# -------------------------
@st.cache_resource(ttl=3600) # Cachear credenciales por 1 hora
def get_google_credentials():
    """Obtiene las credenciales de Google desde secrets."""
    creds_dict = st.secrets.get("google_service_account")
    if not creds_dict:
        st.error("❗ Falta [google_service_account] en secrets.")
        st.stop()
    try:
        # Usar SCOPES aquí asegura que las credenciales tengan los permisos necesarios
        # para todas las APIs que usaremos (Sheets, GCS via Client)
        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except Exception as e:
        st.error(f"❌ Error al crear credenciales desde secrets: {e}")
        st.stop()

@st.cache_resource(ttl=3600) # Cachear cliente gspread
def get_gspread_client():
    """Obtiene el cliente autorizado de gspread."""
    credentials = get_google_credentials()
    try:
        client = gspread.authorize(credentials)
        # Verificar conexión listando hojas (opcional, diagnóstico)
        # client.list_spreadsheet_files(max_results=1)
        return client
    except Exception as e:
        st.error(f"❌ Error al autorizar cliente gspread: {e}")
        st.stop()

@st.cache_resource(ttl=3600) # Cachear el libro abierto
def get_book():
    """Abre el Google Sheet por su ID."""
    client = get_gspread_client()
    try:
        # Asegurarse que SHEET_ID no sea None o vacío
        if not SHEET_ID: raise ValueError("SHEET_ID no está definido.")
        return with_backoff(client.open_by_key, SHEET_ID)
    except Exception as e:
        st.error(f"❌ Error al abrir Google Sheet (ID: {SHEET_ID}): {e}")
        st.stop()

book = get_book()

# --- Cliente para Google Cloud Storage ---
@st.cache_resource(ttl=3600) # Cachear cliente GCS
def get_gcs_client():
    """Crea y retorna un cliente para Google Cloud Storage."""
    credentials = get_google_credentials() # Reutiliza las mismas credenciales
    project_id = st.secrets.get("google_service_account",{}).get("project_id")
    if not project_id:
        st.warning("⚠️ Falta 'project_id' en [google_service_account] secrets para GCS.")
        # Intentar sin project_id explícito, puede funcionar si las creds lo tienen
    try:
        storage_client = storage.Client(project=project_id, credentials=credentials)
        # Opcional: Verificar conexión intentando obtener el bucket
        if GCS_BUCKET_NAME:
             with_backoff(storage_client.get_bucket, GCS_BUCKET_NAME)
        return storage_client
    except Exception as e:
        st.error(f"❌ Error al crear cliente GCS o acceder al bucket '{GCS_BUCKET_NAME}': {e}")
        return None # No detener la app, solo la subida fallará

# --- Función para subir archivo a GCS ---
def upload_to_gcs(file_buffer, filename_in_bucket, content_type):
    """Sube un archivo (desde buffer) a GCS y lo hace público."""
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

        file_buffer.seek(0) # Asegura estar al inicio del buffer
        # Usar with_backoff para la subida también
        with_backoff(blob.upload_from_file, file_buffer, content_type=content_type, rewind=True)

        # Hacer el archivo públicamente legible (necesita permisos correctos en GCS)
        with_backoff(blob.make_public)

        public_url = blob.public_url
        print(f"Archivo subido a GCS. URL pública: {public_url}")
        st.toast(f"Archivo subido a GCS.", icon="☁️")
        return public_url

    except Exception as e:
        st.error(f"❌ Error al subir archivo a GCS: {e}")
        if hasattr(e, 'message'): st.error(f"   Detalles: {e.message}")
        return None

# --- Conexión a las pestañas (Worksheets) ---
sheets = {}
required_sheets = ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]
all_sheets_found = True
for sheet_name in required_sheets:
    try:
        sheets[sheet_name] = book.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        st.error(f"❌ Hoja requerida '{sheet_name}' no encontrada en el Google Sheet.")
        all_sheets_found = False
    except Exception as e:
        st.error(f"❌ Error inesperado al obtener hoja '{sheet_name}': {e}")
        all_sheets_found = False

if not all_sheets_found:
    st.stop() # Detener si falta alguna hoja esencial

# Asignar a variables
sheet_solicitudes = sheets["Sheet1"]
sheet_incidencias = sheets["Incidencias"]
sheet_quejas      = sheets["Quejas"]
sheet_accesos     = sheets["Accesos"]
sheet_usuarios    = sheets["Usuarios"]

# --- Lector de datos (Sin caché) ---
#@st.cache_data(ttl=180)
def get_records_simple(_ws) -> pd.DataFrame:
    # ... (Tu función get_records_simple robusta, sin cambios)...
    ws_title = _ws.title # Guardar título por si falla
    try:
        all_values = with_backoff(_ws.get_all_values)
        if not all_values: return pd.DataFrame() # Hoja vacía
        header = all_values[0]
        data = all_values[1:]
        num_cols = len(header)
        data_fixed = [row[:num_cols] + [""] * (num_cols - len(row)) for row in data]
        df = pd.DataFrame(data_fixed, columns=header)
        df.columns = df.columns.str.strip() # Limpiar nombres de columnas
        # Convertir columnas comunes a string para evitar errores de tipo mixto
        for col in ['EstadoS', 'EstadoI', 'EstadoQ', 'SatisfaccionS', 'SatisfaccionI', 'IDS', 'IDI']:
             if col in df.columns:
                 df[col] = df[col].astype(str)
        return df
    except Exception as e:
        st.error(f"Error al leer '{ws_title}': {e}")
        try: # Diagnóstico
            header_row = with_backoff(_ws.row_values, 1)
            st.error(f"Encabezados encontrados en '{ws_title}': {header_row}")
        except Exception as e2: st.error(f"No se pudieron leer encabezados de '{ws_title}': {e2}")
        return pd.DataFrame()

# -------------------------
# Datos locales (JSON) y Usuarios (desde GSheets)
# -------------------------
# Usar Path para rutas relativas
data_folder = Path("data")
estructura_roles = load_json_safe(data_folder / "estructura_roles.json")
numeros_por_rol  = load_json_safe(data_folder / "numeros_por_rol.json")
horarios_dict    = load_json_safe(data_folder / "horarios.json")

#@st.cache_data(ttl=300) # Sin caché
def cargar_usuarios_df():
    try:
        df = get_records_simple(sheet_usuarios)
        if "Contraseña" not in df.columns or "Correo" not in df.columns:
             st.error("❌ Hoja 'Usuarios' debe tener columnas 'Contraseña' y 'Correo'.")
             return pd.DataFrame(columns=["Contraseña","Correo"])
        df['Contraseña'] = df['Contraseña'].astype(str).str.strip()
        # Filtrar filas donde la contraseña esté vacía
        df = df[df['Contraseña'] != '']
        return df
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
usuarios_dict = {str(p): _email_norm(c)
                 for p, c in zip(usuarios_df.get("Contraseña", []),
                                 usuarios_df.get("Correo", []))
                 if str(p) # Ignorar contraseñas vacías ya filtradas, doble check
                }
if not usuarios_dict:
    st.warning("⚠️ No se cargaron usuarios/contraseñas desde la hoja 'Usuarios'. El login no funcionará.")

# -------------------------
# Email, Sesión, Login/Logout (Funciones sin cambios internos)
# -------------------------
def enviar_correo(asunto, mensaje_resumen, copia_a):
    # ... (Tu función enviar_correo sin cambios)...
    if not SEND_EMAILS:
        print("Envío de correo deshabilitado.")
        return
    email_user = st.secrets.get("email", {}).get("user")
    email_pass = st.secrets.get("email", {}).get("password")
    if not email_user or not email_pass:
        st.warning("⚠️ Faltan credenciales de email en secrets.")
        return
    try:
        yag = yagmail.SMTP(user=email_user, password=email_pass)
        cuerpo = f"""<p>Hola,</p><p>Gracias por registrar tu solicitud en el CRM...</p><p><strong>Resumen:</strong><br>{mensaje_resumen}</p><p>Saludos,<br><b>Equipo CRM UAG</b></p>"""
        to_list = ["luis.alpizar@edu.uag.mx"]
        if copia_a and isinstance(copia_a, str) and '@' in copia_a: to_list.append(_email_norm(copia_a)) # Normalizar
        cc_list = ["carlos.sotelo@edu.uag.mx", "esther.diaz@edu.uag.mx"]
        yag.send(to=to_list, cc=cc_list, subject=asunto, contents=[cuerpo], headers={"From": f"CRM UAG <{email_user}>"})
        print(f"Correo enviado a: {to_list}, CC: {cc_list}")
    except Exception as e: st.warning(f"No se pudo enviar el correo: {e}")

def log_event(usuario, evento, session_id, dur_min=""):
    # ... (Tu función log_event sin cambios)...
    try:
        fila_acceso = [now_mx_str(), usuario or "", evento, session_id or "", str(dur_min or "")]
        with_backoff(sheet_accesos.append_row, fila_acceso, value_input_option='USER_ENTERED')
    except Exception as e: st.warning(f"No se pudo registrar acceso: {e}")

def do_login(correo):
    # ... (Tu función do_login sin cambios)...
    st.session_state.usuario_logueado = _email_norm(correo)
    st.session_state.session_id = str(uuid4())
    st.session_state.login_time = datetime.now(TZ_MX)
    log_event(st.session_state.usuario_logueado, "login", st.session_state.session_id)

def do_logout():
    # ... (Tu función do_logout sin cambios)...
    dur = ""
    if st.session_state.get("login_time"):
        try: dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
        except TypeError: dur = ""
    log_event(st.session_state.get("usuario_logueado"), "logout", st.session_state.get("session_id"), str(dur))
    keys_to_delete = ["usuario_logueado", "session_id", "login_time", "nav_index", "is_admin"] # Limpiar todo
    for key in keys_to_delete:
        if key in st.session_state: del st.session_state[key]
    st.success("Sesión cerrada.")

# Inicialización de estado de sesión
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None
if 'nav_index' not in st.session_state: st.session_state.nav_index = 0 # Para navegación
# -------------------------
# Navegación Principal (Sidebar Radio)
# -------------------------
nav_options = ["🔍 Ver el estado de mis solicitudes",
               "🌟 Solicitudes CRM",
               "🛠️ Incidencias CRM",
               "📝 Mejoras y sugerencias",
               "🔐 Zona Admin"]

# Función para formatear las opciones del radio
def format_nav(index):
    return nav_options[index]

# Leer el índice guardado o usar 0
current_nav_index = st.session_state.get('nav_index', 0)

# Crear el radio en la barra lateral
nav_index = st.sidebar.radio(
    "Navegación",
    range(len(nav_options)),
    format_func=format_nav,
    index=current_nav_index, # Establecer el índice actual
    key="nav_radio_selector" # Clave única para el widget
)

# Guardar el índice seleccionado en el estado de sesión
st.session_state.nav_index = nav_index
seccion = nav_options[nav_index] # Obtener el string de la sección actual


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
                    st.success(f"Bienvenido, {st.session_state.usuario_logueado}")
                    st.rerun()
                elif clave_str: # Solo mostrar error si se ingresó algo
                    st.error("❌ Contraseña incorrecta")
                else:
                     st.warning("Por favor, ingresa tu contraseña.")
    # --- Contenido si está logueado ---
    elif st.session_state.get("usuario_logueado"):
        st.info(f"Usuario: **{st.session_state.usuario_logueado}**")
        if st.button("Cerrar sesión"):
            do_logout()
            st.rerun()

        # -------- Solicitudes --------
        st.subheader("📋 Solicitudes registradas")
        with st.spinner("Cargando solicitudes…"):
            df_s = get_records_simple(sheet_solicitudes)

        if not df_s.empty and "SolicitanteS" in df_s.columns:
            df_s['SolicitanteS'] = df_s['SolicitanteS'].astype(str)
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == st.session_state.usuario_logueado].copy()
            # Intentar ordenar por fecha si existe y es convertible
            try:
                df_mias['FechaS_dt'] = pd.to_datetime(df_mias['FechaS'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                df_mias = df_mias.sort_values(by="FechaS_dt", ascending=False).drop(columns=['FechaS_dt'])
            except: # Si falla la conversión o no existe FechaS, no ordenar
                 pass
        else:
            if not df_s.empty: st.warning("⚠️ No se encontró 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()

        st.caption(f"{len(df_mias)} solicitudes encontradas.")

        # Mostrar solicitudes
        if not df_mias.empty:
            for index, row in df_mias.iterrows():
                with st.container():
                    estado_norm = _norm(row.get("EstadoS", ""))
                    sat_val_raw = row.get("SatisfaccionS", "")
                    id_unico    = str(row.get("IDS", f"idx_{index}")).strip()
                    row_key_base = f"sol_{id_unico}"

                    titulo = f"📌 {row.get('TipoS','Tipo?')} - {row.get('NombreS','Nombre?')} ({row.get('FechaS','Fecha?')}) — Estado: {row.get('EstadoS','?')}"
                    with st.expander(titulo):
                        # ... (Tu markdown para mostrar detalles de solicitud) ...
                        st.markdown(f"**Satisfacción actual:** {sat_val_raw or '(Sin calificar)'}")

                        is_attended = estado_norm.startswith("atendid")
                        unrated     = _is_unrated(sat_val_raw)

                        if is_attended and unrated and id_unico and id_unico != f"idx_{index}": # Solo si hay ID real
                            # ... (Tu código para calificación de solicitud) ...
                            st.markdown("---"); st.caption("Califica la atención:")
                            col1, col2 = st.columns([1,3])
                            with col1: voto = st.radio("Voto:", ["👍","👎"], horizontal=True, key=f"vote_{row_key_base}")
                            with col2: comentario = st.text_input("Comentario (opcional):", key=f"comm_{row_key_base}")
                            if st.button("Enviar calificación", key=f"send_{row_key_base}"): # (Lógica de guardado sin cambios)
                                 # ... (pega tu lógica try/except para guardar calificación aquí) ...
                                 try:
                                     cell = with_backoff(sheet_solicitudes.find, id_unico)
                                     # ... (resto de tu lógica de guardado) ...
                                 except Exception as e: st.error(f"Error: {e}")
                        elif is_attended and not id_unico:
                            st.caption("(No se puede calificar: Falta ID único en la hoja)")

        st.divider()

        # -------- Incidencias --------
        st.subheader("🛠️ Incidencias reportadas")
        with st.spinner("Cargando incidencias…"):
            df_i = get_records_simple(sheet_incidencias)

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
            for index_i, row_i in df_mis_inc.iterrows():
                 with st.container():
                    estado_norm_i = _norm(row_i.get("EstadoI", ""))
                    sat_val_raw_i = row_i.get("SatisfaccionI", "")
                    id_unico_i    = str(row_i.get("IDI", f"idx_i_{index_i}")).strip()
                    media_url     = str(row_i.get("MediaFilenameI", "")).strip() # URL de GCS
                    row_i_key_base = f"inc_{id_unico_i}"

                    titulo = f"🛠️ {row_i.get('Asunto','Asunto?')} ({row_i.get('FechaI','Fecha?')}) — Estado: {row_i.get('EstadoI','?')}"
                    with st.expander(titulo):
                        # ... (Tu markdown para mostrar detalles de incidencia) ...
                        st.markdown(f"**⭐ Satisfacción actual:** {sat_val_raw_i or '(Sin calificar)'}")

                        # --- Mostrar imagen/video desde URL GCS ---
                        if media_url and media_url.startswith("http"):
                             # ... (Tu código para mostrar st.image/st.video desde URL) ...
                              try:
                                  file_ext = Path(media_url).suffix.lower()
                                  st.markdown("---"); st.caption("Archivo Adjunto:")
                                  if file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']: st.image(media_url)
                                  elif file_ext in ['.mp4', '.mov', '.avi', '.wmv', '.mkv', '.webm', '.ogg']: st.video(media_url)
                                  else: st.markdown(f"📎 [Descargar/Ver Archivo]({media_url})")
                              except Exception as e: st.markdown(f"📎 [Ver Archivo]({media_url})")
                        elif media_url: st.caption(f"Adjunto (Info): `{media_url}`")
                        # --- FIN Mostrar ---

                        is_attended_i = estado_norm_i.startswith("atendid")
                        unrated_i     = _is_unrated(sat_val_raw_i)

                        if is_attended_i and unrated_i and id_unico_i and id_unico_i != f"idx_i_{index_i}":
                             # ... (Tu código para calificación de incidencia) ...
                             st.markdown("---"); st.caption("Califica la atención:")
                             col1, col2 = st.columns([1, 3])
                             with col1: voto_i = st.radio("Voto:", ["👍", "👎"], horizontal=True, key=f"vote_{row_i_key_base}")
                             with col2: comentario_i = st.text_input("Comentario (opcional):", key=f"comm_{row_i_key_base}")
                             if st.button("Enviar calificación", key=f"send_{row_i_key_base}"): # (Lógica de guardado sin cambios)
                                 # ... (pega tu lógica try/except para guardar calificación aquí) ...
                                  try:
                                      cell = with_backoff(sheet_incidencias.find, id_unico_i)
                                      # ... (resto de tu lógica de guardado) ...
                                  except Exception as e: st.error(f"Error: {e}")
                        elif is_attended_i and not id_unico_i:
                             st.caption("(No se puede calificar: Falta IDI único en la hoja)")
# ===================== SECCIÓN: SOLICITUDES CRM =====================
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")
    # (Tu formulario de solicitudes, usando st.form como antes - sin cambios funcionales)
    with st.form("solicitud_form", clear_on_submit=True):
        # ... (todos tus st.selectbox y st.text_input para solicitudes) ...
        submitted_sol = st.form_submit_button("✔️ Enviar Solicitud")
        if submitted_sol:
             # ... (tus validaciones if/elif) ...
             # else:
                 try:
                     # ... (armar la fila_sol) ...
                     # with_backoff(sheet_solicitudes.append_row, ...)
                     # st.success(...)
                     # enviar_correo(...)
                 except Exception as e: st.error(f"Error: {e}")


# ===================== SECCIÓN: INCIDENCIAS CRM =====================
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias")

    with st.form("form_incidencia", clear_on_submit=True):
        col_a, col_b = st.columns(2)
        with col_a:
            correo_i = st.text_input("Correo de quien solicita (*)")
            categoria = st.selectbox( # (...)
                 "Categoría",
                 ["Desfase", "Reactivación", "Equivalencia", "Llamadas IVR", "Funcionalidad Zoho", "Mensajes", "Otros","Cambio de Periodo","Cursos Zoho","Asignación"]
            )
        with col_b:
            asunto = st.text_input("Asunto o título (*)")
            link = st.text_input("Link del registro afectado (Zoho)")

        descripcion = st.text_area("Descripción breve (*)", height=100)

        uploaded_file = st.file_uploader(
            f"Adjuntar Imagen (máx {MAX_IMAGE_MB}MB) o Video (máx {MAX_VIDEO_MB}MB)", # Texto actualizado
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
            proceed_with_save = True # Bandera para controlar guardado

            if uploaded_file is not None:
                # --- Verificación de tamaño ---
                file_size = uploaded_file.size
                file_type = uploaded_file.type or "application/octet-stream" # Default si falta tipo

                is_image = file_type.startswith("image/")
                is_video = file_type.startswith("video/")

                if is_image and file_size > MAX_IMAGE_BYTES:
                    st.error(f"❌ Imagen excede {MAX_IMAGE_MB}MB ({file_size / (1024*1024):.1f}MB).")
                    proceed_with_save = False
                elif is_video and file_size > MAX_VIDEO_BYTES:
                    st.error(f"❌ Video excede {MAX_VIDEO_MB}MB ({file_size / (1024*1024):.1f}MB).")
                    proceed_with_save = False
                elif not is_image and not is_video:
                     st.warning(f"Tipo '{file_type}' no es imagen/video. Se intentará subir.")
                # --- FIN Verificación ---

                if proceed_with_save:
                    st.info("Subiendo archivo a Google Cloud Storage...", icon="⏳")
                    file_extension = Path(uploaded_file.name).suffix.lower()
                    unique_filename = f"{uuid4()}{file_extension}"

                    public_url = upload_to_gcs(
                        file_buffer=uploaded_file,
                        filename_in_bucket=unique_filename,
                        content_type=file_type # Usar tipo detectado por Streamlit
                    )
                    if public_url:
                        google_cloud_storage_url = public_url
                    else:
                        st.error("Falló la subida a GCS. Incidencia se guardará sin adjunto.")
                        # Decidir si continuar guardando el texto (probablemente sí)
                        proceed_with_save = True

            # --- Guardar en Sheets si procede ---
            if proceed_with_save:
                try:
                    header_i = sheet_incidencias.row_values(1) # Leer encabezados actuales
                    fila_inc = [ # Crear fila base
                        now_mx_str(), _email_norm(correo_i), asunto.strip(), categoria,
                        descripcion.strip(), link.strip(), "Pendiente", "", "", "", "",
                        str(uuid4()), # IDI
                        google_cloud_storage_url # MediaFilenameI (URL o vacío)
                    ]
                    # Ajustar longitud al número real de columnas en la hoja
                    fila_inc = fila_inc[:len(header_i)]

                    with_backoff(sheet_incidencias.append_row, fila_inc, value_input_option='USER_ENTERED')
                    st.success("✅ Incidencia registrada.")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Error al registrar en Google Sheets: {e}")
                    # Mostrar fila ayuda a depurar si el error es por longitud
                    st.error(f"Fila intentada (puede estar truncada): {fila_inc}")


# ===================== SECCIÓN: QUEJAS =====================
elif seccion == "📝 Sugerencias y Mejoras":
    st.markdown("## 📝 Sugerencias y Mejoras")
    # (Tu formulario de Mejoras, usando st.form como antes - sin cambios funcionales)
    with st.form("queja_form", clear_on_submit=True):
         # ... (todos tus inputs para quejas) ...
         submitted_q = st.form_submit_button("✔️ Enviar")
         if submitted_q:
             # ... (tus validaciones) ...
             # else:
                 try:
                     # ... (armar fila_q) ...
                     # with_backoff(sheet_quejas.append_row, ...)
                     # st.success(...)
                 except Exception as e: st.error(f"Error: {e}")

# ===================== SECCIÓN: ADMIN =====================
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")

    # --- Login Admin ---
    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    raw_emails = st.secrets.get("admin", {}).get("emails", [])
    ADMIN_EMAILS = {raw_emails.strip().lower()} if isinstance(raw_emails, str) else {e.strip().lower() for e in raw_emails}

    admin_ok = False
    if st.session_state.get("is_admin"):
        admin_ok = True
    else: # Mostrar formulario de login si no está logueado como admin
        with st.form("admin_login_form"):
             admin_pass_input = st.text_input("Contraseña admin", type="password")
             admin_login_submitted = st.form_submit_button("Entrar Admin")
             if admin_login_submitted:
                 if admin_pass_input == ADMIN_PASS:
                     st.session_state.is_admin = True
                     st.rerun()
                 else:
                     st.error("❌ Contraseña admin incorrecta")
        # Permitir acceso si el usuario normal está en la lista blanca
        if not admin_ok and st.session_state.get("usuario_logueado"):
             if ADMIN_EMAILS and _email_norm(st.session_state["usuario_logueado"]) in ADMIN_EMAILS:
                  st.session_state.is_admin = True
                  st.rerun()

    # --- Contenido Admin ---
    if admin_ok:
        st.success("🔑 Acceso de administrador concedido.")
        if st.button("Salir de Zona Admin"):
            if "is_admin" in st.session_state: del st.session_state.is_admin
            st.rerun()

        st.info("Cargando datos (puede tardar)...", icon="⏳")
        try: df_s = get_records_simple(sheet_solicitudes)
        except Exception: df_s = pd.DataFrame(); st.error("Error cargando Solicitudes")
        try: df_i = get_records_simple(sheet_incidencias)
        except Exception: df_i = pd.DataFrame(); st.error("Error cargando Incidencias")
        try: df_q = get_records_simple(sheet_quejas)
        except Exception: df_q = pd.DataFrame(); st.error("Error cargando Mejoras")
        st.success("Datos cargados.")

        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Quejas"])

        # ----- Solicitudes Admin -----
        with tab1:
            # (Tu código admin para solicitudes, usando ID si existe)
             st.dataframe(df_s, use_container_width=True)
             if not df_s.empty and "EstadoS" in df_s.columns and "IDS" in df_s.columns:
                 ids_validos = df_s[df_s["IDS"] != '']["IDS"].unique().tolist()
                 if ids_validos:
                     id_s_selected = st.selectbox("ID Solicitud a Modificar/Eliminar", ids_validos, key="id_sol_admin_select")
                     estado_s_admin = st.selectbox("Nuevo estado Solicitud", ["Pendiente", "En proceso", "Atendido"], key="estado_sol_admin")
                     colA, colB = st.columns(2)
                     # ... (botones Actualizar/Eliminar usando id_s_selected y sheet_solicitudes.find) ...
                 else: st.info("No hay solicitudes con IDS válidos para modificar.")
             else: st.info("No hay solicitudes o faltan 'EstadoS'/'IDS'.")

        # ----- Incidencias Admin -----
        with tab2:
            # (Tu código admin para incidencias, usando IDI si existe)
            st.dataframe(df_i, use_container_width=True)
            required_cols_i = {"EstadoI", "AtendidoPorI", "RespuestadeSolicitudI", "IDI"}
            if not df_i.empty and required_cols_i.issubset(df_i.columns):
                 ids_i_validos = df_i[df_i["IDI"] != '']["IDI"].unique().tolist()
                 if ids_i_validos:
                     id_i_selected = st.selectbox("ID Incidencia a Modificar/Eliminar", ids_i_validos, key="id_inc_admin_select")
                     current_row = df_i[df_i["IDI"] == id_i_selected].iloc[0] # Puede fallar si hay duplicados
                     # ... (inputs estado_i_admin, atendido_por_admin, respuesta_admin) ...
                     # ... (botones Actualizar/Eliminar usando id_i_selected y sheet_incidencias.find) ...
                 else: st.info("No hay incidencias con IDI válidos para modificar.")
            else: st.info("No hay incidencias o faltan columnas requeridas.")

        # ----- Quejas Admin -----
        with tab3:
             # (Tu código admin para quejas, usando índice de DF + offset)
             st.dataframe(df_q, use_container_width=True)
             if not df_q.empty and "EstadoQ" in df_q.columns:
                 # Identificar por índice sigue siendo la opción más simple aquí
                 fila_q_idx_df = st.selectbox("Índice Queja a Modificar/Eliminar", df_q.index, key="idx_queja_admin_select")
                 if fila_q_idx_df is not None: # Asegurarse que algo está seleccionado
                     current_row_q = df_q.loc[fila_q_idx_df]
                     estado_q_admin = st.selectbox("Nuevo estado Queja", ["Pendiente", "En proceso", "Atendido"], index=["Pendiente", "En proceso", "Atendido"].index(current_row_q.get("EstadoQ","Pendiente")), key="estado_queja_admin")
                     colA, colB = st.columns(2)
                     # ... (botones Actualizar/Eliminar usando fila_q_idx_df + 2) ...
             else: st.info("No hay quejas o falta 'EstadoQ'.")

    # Si no es admin pero intenta acceder
    elif not admin_ok and st.session_state.get("usuario_logueado"):
        st.warning("🔒 No tienes permisos de administrador.")
    # Si no está logueado y no entró contraseña admin
    elif not admin_ok:
         st.info("🔒 Ingresa la contraseña de administrador para ver esta sección.")


# --- ELEMENTOS MOVIDOS AL FINAL DE LA BARRA LATERAL ---
st.sidebar.divider()

# Botón de Recargar Página (simple rerun)
if st.sidebar.button("♻️ Recargar Página"):
    # Considera añadir limpieza de caché si vuelves a tener problemas
    # st.cache_data.clear()
    # st.cache_resource.clear()
    st.rerun()

# Información del Entorno
# Mostrar ID de Sheet solo en modo DEV por seguridad
if APP_MODE == "dev":
    st.sidebar.caption(f"🧪 ENTORNO: DEV · `{SHEET_ID}`")
else: # En PROD, no mostrar el ID
    st.sidebar.caption(f"🚀 ENTORNO: PROD")

# FIN DEL ARCHIVO
