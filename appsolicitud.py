import os
import json
import time, random
from uuid import uuid4
from datetime import datetime
import re
import unicodedata
from pathlib import Path # Para manejar archivos y carpetas
import io # Para manejar los bytes del archivo

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials # Asegúrate que ya esté
from google.cloud import storage # Para GCS
# Quitar importaciones de Drive si las pusiste
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseUpload
import yagmail
from zoneinfo import ZoneInfo
# from PIL import Image # No se usa directamente, puedes quitarla si quieres

# -------------------------
# Utilidades y normalizadores (sin cambios)
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
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            # Manejar específicamente errores 403 (Permisos) además de 429 (Quota)
            if "429" in str(e) or ("403" in str(e) and "rateLimitExceeded" in str(e)):
                 wait = min(1*(2**i) + random.random(), 16) # Aumentar espera máxima
                 print(f"API Error ({e}). Retrying in {wait:.2f} seconds...")
                 time.sleep(wait)
                 continue
            # Si es otro 403 u otro error, no reintentar y lanzar excepción
            raise
        except Exception as e: # Capturar otros errores de conexión
            print(f"Connection Error: {e}. Retrying...")
            time.sleep(min(1*(2**i) + random.random(), 16))
            continue
    # Si todos los reintentos fallan
    raise Exception(f"Failed after multiple retries for {fn.__name__}")


def load_json_safe(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # Ser más específico sobre por qué no se cargó
        print(f"Warning: Could not load JSON file at {path}")
        return {}

TZ_MX = ZoneInfo("America/Mexico_City")
def now_mx_str() -> str:
    return datetime.now(TZ_MX).strftime("%d/%m/%Y %H:%M:%S") # Añadir segundos para más precisión

# -------------------------
# Config / secrets
# -------------------------
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")

APP_MODE = st.secrets.get("mode", "dev")
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))
SHEET_ID = (st.secrets.get("sheets", {}).get("prod_id") if APP_MODE == "prod"
            else st.secrets.get("sheets", {}).get("dev_id"))
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # Drive scope no afecta GCS

STORAGE_SCOPES = [
    "https://www.googleapis.com/auth/devstorage.read_write"  # o "https://www.googleapis.com/auth/cloud-platform"
]

# --- Leer nombre del bucket de GCS ---
GCS_BUCKET_NAME = st.secrets.get("google_cloud_storage", {}).get("bucket_name", "")
if not GCS_BUCKET_NAME and APP_MODE == "prod":
    st.warning("⚠️ No se encontró google_cloud_storage.bucket_name en secrets. No se podrán subir archivos a GCS.")

if not SHEET_ID:
    st.error("❗ No se encontró SHEET_ID en [sheets] de secrets.toml")
    st.stop()

# (La info de entorno se muestra al final en la sidebar)
# -------------------------
# Conexión a Google Sheets
# -------------------------
@st.cache_resource(ttl=3600) # Cachear conexión por 1 hora
def get_google_credentials():
    """Obtiene las credenciales de Google desde secrets."""
    creds_dict = st.secrets.get("google_service_account")
    if not creds_dict:
        st.error("❗ No se encontró [google_service_account] en secrets.")
        st.stop()
    try:
        # Usar SCOPES aquí asegura que las credenciales tengan los permisos necesarios
        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except Exception as e:
        st.error(f"❌ Error al crear credenciales desde secrets: {e}")
        st.stop()

@st.cache_resource(ttl=3600) # Cachear cliente gspread
def get_gspread_client():
    """Obtiene el cliente autorizado de gspread."""
    credentials = get_google_credentials()
    try:
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"❌ Error al autorizar cliente gspread: {e}")
        st.stop()

@st.cache_resource(ttl=3600) # Cachear el libro abierto
def get_book():
    """Abre el Google Sheet por su ID."""
    client = get_gspread_client()
    try:
        return with_backoff(client.open_by_key, SHEET_ID)
    except Exception as e:
        st.error(f"❌ Error al abrir Google Sheet (ID: {SHEET_ID}): {e}")
        st.stop()

book = get_book()

# --- Cliente para Google Cloud Storage ---
@st.cache_resource(ttl=3600) # Cachear cliente GCS
def get_gcs_client():
    """Crea y retorna un cliente para Google Cloud Storage."""
    credentials = get_google_credentials() # Reutiliza las credenciales
    try:
        storage_client = storage.Client(credentials=credentials)
        # Verificar conexión listando buckets (opcional, pero útil para diagnóstico)
        # list(storage_client.list_buckets(max_results=1))
        return storage_client
    except Exception as e:
        st.error(f"❌ Error al crear cliente GCS: {e}")
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
        blob = bucket.blob(filename_in_bucket) # Define el 'archivo' en el bucket

        # Sube los datos desde el buffer en memoria
        file_buffer.seek(0) # Asegura estar al inicio del buffer
        blob.upload_from_file(file_buffer, content_type=content_type)

        # Hacer el archivo públicamente legible (IMPORTANTE)
        blob.make_public()

        public_url = blob.public_url
        print(f"Archivo subido a GCS. URL pública: {public_url}")
        st.toast(f"Archivo subido a GCS.", icon="☁️")
        return public_url # Retorna la URL pública

    except Exception as e:
        st.error(f"❌ Error al subir archivo a GCS: {e}")
        # Ofrecer más detalles si es posible
        if hasattr(e, 'message'): st.error(f"   Detalles: {e.message}")
        return None

# --- Conexión a las pestañas (Worksheets) ---
# Usar un diccionario para manejar las hojas
sheets = {}
required_sheets = ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]
try:
    for sheet_name in required_sheets:
        sheets[sheet_name] = book.worksheet(sheet_name)
    # Asignar a variables por conveniencia (opcional)
    sheet_solicitudes = sheets["Sheet1"]
    sheet_incidencias = sheets["Incidencias"]
    sheet_quejas      = sheets["Quejas"]
    sheet_accesos     = sheets["Accesos"]
    sheet_usuarios    = sheets["Usuarios"]
except gspread.WorksheetNotFound as e:
    st.error(f"❌ No se encontró la hoja requerida: {e}")
    st.stop()
except Exception as e:
    st.error(f"❌ Error inesperado al obtener hojas: {e}")
    st.stop()


# --- Lector de datos (Sin caché como pediste) ---
#@st.cache_data(ttl=180)
def get_records_simple(_ws) -> pd.DataFrame:
    """Lee toda la hoja usando get_all_records."""
    ws_title = _ws.title # Guardar título por si falla
    try:
        # Forzar lectura de valores y crear DataFrame manualmente para evitar problemas
        # con get_all_records si hay celdas vacías o encabezados raros
        all_values = with_backoff(_ws.get_all_values)
        if not all_values: # Hoja vacía
            return pd.DataFrame()
        header = all_values[0]
        data = all_values[1:]
        # Asegurarse que todas las filas tengan el mismo ancho que el header
        num_cols = len(header)
        data_fixed = [row[:num_cols] + [""] * (num_cols - len(row)) for row in data]

        df = pd.DataFrame(data_fixed, columns=header)
        # Limpiar espacios en blanco de los nombres de columnas
        df.columns = df.columns.str.strip()
        return df

    except Exception as e:
        st.error(f"Error al leer '{ws_title}': {e}")
        # Intentar diagnóstico básico
        try:
            header_row = with_backoff(_ws.row_values, 1)
            st.error(f"Encabezados encontrados en '{ws_title}': {header_row}")
        except Exception as e2:
            st.error(f"No se pudieron leer ni los encabezados de '{ws_title}': {e2}")
        return pd.DataFrame()
# -------------------------
# Datos locales (JSON) y Usuarios (desde GSheets)
# -------------------------
estructura_roles = load_json_safe("data/estructura_roles.json")
numeros_por_rol  = load_json_safe("data/numeros_por_rol.json")
horarios_dict    = load_json_safe("data/horarios.json")

#@st.cache_data(ttl=300) # Sin caché
def cargar_usuarios_df():
    try:
        df = get_records_simple(sheet_usuarios) # Usar nuestro lector robusto
        #df = pd.DataFrame(sheet_usuarios.get_all_records()) # Método anterior
        if "Contraseña" not in df.columns or "Correo" not in df.columns:
             st.error("❌ Hoja 'Usuarios' debe tener columnas 'Contraseña' y 'Correo'.")
             return pd.DataFrame(columns=["Contraseña","Correo"])
        # Asegurarse que Contraseña sea string
        df['Contraseña'] = df['Contraseña'].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
# Crear diccionario, asegurando string y quitando vacíos
usuarios_dict = {str(p): _email_norm(c)
                 for p, c in zip(usuarios_df.get("Contraseña", []),
                                 usuarios_df.get("Correo", []))
                 if str(p) # Ignorar contraseñas vacías o nulas
                }

# -------------------------
# Email, Sesión, Login/Logout (Funciones sin cambios internos)
# -------------------------
def enviar_correo(asunto, mensaje_resumen, copia_a):
    if not SEND_EMAILS:
        # No mostrar nada si está deshabilitado, solo loguear tal vez
        # st.info("✉️ [DEV] Envío de correo deshabilitado.")
        print("Envío de correo deshabilitado.")
        return
    email_user = st.secrets.get("email", {}).get("user")
    email_pass = st.secrets.get("email", {}).get("password")
    if not email_user or not email_pass:
        st.warning("⚠️ Faltan credenciales de email en secrets.")
        return
    try:
        yag = yagmail.SMTP(user=email_user, password=email_pass)
        cuerpo = f"""
        <p>Hola,</p>
        <p>Gracias por registrar tu solicitud en el CRM. Nuestro equipo la revisará y te daremos seguimiento lo antes posible.</p>
        <p><strong>Resumen:</strong><br>{mensaje_resumen}</p>
        <p>Saludos cordiales,<br><b>Equipo CRM UAG</b></p>
        """
        # Asegurarse que destinatarios sean válidos
        to_list = ["luis.alpizar@edu.uag.mx"]
        if copia_a and isinstance(copia_a, str) and '@' in copia_a:
            to_list.append(copia_a)
        cc_list = ["carlos.sotelo@edu.uag.mx", "esther.diaz@edu.uag.mx"]

        yag.send(
            to=to_list,
            cc=cc_list,
            subject=asunto,
            contents=[cuerpo],
            headers={"From": f"CRM UAG <{email_user}>"}
        )
        print(f"Correo enviado a: {to_list}, CC: {cc_list}")
    except Exception as e:
        st.warning(f"No se pudo enviar el correo: {e}")

def log_event(usuario, evento, session_id, dur_min=""):
    try:
        fila_acceso = [now_mx_str(), usuario or "", evento, session_id or "", str(dur_min or "")]
        with_backoff(sheet_accesos.append_row, fila_acceso, value_input_option='USER_ENTERED')
    except Exception as e:
        st.warning(f"No se pudo registrar acceso: {e}")

def do_login(correo):
    st.session_state.usuario_logueado = _email_norm(correo)
    st.session_state.session_id = str(uuid4())
    st.session_state.login_time = datetime.now(TZ_MX)
    log_event(st.session_state.usuario_logueado, "login", st.session_state.session_id)

def do_logout():
    dur = ""
    if st.session_state.get("login_time"): # Usar get para evitar KeyError
        try:
            dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
        except TypeError: # Si login_time no era datetime
             dur = ""
    log_event(st.session_state.get("usuario_logueado"), "logout", st.session_state.get("session_id"), str(dur))
    # Limpiar estado de sesión
    for key in ["usuario_logueado", "session_id", "login_time", "nav_seccion"]:
        if key in st.session_state:
            del st.session_state[key]
    st.success("Sesión cerrada.")
    # No es necesario rerun aquí, la app se recargará al faltar usuario_logueado

# Inicialización de estado de sesión (redundante si se borra en logout, pero seguro)
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None
# ... (session_id y login_time se crean en do_login) ...
# -------------------------
# Navegación Principal (Sidebar Radio)
# -------------------------
# Usamos índice para guardar el estado, más robusto que el string
nav_options = ["🔍 Ver el estado de mis solicitudes",
               "🌟 Solicitudes CRM",
               "🛠️ Incidencias CRM",
               "📝 Quejas y sugerencias",
               "🔐 Zona Admin"]
if 'nav_index' not in st.session_state:
    st.session_state.nav_index = 0 # Default a la primera opción

# El radio ahora usa el índice guardado
nav_index = st.sidebar.radio(
    "Navegación",
    range(len(nav_options)), # Opciones son 0, 1, 2, ...
    format_func=lambda index: nav_options[index], # Muestra el texto
    key="nav_radio_selector" # Clave diferente a la de estado
    # index=st.session_state.nav_index # No necesario si key no cambia
)
# Actualizar el estado si el radio cambia
st.session_state.nav_index = nav_index
seccion = nav_options[nav_index] # Obtener el string de la sección actual

# ===================== SECCIÓN: CONSULTA =====================
if seccion == "🔍 Ver el estado de mis solicitudes":
    st.markdown("## 🔍 Consulta de Estado")

    # --- Login ---
    if st.session_state.get("usuario_logueado") is None:
        # Usar un formulario para el login
        with st.form("login_form"):
            clave = st.text_input("Ingresa tu contraseña", type="password")
            submitted = st.form_submit_button("Entrar")
            if submitted:
                clave_str = str(clave).strip()
                if clave_str in usuarios_dict:
                    do_login(usuarios_dict[clave_str])
                    st.success(f"Bienvenido, {st.session_state.usuario_logueado}")
                    st.rerun() # Recargar después del login exitoso
                else:
                    st.error("❌ Contraseña incorrecta")
    # --- Contenido si está logueado ---
    elif st.session_state.get("usuario_logueado"):
        st.info(f"Usuario: **{st.session_state.usuario_logueado}**") # Mostrar usuario
        if st.button("Cerrar sesión"):
            do_logout()
            st.rerun()

        # -------- Solicitudes --------
        st.subheader("📋 Solicitudes registradas")
        with st.spinner("Cargando solicitudes…"):
            df_s = get_records_simple(sheet_solicitudes)

        if "SolicitanteS" in df_s.columns:
            df_s['SolicitanteS'] = df_s['SolicitanteS'].astype(str)
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == st.session_state.usuario_logueado].copy()
            df_mias = df_mias.sort_values(by="FechaS", ascending=False) # Ordenar por fecha
        else:
            st.warning("⚠️ No se encontró 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()

        st.caption(f"{len(df_mias)} solicitudes encontradas.")

        # Usar st.container para cada solicitud (mejor manejo de keys)
        for index, row in df_mias.iterrows():
            with st.container(): # Contenedor para evitar problemas de key
                estado_norm = _norm(row.get("EstadoS", ""))
                sat_val_raw = row.get("SatisfaccionS", "")
                id_unico    = str(row.get("IDS", f"idx_{index}")).strip() # Usar índice si falta ID

                # Crear un identificador único para las keys de esta fila
                row_key_base = f"sol_{id_unico}"

                titulo = f"📌 {row.get('TipoS','Tipo?')} - {row.get('NombreS','Nombre?')} - {row.get('FechaS','Fecha?')} — Estado: {row.get('EstadoS','?')}"
                with st.expander(titulo):
                    st.markdown(f"""
                    **Área/Perfil/Rol:** {row.get('AreaS','-')} / {row.get('PerfilS','-')} / {row.get('RolS','-')}
                    **Horario/Turno:** {row.get('HorarioS','-')} / {row.get('TurnoS','-')}
                    **Solicitante:** {row.get('SolicitanteS','-')} | **Correo Usuario:** {row.get('CorreoS','-')}
                    """)
                    st.markdown(f"**Satisfacción actual:** {sat_val_raw or '(Sin calificar)'}")

                    is_attended = estado_norm.startswith("atendid")
                    unrated     = _is_unrated(sat_val_raw)

                    if is_attended and unrated and id_unico:
                        st.markdown("---") # Separador
                        st.caption("Califica la atención recibida:")
                        col1, col2 = st.columns([1,3])
                        with col1:
                            voto = st.radio("Voto:", ["👍","👎"], horizontal=True, key=f"vote_{row_key_base}")
                        with col2:
                            comentario = st.text_input("Comentario (opcional):", key=f"comm_{row_key_base}")

                        if st.button("Enviar calificación", key=f"send_{row_key_base}"):
                            try:
                                cell = with_backoff(sheet_solicitudes.find, id_unico)
                                if not cell:
                                    st.warning(f"No se pudo ubicar ID '{id_unico}' en 'Sheet1'.")
                                else:
                                    fila_excel = cell.row
                                    header_s = sheet_solicitudes.row_values(1)
                                    try:
                                        col_sat  = header_s.index("SatisfaccionS") + 1
                                        col_comm = header_s.index("ComentarioSatisfaccionS") + 1
                                        # Actualizar en batch
                                        cells_to_update = [
                                            gspread.Cell(fila_excel, col_sat, voto),
                                            gspread.Cell(fila_excel, col_comm, comentario)
                                        ]
                                        with_backoff(sheet_solicitudes.update_cells, cells_to_update, value_input_option='USER_ENTERED')
                                        st.success("¡Gracias por tu calificación!")
                                        time.sleep(1) # Pequeña pausa para ver mensaje
                                        st.rerun()
                                    except ValueError:
                                        st.error("Error: Faltan columnas 'SatisfaccionS' o 'ComentarioSatisfaccionS'.")
                                    except Exception as e:
                                         st.error(f"Error al actualizar celdas: {e}")
                            except Exception as e:
                                 st.error(f"Error general al buscar/guardar calificación: {e}")
        st.divider()

        # -------- Incidencias --------
        st.subheader("🛠️ Incidencias reportadas")
        with st.spinner("Cargando incidencias…"):
            df_i = get_records_simple(sheet_incidencias)

        if "CorreoI" in df_i.columns:
             df_i['CorreoI'] = df_i['CorreoI'].astype(str)
             df_mis_inc = df_i[df_i["CorreoI"].map(_email_norm) == st.session_state.usuario_logueado].copy()
             df_mis_inc = df_mis_inc.sort_values(by="FechaI", ascending=False) # Ordenar
        else:
            st.warning("⚠️ No se encontró 'CorreoI' en 'Incidencias'.")
            df_mis_inc = pd.DataFrame()

        st.caption(f"{len(df_mis_inc)} incidencias encontradas.")

        for index_i, row_i in df_mis_inc.iterrows():
             with st.container():
                estado_norm_i = _norm(row_i.get("EstadoI", ""))
                sat_val_raw_i = row_i.get("SatisfaccionI", "")
                id_unico_i    = str(row_i.get("IDI", f"idx_i_{index_i}")).strip()
                media_url = str(row_i.get("MediaFilenameI", "")).strip() # URL de GCS

                row_i_key_base = f"inc_{id_unico_i}"

                titulo = f"🛠️ {row_i.get('Asunto','Asunto?')} - {row_i.get('FechaI','Fecha?')} — Estado: {row_i.get('EstadoI','?')}"
                with st.expander(titulo):
                    st.markdown(f"""
                    **Categoría:** {row_i.get('CategoriaI','-')} | **Atendido por:** {row_i.get('AtendidoPorI','Pendiente')}
                    **Link (Zoho):** `{row_i.get('LinkI','-')}`
                    **Descripción:** {row_i.get('DescripcionI','-')}
                    **Respuesta:** {row_i.get('RespuestadeSolicitudI','Aún sin respuesta')}
                    """)

                    # --- Mostrar imagen/video desde URL GCS ---
                    if media_url and media_url.startswith("http"):
                        try:
                            file_ext = Path(media_url).suffix.lower()
                            st.markdown("---")
                            st.caption("Archivo Adjunto:")
                            if file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                                st.image(media_url)
                            elif file_ext in ['.mp4', '.mov', '.avi', '.wmv', '.mkv', '.webm', '.ogg']:
                                st.video(media_url)
                            else: # Para otros tipos como PDF, audio, etc.
                                st.markdown(f"📎 [Descargar/Ver Archivo]({media_url})")
                        except Exception as e:
                            st.warning(f"No se pudo mostrar adjunto. Enlace: {media_url}")
                            st.markdown(f"📎 [Ver Archivo]({media_url})")
                    elif media_url: # Si no es URL, mostrar como texto (ej. ID viejo)
                         st.caption(f"Archivo adjunto (ID?): `{media_url}`")
                    # --- FIN Mostrar ---

                    st.markdown(f"**⭐ Satisfacción actual:** {sat_val_raw_i or '(Sin calificar)'}")

                    is_attended_i = estado_norm_i.startswith("atendid")
                    unrated_i     = _is_unrated(sat_val_raw_i)

                    if is_attended_i and unrated_i and id_unico_i:
                        st.markdown("---")
                        st.caption("Califica la atención recibida:")
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            voto_i = st.radio("Voto:", ["👍", "👎"], horizontal=True, key=f"vote_{row_i_key_base}")
                        with col2:
                            comentario_i = st.text_input("Comentario (opcional):", key=f"comm_{row_i_key_base}")

                        if st.button("Enviar calificación", key=f"send_{row_i_key_base}"):
                            try:
                                cell = with_backoff(sheet_incidencias.find, id_unico_i)
                                if not cell:
                                    st.warning(f"No se encontró IDI '{id_unico_i}' en 'Incidencias'.")
                                else:
                                    fila_excel = cell.row
                                    header_i = sheet_incidencias.row_values(1)
                                    try:
                                        col_sat  = header_i.index("SatisfaccionI") + 1
                                        col_comm = header_i.index("ComentarioSatisfaccionI") + 1
                                        cells_to_update = [
                                            gspread.Cell(fila_excel, col_sat, voto_i),
                                            gspread.Cell(fila_excel, col_comm, comentario_i)
                                        ]
                                        with_backoff(sheet_incidencias.update_cells, cells_to_update, value_input_option='USER_ENTERED')
                                        st.success("¡Gracias por tu calificación!")
                                        time.sleep(1)
                                        st.rerun()
                                    except ValueError:
                                        st.error("Error: Faltan columnas 'SatisfaccionI' o 'ComentarioSatisfaccionI'.")
                                    except Exception as e:
                                         st.error(f"Error al actualizar celdas: {e}")
                            except Exception as e:
                                st.error(f"Error general al buscar/guardar calificación: {e}")
# ===================== SECCIÓN: SOLICITUDES CRM =====================
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")
    # Usar un formulario para evitar recargas parciales
    with st.form("solicitud_form", clear_on_submit=True):
        tipo = st.selectbox("Tipo de Solicitud en Zoho (*)", ["Selecciona...", "Alta", "Modificación", "Baja"])
        nombre = st.text_input("Nombre Completo de Usuario (*)")
        correo = st.text_input("Correo institucional (*)")
        area = st.selectbox("Área (*)", ["Selecciona..."] + list(estructura_roles.keys())) if tipo != "Baja" else "N/A" # Default si es Baja

        perfil = rol = numero_in = numero_saliente = horario = turno = ""
        # Mostrar campos dependientes solo si no es Baja
        if tipo != "Baja":
            if area and area != "Selecciona...":
                perfiles = ["Selecciona..."] + list(estructura_roles[area].keys())
                perfil = st.selectbox("Perfil (*)", perfiles)
                if perfil != "Selecciona...":
                    roles = ["Selecciona..."] + estructura_roles[area][perfil]
                    rol = st.selectbox("Rol (*)", roles)
                    if rol in numeros_por_rol:
                        if numeros_por_rol[rol].get("Numero_IN"):
                            numero_in = st.selectbox("Número IN", ["No aplica"] + numeros_por_rol[rol]["Numero_IN"]) # Opción "No aplica"
                        if numeros_por_rol[rol].get("Numero_Saliente"):
                            numero_saliente = st.selectbox("Número Saliente", ["No aplica"] + numeros_por_rol[rol]["Numero_Saliente"]) # Opción "No aplica"
                    horario = st.selectbox("Horario de trabajo (*)", ["Selecciona..."] + list(horarios_dict.keys()))
                    if horario != "Selecciona...":
                        turno = horarios_dict.get(horario, "")
                        st.text_input("Turno (Automático)", value=turno, disabled=True) # Mostrar turno

        correo_solicitante = st.text_input("Correo de quien lo solicita (*)")
        st.caption("(*) Campos obligatorios")
        submitted_sol = st.form_submit_button("✔️ Enviar Solicitud")

        if submitted_sol:
            # Validaciones dentro del form submit
            if tipo == "Selecciona..." or not nombre or not correo or not correo_solicitante:
                st.warning("⚠️ Faltan campos básicos obligatorios.")
            elif tipo != "Baja" and (area == "Selecciona..." or perfil == "Selecciona..." or rol == "Selecciona..." or horario == "Selecciona..."):
                st.warning("⚠️ Faltan campos de Área/Perfil/Rol/Horario.")
            # Simplificar validación de números (puede ser "No aplica")
            # elif perfil == "Agente de Call Center" and numero_in in ["Selecciona...", "No aplica", ""]: st.warning("⚠️ El perfil requiere Número IN.")
            # elif rol in numeros_por_rol and numeros_por_rol[rol].get("Numero_Saliente") and numero_saliente in ["Selecciona...", "No aplica", ""]: st.warning("⚠️ Este rol requiere Número Saliente.")
            else:
                try:
                    fila_sol = [
                        now_mx_str(), tipo, nombre.strip(), correo.strip(), area or "", perfil or "", rol or "",
                        "" if numero_in in ["Selecciona...", "No aplica"] else numero_in,
                        "" if numero_saliente in ["Selecciona...", "No aplica"] else numero_saliente,
                        "" if horario == "Selecciona..." else horario,
                        turno or "", _email_norm(correo_solicitante), "Pendiente",
                        "", "", str(uuid4()), "", ""
                    ]
                    with_backoff(sheet_solicitudes.append_row, fila_sol, value_input_option='USER_ENTERED')
                    st.success("✅ Solicitud registrada.")
                    st.balloons()
                    enviar_correo(
                        f"Solicitud CRM: {tipo} - {nombre}",
                        f"Tipo: {tipo}<br>Nombre: {nombre}<br>Correo: {correo}<br>Solicitante: {correo_solicitante}",
                        correo_solicitante
                    )
                    # No clear cache, no rerun (clear_on_submit=True lo hace)
                except Exception as e:
                    st.error(f"❌ Error al registrar solicitud: {e}")

# ===================== SECCIÓN: INCIDENCIAS CRM =====================
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias")

    with st.form("form_incidencia", clear_on_submit=True):
        col_a, col_b = st.columns(2)
        with col_a:
            correo_i = st.text_input("Correo de quien solicita (*)")
            categoria = st.selectbox(
                "Categoría",
                ["Desfase", "Reactivación", "Equivalencia", "Llamadas IVR", "Funcionalidad Zoho", "Mensajes", "Otros"]
            )
        with col_b:
            asunto = st.text_input("Asunto o título (*)")
            link = st.text_input("Link del registro afectado (Zoho)")

        descripcion = st.text_area("Descripción breve (*)", height=100)

        uploaded_file = st.file_uploader(
            "Adjuntar Imagen o Video (Opcional)",
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
            if uploaded_file is not None:
                st.info("Subiendo archivo a Google Cloud Storage...", icon="⏳")
                file_extension = Path(uploaded_file.name).suffix.lower()
                unique_filename = f"{uuid4()}{file_extension}"

                public_url = upload_to_gcs(
                    file_buffer=uploaded_file,
                    filename_in_bucket=unique_filename,
                    content_type=uploaded_file.type
                )
                if public_url:
                    google_cloud_storage_url = public_url
                else:
                    # El error ya se mostró en upload_to_gcs
                    st.warning("Incidencia se registrará sin adjunto.")

            try:
                # Verificar si la columna existe antes de armar la fila
                header_i = sheet_incidencias.row_values(1)
                expected_cols = 13 # FechaI..IDI, MediaFilenameI
                
                fila_inc = [
                    now_mx_str(), _email_norm(correo_i), asunto.strip(), categoria,
                    descripcion.strip(), link.strip(), "Pendiente", "", "", "", "",
                    str(uuid4()), # IDI
                    google_cloud_storage_url # MediaFilenameI
                ]
                
                # Ajustar longitud si la hoja tiene menos columnas (compatibilidad)
                fila_inc = fila_inc[:len(header_i)]

                with_backoff(sheet_incidencias.append_row, fila_inc, value_input_option='USER_ENTERED')
                st.success("✅ Incidencia registrada.")
                st.balloons()
            except Exception as e:
                st.error(f"❌ Error al registrar en Google Sheets: {e}")
                st.error(f"Fila que intentó guardar (puede estar truncada): {fila_inc}")

# ===================== SECCIÓN: QUEJAS =====================
elif seccion == "📝 Quejas y sugerencias":
    st.markdown("## 📝 Quejas y sugerencias")
    with st.form("queja_form", clear_on_submit=True):
        q_correo = st.text_input("Tu correo institucional (*)")
        q_tipo = st.selectbox("Tipo", ["Queja","Sugerencia"])
        q_asunto = st.text_input("Asunto (*)")
        q_categoria = st.selectbox("Categoría", ["Uso de CRM","Datos","Reportes","IVR","Mensajería","Soporte","Otro"])
        q_desc = st.text_area("Descripción (*)")
        q_calif = st.slider("Calificación (opcional)", 1, 5, 3) # Default a 3

        st.caption("(*) Campos obligatorios")
        submitted_q = st.form_submit_button("✔️ Enviar")

        if submitted_q:
            if not q_correo or not q_asunto or not q_desc:
                st.warning("Completa correo, asunto y descripción.")
            else:
                try:
                    # Asume orden de columnas como lo tenías
                    fila_q = [
                        now_mx_str(), _email_norm(q_correo), q_tipo, q_asunto,
                        q_desc, q_categoria, "Pendiente", q_calif, q_categoria
                    ]
                    # Ajustar por si 'CategoriaQ' duplicada ya no existe
                    header_q = sheet_quejas.row_values(1)
                    fila_q = fila_q[:len(header_q)]

                    with_backoff(sheet_quejas.append_row, fila_q, value_input_option='USER_ENTERED')
                    st.success("✅ Gracias por tu feedback.")
                    st.balloons()
                except Exception as e:
                    st.error(f"Error al registrar queja: {e}")

# ===================== SECCIÓN: ADMIN =====================
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")

    # --- Login Admin ---
    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    raw_emails = st.secrets.get("admin", {}).get("emails", [])
    ADMIN_EMAILS = {raw_emails.strip().lower()} if isinstance(raw_emails, str) else {e.strip().lower() for e in raw_emails}

    admin_ok = False
    # Chequear si ya está logueado como admin en la sesión
    if st.session_state.get("is_admin"):
        admin_ok = True
    else:
        admin_pass_input = st.text_input("Contraseña admin", type="password", key="admin_pass_input")
        if admin_pass_input:
            if admin_pass_input == ADMIN_PASS:
                st.session_state.is_admin = True # Guardar estado admin en sesión
                admin_ok = True
                st.rerun() # Recargar para mostrar contenido admin
            else:
                st.error("❌ Contraseña admin incorrecta")
        # Permitir acceso si el usuario logueado está en la lista blanca
        elif st.session_state.get("usuario_logueado") and (ADMIN_EMAILS and _email_norm(st.session_state["usuario_logueado"]) in ADMIN_EMAILS):
             st.session_state.is_admin = True
             admin_ok = True
             st.rerun()

    # --- Contenido Admin ---
    if admin_ok:
        st.success("🔑 Acceso de administrador concedido.")
        if st.button("Salir de Zona Admin"):
            del st.session_state.is_admin
            st.rerun()

        st.info("Cargando datos (puede tardar)...", icon="⏳")
        try: df_s = get_records_simple(sheet_solicitudes).sort_values(by="FechaS", ascending=False)
        except Exception: df_s = pd.DataFrame(); st.error("Error cargando Solicitudes")
        try: df_i = get_records_simple(sheet_incidencias).sort_values(by="FechaI", ascending=False)
        except Exception: df_i = pd.DataFrame(); st.error("Error cargando Incidencias")
        try: df_q = get_records_simple(sheet_quejas).sort_values(by="FechaQ", ascending=False)
        except Exception: df_q = pd.DataFrame(); st.error("Error cargando Quejas")

        st.success("Datos cargados.")

        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Quejas"])

        # ----- Solicitudes Admin -----
        with tab1:
             # (Tu código para admin de solicitudes - sin cambios funcionales)
             st.dataframe(df_s, use_container_width=True)
             if not df_s.empty and "EstadoS" in df_s.columns and "IDS" in df_s.columns:
                 # Seleccionar por ID único en lugar de índice
                 id_s_options = df_s["IDS"].tolist()
                 id_s_selected = st.selectbox("Selecciona ID de Solicitud", id_s_options, key="id_sol_admin_select")

                 if id_s_selected:
                     estado_s_admin = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_sol_admin")
                     colA, colB = st.columns(2)
                     with colA:
                         if st.button("Actualizar estado solicitud", key="btn_update_sol_admin"):
                             try:
                                 cell = with_backoff(sheet_solicitudes.find, id_s_selected)
                                 if cell:
                                     fila_excel = cell.row
                                     header_s = sheet_solicitudes.row_values(1)
                                     col_idx = header_s.index("EstadoS") + 1
                                     with_backoff(sheet_solicitudes.update_cell, fila_excel, col_idx, estado_s_admin)
                                     st.success(f"✅ ID {id_s_selected} actualizado.")
                                     time.sleep(1); st.rerun()
                                 else: st.error(f"ID {id_s_selected} no encontrado.")
                             except Exception as e: st.error(f"Error: {e}")
                     with colB:
                         if st.button("Eliminar solicitud", type="primary", key="btn_delete_sol_admin"):
                             try:
                                 cell = with_backoff(sheet_solicitudes.find, id_s_selected)
                                 if cell:
                                     with_backoff(sheet_solicitudes.delete_rows, cell.row)
                                     st.warning(f"⚠️ ID {id_s_selected} eliminado.")
                                     time.sleep(1); st.rerun()
                                 else: st.error(f"ID {id_s_selected} no encontrado.")
                             except Exception as e: st.error(f"Error: {e}")
             else: st.info("No hay solicitudes o faltan columnas 'EstadoS'/'IDS'.")


        # ----- Incidencias Admin -----
        with tab2:
            # (Tu código para admin de incidencias - sin cambios funcionales, solo usa IDI)
            st.dataframe(df_i, use_container_width=True)
            required_cols_i = {"EstadoI", "AtendidoPorI", "RespuestadeSolicitudI", "IDI"}
            if not df_i.empty and required_cols_i.issubset(df_i.columns):
                id_i_options = df_i["IDI"].tolist()
                id_i_selected = st.selectbox("Selecciona ID de Incidencia", id_i_options, key="id_inc_admin_select")

                if id_i_selected:
                     current_row = df_i[df_i["IDI"] == id_i_selected].iloc[0]
                     estado_i_admin = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], index=["Pendiente", "En proceso", "Atendido"].index(current_row.get("EstadoI","Pendiente")), key="estado_inc_admin")
                     atendido_por_admin = st.text_input("👨‍💼 Atendido por", value=current_row.get("AtendidoPorI",""), key="input_atendido_admin")
                     respuesta_admin = st.text_area("📜 Respuesta", value=current_row.get("RespuestadeSolicitudI",""), key="input_respuesta_admin")
                     colA, colB = st.columns(2)
                     with colA:
                         if st.button("Actualizar incidencia", key="btn_update_inc_admin"):
                             try:
                                 cell = with_backoff(sheet_incidencias.find, id_i_selected)
                                 if cell:
                                     fila_excel = cell.row
                                     header_i = sheet_incidencias.row_values(1)
                                     col_estado   = header_i.index("EstadoI") + 1
                                     col_atendido = header_i.index("AtendidoPorI") + 1
                                     col_resp     = header_i.index("RespuestadeSolicitudI") + 1
                                     cells = [
                                         gspread.Cell(fila_excel, col_estado, estado_i_admin),
                                         gspread.Cell(fila_excel, col_atendido, atendido_por_admin),
                                         gspread.Cell(fila_excel, col_resp, respuesta_admin),
                                     ]
                                     with_backoff(sheet_incidencias.update_cells, cells, value_input_option='USER_ENTERED')
                                     st.success(f"✅ ID {id_i_selected} actualizado.")
                                     time.sleep(1); st.rerun()
                                 else: st.error(f"ID {id_i_selected} no encontrado.")
                             except Exception as e: st.error(f"Error: {e}")
                     with colB:
                         if st.button("Eliminar incidencia", type="primary", key="btn_delete_inc_admin"):
                             try:
                                 cell = with_backoff(sheet_incidencias.find, id_i_selected)
                                 if cell:
                                     with_backoff(sheet_incidencias.delete_rows, cell.row)
                                     st.warning(f"⚠️ ID {id_i_selected} eliminado.")
                                     time.sleep(1); st.rerun()
                                 else: st.error(f"ID {id_i_selected} no encontrado.")
                             except Exception as e: st.error(f"Error: {e}")
            else: st.info("No hay incidencias o faltan columnas requeridas (EstadoI, AtendidoPorI, RespuestadeSolicitudI, IDI).")


        # ----- Quejas Admin -----
        with tab3:
            # (Tu código para admin de quejas - sin cambios funcionales)
             st.dataframe(df_q, use_container_width=True)
             if not df_q.empty and "EstadoQ" in df_q.columns and "FechaQ" in df_q.columns: # Usar FechaQ como ID si no hay uno
                 # Crear un ID temporal si no existe uno explícito
                 df_q['_temp_id'] = df_q["FechaQ"] + "_" + df_q["CorreoQ"]
                 id_q_options = df_q['_temp_id'].tolist()
                 id_q_selected = st.selectbox("Selecciona Queja (por Fecha+Correo)", id_q_options, key="id_queja_admin_select")

                 if id_q_selected:
                     current_row_q = df_q[df_q['_temp_id'] == id_q_selected].iloc[0]
                     estado_q_admin = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], index=["Pendiente", "En proceso", "Atendido"].index(current_row_q.get("EstadoQ","Pendiente")), key="estado_queja_admin")
                     # No usamos el índice de pandas (fila_q_idx) directamente
                     fila_q_idx_df = df_q[df_q['_temp_id'] == id_q_selected].index[0]

                     colA, colB = st.columns(2)
                     with colA:
                         if st.button("Actualizar queja", key="btn_update_queja_admin"):
                             try:
                                 # Buscar por contenido es menos fiable, usar índice de DF + offset
                                 fila_excel = int(fila_q_idx_df) + 2
                                 header_q = sheet_quejas.row_values(1)
                                 col_idx = header_q.index("EstadoQ") + 1
                                 with_backoff(sheet_quejas.update_cell, fila_excel, col_idx, estado_q_admin)
                                 st.success(f"✅ Fila {fila_excel} actualizada.")
                                 time.sleep(1); st.rerun()
                             except Exception as e: st.error(f"Error: {e}")
                     with colB:
                         if st.button("Eliminar queja", type="primary", key="btn_delete_queja_admin"):
                             try:
                                 fila_excel = int(fila_q_idx_df) + 2
                                 with_backoff(sheet_quejas.delete_rows, fila_excel)
                                 st.warning(f"⚠️ Fila {fila_excel} eliminada.")
                                 time.sleep(1); st.rerun()
                             except Exception as e: st.error(f"Error: {e}")
             else: st.info("No hay quejas o falta la columna 'EstadoQ'.")

    else:
        st.info("🔒 Ingresa la contraseña admin o usa un correo en la lista blanca para acceder.")


# --- ELEMENTOS MOVIDOS AL FINAL DE LA BARRA LATERAL ---
st.sidebar.divider()

# Botón de Recargar Página
if st.sidebar.button("♻️ Recargar Página"):
    # Limpiar cachés también es útil aquí a veces
    # st.cache_data.clear()
    # st.cache_resource.clear()
    st.rerun()

# Información del Entorno
if APP_MODE == "dev":
    st.sidebar.caption(f"🧪 DEV · `{SHEET_ID}`")
else:
    st.sidebar.caption(f"🚀 PROD · `{SHEET_ID}`")

# FIN DEL ARCHIVO
