import os
import json
import time, random
from uuid import uuid4
from datetime import datetime, timedelta
import re
import unicodedata
from pathlib import Path
import io

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from google.cloud import storage # GCS
import yagmail
from zoneinfo import ZoneInfo

# =========================
# Límites de subida (¡Añadidos!)
# =========================
MAX_IMAGE_MB = 10  # imágenes hasta 10 MB
MAX_VIDEO_MB = 50  # videos   hasta 50 MB
_MB = 1024 * 1024
MAX_IMAGE_BYTES = MAX_IMAGE_MB * _MB
MAX_VIDEO_BYTES = MAX_VIDEO_MB * _MB

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.wmv', '.mkv', '.webm', '.ogg'}

def _guess_is_image_or_video(file_name: str, mime: str | None):
    """Devuelve ('image'|'video'|None) y la extensión en minúsculas."""
    ext = Path(file_name).suffix.lower()
    if mime:
        if mime.startswith("image/"): return "image", ext
        if mime.startswith("video/"): return "video", ext
    if ext in IMAGE_EXTS: return "image", ext
    if ext in VIDEO_EXTS: return "video", ext
    return None, ext

def validate_upload_limits(uploaded_file) -> tuple[bool, str]:
    """Valida tamaño y tipo. Devuelve (ok, mensaje_error)."""
    if uploaded_file is None:
        return True, ""
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
        return False, "❌ Solo se permiten imágenes (jpg, png, webp, …) o videos (mp4, mov, webm, …)."
    return True, ""

# =========================
# Utils
# =========================
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
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        print(f"Warning: Could not load JSON file at {path}")
        return {}

TZ_MX = ZoneInfo("America/Mexico_City")
def now_mx_str() -> str:
    return datetime.now(TZ_MX).strftime("%d/%m/%Y %H:%M:%S")

# =========================
# Config / secrets
# =========================
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")

APP_MODE    = st.secrets.get("mode", "dev")
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))
SHEET_ID    = (st.secrets.get("sheets", {}).get("prod_id")
             if APP_MODE == "prod"
             else st.secrets.get("sheets", {}).get("dev_id"))

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
STORAGE_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform"
]

@st.cache_resource(ttl=3600)
def get_google_credentials_for_scopes(scopes):
    svc = st.secrets.get("google_service_account")
    if not svc:
        st.error("❗ Falta [google_service_account] en secrets.")
        st.stop()
    try:
        return Credentials.from_service_account_info(svc, scopes=scopes)
    except Exception as e:
        st.error(f"❌ Error al crear credenciales desde secrets: {e}")
        st.stop()

@st.cache_resource(ttl=3600)
def get_gspread_client():
    creds = get_google_credentials_for_scopes(SHEETS_SCOPES)
    return gspread.authorize(creds)

@st.cache_resource(ttl=3600)
def get_book():
    if not SHEET_ID:
        st.error("❗ No se encontró SHEET_ID en [sheets] de secrets.toml")
        st.stop()
    client = get_gspread_client()
    try:
        return with_backoff(client.open_by_key, SHEET_ID)
    except Exception as e:
        st.error(f"❌ Error al abrir Google Sheet (ID: {SHEET_ID}): {e}")
        st.stop()

book = get_book()

# --- Google Cloud Storage ---
GCS_BUCKET_NAME = st.secrets.get("google_cloud_storage", {}).get("bucket_name", "")

@st.cache_resource(ttl=3600)
def get_gcs_client():
    """Cliente para GCS con scopes de cloud-platform."""
    creds = get_google_credentials_for_scopes(STORAGE_SCOPES)
    project_id = st.secrets.get("google_service_account", {}).get("project_id", None)
    try:
        return storage.Client(project=project_id, credentials=creds)
    except Exception as e:
        st.error(f"❌ Error al crear cliente GCS: {e}")
        return None

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
        
        # Generar URL firmada en lugar de hacer público
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=expires_minutes), # 12 horas
            method="GET",
        )
        
        st.toast("☁️ Archivo subido a GCS.", icon="☁️")
        return signed_url
    except Exception as e:
        st.error(f"❌ Error al subir archivo a GCS: {e}")
        return None

# =========================
# Hojas / Worksheets
# =========================
sheets = {}
required_sheets = ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]
try:
    for sheet_name in required_sheets:
        sheets[sheet_name] = book.worksheet(sheet_name)
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

# =========================
# Lectores de datos (SIN caché)
# =========================
#@st.cache_data(ttl=180)
def get_records_simple(_ws) -> pd.DataFrame:
    ws_title = _ws.title
    try:
        all_values = with_backoff(_ws.get_all_values)
        if not all_values:
            return pd.DataFrame()
        header = [str(h).strip() for h in all_values[0]] # Limpiar header
        data = all_values[1:]
        num_cols = len(header)
        data_fixed = [row[:num_cols] + [""] * (num_cols - len(row)) for row in data]
        df = pd.DataFrame(data_fixed, columns=header)
        return df
    except Exception as e:
        st.error(f"Error al leer '{ws_title}': {e}")
        return pd.DataFrame()

# =========================
# Datos locales y usuarios
# =========================
data_folder = Path("data")
estructura_roles = load_json_safe(data_folder / "estructura_roles.json")
numeros_por_rol  = load_json_safe(data_folder / "numeros_por_rol.json")
horarios_dict    = load_json_safe(data_folder / "horarios.json")

def cargar_usuarios_df():
    try:
        df = get_records_simple(sheet_usuarios)
        if "Contraseña" not in df.columns or "Correo" not in df.columns:
            st.error("❌ Hoja 'Usuarios' debe tener columnas 'Contraseña' y 'Correo'.")
            return pd.DataFrame(columns=["Contraseña","Correo"])
        df['Contraseña'] = df['Contraseña'].astype(str).str.strip()
        df = df[df['Contraseña'] != ''] # Filtrar vacíos
        return df
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
usuarios_dict = {
    str(p): _email_norm(c)
    for p, c in zip(usuarios_df.get("Contraseña", []), usuarios_df.get("Correo", []))
    if str(p)
}

# =========================
# Email / Sesión
# =========================
def enviar_correo(asunto, mensaje_resumen, copia_a):
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
        cuerpo = f"""
        <p>Hola,</p>
        <p>Gracias por registrar tu solicitud en el CRM. Nuestro equipo la revisará y te daremos seguimiento lo antes posible.</p>
        <p><strong>Resumen:</strong><br>{mensaje_resumen}</p>
        <p>Saludos cordiales,<br><b>Equipo CRM UAG</b></p>
        """
        to_list = ["luis.alpizar@edu.uag.mx"]
        if copia_a and isinstance(copia_a, str) and '@' in copia_a:
            to_list.append(copia_a)
        cc_list = ["carlos.sotelo@edu.uag.mx", "esther.diaz@edu.uag.mx"]
        yag.send(
            to=to_list, cc=cc_list, subject=asunto, contents=[cuerpo],
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
    st.rerun()

def do_logout():
    dur = ""
    if st.session_state.get("login_time"):
        try:
            dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
        except TypeError:
            dur = ""
    log_event(st.session_state.get("usuario_logueado"), "logout", st.session_state.get("session_id"), str(dur))
    keys_to_clear = ["usuario_logueado", "session_id", "login_time", "nav_index", "is_admin"]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    st.success("Sesión cerrada.")
    st.rerun()

# =========================
# Estado inicial
# =========================
if "usuario_logueado" not in st.session_state:
    st.session_state.usuario_logueado = None
# =========================
# Navegación (Sidebar)
# =========================
nav_options = ["🔍 Ver el estado de mis solicitudes",
               "🌟 Solicitudes CRM",
               "🛠️ Incidencias CRM",
               "📝 Mejoras y sugerencias",
               "🔐 Zona Admin"]
if 'nav_index' not in st.session_state:
    st.session_state.nav_index = 0

nav_index = st.sidebar.radio(
    "Navegación",
    range(len(nav_options)),
    format_func=lambda index: nav_options[index],
    index=st.session_state.nav_index, # Usa el índice guardado
    key="nav_radio_selector"
)
st.session_state.nav_index = nav_index # Guarda el índice seleccionado
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
                    do_login(usuarios_dict[clave_str]) # Llama a login, que hace rerun
                else:
                    st.error("❌ Contraseña incorrecta")

    # --- Contenido si está logueado ---
    elif st.session_state.get("usuario_logueado"):
        st.info(f"Usuario: **{st.session_state.usuario_logueado}**")
        if st.button("Cerrar sesión"):
            do_logout() # Llama a logout, que hace rerun

        # -------- Solicitudes --------
        st.subheader("📋 Solicitudes registradas")
        with st.spinner("Cargando solicitudes…"):
            df_s = get_records_simple(sheet_solicitudes)

        if not df_s.empty and "SolicitanteS" in df_s.columns:
            df_s['SolicitanteS'] = df_s['SolicitanteS'].astype(str)
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == st.session_state.usuario_logueado].copy()
            # Ordenar por fecha (si existe)
            if "FechaS" in df_mias.columns:
                try:
                    df_mias['FechaS_dt'] = pd.to_datetime(df_mias['FechaS'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                    df_mias = df_mias.sort_values(by="FechaS_dt", ascending=False).drop(columns=['FechaS_dt'])
                except Exception:
                    pass # Falla silenciosamente si el formato de fecha es incorrecto
        else:
            if not df_s.empty: st.warning("⚠️ No se encontró 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()

        st.caption(f"{len(df_mias)} solicitudes encontradas.")

        # Obtener encabezados una sola vez
        header_s = sheet_solicitudes.row_values(1) if not df_s.empty else []

        for index, row in df_mias.iterrows():
            with st.container():
                estado_norm = _norm(row.get("EstadoS", ""))
                sat_val_raw = row.get("SatisfaccionS", "")
                id_unico    = str(row.get("IDS", f"idx_{index}")).strip()
                row_key_base = f"sol_{id_unico}"

                titulo = f"📌 {row.get('TipoS','?')} - {row.get('NombreS','?')} ({row.get('FechaS','?')}) — Estado: {row.get('EstadoS','?')}"
                with st.expander(titulo):
                    st.markdown(f"""
                    **Área/Perfil/Rol:** {row.get('AreaS','-')} / {row.get('PerfilS','-')} / {row.get('RolS','-')}
                    **Horario/Turno:** {row.get('HorarioS','-')} / {row.get('TurnoS','-')}
                    **Solicitante:** {row.get('SolicitanteS','-')} | **Correo Usuario:** {row.get('CorreoS','-')}
                    """)
                    st.markdown(f"**Satisfacción actual:** {sat_val_raw or '(Sin calificar)'}")

                    is_attended = estado_norm.startswith("atendid")
                    unrated     = _is_unrated(sat_val_raw)

                    if is_attended and unrated and id_unico and id_unico != f"idx_{index}":
                        st.markdown("---")
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
                                    try:
                                        col_sat  = header_s.index("SatisfaccionS") + 1
                                        col_comm = header_s.index("ComentarioSatisfaccionS") + 1
                                        cells_to_update = [gspread.Cell(fila_excel, col_sat, voto), gspread.Cell(fila_excel, col_comm, comentario)]
                                        with_backoff(sheet_solicitudes.update_cells, cells_to_update, value_input_option='USER_ENTERED')
                                        st.success("¡Gracias por tu calificación!")
                                        time.sleep(1); st.rerun()
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

        if not df_i.empty and "CorreoI" in df_i.columns:
             df_i['CorreoI'] = df_i['CorreoI'].astype(str)
             df_mis_inc = df_i[df_i["CorreoI"].map(_email_norm) == st.session_state.usuario_logueado].copy()
             if "FechaI" in df_mis_inc.columns:
                try: # Ordenar
                    df_mis_inc['FechaI_dt'] = pd.to_datetime(df_mis_inc['FechaI'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                    df_mis_inc = df_mis_inc.sort_values(by="FechaI_dt", ascending=False).drop(columns=['FechaI_dt'])
                except: pass
        else:
            if not df_i.empty: st.warning("⚠️ No se encontró 'CorreoI' en 'Incidencias'.")
            df_mis_inc = pd.DataFrame()

        st.caption(f"{len(df_mis_inc)} incidencias encontradas.")

        if not df_mis_inc.empty:
            header_i = sheet_incidencias.row_values(1) if not df_i.empty else []
            for index_i, row_i in df_mis_inc.iterrows():
                 with st.container():
                    estado_norm_i = _norm(row_i.get("EstadoI", ""))
                    sat_val_raw_i = row_i.get("SatisfaccionI", "")
                    id_unico_i    = str(row_i.get("IDI", f"idx_i_{index_i}")).strip()
                    media_url     = str(row_i.get("MediaFilenameI", "")).strip() # URL de GCS
                    row_i_key_base = f"inc_{id_unico_i}"

                    titulo = f"🛠️ {row_i.get('Asunto','?')} ({row_i.get('FechaI','?')}) — Estado: {row_i.get('EstadoI','?')}"
                    with st.expander(titulo):
                        st.markdown(f"""
                        **Categoría:** {row_i.get('CategoriaI','-')} | **Atendido por:** {row_i.get('AtendidoPorI','Pendiente')}
                        **Link (Zoho):** `{row_i.get('LinkI','-')}`
                        **Descripción:** {row_i.get('DescripcionI','-')}
                        **Respuesta:** {row_i.get('RespuestadeSolicitudI','Aún sin respuesta')}
                        """)

                        if media_url and media_url.startswith("http"):
                            try:
                                file_ext = Path(media_url.split('?')[0]).suffix.lower() # Quitar query params para extension
                                st.markdown("---"); st.caption("Archivo Adjunto:")
                                if file_ext in IMAGE_EXTS: st.image(media_url)
                                elif file_ext in VIDEO_EXTS: st.video(media_url)
                                else: st.markdown(f"📎 [Descargar/Ver Archivo]({media_url})")
                            except Exception as e: st.markdown(f"📎 [Ver Archivo (error: {e})]({media_url})")
                        elif media_url: st.caption(f"Adjunto (Info): `{media_url}`")
                        
                        st.markdown(f"**⭐ Satisfacción actual:** {sat_val_raw_i or '(Sin calificar)'}")

                        is_attended_i = estado_norm_i.startswith("atendid")
                        unrated_i     = _is_unrated(sat_val_raw_i)

                        if is_attended_i and unrated_i and id_unico_i and id_unico_i != f"idx_i_{index_i}":
                            st.markdown("---"); st.caption("Califica la atención:")
                            col1, col2 = st.columns([1, 3])
                            with col1: voto_i = st.radio("Voto:", ["👍", "👎"], horizontal=True, key=f"vote_{row_i_key_base}")
                            with col2: comentario_i = st.text_input("Comentario (opcional):", key=f"comm_{row_i_key_base}")
                            if st.button("Enviar calificación", key=f"send_{row_i_key_base}"):
                                try:
                                    cell = with_backoff(sheet_incidencias.find, id_unico_i)
                                    if not cell:
                                        st.warning(f"No se encontró IDI '{id_unico_i}' en 'Incidencias'.")
                                    else:
                                        fila_excel = cell.row
                                        try:
                                            col_sat  = header_i.index("SatisfaccionI") + 1
                                            col_comm = header_i.index("ComentarioSatisfaccionI") + 1
                                            cells_to_update = [gspread.Cell(fila_excel, col_sat, voto_i), gspread.Cell(fila_excel, col_comm, comentario_i)]
                                            with_backoff(sheet_incidencias.update_cells, cells_to_update, value_input_option='USER_ENTERED')
                                            st.success("¡Gracias por tu calificación!")
                                            time.sleep(1); st.rerun()
                                        except ValueError:
                                            st.error("Error: Faltan 'SatisfaccionI' o 'ComentarioSatisfaccionI'.")
                                        except Exception as e:
                                            st.error(f"Error al actualizar celdas: {e}")
                                except Exception as e:
                                    st.error(f"Error general al buscar/guardar calificación: {e}")
# ===================== SECCIÓN: SOLICITUDES CRM =====================
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
    for k, v in defaults.items():
        if k not in ss: ss[k] = v

    # --- Callbacks para resetear dropdowns ---
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
    # --- FORMULARIO ÚNICO: Todo debe ir DENTRO de st.form ---
    # -----------------------------------------------------------------
    with st.form("solicitud_form_unificado", clear_on_submit=True):
        st.markdown("### 1) Tipo de Solicitud")
        # 0) Tipo de solicitud
        tipo_solicitud = st.selectbox(
            "Tipo de Solicitud en Zoho (*)",
            ["Selecciona...", "Alta", "Modificación", "Baja"],
            key="sol_tipo" # Usamos key para que el estado se guarde
            # No usamos on_change aquí, la lógica de UI se manejará abajo
        )

        st.markdown("### 2) Datos del Usuario")
        c1, c2 = st.columns(2)
        with c1:
            nombre = st.text_input("Nombre Completo de Usuario (*)", key="sol_nombre")
        with c2:
            correo_user = st.text_input("Correo institucional del usuario (*)", key="sol_correo_user")

        # --- Campos Condicionales (Solo para Alta/Modificación) ---
        if tipo_solicitud in ["Alta", "Modificación"]:
            st.markdown("### 3) Definición del Puesto (cascada)")
            
            # Área
            areas = ["Selecciona..."] + list(estructura_roles.keys())
            area_idx = areas.index(ss.sol_area) if ss.sol_area in areas else 0
            st.selectbox("Área (*)", areas, index=area_idx, key="sol_area", on_change=on_change_area)
            
            # Perfil
            perfiles_disp = ["Selecciona..."]
            if ss.sol_area in estructura_roles:
                perfiles_disp += list(estructura_roles[ss.sol_area].keys())
            if ss.sol_perfil not in perfiles_disp: ss.sol_perfil = "Selecciona..." # Reset si no es válido
            perfil_idx = perfiles_disp.index(ss.sol_perfil)
            st.selectbox("Perfil (*)", perfiles_disp, index=perfil_idx, key="sol_perfil", on_change=on_change_perfil)
            
            # Rol
            roles_disp = ["Selecciona..."]
            if ss.sol_area in estructura_roles and ss.sol_perfil in estructura_roles[ss.sol_area]:
                roles_disp += estructura_roles[ss.sol_area][ss.sol_perfil]
            if ss.sol_rol not in roles_disp: ss.sol_rol = "Selecciona..." # Reset
            rol_idx = roles_disp.index(ss.sol_rol)
            st.selectbox("Rol (*)", roles_disp, index=rol_idx, key="sol_rol") # No necesita callback

            # 4) Números IN / Saliente
            show_numeros = ss.sol_rol in numeros_por_rol
            if show_numeros:
                st.markdown("### 4) Extensiones y salida (si aplica)")
                nums_cfg = numeros_por_rol.get(ss.sol_rol, {})
                nums_in = ["No aplica"] + nums_cfg.get("Numero_IN", [])
                nums_out = ["No aplica"] + nums_cfg.get("Numero_Saliente", [])
                c3, c4 = st.columns(2)
                with c3: st.selectbox("Número IN", nums_in, key="sol_num_in")
                with c4: st.selectbox("Número Saliente", nums_out, key="sol_num_out")

            # 5) Horario / Turno
            requiere_horario = ss.sol_perfil in {"Agente de Call Center", "Ejecutivo AC"}
            if requiere_horario:
                st.markdown("### 5) Horario de trabajo (*)")
                horarios_disp = ["Selecciona..."] + list(horarios_dict.keys())
                st.selectbox("Horario", horarios_disp, key="sol_horario", on_change=on_change_horario)
                st.text_input("Turno (Automático)", value=ss.sol_turno, disabled=True, key="sol_turno_display")

        st.markdown("### 6) Quién Solicita")
        correo_solicitante = st.text_input("Correo de quien lo solicita (*)", key="sol_correo_solicita")

        st.caption("(*) Campos obligatorios")
        submitted_sol = st.form_submit_button("✔️ Enviar Solicitud", use_container_width=True)

    # --- Lógica de envío (se ejecuta DESPUÉS del form) ---
    if submitted_sol:
        # Leer todos los valores desde session_state (porque los widgets están en el form)
        tipo    = ss.sol_tipo
        nombre  = ss.sol_nombre
        correo  = ss.sol_correo_user
        area    = ss.sol_area
        perfil  = ss.sol_perfil
        rol     = ss.sol_rol
        num_in  = ss.sol_num_in
        num_out = ss.sol_num_out
        horario = ss.sol_horario
        turno   = ss.sol_turno
        solicita = ss.sol_correo_solicita
        
        # --- Validaciones ---
        if tipo == "Selecciona..." or not nombre or not correo or not solicita:
            st.warning("⚠️ Faltan campos básicos obligatorios (Tipo, Nombre, Correo, Solicitante)."); st.stop()
        
        if tipo == "Baja":
             # Lógica para guardar Baja
            try:
                fila_sol = [now_mx_str(), "Baja", nombre.strip(), correo.strip(), "N/A", "N/A", "N/A", "", "", "", "", _email_norm(solicita), "Pendiente", "", "", str(uuid4()), "", ""]
                header_s = sheet_solicitudes.row_values(1); fila_sol = fila_sol[:len(header_s)]
                with_backoff(sheet_solicitudes.append_row, fila_sol, value_input_option='USER_ENTERED')
                st.success("✅ Baja registrada."); st.balloons()
                enviar_correo(f"Solicitud CRM: Baja - {nombre}", f"...", solicita)
                # Resetear valores visibles (clear_on_submit=True lo hace)
                ss.sol_nombre = ss.sol_correo_user = ss.sol_correo_solicita = "" # Limpia los inputs
            except Exception as e: st.error(f"❌ Error al registrar baja: {e}")

        elif tipo in ["Alta", "Modificación"]:
            # Validaciones para Alta/Mod
            if area == "Selecciona..." or perfil == "Selecciona..." or rol == "Selecciona...":
                st.warning("⚠️ Faltan campos de Área/Perfil/Rol."); st.stop()
            
            requiere_horario_check = perfil in {"Agente de Call Center", "Ejecutivo AC"}
            if requiere_horario_check and horario == "Selecciona...":
                st.warning("⚠️ Selecciona un horario de trabajo válido."); st.stop()

            # Lógica para guardar Alta/Mod
            try:
                num_in_val  = "" if (num_in == "No aplica") else str(num_in)
                num_out_val = "" if (num_out == "No aplica") else str(num_out)
                horario_val = "" if (not requiere_horario_check or horario == "Selecciona...") else horario
                turno_val   = "" if (not requiere_horario_check) else turno

                fila_sol = [
                    now_mx_str(), tipo, nombre.strip(), correo.strip(),
                    area, perfil, rol,
                    num_in_val, num_out_val, horario_val, turno_val,
                    _email_norm(solicita), "Pendiente",
                    "", "", str(uuid4()), "", ""
                ]
                header_s = sheet_solicitudes.row_values(1); fila_sol = fila_sol[:len(header_s)]
                with_backoff(sheet_solicitudes.append_row, fila_sol, value_input_option='USER_ENTERED')
                st.success("✅ Solicitud registrada."); st.balloons()
                enviar_correo(f"Solicitud CRM: {tipo} - {nombre}", f"...", solicita)
                # Limpiar los dropdowns de cascada
                for k in defaults: ss[k] = defaults[k]
                # st.rerun() # No es necesario, clear_on_submit limpia los inputs
            except Exception as e: st.error(f"❌ Error al registrar solicitud: {e}")

# ===================== SECCIÓN: INCIDENCIAS CRM =====================
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias")

    with st.form("form_incidencia", clear_on_submit=True):
        col_a, col_b = st.columns(2)
        with col_a:
            correo_i = st.text_input("Correo de quien solicita (*)")
            categoria = st.selectbox(
                 "Categoría",
                 ["Desfase", "Reactivación", "Equivalencia", "Llamadas IVR", 
                  "Funcionalidad Zoho", "Mensajes", "Otros", "Cambio de Periodo", 
                  "Cursos Zoho", "Asignación"] # Tu lista actualizada
            )
        with col_b:
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
            proceed_with_save = True

            if uploaded_file is not None:
                # --- Verificación de tamaño y tipo ---
                valid, error_msg = validate_upload_limits(uploaded_file)
                if not valid:
                    st.error(error_msg)
                    proceed_with_save = False
                # --- FIN Verificación ---

                if proceed_with_save:
                    st.info("Subiendo archivo a GCS...", icon="⏳")
                    file_extension = Path(uploaded_file.name).suffix.lower()
                    unique_filename = f"{uuid4()}{file_extension}"
                    
                    file_type = uploaded_file.type or "application/octet-stream"
                    public_url = upload_to_gcs(uploaded_file, unique_filename, file_type)
                    
                    if public_url:
                        google_cloud_storage_url = public_url
                    else:
                        st.error("Falló la subida a GCS. Incidencia se guardará sin adjunto.")
                        # Aún así guardamos la incidencia
                        proceed_with_save = True 

            if proceed_with_save:
                try:
                    header_i = sheet_incidencias.row_values(1)
                    fila_inc = [
                        now_mx_str(), _email_norm(correo_i), asunto.strip(), categoria,
                        descripcion.strip(), link.strip(), "Pendiente", "", "", "", "",
                        str(uuid4()), # IDI
                        google_cloud_storage_url # MediaFilenameI
                    ]
                    # Asegurar que la fila no sea más larga que el header
                    fila_inc = fila_inc[:len(header_i)]

                    with_backoff(sheet_incidencias.append_row, fila_inc, value_input_option='USER_ENTERED')
                    st.success("✅ Incidencia registrada.")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Error al registrar en Sheets: {e}")
                    st.error(f"Fila intentada: {fila_inc}")

# ===================== SECCIÓN: Sugerencias y Mejoras =====================
elif seccion == "📝 Mejoras y sugerencias":
    st.markdown("## 📝 Mejoras y sugerencias")
    with st.form("queja_form", clear_on_submit=True):
        q_correo = st.text_input("Tu correo institucional (*)")
        q_tipo = st.selectbox("Tipo", ["Mejora","Sugerencia"]) # Actualizado
        q_asunto = st.text_input("Asunto (*)")
        q_categoria = st.selectbox("Categoría", ["Uso de CRM","Datos","Reportes","IVR","Mensajería","Soporte","Otro"])
        q_desc = st.text_area("Descripción (*)")
        q_calif = st.slider("Prioridad (1=Baja, 5=Alta)", 1, 5, 3) # Etiqueta actualizada
        st.caption("(*) Campos obligatorios")
        submitted_q = st.form_submit_button("✔️ Enviar")

        if submitted_q:
            if not q_correo or not q_asunto or not q_desc:
                st.warning("Completa correo, asunto y descripción.")
            else:
                try:
                    header_q = sheet_quejas.row_values(1)
                    # Ajustar fila a las columnas de "Quejas"
                    fila_q = [
                        now_mx_str(), _email_norm(q_correo), q_tipo, q_asunto,
                        q_desc, q_categoria, "Pendiente", q_calif
                    ]
                    # Manejar columna duplicada si existe
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

    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    raw_emails = st.secrets.get("admin", {}).get("emails", [])
    ADMIN_EMAILS = {raw_emails.strip().lower()} if isinstance(raw_emails, str) else {e.strip().lower() for e in raw_emails}

    admin_ok = False
    if st.session_state.get("is_admin"):
        admin_ok = True
    else:
        with st.form("admin_login_form"):
             admin_pass_input = st.text_input("Contraseña admin", type="password")
             admin_login_submitted = st.form_submit_button("Entrar Admin")
             if admin_login_submitted:
                 if admin_pass_input and admin_pass_input == ADMIN_PASS:
                     st.session_state.is_admin = True; st.rerun()
                 else: st.error("❌ Contraseña incorrecta")
        # Acceso por lista blanca
        if not admin_ok and st.session_state.get("usuario_logueado"):
             if ADMIN_EMAILS and _email_norm(st.session_state["usuario_logueado"]) in ADMIN_EMAILS:
                  st.session_state.is_admin = True; st.rerun()

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
        except Exception: df_q = pd.DataFrame(); st.error("Error cargando Quejas")
        
        # Ordenar (los errores se manejan en get_records_simple, aquí solo ordenar si hay datos)
        if not df_s.empty and "FechaS" in df_s.columns:
            try: df_s['FechaS_dt'] = pd.to_datetime(df_s['FechaS'], format="%d/%m/%Y %H:%M:%S", errors='coerce'); df_s = df_s.sort_values(by="FechaS_dt", ascending=False).drop(columns=['FechaS_dt'])
            except: pass
        if not df_i.empty and "FechaI" in df_i.columns:
            try: df_i['FechaI_dt'] = pd.to_datetime(df_i['FechaI'], format="%d/%m/%Y %H:%M:%S", errors='coerce'); df_i = df_i.sort_values(by="FechaI_dt", ascending=False).drop(columns=['FechaI_dt'])
            except: pass
        if not df_q.empty and "FechaQ" in df_q.columns:
            try: df_q['FechaQ_dt'] = pd.to_datetime(df_q['FechaQ'], format="%d/%m/%Y %H:%M:%S", errors='coerce'); df_q = df_q.sort_values(by="FechaQ_dt", ascending=False).drop(columns=['FechaQ_dt'])
            except: pass
        st.success("Datos cargados.")

        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Mejoras"])

        # ----- Solicitudes Admin -----
        with tab1:
            st.dataframe(df_s, use_container_width=True)
            if not df_s.empty and "EstadoS" in df_s.columns and "IDS" in df_s.columns:
                ids_validos = df_s[df_s["IDS"] != '']["IDS"].unique().tolist()
                if ids_validos:
                    id_s_selected = st.selectbox("ID Solicitud a Modificar/Eliminar", ids_validos, key="id_sol_admin_select")
                    estado_s_admin = st.selectbox("Nuevo estado Solicitud", ["Pendiente", "En proceso", "Atendido"], key="estado_sol_admin")
                    colA, colB = st.columns(2)
                    with colA:
                        if st.button("Actualizar estado solicitud", key="btn_update_sol_admin"):
                            try:
                                cell = with_backoff(sheet_solicitudes.find, id_s_selected)
                                if cell:
                                    fila_excel = cell.row; header_s = sheet_solicitudes.row_values(1)
                                    col_idx = header_s.index("EstadoS") + 1
                                    with_backoff(sheet_solicitudes.update_cell, fila_excel, col_idx, estado_s_admin)
                                    st.success(f"✅ ID {id_s_selected} actualizado."); time.sleep(1); st.rerun()
                                else: st.error(f"ID {id_s_selected} no encontrado.")
                            except Exception as e: st.error(f"Error: {e}")
                    with colB:
                        if st.button("Eliminar solicitud", type="primary", key="btn_delete_sol_admin"):
                            try:
                                # --- INICIO CORRECCIÓN INDENTACIÓN ---
                                cell = with_backoff(sheet_solicitudes.find, id_s_selected)
                                if cell:
                                    with_backoff(sheet_solicitudes.delete_rows, cell.row)
                                    st.warning(f"⚠️ ID {id_s_selected} eliminado."); time.sleep(1); st.rerun()
                                else: 
                                    st.error(f"ID {id_s_selected} no encontrado.")
                                # --- FIN CORRECCIÓN INDENTACIÓN ---
                            except Exception as e: st.error(f"Error: {e}")
                else: st.info("No hay solicitudes con IDS válidos.")
            else: st.info("No hay solicitudes o faltan 'EstadoS'/'IDS'.")

        # ----- Incidencias Admin -----
        with tab2:
            st.dataframe(df_i, use_container_width=True)
            required_cols_i = {"EstadoI", "AtendidoPorI", "RespuestadeSolicitudI", "IDI"}
            if not df_i.empty and required_cols_i.issubset(df_i.columns):
                ids_i_validos = df_i[df_i["IDI"] != '']["IDI"].unique().tolist()
                if ids_i_validos:
                    id_i_selected = st.selectbox("ID Incidencia a Modificar/Eliminar", ids_i_validos, key="id_inc_admin_select")
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
                                    fila_excel = cell.row; header_i = sheet_incidencias.row_values(1)
                                    col_estado   = header_i.index("EstadoI") + 1
                                    col_atendido = header_i.index("AtendidoPorI") + 1
                                    col_resp     = header_i.index("RespuestadeSolicitudI") + 1
                                    cells = [gspread.Cell(fila_excel, col_estado, estado_i_admin), gspread.Cell(fila_excel, col_atendido, atendido_por_admin), gspread.Cell(fila_excel, col_resp, respuesta_admin)]
                                    with_backoff(sheet_incidencias.update_cells, cells, value_input_option='USER_ENTERED')
                                    st.success(f"✅ ID {id_i_selected} actualizado."); time.sleep(1); st.rerun()
                                else: st.error(f"ID {id_i_selected} no encontrado.")
                            except Exception as e: st.error(f"Error: {e}")
                    with colB:
                        if st.button("Eliminar incidencia", type="primary", key="btn_delete_inc_admin"):
                            try:
                                cell = with_backoff(sheet_incidencias.find, id_i_selected)
                                if cell:
                                    with_backoff(sheet_incidencias.delete_rows, cell.row)
                                    st.warning(f"⚠️ ID {id_i_selected} eliminado."); time.sleep(1); st.rerun()
                                else: st.error(f"ID {id_i_selected} no encontrado.")
                            except Exception as e: st.error(f"Error: {e}")
                else: st.info("No hay incidencias con IDI válidos.")
            else: st.info("No hay incidencias o faltan columnas (EstadoI, AtendidoPorI, RespuestadeSolicitudI, IDI).")

        # ----- Quejas Admin -----
        with tab3:
             st.dataframe(df_q, use_container_width=True)
             if not df_q.empty and "EstadoQ" in df_q.columns and "FechaQ" in df_q.columns:
                 df_q['_temp_id'] = df_q["FechaQ"] + "_" + df_q["CorreoQ"]
                 id_q_options = df_q['_temp_id'].tolist()
                 id_q_selected = st.selectbox("Selecciona Queja (por Fecha+Correo)", id_q_options, key="id_queja_admin_select")
                 if id_q_selected:
                     current_row_q = df_q[df_q['_temp_id'] == id_q_selected].iloc[0]
                     estado_queja_lista = ["Pendiente", "En proceso", "Atendido"]
                     estado_q_admin = st.selectbox("Nuevo estado", estado_queja_lista, index=estado_queja_lista.index(current_row_q.get("EstadoQ","Pendiente")), key="estado_queja_admin")
                     fila_q_idx_df = df_q[df_q['_temp_id'] == id_q_selected].index[0]
                     colA, colB = st.columns(2)
                     with colA:
                         if st.button("Actualizar queja", key="btn_update_queja_admin"):
                             try:
                                 fila_excel = int(fila_q_idx_df) + 2
                                 header_q = sheet_quejas.row_values(1)
                                 col_idx = header_q.index("EstadoQ") + 1
                                 with_backoff(sheet_quejas.update_cell, fila_excel, col_idx, estado_q_admin)
                                 st.success(f"✅ Fila {fila_excel} actualizada."); time.sleep(1); st.rerun()
                             except Exception as e: st.error(f"Error: {e}")
                     with colB:
                         if st.button("Eliminar queja", type="primary", key="btn_delete_queja_admin"):
                             try:
                                 fila_excel = int(fila_q_idx_df) + 2
                                 with_backoff(sheet_quejas.delete_rows, fila_excel)
                                 st.warning(f"⚠️ Fila {fila_excel} eliminada."); time.sleep(1); st.rerun()
                             except Exception as e: st.error(f"Error: {e}")
             else: st.info("No hay quejas o falta 'EstadoQ'.")

    # Si no es admin
    elif not admin_ok and st.session_state.get("usuario_logueado"):
        st.warning("🔒 No tienes permisos de administrador.")
    elif not admin_ok:
         st.info("🔒 Ingresa la contraseña de administrador para ver esta sección.")


# --- ELEMENTOS MOVIDOS AL FINAL DE LA BARRA LATERAL ---
st.sidebar.divider()
if st.sidebar.button("♻️ Recargar Página"):
    # Limpiar cachés de recursos puede ser útil si los permisos o conexiones cambian
    # st.cache_resource.clear() 
    st.rerun()

# Info de Entorno (sin ID en PROD)
env_id_info = f"· `{SHEET_ID}`" if APP_MODE == "dev" else ""
env_icon = "🧪" if APP_MODE == "dev" else "🚀"
st.sidebar.caption(f"{env_icon} ENTORNO: {APP_MODE.upper()} {env_id_info}")

# FIN DEL ARCHIVO
