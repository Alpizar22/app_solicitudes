import os
import json
import time, random
from uuid import uuid4
from datetime import datetime
import re
from pathlib import Path
import io

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from google.cloud import storage  # GCS
import yagmail
from zoneinfo import ZoneInfo
from datetime import timedelta

# -------------------------
# Límites de subida (AJUSTA si deseas)
# -------------------------
MAX_IMAGE_MB = 10   # imágenes hasta 10 MB
MAX_VIDEO_MB = 50   # videos   hasta 50 MB
_MB = 1024 * 1024
MAX_IMAGE_BYTES = MAX_IMAGE_MB * _MB
MAX_VIDEO_BYTES = MAX_VIDEO_MB * _MB

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.wmv', '.mkv', '.webm', '.ogg'}

def _guess_is_image_or_video(file_name: str, mime: str | None):
    """
    Devuelve ('image'|'video'|None) y la extensión (en minúsculas).
    Usa MIME si existe, si no cae a extensión.
    """
    ext = Path(file_name).suffix.lower()
    if mime:
        if mime.startswith("image/"): return "image", ext
        if mime.startswith("video/"): return "video", ext
    if ext in IMAGE_EXTS: return "image", ext
    if ext in VIDEO_EXTS: return "video", ext
    return None, ext

def validate_upload_limits(uploaded_file) -> tuple[bool, str]:
    """
    Valida tamaño y tipo. Devuelve (ok, mensaje_error_si_hay).
    """
    if uploaded_file is None:
        return True, ""
    kind, ext = _guess_is_image_or_video(uploaded_file.name, getattr(uploaded_file, "type", None))
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

# -------------------------
# Config / secrets
# -------------------------
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")

APP_MODE   = st.secrets.get("mode", "dev")
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))
SHEET_ID   = (st.secrets.get("sheets", {}).get("prod_id")
              if APP_MODE == "prod"
              else st.secrets.get("sheets", {}).get("dev_id"))

# --- Scopes separados ---
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
STORAGE_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform"  # recomendado para GCS
]

# --- Utilidades de credenciales por scope ---
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

# --- Cliente gspread (Sheets) ---
@st.cache_resource(ttl=3600)
def get_gspread_client():
    creds = get_google_credentials_for_scopes(SHEETS_SCOPES)
    return gspread.authorize(creds)

# --- Abrir el Google Sheet ---
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
    project_id = st.secrets["google_service_account"]["project_id"]
    try:
        return storage.Client(project=project_id, credentials=creds)
    except Exception as e:
        st.error(f"❌ Error al crear cliente GCS: {e}")
        return None

# --- Subida a GCS usando make_public (tu lógica actual) ---
# --- Subida a GCS con URL firmada (compatible con UBLA/PAP) ---
def upload_to_gcs(file_buffer, filename_in_bucket, content_type, expires_minutes=720):
    """
    Sube un archivo a GCS y devuelve una URL firmada temporal (no usa ACLs ni make_public).
    - file_buffer: archivo de st.file_uploader (file-like)
    - filename_in_bucket: nombre final en el bucket (p.ej., 'abc123.png')
    - content_type: MIME (p.ej., 'image/png' o 'video/mp4')
    - expires_minutes: vigencia de la URL firmada
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

        # subir contenido
        file_buffer.seek(0)
        with_backoff(blob.upload_from_file, file_buffer, content_type=content_type, rewind=True)

        # generar URL firmada (no pública, pero accesible con el link)
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=expires_minutes),
            method="GET",
        )

        st.toast("☁️ Archivo subido a GCS.", icon="☁️")
        return signed_url

    except Exception as e:
        st.error(f"❌ Error al subir archivo a GCS: {e}")
        return None


# -------------------------
# Conexión a las hojas/worksheets
# -------------------------
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

# --- Lector de datos (Sin caché) ---
def get_records_simple(_ws) -> pd.DataFrame:
    ws_title = _ws.title
    try:
        all_values = with_backoff(_ws.get_all_values)
        if not all_values:
            return pd.DataFrame()
        header = [str(h).strip() for h in all_values[0]]
        data = all_values[1:]
        num_cols = len(header)
        data_fixed = [row[:num_cols] + [""] * (num_cols - len(row)) for row in data]
        df = pd.DataFrame(data_fixed, columns=header)
        return df
    except Exception as e:
        st.error(f"Error al leer '{ws_title}': {e}")
        return pd.DataFrame()

# -------------------------
# Datos locales (JSON) y Usuarios (desde GSheets)
# -------------------------
estructura_roles = load_json_safe("data/estructura_roles.json")
numeros_por_rol  = load_json_safe("data/numeros_por_rol.json")
horarios_dict    = load_json_safe("data/horarios.json")

def cargar_usuarios_df():
    try:
        df = get_records_simple(sheet_usuarios)
        if "Contraseña" not in df.columns or "Correo" not in df.columns:
            st.error("❌ Hoja 'Usuarios' debe tener columnas 'Contraseña' y 'Correo'.")
            return pd.DataFrame(columns=["Contraseña","Correo"])
        df['Contraseña'] = df['Contraseña'].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
usuarios_dict = {str(p): _email_norm(c)
                 for p, c in zip(usuarios_df.get("Contraseña", []),
                                 usuarios_df.get("Correo", []))
                 if str(p)}

# -------------------------
# Email, Sesión, Login/Logout
# -------------------------
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

def do_logout():
    dur = ""
    if st.session_state.get("login_time"):
        try:
            dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
        except TypeError:
            dur = ""
    log_event(st.session_state.get("usuario_logueado"), "logout", st.session_state.get("session_id"), str(dur))
    for key in ["usuario_logueado", "session_id", "login_time", "nav_seccion"]:
        if key in st.session_state:
            del st.session_state[key]
    st.success("Sesión cerrada.")

# Estado inicial
if "usuario_logueado" not in st.session_state:
    st.session_state.usuario_logueado = None

# -------------------------
# Navegación (Sidebar)
# -------------------------
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
    key="nav_radio_selector"
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
                    st.success(f"Bienvenido, {st.session_state.usuario_logueado}")
                    st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")

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

        if "SolicitanteS" in df_s.columns:
            df_s['SolicitanteS'] = df_s['SolicitanteS'].astype(str)
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == st.session_state.usuario_logueado].copy()
            df_mias = df_mias.sort_values(by="FechaS", ascending=False)
        else:
            st.warning("⚠️ No se encontró 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()

        st.caption(f"{len(df_mias)} solicitudes encontradas.")

        for index, row in df_mias.iterrows():
            with st.container():
                estado_norm = _norm(row.get("EstadoS", ""))
                sat_val_raw = row.get("SatisfaccionS", "")
                id_unico    = str(row.get("IDS", f"idx_{index}")).strip()

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
                                    header_s = sheet_solicitudes.row_values(1)
                                    try:
                                        col_sat  = header_s.index("SatisfaccionS") + 1
                                        col_comm = header_s.index("ComentarioSatisfaccionS") + 1
                                        cells_to_update = [
                                            gspread.Cell(fila_excel, col_sat, voto),
                                            gspread.Cell(fila_excel, col_comm, comentario)
                                        ]
                                        with_backoff(sheet_solicitudes.update_cells, cells_to_update, value_input_option='USER_ENTERED')
                                        st.success("¡Gracias por tu calificación!")
                                        time.sleep(1)
                                        st.rerun()
                                    except ValueError:
                                        st.error("Error: Faltan columnas 'SatisfaccionS' o 'ComentarioSatisfaccionS'.")
                                    except Exception as e:
                                        st.error(f"Error al actualizar celdas: {e}")

                            except Exception as e:
                                st.error(f"Error general al buscar/guardar calificación: {e}")

# ===================== SECCIÓN: SOLICITUDES CRM =====================
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")

    with st.form("solicitud_form", clear_on_submit=True):
        tipo = st.selectbox("Tipo de Solicitud en Zoho (*)", ["Selecciona...", "Alta", "Modificación", "Baja"])
        nombre = st.text_input("Nombre Completo de Usuario (*)")
        correo = st.text_input("Correo institucional (*)")
        area = st.selectbox("Área (*)", ["Selecciona..."] + list(estructura_roles.keys())) if tipo != "Baja" else "N/A"

        perfil = rol = numero_in = numero_saliente = horario = turno = ""
        if tipo != "Baja":
            if area and area != "Selecciona...":
                perfiles = ["Selecciona..."] + list(estructura_roles[area].keys())
                perfil = st.selectbox("Perfil (*)", perfiles)
                if perfil != "Selecciona...":
                    roles = ["Selecciona..."] + estructura_roles[area][perfil]
                    rol = st.selectbox("Rol (*)", roles)
                    if rol in numeros_por_rol:
                        if numeros_por_rol[rol].get("Numero_IN"):
                            numero_in = st.selectbox("Número IN", ["No aplica"] + numeros_por_rol[rol]["Numero_IN"])
                        if numeros_por_rol[rol].get("Numero_Saliente"):
                            numero_saliente = st.selectbox("Número Saliente", ["No aplica"] + numeros_por_rol[rol]["Numero_Saliente"])
                    horario = st.selectbox("Horario de trabajo (*)", ["Selecciona..."] + list(horarios_dict.keys()))
                    if horario != "Selecciona...":
                        turno = horarios_dict.get(horario, "")
                        st.text_input("Turno (Automático)", value=turno, disabled=True)

        correo_solicitante = st.text_input("Correo de quien lo solicita (*)")
        st.caption("(*) Campos obligatorios")
        submitted_sol = st.form_submit_button("✔️ Enviar Solicitud")

        if submitted_sol:
            if tipo == "Selecciona..." or not nombre or not correo or not correo_solicitante:
                st.warning("⚠️ Faltan campos básicos obligatorios.")
            elif tipo != "Baja" and (area == "Selecciona..." or perfil == "Selecciona..." or rol == "Selecciona..." or horario == "Selecciona..."):
                st.warning("⚠️ Faltan campos de Área/Perfil/Rol/Horario.")
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
            f"Adjuntar Imagen (≤ {MAX_IMAGE_MB} MB) o Video (≤ {MAX_VIDEO_MB} MB) — Opcional",
            type=['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp',
                  'mp4', 'mov', 'avi', 'wmv', 'mkv', 'webm', 'ogg'],
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
                # 1) Validar tamaño y tipo
                ok, err = validate_upload_limits(uploaded_file)
                if not ok:
                    st.error(err)
                    proceed_with_save = False
                else:
                    # 2) Subir a GCS (solo si pasó validaciones)
                    st.info("Subiendo archivo a Google Cloud Storage...", icon="⏳")
                    file_extension = Path(uploaded_file.name).suffix.lower()
                    unique_filename = f"{uuid4()}{file_extension}"

                    public_url = upload_to_gcs(
                        file_buffer=uploaded_file,
                        filename_in_bucket=unique_filename,
                        content_type=(uploaded_file.type or "application/octet-stream")
                    )
                    if public_url:
                        google_cloud_storage_url = public_url
                    else:
                        st.warning("No se pudo adjuntar el archivo. La incidencia se registrará sin adjunto.")

            # Si deseas BLOQUEAR el guardado cuando falle el adjunto, descomenta:
            # if not proceed_with_save:
            #     st.stop()

            try:
                header_i = sheet_incidencias.row_values(1)
                fila_inc = [
                    now_mx_str(), _email_norm(correo_i), asunto.strip(), categoria,
                    descripcion.strip(), link.strip(), "Pendiente", "", "", "", "",
                    str(uuid4()),  # IDI
                    google_cloud_storage_url  # MediaFilenameI
                ]
                fila_inc = fila_inc[:len(header_i)]

                with_backoff(sheet_incidencias.append_row, fila_inc, value_input_option='USER_ENTERED')
                st.success("✅ Incidencia registrada.")
                st.balloons()
            except Exception as e:
                st.error(f"❌ Error al registrar en Google Sheets: {e}")
                try:
                    st.error(f"Fila que intentó guardar (puede estar truncada): {fila_inc}")
                except:
                    pass

# ===================== SECCIÓN: QUEJAS =====================
elif seccion == "📝 Mejoras y sugerencias":
    st.markdown("## 📝 Mejoras y sugerencias")
    with st.form("queja_form", clear_on_submit=True):
        q_correo = st.text_input("Tu correo institucional (*)")
        q_tipo = st.selectbox("Tipo", ["Mejora","Sugerencia"])
        q_asunto = st.text_input("Asunto (*)")
        q_categoria = st.selectbox("Categoría", ["Uso de CRM","Datos","Reportes","IVR","Mensajería","Soporte","Otro"])
        q_desc = st.text_area("Descripción (*)")
        q_calif = st.slider("Calificación (opcional)", 1, 5, 3)

        st.caption("(*) Campos obligatorios")
        submitted_q = st.form_submit_button("✔️ Enviar")

        if submitted_q:
            if not q_correo or not q_asunto or not q_desc:
                st.warning("Completa correo, asunto y descripción.")
            else:
                try:
                    fila_q = [
                        now_mx_str(), _email_norm(q_correo), q_tipo, q_asunto,
                        q_desc, q_categoria, "Pendiente", q_calif, q_categoria
                    ]
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

    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    raw_emails = st.secrets.get("admin", {}).get("emails", [])
    ADMIN_EMAILS = {raw_emails.strip().lower()} if isinstance(raw_emails, str) else {e.strip().lower() for e in raw_emails}

    admin_ok = False
    if st.session_state.get("is_admin"):
        admin_ok = True
    else:
        admin_pass_input = st.text_input("Contraseña admin", type="password", key="admin_pass_input")
        if admin_pass_input:
            if admin_pass_input == ADMIN_PASS:
                st.session_state.is_admin = True
                admin_ok = True
                st.rerun()
            else:
                st.error("❌ Contraseña admin incorrecta")
        elif st.session_state.get("usuario_logueado") and (ADMIN_EMAILS and _email_norm(st.session_state["usuario_logueado"]) in ADMIN_EMAILS):
            st.session_state.is_admin = True
            admin_ok = True
            st.rerun()

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
            st.dataframe(df_s, use_container_width=True)
            if not df_s.empty and "EstadoS" in df_s.columns and "IDS" in df_s.columns:
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
            else:
                st.info("No hay solicitudes o faltan columnas 'EstadoS'/'IDS'.")

        # ----- Incidencias Admin -----
        with tab2:
            st.dataframe(df_i, use_container_width=True)
            required_cols_i = {"EstadoI", "AtendidoPorI", "RespuestadeSolicitudI", "IDI"}
            if not df_i.empty and required_cols_i.issubset(df_i.columns):
                id_i_options = df_i["IDI"].tolist()
                id_i_selected = st.selectbox("Selecciona ID de Incidencia", id_i_options, key="id_inc_admin_select")

                if id_i_selected:
                    current_row = df_i[df_i["IDI"] == id_i_selected].iloc[0]
                    estado_i_admin = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"],
                                                  index=["Pendiente", "En proceso", "Atendido"].index(current_row.get("EstadoI","Pendiente")),
                                                  key="estado_inc_admin")
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
            else:
                st.info("No hay incidencias o faltan columnas requeridas (EstadoI, AtendidoPorI, RespuestadeSolicitudI, IDI).")

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
                    estado_q_admin = st.selectbox(
                        "Nuevo estado",
                        estado_queja_lista,
                        index=estado_queja_lista.index(current_row_q.get("EstadoQ","Pendiente")),
                        key="estado_queja_admin"
                    )
                    fila_q_idx_df = df_q[df_q['_temp_id'] == id_q_selected].index[0]

                    colA, colB = st.columns(2)
                    with colA:
                        if st.button("Actualizar queja", key="btn_update_queja_admin"):
                            try:
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
            else:
                st.info("No hay quejas o falta la columna 'EstadoQ'.")

    else:
        st.info("🔒 Ingresa la contraseña admin o usa un correo en la lista blanca para acceder.")

# -------------------------
# Sidebar final
# -------------------------
st.sidebar.divider()
if st.sidebar.button("♻️ Recargar Página"):
    st.rerun()

if APP_MODE == "dev":
    st.sidebar.caption(f"🧪 DEV · `{SHEET_ID}`")
else:
    st.sidebar.caption(f"🚀 PROD · `{SHEET_ID}`")

# FIN DEL ARCHIVO
