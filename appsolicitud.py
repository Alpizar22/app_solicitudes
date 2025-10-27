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
from google.oauth2.service_account import Credentials # Asegúrate que esté
from googleapiclient.discovery import build # Para Drive
from googleapiclient.http import MediaIoBaseUpload # Para subir a Drive
import yagmail
from zoneinfo import ZoneInfo
from PIL import Image # No se usa directamente pero puede ser útil si procesas imágenes

# -------------------------
# Constantes y Configuración Inicial
# -------------------------
# MEDIA_FOLDER = Path("media") # Ya no necesitamos guardar localmente
# MEDIA_FOLDER.mkdir(exist_ok=True)

# -------------------------
# Utilidades y normalizadores (sin cambios)
# -------------------------
EMAIL_RE = re.compile(r'([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})', re.I)

def _email_norm(s: str) -> str:
    # ... (código igual) ...
    if s is None:
        return ""
    text = str(s)
    m = EMAIL_RE.search(text)
    if m:
        return m.group(1).strip().lower()
    return text.strip().lower()

def _norm(x):
    # ... (código igual) ...
     return str(x).strip().lower() if pd.notna(x) else ""

def _is_unrated(val: str) -> bool:
    # ... (código igual) ...
     v = _norm(val)
     return v in ("", "pendiente", "na", "n/a", "sin calificacion", "sin calificación", "none", "null", "-")

def with_backoff(fn, *args, **kwargs):
    # ... (código igual) ...
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e):
                time.sleep(min(1*(2**i) + random.random(), 8))
                continue
            raise

def load_json_safe(path: str) -> dict:
    # ... (código igual) ...
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

TZ_MX = ZoneInfo("America/Mexico_City")
def now_mx_str() -> str:
    # ... (código igual) ...
    return datetime.now(TZ_MX).strftime("%d/%m/%Y %H:%M")

# -------------------------
# Config / secrets
# -------------------------
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")

APP_MODE = st.secrets.get("mode", "dev")
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))
SHEET_ID = (st.secrets.get("sheets", {}).get("prod_id") if APP_MODE == "prod"
            else st.secrets.get("sheets", {}).get("dev_id"))
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # Drive scope ya estaba

# --- NUEVO: Leer ID de carpeta Drive ---
DRIVE_FOLDER_ID = st.secrets.get("google_drive", {}).get("folder_id", "")
if not DRIVE_FOLDER_ID and APP_MODE == "prod": # Solo advertir en producción
    st.warning("⚠️ No se encontró google_drive.folder_id en secrets. No se podrán subir archivos a Drive.")

if not SHEET_ID:
    st.error("❗ No se encontró SHEET_ID en [sheets] de secrets.toml")
    st.stop()

# (El código para mostrar info en sidebar se movió al final)
# -------------------------
# Conexión a Google Sheets
# -------------------------
@st.cache_resource
def get_book():
    creds_dict = st.secrets.get("google_service_account")
    if not creds_dict:
        st.error("❗ No se encontró [google_service_account] en secrets.")
        st.stop()
    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)
        return with_backoff(client.open_by_key, SHEET_ID)
    except Exception as e:
        st.error(f"❌ Error al conectar con Google Sheets: {e}")
        keys_found = {k: v for k, v in creds_dict.items() if k != 'private_key'}
        st.error(f"Claves encontradas en [google_service_account]: {list(keys_found.keys())}")
        st.stop()

book = get_book()

# --- NUEVO: Conexión a Google Drive ---
@st.cache_resource # Cacheamos la conexión a Drive
def get_drive_service():
    """Crea y retorna el objeto de servicio para interactuar con Google Drive API v3."""
    creds_dict = st.secrets.get("google_service_account")
    if not creds_dict:
        # No detenemos la app, solo advertimos
        st.warning("⚠️ [Drive] No se encontró [google_service_account] en secrets.")
        return None
    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        st.error(f"❌ Error al conectar con Google Drive API: {e}")
        return None

# --- Función para subir archivo a Drive ---
def upload_to_drive(file_buffer, filename, mimetype):
    """Sube un archivo (desde buffer en memoria) a la carpeta especificada en Google Drive."""
    service = get_drive_service() # Obtiene el servicio cacheado
    if not service:
        st.error("❌ No se puede subir a Drive: servicio no disponible.")
        return None, None
    if not DRIVE_FOLDER_ID:
        st.error("❌ No se puede subir a Drive: falta google_drive.folder_id en secrets.")
        return None, None

    try:
        file_metadata = {
            'name': filename,
            'parents': [DRIVE_FOLDER_ID] # ID de la carpeta destino
        }
        # Usamos io.BytesIO para crear un 'file-like object' desde los bytes
        media = MediaIoBaseUpload(io.BytesIO(file_buffer.getvalue()),
                                  mimetype=mimetype,
                                  resumable=True)
        # Ejecutamos la subida
        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      fields='id, webViewLink').execute() # Pedimos ID y link
        file_id = file.get('id')
        view_link = file.get('webViewLink')
        print(f"Archivo subido a Drive. ID: {file_id}, Link: {view_link}") # Log para depuración
        st.toast(f"Archivo '{filename}' subido a Drive.", icon="☁️")
        return file_id, view_link # Retornamos ID y Link
    except Exception as e:
        st.error(f"❌ Error al subir archivo a Google Drive: {e}")
        return None, None

# --- Conexión a las pestañas ---
try:
    sheet_solicitudes = book.worksheet("Sheet1")
    sheet_incidencias = book.worksheet("Incidencias")
    sheet_quejas      = book.worksheet("Quejas")
    sheet_accesos     = book.worksheet("Accesos")
    sheet_usuarios    = book.worksheet("Usuarios")
except gspread.WorksheetNotFound as e:
    st.error(f"❌ No se encontró una de las hojas requeridas: {e}")
    st.stop()

# --- Lector de datos (sin caché como solicitaste) ---
#@st.cache_data(ttl=180)
def get_records_simple(_ws) -> pd.DataFrame:
    """Lee toda la hoja usando get_all_records (sin caché)"""
    try:
        # Usamos header=1 asumiendo que la fila 1 son encabezados
        # get_all_records() interpreta la primera fila como header
        return pd.DataFrame(_ws.get_all_records())
    except Exception as e:
        # Intenta leer raw si get_all_records falla (ej. encabezados duplicados)
        try:
            st.warning(f"get_all_records falló para '{_ws.title}', intentando leer raw: {e}")
            all_values = _ws.get_all_values()
            if len(all_values) > 1:
                return pd.DataFrame(all_values[1:], columns=all_values[0])
            else:
                return pd.DataFrame(columns=all_values[0] if all_values else [])
        except Exception as e2:
            st.error(f"Error grave al leer '{_ws.title}': {e2}")
            return pd.DataFrame()

# -------------------------
# Datos locales y Usuarios (desde GSheets)
# -------------------------
estructura_roles = load_json_safe("data/estructura_roles.json")
numeros_por_rol  = load_json_safe("data/numeros_por_rol.json")
horarios_dict    = load_json_safe("data/horarios.json")

#@st.cache_data(ttl=300) # Sin caché
def cargar_usuarios_df():
    try:
        df = pd.DataFrame(sheet_usuarios.get_all_records())
        if "Contraseña" not in df.columns or "Correo" not in df.columns:
             st.error("❌ Hoja 'Usuarios' debe tener columnas 'Contraseña' y 'Correo'.")
             return pd.DataFrame(columns=["Contraseña","Correo"])
        # Convertir contraseña a string explícitamente
        df['Contraseña'] = df['Contraseña'].astype(str)
        return df
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
# Asegurarse que la contraseña sea string antes de crear el dict
usuarios_dict = {str(p).strip(): _email_norm(c)
                 for p, c in zip(usuarios_df.get("Contraseña", []),
                                 usuarios_df.get("Correo", [])) if str(p).strip()} # Ignorar contraseñas vacías

# -------------------------
# Email, Sesión, Login/Logout (Funciones sin cambios internos)
# -------------------------
def enviar_correo(asunto, mensaje_resumen, copia_a):
    # ... (pega tu código aquí) ...
    if not SEND_EMAILS:
        st.info("✉️ [DEV] Envío de correo deshabilitado.")
        return
    try:
        yag = yagmail.SMTP(user=str(st.secrets["email"]["user"]),
                           password=str(st.secrets["email"]["password"]))
        cuerpo = f"""
        <p>Hola,</p>
        <p>Gracias por registrar tu solicitud en el CRM. Nuestro equipo la revisará y te daremos seguimiento lo antes posible.</p>
        <p><strong>Resumen:</strong><br>{mensaje_resumen}</p>
        <p>Saludos cordiales,<br><b>Equipo CRM UAG</b></p>
        """
        yag.send(
            to=["luis.alpizar@edu.uag.mx", copia_a],
            cc=["carlos.sotelo@edu.uag.mx", "esther.diaz@edu.uag.mx"],
            subject=asunto,
            contents=[cuerpo],
            headers={"From": "CRM UAG <" + st.secrets["email"]["user"] + ">"}
        )
    except Exception as e:
        st.warning(f"No se pudo enviar el correo: {e}")

def log_event(usuario, evento, session_id, dur_min=""):
    # ... (pega tu código aquí) ...
    try:
        with_backoff(
            sheet_accesos.append_row,
            [now_mx_str(), usuario or "", evento, session_id or "", str(dur_min or "")]
        )
    except Exception as e:
        st.warning(f"No se pudo registrar acceso: {e}")

def do_login(correo):
    # ... (pega tu código aquí) ...
    st.session_state.usuario_logueado = _email_norm(correo)
    st.session_state.session_id = str(uuid4())
    st.session_state.login_time = datetime.now(TZ_MX)
    log_event(st.session_state.usuario_logueado, "login", st.session_state.session_id)

def do_logout():
    # ... (pega tu código aquí) ...
    dur = ""
    if st.session_state.login_time:
        dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
    log_event(st.session_state.usuario_logueado, "logout", st.session_state.session_id, str(dur))
    st.session_state.usuario_logueado = None
    st.session_state.session_id = None
    st.session_state.login_time = None
    st.success("Sesión cerrada.")
    # st.rerun() # No es necesario rerun aquí, la página se recargará

# Inicialización de estado de sesión
if "usuario_logueado" not in st.session_state: st.session_state.usuario_logueado = None
if "session_id" not in st.session_state: st.session_state.session_id = None
if "login_time" not in st.session_state: st.session_state.login_time = None
# -------------------------
# Navegación Principal (Sidebar Radio)
# -------------------------
seccion = st.sidebar.radio(
    "Navegación",
    ["🔍 Ver el estado de mis solicitudes",
     "🌟 Solicitudes CRM",
     "🛠️ Incidencias CRM",
     "📝 Quejas y sugerencias",
     "🔐 Zona Admin"],
    key="nav_seccion" # Clave persistente
)

# ===================== SECCIÓN: CONSULTA =====================
if seccion == "🔍 Ver el estado de mis solicitudes":
    st.markdown("## 🔍 Consulta de Estado")

    # --- Login ---
    if st.session_state.get("usuario_logueado") is None:
        clave = st.text_input("Ingresa tu contraseña", type="password")
        if clave:
             # Convertir clave ingresada a string y quitar espacios
             clave_str = str(clave).strip()
             if clave_str in usuarios_dict:
                 do_login(usuarios_dict[clave_str])
                 st.success(f"Bienvenido, {st.session_state.usuario_logueado}")
                 st.rerun() # Recarga para mostrar contenido
             else:
                 st.error("❌ Contraseña incorrecta")

    # --- Contenido si está logueado ---
    elif st.session_state.get("usuario_logueado"):
        correo_usuario = _email_norm(st.session_state.usuario_logueado)

        if st.button("Cerrar sesión"):
            do_logout()
            st.rerun() # Recarga para mostrar pantalla de login

        # -------- Solicitudes --------
        st.subheader("📋 Solicitudes registradas")
        with st.spinner("Cargando solicitudes…"):
            df_s = get_records_simple(sheet_solicitudes)

        if "SolicitanteS" in df_s.columns:
            # Asegurarse que SolicitanteS sea string antes de comparar
            df_s['SolicitanteS'] = df_s['SolicitanteS'].astype(str)
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == correo_usuario].copy()
        else:
            st.warning("⚠️ No se encontró 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()

        st.caption(f"{len(df_mias)} solicitudes encontradas.")

        for _, row in df_mias.iterrows():
            estado_norm = _norm(row.get("EstadoS", ""))
            sat_val_raw = row.get("SatisfaccionS", "")
            id_unico    = str(row.get("IDS", "")).strip()

            titulo = f"📌 {row.get('TipoS','')} - {row.get('NombreS','')} ({row.get('CorreoS','')}) — Estado: {row.get('EstadoS','')}"
            with st.expander(titulo):
                st.markdown(f"""
                **📅 Fecha:** {row.get('FechaS','')}
                **Área/Perfil/Rol:** {row.get('AreaS','')} / {row.get('PerfilS','')} / {row.get('RolS','')}
                **Horario/Turno:** {row.get('HorarioS','')} / {row.get('TurnoS','')}
                **Solicitante:** {row.get('SolicitanteS','')}
                """)
                st.markdown(f"**Satisfacción actual:** {row.get('SatisfaccionS','')}")

                is_attended = estado_norm.startswith("atendid")
                unrated     = _is_unrated(sat_val_raw)

                if is_attended and unrated and id_unico:
                    col1, col2 = st.columns([1,3])
                    with col1:
                        voto = st.radio("¿Cómo te atendimos?", ["👍","👎"], horizontal=True, key=f"vote_s_{id_unico}")
                    with col2:
                        comentario = st.text_input("Comentario (opcional)", key=f"comm_s_{id_unico}")

                    if st.button("Enviar calificación", key=f"send_s_{id_unico}"):
                        try:
                            # Buscar celda por ID
                            cell = with_backoff(sheet_solicitudes.find, id_unico)
                            if not cell:
                                st.warning("No se pudo ubicar el registro (IDS no encontrado en 'Sheet1').")
                            else:
                                fila_excel = cell.row
                                # Obtener encabezados actuales para encontrar columnas
                                header_s = sheet_solicitudes.row_values(1) # Asume encabezados en fila 1
                                try:
                                    col_sat  = header_s.index("SatisfaccionS") + 1
                                    col_comm = header_s.index("ComentarioSatisfaccionS") + 1
                                    with_backoff(sheet_solicitudes.update_cell, fila_excel, col_sat, voto)
                                    with_backoff(sheet_solicitudes.update_cell, fila_excel, col_comm, comentario)
                                    st.success("¡Gracias por tu calificación!")
                                    # st.cache_data.clear() # Si reactivas caché
                                    st.rerun() # Recarga para actualizar vista
                                except ValueError:
                                    st.error("Error: No se encontraron las columnas 'SatisfaccionS' o 'ComentarioSatisfaccionS' en 'Sheet1'.")
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
             # Asegurarse que CorreoI sea string
             df_i['CorreoI'] = df_i['CorreoI'].astype(str)
             df_mis_inc = df_i[df_i["CorreoI"].map(_email_norm) == correo_usuario].copy()
        else:
            st.warning("⚠️ No se encontró 'CorreoI' en 'Incidencias'.")
            df_mis_inc = pd.DataFrame()

        st.caption(f"{len(df_mis_inc)} incidencias encontradas.")

        for _, row in df_mis_inc.iterrows():
            estado_norm_i = _norm(row.get("EstadoI", ""))
            sat_val_raw_i = row.get("SatisfaccionI", "")
            id_unico_i    = str(row.get("IDI", "")).strip()
            # --- MODIFICADO: Leer ID de Drive ---
            media_identifier = str(row.get("MediaFilenameI", "")).strip() # Puede ser ID de Drive

            titulo = f"🛠️ {row.get('Asunto','')} — Estado: {row.get('EstadoI','')}"
            with st.expander(titulo):
                st.markdown(f"""
                **📅 Fecha:** {row.get('FechaI','')} | **📝 Categoría:** {row.get('CategoriaI','')}
                **🔗 Link (Zoho):** {row.get('LinkI','')}
                **📄 Descripción:** {row.get('DescripcionI','')}
                **👨‍💼 Atendido por:** {row.get('AtendidoPorI','Pendiente')} | **🔁 Respuesta:** {row.get('RespuestadeSolicitudI','Aún sin respuesta')}
                """)

                # --- MODIFICADO: Mostrar enlace a Drive ---
                if media_identifier:
                    # Asumimos que es un ID de Drive. Construimos el enlace.
                    drive_view_link = f"https://drive.google.com/file/d/{media_identifier}/view?usp=sharing"
                    st.markdown(f"📎 **Archivo Adjunto:** [Ver en Google Drive]({drive_view_link})")
                    # (Mostrar previsualización directa es complejo con Drive y Service Accounts)
                # --- FIN MODIFICADO ---

                st.markdown(f"**⭐ Satisfacción actual:** {row.get('SatisfaccionI','')}")

                is_attended_i = estado_norm_i.startswith("atendid")
                unrated_i     = _is_unrated(sat_val_raw_i)

                if is_attended_i and unrated_i and id_unico_i:
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        voto_i = st.radio("¿Cómo fue la atención?", ["👍", "👎"], horizontal=True, key=f"vote_i_{id_unico_i}")
                    with col2:
                        comentario_i = st.text_input("Comentario (opcional)", key=f"comm_i_{id_unico_i}")

                    if st.button("Enviar calificación", key=f"send_i_{id_unico_i}"):
                        try:
                            cell = with_backoff(sheet_incidencias.find, id_unico_i)
                            if not cell:
                                st.warning("No se encontró IDI en 'Incidencias'.")
                            else:
                                fila_excel = cell.row
                                header_i = sheet_incidencias.row_values(1)
                                try:
                                    col_sat  = header_i.index("SatisfaccionI") + 1
                                    col_comm = header_i.index("ComentarioSatisfaccionI") + 1
                                    with_backoff(sheet_incidencias.update_cell, fila_excel, col_sat, voto_i)
                                    with_backoff(sheet_incidencias.update_cell, fila_excel, col_comm, comentario_i)
                                    st.success("¡Gracias por tu calificación!")
                                    # st.cache_data.clear() # Si reactivas caché
                                    st.rerun()
                                except ValueError:
                                    st.error("Error: No se encontraron 'SatisfaccionI' o 'ComentarioSatisfaccionI' en 'Incidencias'.")
                                except Exception as e:
                                     st.error(f"Error al actualizar celdas: {e}")
                        except Exception as e:
                            st.error(f"Error general al buscar/guardar calificación: {e}")

# ===================== SECCIÓN: SOLICITUDES CRM =====================
elif seccion == "🌟 Solicitudes CRM":
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")

    tipo = st.selectbox("Tipo de Solicitud en Zoho", ["Selecciona...", "Alta", "Modificación", "Baja"])
    nombre = st.text_input("Nombre Completo de Usuario")
    correo = st.text_input("Correo institucional")
    area = st.selectbox("Área", ["Selecciona..."] + list(estructura_roles.keys())) if tipo != "Baja" else None

    perfil = rol = numero_in = numero_saliente = horario = turno = ""
    if area and area != "Selecciona...":
        perfiles = ["Selecciona..."] + list(estructura_roles[area].keys())
        perfil = st.selectbox("Perfil", perfiles)
        if perfil != "Selecciona...":
            roles = ["Selecciona..."] + estructura_roles[area][perfil]
            rol = st.selectbox("Rol", roles)
            if rol in numeros_por_rol:
                if numeros_por_rol[rol].get("Numero_IN"):
                    numero_in = st.selectbox("Número IN", ["Selecciona..."] + numeros_por_rol[rol]["Numero_IN"])
                if numeros_por_rol[rol].get("Numero_Saliente"):
                    numero_saliente = st.selectbox("Número Saliente", ["Selecciona..."] + numeros_por_rol[rol]["Numero_Saliente"])
            horario = st.selectbox("Horario de trabajo", ["Selecciona..."] + list(horarios_dict.keys()))
            if horario != "Selecciona...":
                turno = horarios_dict.get(horario, "")

    correo_solicitante = st.text_input("Correo de quien lo solicita")

    if st.button("Enviar Solicitud"):
        if tipo == "Selecciona..." or not nombre or not correo or not correo_solicitante:
            st.warning("⚠️ Todos los campos son obligatorios.")
        elif tipo != "Baja" and (area == "Selecciona..." or perfil == "Selecciona..." or rol == "Selecciona..." or horario == "Selecciona..."):
            st.warning("⚠️ Faltan campos obligatorios.")
        else:
            try:
                fila = [
                    now_mx_str(),
                    tipo, nombre, correo, area or "", perfil or "", rol or "",
                    "" if numero_in == "Selecciona..." else numero_in,
                    "" if numero_saliente == "Selecciona..." else numero_saliente,
                    "" if horario == "Selecciona..." else horario,
                    turno or "", _email_norm(correo_solicitante), "Pendiente",
                    "", "", str(uuid4()), "", ""
                ]
                with_backoff(sheet_solicitudes.append_row, fila, value_input_option='USER_ENTERED')
                st.success("✅ Solicitud registrada.")
                enviar_correo(
                    f"Solicitud {tipo} - {nombre}",
                    f"Tipo: {tipo}<br>Nombre: {nombre}<br>Correo: {correo}",
                    correo_solicitante
                )
                # st.cache_data.clear() # Si reactivas caché
            except Exception as e:
                st.error(f"Error al registrar solicitud: {e}")

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

        # --- File Uploader para Drive ---
        uploaded_file = st.file_uploader(
            "Adjuntar Imagen o Video (Opcional)",
            type=['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp',
                  'mp4', 'mov', 'avi', 'wmv', 'mkv', 'webm'],
            accept_multiple_files=False
        )
        # --- FIN File Uploader ---

        st.caption("(*) Campos obligatorios")
        enviado = st.form_submit_button("✔️ Enviar Incidencia")

    if enviado:
        if not correo_i or not asunto or not descripcion:
            st.warning("⚠️ Completa correo, asunto y descripción.")
        else:
            google_drive_file_id = "" # Inicializa vacío
            if uploaded_file is not None:
                st.info("Subiendo archivo a Google Drive...", icon="⏳")
                file_extension = Path(uploaded_file.name).suffix
                unique_filename = f"{uuid4()}{file_extension}"

                # Llamada a la función de subida
                file_id, view_link = upload_to_drive(
                    file_buffer=uploaded_file,
                    filename=unique_filename,
                    mimetype=uploaded_file.type
                )
                if file_id:
                    google_drive_file_id = file_id # Guarda el ID si tuvo éxito
                else:
                    st.error("Falló la subida a Drive. Se registrará sin adjunto.")

            try:
                # --- Fila con ID de Drive ---
                fila = [
                    now_mx_str(),            # FechaI
                    _email_norm(correo_i),   # CorreoI
                    asunto.strip(),          # Asunto
                    categoria,               # CategoriaI
                    descripcion.strip(),     # DescripcionI
                    link.strip(),            # LinkI
                    "Pendiente",             # EstadoI
                    "",                      # AtendidoPorI
                    "",                      # RespuestadeSolicitudI
                    "",                      # SatisfaccionI
                    "",                      # ComentarioSatisfaccionI
                    str(uuid4()),            # IDI (ID de Incidencia)
                    google_drive_file_id     # MediaFilenameI (ID de Drive)
                ]
                with_backoff(sheet_incidencias.append_row, fila, value_input_option='USER_ENTERED')
                st.success("✅ Incidencia registrada.")
                # st.cache_data.clear() # Si reactivas caché
            except Exception as e:
                st.error(f"❌ Error al registrar en Google Sheets: {e}")
                st.error(f"Fila que intentó guardar: {fila}") # Ayuda a depurar
# ===================== SECCIÓN: QUEJAS =====================
elif seccion == "📝 Quejas y sugerencias":
    st.markdown("## 📝 Quejas y sugerencias")
    q_correo = st.text_input("Tu correo institucional")
    q_tipo = st.selectbox("Tipo", ["Queja","Sugerencia"])
    q_asunto = st.text_input("Asunto")
    q_categoria = st.selectbox("Categoría", ["Uso de CRM","Datos","Reportes","IVR","Mensajería","Soporte","Otro"])
    q_desc = st.text_area("Descripción")
    q_calif = st.slider("Calificación (opcional)", 1, 5, 5)

    if st.button("Enviar queja/sugerencia"):
        if not q_correo or not q_asunto or not q_desc:
            st.warning("Completa correo, asunto y descripción.")
        else:
            try:
                fila = [
                    now_mx_str(),           # FechaQ
                    _email_norm(q_correo),  # CorreoQ
                    q_tipo,                 # TipoQ
                    q_asunto,               # AsuntoQ
                    q_desc,                 # DescripciónQ
                    q_categoria,            # CategoríaQ
                    "Pendiente",            # EstadoQ
                    q_calif,                # CalificacionQ
                    q_categoria             # CategoriaQ (duplicada)
                ]
                with_backoff(sheet_quejas.append_row, fila, value_input_option='USER_ENTERED')
                st.success("✅ Gracias por tu feedback.")
                # st.cache_data.clear() # Si reactivas caché
            except Exception as e:
                st.error(f"Error al registrar queja: {e}")

# ===================== SECCIÓN: ADMIN =====================
elif seccion == "🔐 Zona Admin":
    st.markdown("## 🔐 Zona Administrativa")

    ADMIN_PASS = st.secrets.get("admin", {}).get("password", "")
    raw_emails = st.secrets.get("admin", {}).get("emails", [])
    ADMIN_EMAILS = {raw_emails.strip().lower()} if isinstance(raw_emails, str) else {e.strip().lower() for e in raw_emails}

    admin_pass_input = st.text_input("Contraseña admin", type="password", key="admin_pass_input")
    admin_ok = False
    if admin_pass_input and admin_pass_input == ADMIN_PASS:
        admin_ok = True
    elif st.session_state.get("usuario_logueado") and (not ADMIN_EMAILS or _email_norm(st.session_state["usuario_logueado"]) in ADMIN_EMAILS):
        admin_ok = True

    if admin_ok:
        st.success("✅ Acceso de administrador concedido.")

        with st.spinner("Cargando datos…"):
            # Usar try-except por si alguna hoja falla al leer
            try: df_s = get_records_simple(sheet_solicitudes)
            except Exception: df_s = pd.DataFrame(); st.error("Error cargando Solicitudes")
            try: df_i = get_records_simple(sheet_incidencias)
            except Exception: df_i = pd.DataFrame(); st.error("Error cargando Incidencias")
            try: df_q = get_records_simple(sheet_quejas)
            except Exception: df_q = pd.DataFrame(); st.error("Error cargando Quejas")


        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Quejas"])

        # ----- Solicitudes Admin -----
        with tab1:
            st.dataframe(df_s, use_container_width=True)
            if not df_s.empty and "EstadoS" in df_s.columns:
                fila_s_idx = st.selectbox("Fila solicitud (índice)", df_s.index, key="fila_solicitud_admin")
                estado_s = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_solicitud_admin")
                colA, colB = st.columns(2)
                with colA:
                    if st.button("Actualizar estado solicitud", key="btn_update_sol_admin"):
                        try:
                            fila_excel = int(fila_s_idx) + 2 # Índice DF + encabezado + base 1
                            header_s = sheet_solicitudes.row_values(1)
                            col_idx = header_s.index("EstadoS") + 1
                            with_backoff(sheet_solicitudes.update_cell, fila_excel, col_idx, estado_s)
                            st.success(f"✅ Fila {fila_excel} actualizada.")
                            st.rerun()
                        except Exception as e: st.error(f"Error: {e}")
                with colB:
                    if st.button("Eliminar solicitud", type="primary", key="btn_delete_sol_admin"):
                        try:
                            fila_excel = int(fila_s_idx) + 2
                            with_backoff(sheet_solicitudes.delete_rows, fila_excel)
                            st.warning(f"⚠️ Fila {fila_excel} eliminada.")
                            st.rerun()
                        except Exception as e: st.error(f"Error: {e}")

        # ----- Incidencias Admin -----
        with tab2:
            st.dataframe(df_i, use_container_width=True)
            required_cols_i = {"EstadoI", "AtendidoPorI", "RespuestadeSolicitudI"}
            if not df_i.empty and required_cols_i.issubset(df_i.columns):
                fila_i_idx = st.selectbox("Fila incidencia (índice)", df_i.index, key="fila_incidencia_admin")
                estado_i = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_incidencia_admin")
                atendido_por = st.text_input("👨‍💼 Atendido por", key="input_atendido_admin")
                respuesta = st.text_area("📜 Respuesta", key="input_respuesta_admin")
                colA, colB = st.columns(2)
                with colA:
                    if st.button("Actualizar incidencia", key="btn_update_inc_admin"):
                        try:
                            fila_excel = int(fila_i_idx) + 2
                            header_i = sheet_incidencias.row_values(1)
                            col_estado   = header_i.index("EstadoI") + 1
                            col_atendido = header_i.index("AtendidoPorI") + 1
                            col_resp     = header_i.index("RespuestadeSolicitudI") + 1
                            cells = [
                                gspread.Cell(fila_excel, col_estado, estado_i),
                                gspread.Cell(fila_excel, col_atendido, atendido_por),
                                gspread.Cell(fila_excel, col_resp, respuesta),
                            ]
                            with_backoff(sheet_incidencias.update_cells, cells)
                            st.success(f"✅ Fila {fila_excel} actualizada.")
                            st.rerun()
                        except Exception as e: st.error(f"Error: {e}")
                with colB:
                    if st.button("Eliminar incidencia", type="primary", key="btn_delete_inc_admin"):
                        try:
                            fila_excel = int(fila_i_idx) + 2
                            with_backoff(sheet_incidencias.delete_rows, fila_excel)
                            st.warning(f"⚠️ Fila {fila_excel} eliminada.")
                            st.rerun()
                        except Exception as e: st.error(f"Error: {e}")

        # ----- Quejas Admin -----
        with tab3:
            st.dataframe(df_q, use_container_width=True)
            if not df_q.empty and "EstadoQ" in df_q.columns:
                fila_q_idx = st.selectbox("Fila queja (índice)", df_q.index, key="fila_queja_admin")
                nuevo_estado_q = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_queja_admin")
                colA, colB = st.columns(2)
                with colA:
                    if st.button("Actualizar queja", key="btn_update_queja_admin"):
                        try:
                            fila_excel = int(fila_q_idx) + 2
                            header_q = sheet_quejas.row_values(1)
                            col_idx = header_q.index("EstadoQ") + 1
                            with_backoff(sheet_quejas.update_cell, fila_excel, col_idx, nuevo_estado_q)
                            st.success(f"✅ Fila {fila_excel} actualizada.")
                            st.rerun()
                        except Exception as e: st.error(f"Error: {e}")
                with colB:
                     if st.button("Eliminar queja", type="primary", key="btn_delete_queja_admin"):
                        try:
                            fila_excel = int(fila_q_idx) + 2
                            with_backoff(sheet_quejas.delete_rows, fila_excel)
                            st.warning(f"⚠️ Fila {fila_excel} eliminada.")
                            st.rerun()
                        except Exception as e: st.error(f"Error: {e}")
    else:
        st.info("🔒 Ingresa la contraseña admin o usa un correo en la lista blanca para acceder.")

# --- ELEMENTOS MOVIDOS AL FINAL DE LA BARRA LATERAL ---
st.sidebar.divider()

# Botón de Recargar Página (simple rerun)
if st.sidebar.button("♻️ Recargar Página"):
    st.rerun()

# Información del Entorno (más pequeña con caption)
if APP_MODE == "dev":
    st.sidebar.caption(f"🧪 DEV · `{SHEET_ID}`")
else:
    st.sidebar.caption(f"🚀 PROD · `{SHEET_ID}`")

# FIN DEL ARCHIVO
