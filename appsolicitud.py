import os
import json
import time, random
from uuid import uuid4
from datetime import datetime
import re
import unicodedata

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
import yagmail
from zoneinfo import ZoneInfo

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
    """True si la celda de satisfacción está vacía o sin calificar."""
    v = _norm(val)
    return v in ("", "pendiente", "na", "n/a", "sin calificacion", "sin calificación", "none", "null", "-")

def with_backoff(fn, *args, **kwargs):
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e):
                time.sleep(min(1*(2**i) + random.random(), 8))
                continue
            raise

def load_json_safe(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

TZ_MX = ZoneInfo("America/Mexico_City")
def now_mx_str() -> str:
    return datetime.now(TZ_MX).strftime("%d/%m/%Y %H:%M")

# -------------------------
# Config / secrets
# -------------------------
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")

APP_MODE = st.secrets.get("mode", "dev")  # "dev" | "prod"
SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))

USUARIOS_XLSX_PATH = st.secrets.get("security", {}).get("usuarios_excel_path", "")
SHEET_ID = (st.secrets.get("sheets", {}).get("prod_id") if APP_MODE == "prod"
            else st.secrets.get("sheets", {}).get("dev_id"))
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
json_path = st.secrets.get("google_service_account", {}).get("json_path", "")

if not SHEET_ID:
    st.error("❗ No se encontró SHEET_ID en [sheets] de secrets.toml")
    st.stop()
if not json_path:
    st.error("❗ No se encontró google_service_account.json_path en secrets.toml")
    st.stop()

if APP_MODE == "dev":
    st.sidebar.info(f"🧪 **Entorno:** DEV (no envía correos) · Sheet: `{SHEET_ID}`")
else:
    st.sidebar.info(f"🚀 **Entorno:** PROD · Sheet: `{SHEET_ID}`")

# -------------------------
# Conexión a Google Sheets
# -------------------------
def get_book():
    creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return with_backoff(client.open_by_key, SHEET_ID)

book = get_book()
try:
    sheet_solicitudes = book.worksheet("Sheet1")
    sheet_incidencias = book.worksheet("Incidencias")
    sheet_quejas      = book.worksheet("Quejas")
    sheet_accesos     = book.worksheet("Accesos")
    sheet_usuarios    = book.worksheet("Usuarios") 
except gspread.WorksheetNotFound as e:
    st.error(f"❌ No se encontró una de las hojas requeridas: {e}")
    st.stop()

def get_records_simple(_ws) -> pd.DataFrame:
    """Lee toda la hoja usando get_all_records (sin caché)"""
    try:
        return pd.DataFrame(_ws.get_all_records())
    except Exception as e:
        st.error(f"Error al leer '{_ws.title}': {e}")
        return pd.DataFrame()

# -------------------------
# Datos locales
# -------------------------
estructura_roles = load_json_safe("data/estructura_roles.json")
numeros_por_rol  = load_json_safe("data/numeros_por_rol.json")
horarios_dict    = load_json_safe("data/horarios.json")

# -------------------------
# Usuarios (Excel local)
# -------------------------
def cargar_usuarios_df():
    try:
        return pd.DataFrame(sheet_usuarios.get_all_records())
    except Exception as e:
        st.error(f"❌ No pude leer la hoja 'Usuarios': {e}")
        return pd.DataFrame(columns=["Contraseña","Correo"])

usuarios_df = cargar_usuarios_df()
usuarios_dict = {str(p): _email_norm(c) 
                 for p, c in zip(usuarios_df.get("Contraseña", []),
                                 usuarios_df.get("Correo", []))}

# -------------------------
# Email
# -------------------------
def enviar_correo(asunto, mensaje_resumen, copia_a):
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

# -------------------------
# Sesión / accesos
# -------------------------
if "usuario_logueado" not in st.session_state:
    st.session_state.usuario_logueado = None
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "login_time" not in st.session_state:
    st.session_state.login_time = None

def log_event(usuario, evento, session_id, dur_min=""):
    try:
        with_backoff(
            sheet_accesos.append_row,
            [now_mx_str(), usuario or "", evento, session_id or "", str(dur_min or "")]
        )
    except Exception as e:
        st.warning(f"No se pudo registrar acceso: {e}")

def do_login(correo):
    st.session_state.usuario_logueado = _email_norm(correo)
    st.session_state.session_id = str(uuid4())
    st.session_state.login_time = datetime.now(TZ_MX)
    log_event(st.session_state.usuario_logueado, "login", st.session_state.session_id)

def do_logout():
    dur = ""
    if st.session_state.login_time:
        dur = round((datetime.now(TZ_MX) - st.session_state.login_time).total_seconds() / 60, 1)
    log_event(st.session_state.usuario_logueado, "logout", st.session_state.session_id, str(dur))
    st.session_state.usuario_logueado = None
    st.session_state.session_id = None
    st.session_state.login_time = None
    st.success("Sesión cerrada.")
    st.rerun()
# -------------------------
# Botón de refresco global
# -------------------------
if st.sidebar.button("♻️ Refrescar (mantener sección)"):
    st.rerun()

# -------------------------
# Navegación que no se resetea
# -------------------------
seccion = st.sidebar.radio(
    "Navegación",
    ["🔍 Ver el estado de mis solicitudes",
     "🌟 Solicitudes CRM",
     "🛠️ Incidencias CRM",
     "📝 Quejas y sugerencias",
     "🔐 Zona Admin"],
    key="nav_seccion"
)

# ===================== SECCIÓN: CONSULTA =====================
if seccion == "🔍 Ver el estado de mis solicitudes":
    st.markdown("## 🔍 Consulta de Estado de Mis Solicitudes e Incidencias")

    if st.session_state.get("usuario_logueado") is None:
        clave = st.text_input("Ingresa tu contraseña", type="password")
        if clave:
            if clave in usuarios_dict:
                do_login(usuarios_dict[clave])
                st.success(f"Bienvenido, {st.session_state.usuario_logueado}")
                st.toast("Sesión iniciada.", icon="🔓")
            else:
                st.error("❌ Contraseña incorrecta")

    if st.session_state.get("usuario_logueado"):
        correo_usuario = _email_norm(st.session_state.usuario_logueado)

        if st.button("Cerrar sesión"):
            do_logout()

        # -------- Solicitudes --------
        st.subheader("📋 Solicitudes registradas")
        with st.spinner("Cargando solicitudes…"):
            df_s = get_records_simple(sheet_solicitudes)

        if "SolicitanteS" in df_s.columns:
            df_mias = df_s[df_s["SolicitanteS"].map(_email_norm) == correo_usuario].copy()
        else:
            st.warning("No se encontró la columna 'SolicitanteS' en 'Sheet1'.")
            df_mias = pd.DataFrame()

        st.caption(f"Se encontraron {len(df_mias)} solicitudes para {st.session_state.usuario_logueado}")

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

                # Mostrar control de satisfacción si corresponde
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
                            cell = with_backoff(sheet_solicitudes.find, id_unico)
                            if not cell:
                                st.warning("No se pudo ubicar el registro (IDS no encontrado en 'Sheet1').")
                            else:
                                fila_excel = cell.row
                                # Buscar índices por nombre para robustez
                                cols = list(df_s.columns)
                                col_sat  = cols.index("SatisfaccionS") + 1
                                col_comm = cols.index("ComentarioSatisfaccionS") + 1
                                with_backoff(sheet_solicitudes.update_cell, fila_excel, col_sat, voto)
                                with_backoff(sheet_solicitudes.update_cell, fila_excel, col_comm, comentario)
                                st.success("¡Gracias por tu calificación!")
                        except Exception as e:
                            st.error(f"Error al guardar la calificación: {e}")

        # -------- Incidencias --------
        st.subheader("🛠️ Incidencias reportadas")
        with st.spinner("Cargando incidencias…"):
            df_i = get_records_simple(sheet_incidencias)

        if "CorreoI" in df_i.columns:
            df_mis_inc = df_i[df_i["CorreoI"].map(_email_norm) == correo_usuario].copy()
        else:
            st.warning("No se encontró la columna 'CorreoI' en 'Incidencias'.")
            df_mis_inc = pd.DataFrame()

        st.caption(f"Se encontraron {len(df_mis_inc)} incidencias para {st.session_state.usuario_logueado}")

        for _, row in df_mis_inc.iterrows():
            estado_norm_i = _norm(row.get("EstadoI", ""))
            sat_val_raw_i = row.get("SatisfaccionI", "")
            id_unico_i    = str(row.get("IDI", "")).strip()  # requiere columna IDI en header

            titulo = f"🛠️ {row.get('Asunto','')} — Estado: {row.get('EstadoI','')}"
            with st.expander(titulo):
                st.markdown(f"""
                **📅 Fecha:** {row.get('FechaI','')}
                **📝 Categoría:** {row.get('CategoriaI','')}
                **🔗 Link:** {row.get('LinkI','')}
                **📄 Descripción:** {row.get('DescripcionI','')}
                **👨‍💼 Atendido por:** {row.get('AtendidoPorI','Pendiente')}
                **🔁 Respuesta:** {row.get('RespuestadeSolicitudI','Aún sin respuesta')}
                """)
                st.markdown(f"**Satisfacción actual:** {row.get('SatisfaccionI','')}")

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
                                st.warning("No se encontró IDI en 'Incidencias'. Verifica que la columna **IDI** exista en el encabezado.")
                            else:
                                fila_excel = cell.row
                                cols_i = list(df_i.columns)
                                col_sat  = cols_i.index("SatisfaccionI") + 1
                                col_comm = cols_i.index("ComentarioSatisfaccionI") + 1
                                with_backoff(sheet_incidencias.update_cell, fila_excel, col_sat, voto_i)
                                with_backoff(sheet_incidencias.update_cell, fila_excel, col_comm, comentario_i)
                                st.success("¡Gracias por tu calificación!")
                        except Exception as e:
                            st.error(f"Error al guardar la calificación: {e}")
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
                    now_mx_str(),                 # FechaS
                    tipo,                         # TipoS
                    nombre,                       # NombreS
                    correo,                       # CorreoS
                    area or "",                   # AreaS
                    perfil or "",                 # PerfilS
                    rol or "",                    # RolS
                    "" if numero_in == "Selecciona..." else numero_in,           # NumeroINS
                    "" if numero_saliente == "Selecciona..." else numero_saliente,# NumeroSalienteS
                    "" if horario == "Selecciona..." else horario,               # HorarioS
                    turno or "",                 # TurnoS
                    _email_norm(correo_solicitante), # SolicitanteS
                    "Pendiente",                 # EstadoS
                    "",                          # CredencialesZohoS
                    "",                          # CredencialesCursosS
                    str(uuid4()),                # IDS
                    "",                          # SatisfaccionS
                    ""                           # ComentarioSatisfaccionS
                ]
                with_backoff(sheet_solicitudes.append_row, fila)
                st.success("✅ Solicitud registrada.")
                enviar_correo(
                    f"Solicitud {tipo} - {nombre}",
                    f"Tipo: {tipo}<br>Nombre: {nombre}<br>Correo: {correo}",
                    correo_solicitante
                )
            except Exception as e:
                st.error(f"Error: {e}")

# ===================== SECCIÓN: INCIDENCIAS CRM =====================
elif seccion == "🛠️ Incidencias CRM":
    st.markdown("## 🛠️ Reporte de Incidencias")

    with st.form("form_incidencia", clear_on_submit=True):
        col_a, col_b = st.columns(2)
        with col_a:
            correo_i = st.text_input("Correo de quien solicita")
            categoria = st.selectbox(
                "Categoría",
                ["Desfase", "Reactivación", "Equivalencia", "Llamadas IVR", "Funcionalidad Zoho", "Mensajes", "Otros"]
            )
        with col_b:
            asunto = st.text_input("Asunto o título de la incidencia")
            link = st.text_input("Link del registro afectado")

        descripcion = st.text_area("Descripción breve", height=140)
        enviado = st.form_submit_button("Enviar Incidencia")

    if enviado:
        if not correo_i or not asunto or not descripcion:
            st.warning("Completa **correo**, **asunto** y **descripción**.")
        else:
            try:
                # Asegúrate de tener la columna IDI en el header de Incidencias
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
                    str(uuid4())             # IDI  ← en el header debe existir "IDI"
                ]
                with_backoff(sheet_incidencias.append_row, fila)
                st.toast("✅ Incidencia registrada.", icon="✅")
                st.success("Listo, tu incidencia quedó registrada.")
            except Exception as e:
                st.error(f"Error al registrar la incidencia: {e}")
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
                    q_categoria             # CategoriaQ (duplicada por compatibilidad)
                ]
                with_backoff(sheet_quejas.append_row, fila)
                st.success("✅ Gracias por tu feedback.")
            except Exception as e:
                st.error(f"Error: {e}")

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
            df_s = get_records_simple(sheet_solicitudes)
            df_i = get_records_simple(sheet_incidencias)
            df_q = get_records_simple(sheet_quejas)

        tab1, tab2, tab3 = st.tabs(["Solicitudes", "Incidencias", "Quejas"])

        # ----- Solicitudes -----
        with tab1:
            st.dataframe(df_s, use_container_width=True)
            if not df_s.empty and "EstadoS" in df_s.columns:
                fila_s = st.selectbox("Fila solicitud (índice)", df_s.index, key="fila_solicitud")
                estado_s = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_solicitud")

                colA, colB = st.columns(2)
                with colA:
                    if st.button("Actualizar estado solicitud", key="btn_actualizar_solicitud"):
                        try:
                            col_idx = list(df_s.columns).index("EstadoS") + 1
                            with_backoff(sheet_solicitudes.update_cell, fila_s + 2, col_idx, estado_s)
                            st.success("✅ Estado actualizado.")
                        except Exception as e:
                            st.error(f"Error: {e}")
                with colB:
                    if st.button("Eliminar solicitud", key="btn_eliminar_solicitud"):
                        try:
                            with_backoff(sheet_solicitudes.delete_rows, fila_s + 2)
                            st.warning("⚠️ Solicitud eliminada.")
                        except Exception as e:
                            st.error(f"Error: {e}")

        # ----- Incidencias -----
        with tab2:
            st.dataframe(df_i, use_container_width=True)
            if not df_i.empty and {"EstadoI","AtendidoPorI","RespuestadeSolicitudI"}.issubset(df_i.columns):
                fila_i = st.selectbox("Fila incidencia (índice)", df_i.index, key="fila_incidencia")
                estado_i = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_incidencia")
                atendido_por = st.text_input("👨‍💼 Atendido por", key="input_atendido_por")
                respuesta = st.text_area("📜 Respuesta de la solicitud", key="input_respuesta")

                colA, colB = st.columns(2)
                with colA:
                    if st.button("Actualizar incidencia", key="btn_actualizar_incidencia"):
                        try:
                            fila_excel = fila_i + 2
                            cols = list(df_i.columns)
                            col_estado   = cols.index("EstadoI") + 1
                            col_atendido = cols.index("AtendidoPorI") + 1
                            col_resp     = cols.index("RespuestadeSolicitudI") + 1

                            cells = [
                                gspread.Cell(fila_excel, col_estado,   estado_i),
                                gspread.Cell(fila_excel, col_atendido, atendido_por),
                                gspread.Cell(fila_excel, col_resp,     respuesta),
                            ]
                            with_backoff(sheet_incidencias.update_cells, cells)
                            st.success("✅ Incidencia actualizada.")
                        except Exception as e:
                            st.error(f"Error: {e}")
                with colB:
                    if st.button("Eliminar incidencia", key="btn_eliminar_incidencia"):
                        try:
                            with_backoff(sheet_incidencias.delete_rows, fila_i + 2)
                            st.warning("⚠️ Incidencia eliminada.")
                        except Exception as e:
                            st.error(f"Error: {e}")

        # ----- Quejas -----
        with tab3:
            st.dataframe(df_q, use_container_width=True)
            if not df_q.empty and "EstadoQ" in df_q.columns:
                fila_q = st.selectbox("Fila queja/sugerencia (índice)", df_q.index, key="fila_queja")
                nuevo_estado_q = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"], key="estado_queja")

                colA, colB = st.columns(2)
                with colA:
                    if st.button("Actualizar queja/sugerencia", key="btn_actualizar_queja"):
                        try:
                            col_idx = list(df_q.columns).index("EstadoQ") + 1
                            with_backoff(sheet_quejas.update_cell, fila_q + 2, col_idx, nuevo_estado_q)
                            st.success("✅ Queja/Sugerencia actualizada.")
                        except Exception as e:
                            st.error(f"Error: {e}")
                with colB:
                    if st.button("Eliminar queja/sugerencia", key="btn_eliminar_queja"):
                        try:
                            with_backoff(sheet_quejas.delete_rows, fila_q + 2)
                            st.warning("⚠️ Registro eliminado.")
                        except Exception as e:
                            st.error(f"Error: {e}")
    else:
        st.info("🔒 Ingresa la contraseña admin o usa un correo en la lista blanca para acceder.")
