# appsolicitud.py
import streamlit as st
import pandas as pd
import json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import yagmail
import os

# === Conexión segura con Google Sheets ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
service_account_info = st.secrets["google_service_account"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
client = gspread.authorize(creds)
sheet_solicitudes = client.open_by_key("18uBeG2cCDpb4I2M3OkEZX9H8jb94LTMEeImJKXxtXHg").sheet1
sheet_incidencias = client.open_by_key("18uBeG2cCDpb4I2M3OkEZX9H8jb94LTMEeImJKXxtXHg").worksheet("Incidencias")

# === Cargar JSON locales ===
with open("data/estructura_roles.json", encoding="utf-8") as f:
    estructura_roles = json.load(f)
with open("data/numeros_por_rol.json", encoding="utf-8") as f:
    numeros_por_rol = json.load(f)
with open("data/horarios.json", encoding="utf-8") as f:
    horarios_dict = json.load(f)

# === Correo ===
def enviar_correo(asunto, mensaje, copia_a):
    try:
        yag = yagmail.SMTP(user=st.secrets["email"]["user"], password=st.secrets["email"]["password"])
        destinatarios = ["luis.alpizar@edu.uag.mx", copia_a]
        yag.send(to=destinatarios, subject=asunto, contents=mensaje)
    except Exception as e:
        st.warning(f"No se pudo enviar el correo: {e}")

# === Tabs ===
st.set_page_config(page_title="Gestor Zoho CRM", layout="wide")
tabs = st.tabs(["🌟 Solicitudes", "🛠️ Incidencias", "🔍 Ver mi estado", "🔐 Zona Admin"])

# === Solicitudes ===
with tabs[0]:
    st.markdown("## 🌟 Formulario de Solicitudes Zoho CRM")
    with st.expander("🔹 ¿Cómo usar este sistema? Haz clic aquí para ver la guía completa"):
        st.markdown("""
        ### 🌟 Guía para Solicitudes de Usuario en Zoho CRM

        Este módulo permite registrar, modificar o dar de baja usuarios en el CRM institucional.

        #### 📌 Cuándo usar:
        - **Alta**: nuevo usuario.
        - **Modificación**: cambios de rol, horario, nombre, correo.
        - **Baja**: elimina acceso.

        #### 📅 Campos obligatorios:
        - Nombre, correo, solicitante.
        - Para Alta/Modificación: área > perfil > rol, horario y turno.
        - Número IN/Saliente si aplica.

        #### 📧 Correo de confirmación:
        Se envía a quien solicita y a administración.

        #### 🛋️ Historial:
        Con contraseña puedes eliminar y cambiar estado.
        """)

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
        elif perfil == "Agente de Call Center" and numero_in == "Selecciona...":
            st.warning("⚠️ El perfil requiere Número IN.")
        elif rol in numeros_por_rol and numeros_por_rol[rol].get("Numero_Saliente") and numero_saliente == "Selecciona...":
            st.warning("⚠️ Este rol requiere Número Saliente.")
        else:
            fila = [datetime.now().strftime("%d/%m/%Y %H:%M"), tipo, nombre, correo, area or "", perfil or "", rol or "", numero_in, numero_saliente, horario, turno, correo_solicitante, "Pendiente"]
            try:
                sheet_solicitudes.append_row(fila)
                st.success("✅ Solicitud registrada.")
                enviar_correo(f"Solicitud {tipo} - {nombre}", f"Se registró solicitud de tipo {tipo} para {nombre}", correo_solicitante)
            except Exception as e:
                st.error(f"Error: {e}")

# === Incidencias ===
with tabs[1]:
    st.markdown("## 🛠️ Reporte de Incidencias")
    with st.expander("🔹 ¿Cómo reportar una incidencia?"):
        st.markdown("""
        ### 📄 Guía para incidencias

        Reporta problemas menores:

        - **Desfase**: datos desincronizados.
        - **Reactivación**: revivir leads u oportunidades.
        - **Equivalencia**: ajustes administrativos.
        - **IVR**: llamadas automáticas.
        - **Funcionalidad Zoho**, **Mensajes**, **Otros**.
        - **Nota: Sobre el link tiene que ser el de Zoho**.
        """)

    correo = st.text_input("Correo del solicitante")
    asunto = st.text_input("Asunto o título de la incidencia")
    categoria = st.selectbox("Categoría", ["Desfase", "Reactivación", "Equivalencia", "Llamadas IVR", "Funcionalidad Zoho", "Mensajes", "Otros"])
    descripcion = st.text_area("Descripción breve")
    link = st.text_input("Link del registro afectado")

    archivo = None
    if os.getenv("STREAMLIT_SERVER_HEADLESS") != "1":
        archivo = st.file_uploader("Subir archivo (solo local)", type=["png", "jpg", "pdf", "xlsx", "csv"])

    if st.button("Enviar Incidencia"):
        fila = [datetime.now().strftime("%d/%m/%Y %H:%M"), correo, asunto, categoria, descripcion, link, "Pendiente"]
        try:
            sheet_incidencias.append_row(fila)
            st.success("✅ Incidencia registrada.")
            if archivo:
                with open(f"incidencias_archivos/{archivo.name}", "wb") as f:
                    f.write(archivo.read())
        except Exception as e:
            st.error(f"Error: {e}")

# === Consulta ===
with tabs[2]:
    st.markdown("## 🔍 Consulta de Estado")
    correo = st.text_input("Ingresa tu correo institucional")
    if correo:
        df_s = pd.DataFrame(sheet_solicitudes.get_all_records())
        df_i = pd.DataFrame(sheet_incidencias.get_all_records())
        st.subheader("Solicitudes")
        st.dataframe(df_s[df_s["Solicitante"] == correo])
        st.subheader("Incidencias")
        st.dataframe(df_i[df_i["Correo"] == correo])

# === Admin ===
with tabs[3]:
    st.markdown("## 🔐 Zona Administrativa")
    clave = st.text_input("Contraseña", type="password")
    if clave == "Generardo2":
        df_s = pd.DataFrame(sheet_solicitudes.get_all_records())
        df_i = pd.DataFrame(sheet_incidencias.get_all_records())
        tab1, tab2 = st.tabs(["Solicitudes", "Incidencias"])
        with tab1:
            st.dataframe(df_s)
            fila = st.selectbox("Fila solicitud", df_s.index)
            estado = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"])
            if st.button("Actualizar estado solicitud"):
                sheet_solicitudes.update_cell(fila + 2, 14, estado)
                st.success("Actualizado")
            if st.button("Eliminar solicitud"):
                sheet_solicitudes.delete_rows(fila + 2)
                st.warning("Eliminada")
        with tab2:
            st.dataframe(df_i)
            fila = st.selectbox("Fila incidencia", df_i.index)
            estado = st.selectbox("Nuevo estado", ["Pendiente", "En proceso", "Atendido"])
            if st.button("Actualizar estado incidencia"):
                sheet_incidencias.update_cell(fila + 2, 7, estado)
                st.success("Actualizado")
            if st.button("Eliminar incidencia"):
                sheet_incidencias.delete_rows(fila + 2)
                st.warning("Eliminada")
    elif clave:
        st.error("Contraseña incorrecta")


