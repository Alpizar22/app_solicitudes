
import streamlit as st
import pandas as pd
import json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Conexión segura con Google Sheets ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
service_account_info = st.secrets["google_service_account"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key("18uBeG2cCDpb4I2M3OkEZX9H8jb94LTMEeImJKXxtXHg").sheet1

# === Cargar JSON locales ===
with open("data/estructura_roles.json", encoding="utf-8") as f:
    estructura_roles = json.load(f)

with open("data/numeros_por_perfil.json", encoding="utf-8") as f:
    numeros_por_perfil = json.load(f)

with open("data/horarios.json", encoding="utf-8") as f:
    horarios_dict = json.load(f)

# === UI: Encabezado estilizado ===
st.markdown("## ✨ Formulario de Solicitudes")
st.markdown('<div style="color:gray; font-style:italic;">Completa todos los campos para registrar una nueva solicitud</div>', unsafe_allow_html=True)
st.markdown("---")

tipo = st.selectbox("Tipo de Solicitud", ["Selecciona...", "Alta", "Modificación", "Baja"])

nombre = st.text_input("Nombre Completo")
correo = st.text_input("Correo")
area = None
if tipo not in ["Selecciona...", "Baja"]:
    areas = ["Selecciona..."] + list(estructura_roles.keys())
    area = st.selectbox("Área", areas)

perfil = rol = numero_in = numero_saliente = horario = turno = ""
if area and area != "Selecciona...":
    perfiles = ["Selecciona..."] + list(estructura_roles[area].keys())
    perfil = st.selectbox("Perfil", perfiles)

    if perfil != "Selecciona...":
        roles = ["Selecciona..."] + estructura_roles[area][perfil]
        rol = st.selectbox("Rol", roles)

        if perfil in numeros_por_perfil:
            if perfil == "Agente de Call Center":
                numero_in = st.selectbox("Número IN", ["Selecciona..."] + numeros_por_perfil[perfil]["Numero_IN"])
            numero_saliente = st.selectbox("Número Saliente", ["Selecciona..."] + numeros_por_perfil[perfil]["Numero_Saliente"])

        horario = st.selectbox("Horario de trabajo", ["Selecciona..."] + list(horarios_dict.keys()))
        if horario != "Selecciona...":
            turno = horarios_dict.get(horario, "")

solicitado_por = st.text_input("¿Quién lo solicitó?")

if st.button("Enviar Solicitud"):
    if tipo == "Selecciona..." or not nombre or not correo or not solicitado_por:
        st.warning("⚠️ Todos los campos son obligatorios.")
    elif tipo != "Baja" and (area == "Selecciona..." or perfil == "Selecciona..." or rol == "Selecciona..." or horario == "Selecciona..."):
        st.warning("⚠️ Por favor selecciona valores válidos en los desplegables.")
    elif perfil == "Agente de Call Center" and numero_in == "Selecciona...":
        st.warning("⚠️ El perfil Agente de Call Center requiere Número IN.")
    elif perfil in numeros_por_perfil and numero_saliente == "Selecciona...":
        st.warning("⚠️ Este perfil requiere seleccionar Número Saliente.")
    else:
        fila = [
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            tipo, nombre, correo, area or "", perfil or "", rol or "",
            numero_in, numero_saliente, horario, turno, solicitado_por
        ]
        try:
            sheet.append_row(fila)
            st.success("✅ Solicitud registrada correctamente en Google Sheets.")
        except Exception as e:
            st.error(f"❌ Error al guardar: {e}")

# === Historial protegido con eliminación ===
st.markdown("---")
st.subheader("🔒 Historial y eliminación de solicitudes")
password = st.text_input("Contraseña para ver y eliminar registros", type="password")
if password == "Generardo2":
    try:
        df = pd.DataFrame(sheet.get_all_records())
        st.success("🔓 Acceso concedido al historial")
        st.dataframe(df)

        st.markdown("### 🗑️ Eliminar solicitudes")
        seleccion = st.multiselect("Selecciona las filas a eliminar (por índice):", df.index.tolist())

        if st.button("Eliminar seleccionadas"):
            if seleccion:
                for i in sorted(seleccion, reverse=True):
                    sheet.delete_rows(i + 2)  # +2 por encabezado y base 1
                st.success("✅ Solicitudes eliminadas correctamente.")
            else:
                st.warning("⚠️ No se seleccionaron filas para eliminar.")
    except Exception as e:
        st.error(f"❌ No se pudo leer o eliminar: {e}")
elif password:
    st.error("❌ Contraseña incorrecta")
