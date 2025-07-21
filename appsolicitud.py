
import streamlit as st
import pandas as pd
import json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Conectar con Google Sheets desde st.secrets ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
service_account_info = st.secrets["google_service_account"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key("18uBeG2cCDpb4I2M3OkEZX9H8jb94LTMEeImJKXxtXHg").sheet1

# === Cargar archivos JSON locales ===
with open("data/estructura_roles.json", encoding="utf-8") as f:
    estructura_roles = json.load(f)

with open("data/numeros_por_perfil.json", encoding="utf-8") as f:
    numeros_por_perfil = json.load(f)

with open("data/horarios.json", encoding="utf-8") as f:
    horarios_dict = json.load(f)

# === INTERFAZ ===
st.title("Formulario de Solicitudes de Usuario")

tipo = st.selectbox("Tipo de Solicitud", ["Alta", "Modificación", "Baja"])
nombre = st.text_input("Nombre Completo")
correo = st.text_input("Correo")
area = st.selectbox("Área", list(estructura_roles.keys())) if tipo != "Baja" else None

perfil = rol = numero_in = numero_saliente = horario = turno = ""
if area:
    perfiles = list(estructura_roles[area].keys())
    perfil = st.selectbox("Perfil", perfiles)

    roles = estructura_roles[area][perfil]
    rol = st.selectbox("Rol", roles)

    if perfil in numeros_por_perfil:
        if perfil == "Agente de Call Center":
            numero_in = st.selectbox("Número IN", [""] + numeros_por_perfil[perfil]["Numero_IN"])
        numero_saliente = st.selectbox("Número Saliente", [""] + numeros_por_perfil[perfil]["Numero_Saliente"])

    horario = st.selectbox("Horario de trabajo", [""] + list(horarios_dict.keys()))
    if horario:
        turno = horarios_dict.get(horario, "")

solicitado_por = st.text_input("¿Quién lo solicitó?")

# === Enviar solicitud ===
if st.button("Enviar Solicitud"):
    if not nombre or not correo or not solicitado_por:
        st.warning("⚠️ Nombre, correo y quién lo solicitó son obligatorios.")
    elif tipo != "Baja" and (not area or not perfil or not rol or not horario):
        st.warning("⚠️ Completa todos los campos requeridos para altas o modificaciones.")
    elif perfil == "Agente de Call Center" and not numero_in:
        st.warning("⚠️ El perfil Agente de Call Center requiere Número IN.")
    elif perfil in numeros_por_perfil and not numero_saliente:
        st.warning("⚠️ El perfil seleccionado requiere Número Saliente.")
    else:
        fila = [
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            tipo, nombre, correo, area or "", perfil or "", rol or "",
            numero_in, numero_saliente, horario, turno, solicitado_por
        ]
        try:
            sheet.append_row(fila)
            st.success("✅ Solicitud enviada y registrada en Google Sheets.")
        except Exception as e:
            st.error(f"❌ Error al guardar en Google Sheets: {e}")

# === Historial protegido ===
st.subheader("Historial de Solicitudes (solo acceso autorizado)")
password = st.text_input("Ingresa la contraseña para ver el historial", type="password")
if password == "Generardo2":
    try:
        data = sheet.get_all_records()
        st.success("🔓 Acceso concedido al historial")
        st.dataframe(pd.DataFrame(data))
    except Exception as e:
        st.error(f"❌ No se pudo leer la hoja: {e}")
elif password:
    st.error("❌ Contraseña incorrecta")
