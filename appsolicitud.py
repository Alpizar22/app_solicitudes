
import streamlit as st
import pandas as pd
import json
from datetime import datetime

# Cargar archivos JSON
with open("data/estructura_roles.json", encoding="utf-8") as f:
    estructura_roles = json.load(f)

with open("data/numeros_por_perfil.json", encoding="utf-8") as f:
    numeros_por_perfil = json.load(f)

with open("data/horarios.json", encoding="utf-8") as f:
    horarios_dict = json.load(f)

# Cargar solicitudes previas
try:
    df = pd.read_excel("Solicitudes.xlsx")
except FileNotFoundError:
    df = pd.DataFrame()

st.title("Formulario de Solicitudes de Usuario")

tipo = st.selectbox("Tipo de Solicitud", ["Alta", "Modificación", "Baja"])

# --- Campos comunes
nombre = st.text_input("Nombre Completo")
correo = st.text_input("Correo")

area = st.selectbox("Área", list(estructura_roles.keys())) if tipo != "Baja" else None

# Inicializar variables para evitar errores
perfil = rol = numero_in = numero_saliente = horario = turno = ""

# --- Campos condicionales
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

# --- Botón de envío
if st.button("Enviar Solicitud"):
    # Validaciones
    if not nombre or not correo or not solicitado_por:
        st.warning("⚠️ Nombre, correo y quién lo solicitó son obligatorios.")
    elif tipo != "Baja" and (not area or not perfil or not rol or not horario):
        st.warning("⚠️ Por favor completa todos los campos requeridos para altas o modificaciones.")
    elif perfil == "Agente de Call Center" and not numero_in:
        st.warning("⚠️ El perfil Agente de Call Center requiere Número IN.")
    elif perfil in numeros_por_perfil and not numero_saliente:
        st.warning("⚠️ El perfil seleccionado requiere Número Saliente.")
    else:
        nueva_solicitud = {
            "Fecha": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "Tipo": tipo,
            "Nombre": nombre,
            "Correo": correo,
            "Área": area if tipo != "Baja" else "",
            "Perfil": perfil if tipo != "Baja" else "",
            "Rol": rol if tipo != "Baja" else "",
            "Número IN": numero_in if perfil == "Agente de Call Center" else "",
            "Número Saliente": numero_saliente if perfil in numeros_por_perfil else "",
            "Horario": horario if tipo != "Baja" else "",
            "Turno": turno if tipo != "Baja" else "",
            "Solicitante": solicitado_por
        }

        df = pd.concat([df, pd.DataFrame([nueva_solicitud])], ignore_index=True)
        try:
            df.to_excel("Solicitudes.xlsx", index=False)
            st.success("✅ Solicitud registrada correctamente.")
        except PermissionError:
            st.error("❌ No se pudo guardar. Cierra el archivo 'Solicitudes.xlsx' si está abierto.")

# --- Protección para mostrar historial
st.subheader("Historial de Solicitudes (solo acceso autorizado)")
password = st.text_input("Ingresa la contraseña para ver el historial", type="password")
if password == "Generardo2":
    st.success("🔓 Acceso concedido al historial")
    st.dataframe(df)
elif password:
    st.error("❌ Contraseña incorrecta")
