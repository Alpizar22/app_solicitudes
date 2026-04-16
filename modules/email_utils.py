import logging

import streamlit as st
import yagmail

log = logging.getLogger("email_utils")

SEND_EMAILS = bool(st.secrets.get("email", {}).get("send_enabled", False))


def enviar_correo(asunto, cuerpo_detalle, para):
    if not SEND_EMAILS:
        return
    try:
        # Obtenemos el usuario y password de los secrets
        user_email = st.secrets["email"]["user"]
        password = st.secrets["email"]["password"]

        yag = yagmail.SMTP(user=user_email, password=password)

        # --- LISTA DE COPIAS (CC) ---
        # Aquí pones los correos de los jefes/supervisores.
        # Al ponerlos aquí, se aplicará para TODOS los envíos del sistema.
        cc_list = list(st.secrets["admin"]["emails"])

        to = [para]
        headers = {"From": f"Equipo CRM <{user_email}>"}

        # --- TU DISEÑO HTML (INTACTO) ---
        mensaje_html = f"""
        <div style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #004B93;">Confirmación de Recepción</h2>
            <p>Hola,</p>
            <p>Hemos recibido tu solicitud con el asunto: <strong>{asunto}</strong>.</p>
            <p>Se ha notificado al equipo de CRM y tu caso ha entrado en la cola de gestión.
            Será atendido en su momento conforme a la carga de trabajo.</p>
            <p><strong>No es necesario que respondas a este correo.</strong>
            Te notificaremos nuevamente por este medio en cuanto haya una actualización o resolución.</p>
            <hr>
            <p style="font-size: 12px; color: #666;">Detalle recibido:<br>{cuerpo_detalle}</p>
            <br>
            <p>Atentamente,<br><strong>Equipo de Gestión CRM</strong></p>
        </div>
        """

        # --- EL ENVÍO CON CC ---
        yag.send(
            to=to,
            cc=cc_list,  # <--- AQUÍ SE AGREGAN LAS COPIAS
            subject=f"Recibido: {asunto}",
            contents=[mensaje_html],
            headers=headers,
        )
        log.info(f"Correo enviado a {to} con copia a {cc_list}")

    except Exception as e:
        log.error(f"enviar_correo: error enviando a {para}: {e}")
