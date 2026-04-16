import re
import logging
from uuid import uuid4

import streamlit as st
import pandas as pd

from modules.sheets import get_records_simple, sheet_usuarios

log = logging.getLogger("auth")

EMAIL_RE = re.compile(r'([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})', re.I)


def _email_norm(s: str) -> str:
    if s is None:
        return ""
    m = EMAIL_RE.search(str(s))
    return m.group(1).strip().lower() if m else str(s).strip().lower()


def _norm(x):
    return str(x).strip().lower() if pd.notna(x) else ""


def _is_unrated(val: str) -> bool:
    return _norm(val) in ("", "pendiente", "na", "n/a", "sin calificacion", "-")


def do_login(m):
    st.session_state.update({"usuario_logueado": _email_norm(m), "session_id": str(uuid4())})
    st.rerun()


def do_logout():
    st.session_state.clear()
    st.rerun()


@st.cache_data(ttl=300, show_spinner=False)
def get_usuarios_dict() -> dict:
    """Carga el dict contraseña→email desde la hoja Usuarios.
    TTL de 5 min para que los usuarios nuevos sean visibles sin reiniciar.
    """
    udf = get_records_simple(sheet_usuarios, "Usuarios")
    return {
        str(p).strip(): _email_norm(c)
        for p, c in zip(udf.get("Contraseña", []), udf.get("Correo", []))
        if str(p).strip()
    }
