import time
import random
import logging

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger("sheets")

APP_MODE = st.secrets.get("mode", "dev")
SHEET_ID = (
    st.secrets.get("sheets", {}).get("prod_id")
    if APP_MODE == "prod"
    else st.secrets.get("sheets", {}).get("dev_id")
)


def with_backoff(fn, *args, **kwargs):
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.warning(f"with_backoff: intento {i + 1}/5 fallido en '{getattr(fn, '__name__', fn)}': {e}")
            time.sleep(min(1 * (2 ** i) + random.random(), 16))
    log.error(f"with_backoff: todos los intentos fallaron para '{getattr(fn, '__name__', fn)}'")
    raise Exception("API Failed")


@st.cache_resource(ttl=3600)
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        st.secrets["google_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


@st.cache_resource(ttl=3600)
def get_spreadsheet():
    return with_backoff(get_gspread_client().open_by_key, SHEET_ID)


@st.cache_resource(ttl=3600)
def get_sheets():
    b = get_spreadsheet()
    return {k: b.worksheet(k) for k in ["Sheet1", "Incidencias", "Quejas", "Accesos", "Usuarios"]}


@st.cache_data(ttl=60, show_spinner=False)
def get_records_simple(_ws, sheet_name: str = "") -> pd.DataFrame:
    """Lee una hoja de cálculo y la devuelve como DataFrame.

    `sheet_name` se incluye en el cache key para que cada hoja tenga
    su propia entrada — sin él, @st.cache_data ignora _ws (guión bajo)
    y todas las hojas compartirían el mismo resultado cacheado.
    """
    try:
        v = with_backoff(_ws.get_all_values)
        if not v:
            return pd.DataFrame()
        h, d = v[0], v[1:]
        return pd.DataFrame([r + [""] * (len(h) - len(r)) for r in d], columns=h)
    except Exception as e:
        log.error(f"get_records_simple: error leyendo hoja '{sheet_name or getattr(_ws, 'title', _ws)}': {e}")
        return pd.DataFrame()


_sheets = get_sheets()
sheet_solicitudes = _sheets["Sheet1"]
sheet_incidencias = _sheets["Incidencias"]
sheet_quejas      = _sheets["Quejas"]
sheet_usuarios    = _sheets["Usuarios"]
