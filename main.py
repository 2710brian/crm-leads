import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date
from openai import OpenAI
import json

# --- 1. SPROG-DEFINITIONER ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "login_title": "CRM Master Login",
        "login_btn": "LOG IND",
        "user_mgmt": "👤 Brugerstyring",
        "new_user": "Nyt brugernavn",
        "new_pass": "Ny adgangskode",
        "add_btn": "Opret Bruger/Agent",
        "search": "Søg i CRM...",
        "sidebar_admin": "🛠️ Dropdown Administration",
        "sidebar_filters": "🎯 Kampagne Filtre",
        "sidebar_export": "📤 Eksport",
        "btn_save_all": "💾 GEM ALT PÅ KLIENT",
        "btn_delete": "🗑️ SLET",
        "btn_create": "➕ OPRET MANUELT",
        "tab_contact": "📞 Kontakt",
        "tab_geo": "🌍 Geografi",
        "tab_sales": "⚙️ Salg",
        "tab_desc": "📝 Beskrivelser",
        "tab_media": "📁 Medier & Noter",
        "status_msg": "Viser {n} leads",
        "agent_label": "Agent",
        "branch_label": "Branche",
        "town_label": "By",
        "status_label": "Status",
    },
    "🇬🇧 English": {
        "login_title": "CRM Master Login",
        "login_btn": "LOG IN",
        "user_mgmt": "👤 User Management",
        "new_user": "New Username",
        "new_pass": "New Password",
        "add_btn": "Create User/Agent",
        "search": "Search CRM...",
        "sidebar_admin": "🛠️ Dropdown Admin",
        "sidebar_filters": "🎯 Campaign Filters",
        "sidebar_export": "📤 Export",
        "btn_save_all": "💾 SAVE ALL CHANGES",
        "btn_delete": "🗑️ DELETE",
        "btn_create": "➕ CREATE MANUALLY",
        "tab_contact": "📞 Contact",
        "tab_geo": "🌍 Geography",
        "tab_sales": "⚙️ Sales",
        "tab_desc": "📝 Descriptions",
        "tab_media": "📁 Media & Notes",
        "status_msg": "Showing {n} leads",
        "agent_label": "Agent",
        "branch_label": "Industry",
        "town_label": "City",
        "status_label": "Status",
    },
    "🇪🇸 Español": {
        "login_title": "Inicio de Sesión CRM",
        "login_btn": "ENTRAR",
        "user_mgmt": "👤 Gestión de Usuarios",
        "new_user": "Nuevo usuario",
        "new_pass": "Nueva contraseña",
        "add_btn": "Crear Usuario/Agente",
        "search": "Buscar en CRM...",
        "sidebar_admin": "🛠️ Administración",
        "sidebar_filters": "🎯 Filtros de Campaña",
        "sidebar_export": "📤 Exportar",
        "btn_save_all": "💾 GUARDAR CAMBIOS",
        "btn_delete": "🗑️ ELIMINAR",
        "btn_create": "➕ CREAR MANUALMENTE",
        "tab_contact": "📞 Contacto",
        "tab_geo": "🌍 Geografía",
        "tab_sales": "⚙️ Ventas",
        "tab_desc": "📝 Descripciones",
        "tab_media": "📁 Archivos y Notas",
        "status_msg": "Mostrando {n} leads",
        "agent_label": "Agente",
        "branch_label": "Sector",
        "town_label": "Ciudad",
        "status_label": "Estado",
    }
}

# --- 2. KONFIGURATION ---
st.set_page_config(page_title="Business CRM Master AI", layout="wide", page_icon="🎯")

if "lang" not in st.session_state:
    st.session_state.lang = "🇩🇰 Dansk"

# --- 3. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, data JSONB)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
                res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
                if res[0] == 0:
                    u, p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
                    conn.execute(text("INSERT INTO users VALUES (:u, :p, 'admin')"), {"u": u, "p": p})
            return engine
        except: return None
    return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def db_execute(query, params=None):
    try:
        with db_engine.begin() as conn:
            conn.execute(text(query), params or {})
        return True
    except: return False

# --- 4. LOGIN ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    rail_u, rail_p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
    if u == rail_u and p == rail_p:
        st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, "admin", u
    elif db_engine:
        with db_engine.connect() as conn:
            res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
            if res and res[0] == p:
                st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
            else: st.error("❌ Login failed")

if not st.session_state.authenticated:
    st.session_state.lang = st.selectbox("🌐 Language", list(TRANSLATIONS.keys()))
    lang = TRANSLATIONS[st.session_state.lang]
    st.title(f"💼 {lang['login_title']}")
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("Username", key="l_u")
        st.text_input("Password", type="password", key="l_p")
        st.button(lang['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

# --- 5. CRM SYSTEM ---
lang = TRANSLATIONS[st.session_state.lang]
MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data'
]
DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Company Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

# --- 6. POPUP KORT ---
@st.dialog("🎯 CRM", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name')}")
    t1, t2, t3, t4, t5 = st.tabs([lang['tab_contact'], lang['tab_geo'], lang['tab_sales'], lang['tab_desc'], lang['tab_media']])
    upd = {}
    with t1:
        c1, c2 = st.columns(2)
        with c1:
            for f in ['Company Name', 'CIF Number VAT', 'Kontaktperson', 'Email', 'Mobilnr']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            for f in ['WhatsApp', 'Telegram', 'Facebook', 'Instagram', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
    with t3:
        # Simplificeret salgs-fane for stabilitet
        upd['Status on lead'] = st.text_input("Status", value=row.get('Status on lead',''))
        upd['Agent'] = st.text_input("Agent", value=row.get('Agent',''))
    with t5:
        upd['Noter'] = st.text_area("Notes", value=row.get('Noter',''), height=300)

    if st.button(lang['btn_save_all'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.session_state.lang = st.selectbox("🌐 Sprog", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    # BRUGERSTYRING MED PASSWORD
    if st.session_state.user_role == "admin":
        with st.expander(lang['user_mgmt']):
            new_u = st.text_input(lang['new_user'], key="new_u_input")
            new_p = st.text_input(lang['new_pass'], type="password", key="new_p_input")
            if st.button(lang['add_btn']):
                if new_u and new_p:
                    if db_execute("INSERT INTO users VALUES (:u,:p,'agent')", {"u":new_u, "p":new_p}):
                        st.success("User added!")
                        st.rerun()

    st.divider()
    if st.button(lang['btn_create'], type="primary", use_container_width=True):
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Agent'] = st.session_state.username
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    
    st.download_button(lang['sidebar_export'], st.session_state.df_leads.to_csv(index=False), "master.csv", use_container_width=True)
    if st.button("🚪 Log out"): st.session_state.authenticated = False; st.rerun()

# --- 8. DASHBOARD ---
st.title(f"💼 Business CRM")
search = st.text_input(lang['search'])
df_v = st.session_state.df_leads.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
