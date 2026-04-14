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

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Business CRM Master Pro AI", layout="wide", page_icon="🎯")

# --- 2. SPROG-DATABASE (KOMPLET) ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i alt data...", "total_leads": "Viste leads: {n}",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Kampagne Filtre", "sidebar_admin": "🛠️ Admin Kontrol",
        "sidebar_user": "👤 Brugerstyring", "sidebar_export": "📤 Eksport", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "btn_delete": "🗑️ SLET", "tab1": "📞 Kontakt & Social",
        "tab2": "🌍 Geografi & Brancher", "tab3": "⚙️ Salg & Pipeline", "tab4": "📝 Beskrivelser", "tab5": "📁 Medier & Noter",
        "field_id": "Klient ID", "field_name": "Virksomhed (Legal)", "field_cif": "CIF / VAT", "field_person": "Kontaktperson", 
        "field_title": "Titel", "field_mail": "E-mail", "field_phone": "Kontor Tlf", "field_mobile": "Mobil", "field_wa": "WhatsApp", 
        "field_tg": "Telegram", "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Website URL", "field_reg": "Region", 
        "field_area": "Område", "field_town": "By", "field_addr": "Adresse", "field_zip": "Postnr.", "field_loc": "Google Maps Link", 
        "field_br": "Brancher", "field_ubr": "Underbrancher", "field_lang": "Sprog", "field_work": "Åbningstider", "field_st": "Status", 
        "field_mem": "Medlemskab", "field_adv": "Annonceprofil", "field_agent": "Agent", "field_src": "Kilde", "field_created": "Oprettet", 
        "field_follow": "Opfølgning", "field_last": "Sidste kontakt", "field_pitch": "Kort Pitch", "field_desc": "Annoncetekst", 
        "field_qr": "QR Tracking URL", "field_notes": "Interne CRM Noter"
    },
    "🇬🇧 English": {
        "title": "Business CRM AI", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search data...", "total_leads": "Leads: {n}", "tab1": "📞 Contact", "tab2": "🌍 Geo", 
        "btn_save": "💾 SAVE CLIENT", "field_name": "Company Name", "field_st": "Status", "field_br": "Industry"
    },
    "🇪🇸 Español": {
        "title": "CRM Maestro AI", "login_title": "Entrar", "login_btn": "ENTRAR", "logout": "🚪 Salir",
        "search": "🔍 Buscar...", "total_leads": "Leads: {n}", "tab1": "📞 Contacto", "tab2": "🌍 Geo",
        "btn_save": "💾 GUARDAR", "field_name": "Empresa", "field_st": "Estado", "field_br": "Sector"
    }
}

# --- 3. DATABASE MOTOR & INITIALISERING ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return None
    if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
    try:
        engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, client_id INTEGER, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
            # Relationelle tabeller
            conn.execute(text("CREATE TABLE IF NOT EXISTS regions (id TEXT PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS cities (id TEXT PRIMARY KEY, name TEXT, region_id TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS categories (id TEXT PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS subcategories (id SERIAL PRIMARY KEY, name TEXT, cat_id TEXT)"))
            
            # Seed data hvis tomt (Baseret på din JSON)
            if conn.execute(text("SELECT COUNT(*) FROM regions")).fetchone()[0] == 0:
                conn.execute(text("INSERT INTO regions VALUES ('andalusia', 'Andalusia'), ('madrid', 'Madrid'), ('valencian', 'Valencian Community')"))
                conn.execute(text("INSERT INTO cities VALUES ('malaga', 'Málaga', 'andalusia'), ('marbella', 'Marbella', 'andalusia'), ('madrid_city', 'Madrid', 'madrid')"))
                conn.execute(text("INSERT INTO categories VALUES ('real_estate', 'Ejendom'), ('tourism', 'Turisme')"))
                conn.execute(text("INSERT INTO subcategories (name, cat_id) VALUES ('Køb bolig', 'real_estate'), ('Hoteller', 'tourism')"))
            
            # Opret admin
            res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
            if res[0] == 0:
                u, p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
                conn.execute(text("INSERT INTO users VALUES (:u,:p,'admin')"), {"u":u, "p":p})
        return engine
    except Exception as e:
        st.error(f"DB Error: {e}")
        return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 4. LOGIN LOGIK ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False
if "lang" not in st.session_state: st.session_state.lang = "🇩🇰 Dansk"

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
                st.rerun()

if not st.session_state.authenticated:
    st.session_state.lang = st.selectbox("🌐 Language", list(TRANSLATIONS.keys()))
    l_login = TRANSLATIONS[st.session_state.lang]
    st.title(l_login['login_title'])
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("Username", key="l_u")
        st.text_input("Password", type="password", key="l_p")
        st.button(l_login['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang]

# --- 5. DATA LOGIK & MASTER STRUKTUR ---
MASTER_COLS = [
    'Client ID', 'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Data'
]
DISPLAY_COLS = ['Client ID', 'Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

def load_options():
    with db_engine.connect() as conn:
        return {
            "regions": pd.read_sql("SELECT * FROM regions", conn),
            "cities": pd.read_sql("SELECT * FROM cities", conn),
            "categories": pd.read_sql("SELECT * FROM categories", conn),
            "subcategories": pd.read_sql("SELECT * FROM subcategories", conn),
            "configs": pd.read_sql("SELECT * FROM crm_configs", conn)
        }

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_leads(df):
    if db_engine:
        df = force_clean(df)
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 6. KLIENT KORT POPUP (ALT ER MED) ---
@st.dialog("🎯 Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"ID: {row.get('Client ID')} | {row.get('Company Name') or 'Lead'}")
    
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}
    with t1:
        st.markdown(f"##### {L['tab1']}")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input(L['field_name'], value=row.get('Company Name'))
        upd['CIF Number VAT'] = ct2.text_input(L['field_cif'], value=row.get('CIF Number VAT'))
        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            for f, k in [('Kontaktperson','field_person'), ('Titel','field_title'), ('Email','field_mail'), ('Mobilnr','field_mobile')]:
                upd[f] = st.text_input(L[k], value=row.get(f,''))
        with c2:
            for f, k in [('WhatsApp','field_wa'), ('Telegram','field_tg'), ('Facebook','field_fb'), ('Instagram','field_ig'), ('Website','field_web')]:
                upd[f] = st.text_input(L[k], value=row.get(f,''))
    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            r_names = opts['regions']['name'].tolist()
            upd['Region'] = st.selectbox(L['field_reg'], r_names, index=r_names.index(row['Region']) if row['Region'] in r_names else 0)
            rid = opts['regions'][opts['regions']['name'] == upd['Region']]['id'].values[0]
            c_names = opts['cities'][opts['cities']['region_id'] == rid]['name'].tolist()
            upd['Town'] = st.selectbox(L['field_town'], c_names, index=c_names.index(row['Town']) if row['Town'] in c_names else 0)
            upd['Area'] = st.text_input(L['field_area'], value=row.get('Area'))
        with c2:
            cat_names = opts['categories']['name'].tolist()
            upd['Brancher'] = st.selectbox(L['field_br'], cat_names, index=cat_names.index(row['Brancher']) if row.get('Brancher') in cat_names else 0)
            cid = opts['categories'][opts['categories']['name'] == upd['Brancher']]['id'].values[0]
            s_names = opts['subcategories'][opts['subcategories']['category_id'] == cid]['name'].tolist()
            curr_s = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(st.multiselect(L['field_ubr'], s_names, default=[x for x in curr_s if x in s_names]))
    with t3:
        st.markdown(f"##### {L['tab3']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['field_st'], ["Ny", "Dialog", "Vundet", "Tabt", "Pause"], index=0)
            upd['Membership'] = st.selectbox(L['field_mem'], ["Ingen", "BASIC", "VIP", "Premium", "Gold"], index=0)
        with c2:
            upd['Agent'] = st.selectbox(L['field_agent'], ["Brian", "Olga"], index=0)
            for f, k in [('Date created','field_created'), ('Date for follow up','field_follow')]:
                d_val = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(L[k], value=d_val, key=f"d_{f}_{idx}").strftime('%d/%m/%Y')
    with t5:
        upd['Noter'] = st.text_area(L['field_notes'], value=row.get('Noter'), height=200)
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=150)
        l_up = st.file_uploader(L['field_logo'], type=['png','jpg'])
        if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_leads(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete']):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_leads(st.session_state.df_leads); st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.session_state.lang = st.selectbox("🌐 Sprog", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    # AI SCANNER
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan")
        if cam:
            nr = {c: "" for c in MASTER_COLS}; nr['Company Name'] = f"AI Scan {datetime.now().strftime('%H:%M')}"
            st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
            save_leads(st.session_state.df_leads); st.rerun()

    # FILTRE
    st.header(L['sidebar_filter'])
    f_st = st.multiselect(L['field_st'], ["Ny", "Dialog", "Vundet", "Tabt"])
    f_br = st.multiselect(L['field_br'], opts['categories']['name'].tolist())
    f_re = st.multiselect(L['field_reg'], opts['regions']['name'].tolist())

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nid = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = nid
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_leads(st.session_state.df_leads); st.rerun()
    if st.button(L['logout']): st.session_state.authenticated = False; st.rerun()

# --- 8. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_br: df_v = df_v[df_v['Brancher'].isin(f_br)]
if f_re: df_v = df_v[df_v['Region'].isin(f_re)]
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(L['total_leads'].format(n=len(df_v)))
sel = st.dataframe(df_v[['Client ID', 'Date created', 'Company Name', 'Region', 'Town', 'Status on lead']], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    lead_popup(df_v.index[sel.selection.rows[0]])
