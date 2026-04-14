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
st.set_page_config(page_title="Business CRM Master AI", layout="wide", page_icon="🎯")

# --- 2. FASTLÅSTE LISTER (REGIONER, BYER, BRANCHER) ---
GEOGRAPHY_DATA = {
    "Andalusia": ["Estepona", "Marbella", "Benahavís", "San Pedro de Alcántara", "Nueva Andalucía", "Puerto Banús", "Casares", "Manilva", "Sotogrande", "Fuengirola", "Mijas", "Mijas Costa", "Benalmádena", "Torremolinos", "Málaga", "Rincón de la Victoria", "Torre del Mar", "Vélez-Málaga", "Nerja", "Frigiliana", "Granada", "Seville"],
    "Catalonia": ["Barcelona"],
    "Madrid": ["Madrid"],
    "Valencian Community": ["Alicante", "Torrevieja", "Valencia", "Benidorm", "Altea", "Calpe", "Denia", "Javea"],
    "Murcia": ["Murcia", "Cartagena"],
    "Balearic Islands": ["Palma de Mallorca", "Ibiza"],
    "Canary Islands": ["Tenerife", "Gran Canaria"],
    "Basque Country": ["Bilbao"],
    "Galicia": ["Vigo", "A Coruña"],
    "Castile and León": ["Valladolid"],
    "Castile-La Mancha": ["Toledo"],
    "Aragon": ["Zaragoza"],
    "Extremadura": ["Mérida"],
    "Asturias": ["Gijón", "Oviedo"],
    "Cantabria": ["Santander"],
    "Navarre": ["Pamplona"],
    "La Rioja": ["Logroño"]
}

INDUSTRY_DATA = {
    "Ejendom": ["Køb bolig", "Sælge bolig", "Nybyggeri", "Investering", "Udlejning kort", "Udlejning lang"],
    "Turisme & ferie": ["Hoteller", "Ferieboliger", "Resorts", "Fly & transport", "Oplevelser & aktiviteter"],
    "Transport": ["Biludlejning", "Luksusbiler", "Lufthavn transfer", "Leasing"],
    "Juridisk & rådgivning": ["Advokat", "Skatterådgivning", "NIE nummer", "Residency / visa"],
    "Finans & bank": ["Boliglån", "Bank", "Valuta exchange", "Forsikring"],
    "Bolig & renovation": ["Byggefirma", "Renovering", "Interiør", "Møbler", "Pool / have"],
    "Service & drift": ["Rengøring", "Property management", "Nøgleservice", "Udlejning management"],
    "Sundhed & velvære": ["Hospital", "Læge", "Tandlæge", "Wellness / spa"],
    "Uddannelse": ["Internationale skoler", "Sprogskoler"],
    "Lifestyle": ["Restauranter", "Golf", "Fitness", "Beach clubs"],
    "Hverdagsliv": ["Supermarked", "Internet", "El / vand"],
    "Flytning & relocation": ["Flyttefirma", "Bilimport", "Pet relocation"]
}

# --- 3. SPROG-DATABASE ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "CRM Master AI", "login_title": "MGM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i CRM...", "total_leads": "Antal leads: {n}",
        "sidebar_scan": "📸 AI Scanner", "sidebar_filter": "🎯 Filtre", "sidebar_admin": "🛠️ Admin",
        "btn_create": "➕ OPRET MANUELT", "btn_save": "💾 GEM KLIENT", "btn_delete": "🗑️ SLET",
        "tab1": "📞 Kontakt", "tab2": "🌍 Geografi", "tab3": "⚙️ Salg", "tab4": "📝 Beskrivelse", "tab5": "📁 Medier",
        "field_logo": "Logo", "field_docs": "Dokumenter", "field_gal": "Galleri"
    },
    "🇬🇧 English": {
        "title": "CRM Master AI", "login_title": "MGM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search...", "total_leads": "Leads: {n}",
        "sidebar_scan": "📸 AI Scanner", "sidebar_filter": "🎯 Filters", "sidebar_admin": "🛠️ Admin",
        "btn_create": "➕ CREATE MANUAL", "btn_save": "💾 SAVE CLIENT", "btn_delete": "🗑️ DELETE",
        "tab1": "📞 Contact", "tab2": "🌍 Geo", "tab3": "⚙️ Sales", "tab4": "📝 Desc", "tab5": "📁 Media",
        "field_logo": "Logo", "field_docs": "Documents", "field_gal": "Gallery"
    },
    "🇪🇸 Español": {
        "title": "CRM Maestro AI", "login_title": "Iniciar Sesión", "login_btn": "ENTRAR", "logout": "🚪 Salir",
        "search": "🔍 Buscar...", "total_leads": "Leads: {n}",
        "sidebar_scan": "📸 Escáner IA", "sidebar_filter": "🎯 Filtros", "sidebar_admin": "🛠️ Admin",
        "btn_create": "➕ CREAR MANUAL", "btn_save": "💾 GUARDAR", "btn_delete": "🗑️ ELIMINAR",
        "tab1": "📞 Contacto", "tab2": "🌍 Geo", "tab3": "⚙️ Ventas", "tab4": "📝 Desc", "tab5": "📁 Medios",
        "field_logo": "Logo", "field_docs": "Documentos", "field_gal": "Galería"
    }
}

# --- 4. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, client_id INTEGER, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            if conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()[0] == 0:
                conn.execute(text("INSERT INTO users VALUES ('admin', :p, 'admin')"), {"p": os.getenv("APP_PASSWORD", "mgm2024")})
        return engine
    return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 5. LOGIN ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    r_p = os.getenv("APP_PASSWORD", "mgm2024")
    if u == "admin" and p == r_p:
        st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, "admin", u
    elif db_engine:
        with db_engine.connect() as conn:
            res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
            if res and res[0] == p:
                st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
                st.rerun()

if not st.session_state.authenticated:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    L = TRANSLATIONS[st.session_state.lang_choice]
    st.title(L['login_title'])
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("Username", key="l_u")
        st.text_input("Password", type="password", key="l_p")
        st.button(L['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang_choice]

# --- 6. DATA STRUKTUR ---
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
    defaults = {
        "status": ["Ny", "Dialog", "Vundet", "Tabt"],
        "membership": ["BASIC", "VIP", "Premium", "Gold"],
        "ad_profile": ["Standard", "Medium", "Proff.", "SoMe", "FB", "Google", "Website", "Webshop", "Landingpage"],
        "kilde": ["Inbound", "AI Scan", "Andet"],
        "agents": ["Brian", "Agent 1"],
        "titles": ["CEO", "Ejer", "Manager"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM crm_configs", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

if 'df_leads' not in st.session_state:
    try: 
        st.session_state.df_leads = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

opts = load_options()

# --- 7. KLIENT KORT POPUP ---
@st.dialog("🎯 Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"ID: {row.get('Client ID')} | {row.get('Company Name') or 'New'}")
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}
    
    with t1:
        c1, c2 = st.columns(2)
        upd['Company Name'] = c1.text_input("Virksomhedsnavn (Legal)", value=row.get('Company Name'))
        upd['CIF Number VAT'] = c2.text_input("CIF / VAT", value=row.get('CIF Number VAT'))
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            for f in ['Kontaktperson', 'Email', 'Phone number', 'Mobilnr']: upd[f] = st.text_input(f, value=row.get(f,''))
        with col2:
            for f in ['WhatsApp', 'Telegram', 'Facebook', 'Instagram', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            r_list = sorted(list(GEOGRAPHY_DATA.keys()))
            upd['Region'] = st.selectbox("Region", r_list, index=r_list.index(row['Region']) if row['Region'] in r_list else 0)
            town_list = GEOGRAPHY_DATA[upd['Region']]
            upd['Town'] = st.selectbox("By", town_list, index=town_list.index(row['Town']) if row['Town'] in town_list else 0)
            upd['Area'] = st.text_input("Område", value=row.get('Area'))
        with c2:
            ind_list = sorted(list(INDUSTRY_DATA.keys()))
            upd['Brancher'] = st.selectbox("Branche", ind_list, index=ind_list.index(row['Brancher']) if row['Brancher'] in ind_list else 0)
            sub_list = INDUSTRY_DATA[upd['Brancher']]
            curr_s = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(st.multiselect("Underbrancher", sub_list, default=[x for x in curr_s if x in sub_list]))
            upd['Languages'] = st.text_input("Languages", value=row.get('Languages'))

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox("Membership", opts['membership'], index=opts['membership'].index(row.get('Membership')) if row.get('Membership') in opts['membership'] else 0)
            upd['Advertising'] = st.selectbox("Annonceprofil", opts['ad_profile'], index=opts['ad_profile'].index(row.get('Advertising')) if row.get('Advertising') in opts['ad_profile'] else 0)
        with c2:
            upd['Agent'] = st.selectbox("Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
            upd['Leadtype'] = st.selectbox("Kilde", opts['kilde'], index=opts['kilde'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['kilde'] else 0)
            upd['Date for follow up'] = st.date_input("Follow up", value=pd.to_datetime(row.get('Date for follow up'), dayfirst=True, errors='coerce').date() or date.today()).strftime('%d/%m/%Y')

    with t5:
        upd['Noter'] = st.text_area("Notes", value=row.get('Noter'), height=200)
        l_up = st.file_uploader(L['field_logo'], type=['png','jpg'])
        if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete']):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.session_state.lang = st.selectbox("🌐 Sprog", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan Card")
        if cam:
            nr = {c: "" for c in MASTER_COLS}; nr['Company Name'] = f"AI Scan {datetime.now().strftime('%H:%M')}"
            nr['Client ID'] = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
            st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
            save_db(st.session_state.df_leads); st.rerun()

    st.header(L['sidebar_filter'])
    f_st = st.multiselect("Status", opts['status'])
    f_br = st.multiselect("Branche", list(INDUSTRY_DATA.keys()))
    f_re = st.multiselect("Region", list(GEOGRAPHY_DATA.keys()))

    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            cat = st.selectbox("Edit List:", ["agents", "status", "membership", "ad_profile", "kilde"])
            v_new = st.text_input("New Value:")
            if st.button("Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat, "v":v_new})
                st.rerun()

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nid = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = nid
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    if st.button(L['logout']): st.session_state.authenticated = False; st.rerun()

# --- 9. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_br: df_v = df_v[df_v['Brancher'].isin(f_br)]
if f_re: df_v = df_v[df_v['Region'].isin(f_re)]
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(L['total_leads'].format(n=len(df_v)))
sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    lead_popup(df_v.index[sel.selection.rows[0]])
