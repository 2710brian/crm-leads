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
st.set_page_config(page_title="Business CRM Master AI Pro", layout="wide", page_icon="🎯")

# --- 2. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return None
    if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
    try:
        engine = create_engine(db_url, pool_size=20, max_overflow=30, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, client_id INTEGER, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            if conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()[0] == 0:
                conn.execute(text("INSERT INTO users VALUES ('admin', 'mgm2024', 'admin')"))
        return engine
    except: return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# SIKKER DATABASE-SKRIVNING
def db_execute(query, params=None):
    try:
        with db_engine.begin() as conn: conn.execute(text(query), params or {})
        return True
    except: return False

# --- 3. SPROG-DATABASE ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business Master CRM", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i CRM...", "total_leads": "Viste leads: {n}", "sidebar_scan": "📸 AI Scanner", 
        "sidebar_admin": "🛠️ Admin Kontrol", "btn_save": "💾 GEM KLIENT", "btn_delete": "🗑️ SLET",
        "tab1": "📞 Kontakt", "tab2": "🌍 Geografi", "tab3": "⚙️ Salg", "tab4": "📝 Beskrivelse", "tab5": "📁 Medier",
        "field_logo": "Upload Logo", "field_docs": "Upload Dokumenter (PDF/Excel)", "field_gal": "Billedgalleri"
    },
    "🇬🇧 English": {
        "title": "Business Master CRM", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "Search...", "sidebar_scan": "📸 AI Scanner", "sidebar_admin": "🛠️ Admin",
        "btn_save": "💾 SAVE CLIENT", "btn_delete": "🗑️ DELETE",
        "tab1": "📞 Contact", "tab2": "🌍 Geo", "tab3": "⚙️ Sales", "tab4": "📝 Desc", "tab5": "📁 Media",
        "field_logo": "Upload Logo", "field_docs": "Upload Docs", "field_gal": "Gallery"
    }
}

# --- 4. FASTLÅSTE DATA ---
GEOGRAPHY_DEFAULTS = {
    "Andalusia": ["Estepona", "Marbella", "Benahavís", "San Pedro de Alcántara", "Nueva Andalucía", "Puerto Banús", "Casares", "Manilva", "Sotogrande", "Fuengirola", "Mijas", "Mijas Costa", "Benalmádena", "Torremolinos", "Málaga", "Rincón de la Victoria", "Torre del Mar", "Vélez-Málaga", "Nerja", "Frigiliana", "Granada", "Seville"],
    "Catalonia": ["Barcelona"], "Madrid": ["Madrid"], "Valencian Community": ["Alicante", "Torrevieja", "Valencia", "Benidorm", "Altea", "Calpe", "Denia", "Javea"]
}
INDUSTRY_DEFAULTS = {
    "Ejendom": ["Køb bolig", "Sælge bolig", "Nybyggeri", "Investering", "Udlejning"],
    "Turisme": ["Hoteller", "Oplevelser"], "Lifestyle": ["Restaurant", "Golf"]
}

# --- 5. SYSTEM LOGIK ---
MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Område Type', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Data', 'Logo_Navn', 'Gallery_Data', 'Client ID'
]
DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename_map = {'Merchant': 'Company Name', 'Programnavn': 'Company Name'}
    df = df.rename(columns=rename_map)
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = ""
    return df[MASTER_COLS]

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Company Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

# --- 6. LOGIN ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False
if "lang_choice" not in st.session_state: st.session_state.lang_choice = "🇩🇰 Dansk"

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    if u == "admin" and p == os.getenv("APP_PASSWORD", "mgm2024"):
        st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, "admin", u
    elif db_engine:
        with db_engine.connect() as conn:
            res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
            if res and res[0] == p:
                st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
                st.rerun()

if not st.session_state.authenticated:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    L_log = TRANSLATIONS[st.session_state.lang_choice]
    st.title(f"💼 {L_log['login_title']}")
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("User", key="l_u")
        st.text_input("Pass", type="password", key="l_p")
        st.button(L_log['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang_choice]

# Indlæs data
if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

def load_options():
    opts = {"agents": ["Brian", "Olga"], "status": ["Ny", "Dialog"], "membership": ["Basis", "VIP"], "ad_profile": ["Standard"], "kilde": ["Inbound"], "titles": ["CEO"], "sprog": ["Dansk"], "regions": sorted(list(GEOGRAPHY_DEFAULTS.keys())), "towns": sorted([t for sub in GEOGRAPHY_DEFAULTS.values() for t in sub]), "area_types": ["coast", "island", "inland"]}
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM crm_configs", db_engine)
            for k in opts.keys():
                stored = df_opt[df_opt['type'] == k]['value'].tolist()
                if stored: opts[k] = sorted(list(set(opts[k] + stored)))
        except: pass
    return opts

opts = load_options()

# --- 7. KLIENT KORT POPUP (MED VISUELT GALLERI OG DOKUMENTER) ---
@st.dialog("🎯 CRM Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    
    # VIS KUNDE LOGO ØVERST
    c_logo1, c_logo2 = st.columns([0.8, 0.2])
    with c_logo1: st.title(f"ID: {row.get('Client ID')} | {row.get('Company Name') or 'Lead'}")
    with c_logo2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=100)

    st.divider()
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        upd['Company Name'] = c1.text_input("Firma", value=row.get('Company Name'))
        upd['CIF Number VAT'] = c2.text_input("CIF / VAT", value=row.get('CIF Number VAT'))
        for f in ['Kontaktperson', 'Email', 'Mobilnr', 'WhatsApp', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
    
    with t2:
        c1, c2 = st.columns(2)
        upd['Region'] = c1.selectbox("Region", opts['regions'], index=opts['regions'].index(row['Region']) if row['Region'] in opts['regions'] else 0)
        upd['Town'] = c1.selectbox("By", GEOGRAPHY_DEFAULTS.get(upd['Region'], opts['towns']), index=0)
        upd['Brancher'] = c2.selectbox("Branche", sorted(list(INDUSTRY_DEFAULTS.keys())))
    
    with t3:
        upd['Status on lead'] = st.selectbox("Status", opts['status'], index=0)
        upd['Date for follow up'] = st.date_input("Follow up", value=date.today()).strftime('%d/%m/%Y')
    
    with t4:
        upd['Business Description'] = st.text_area("Kort Pitch", value=row.get('Business Description'))
        upd['Description'] = st.text_area("Lang beskrivelse", value=row.get('Description'), height=200)

    with t5:
        upd['Noter'] = st.text_area("Notes", value=row.get('Noter'), height=150)
        st.divider()
        
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown("🖼️ **Logo & Dokument**")
            l_up = st.file_uploader("Nyt Logo", type=['png','jpg'], key=f"l_{idx}")
            if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
            
            f_up = st.file_uploader("Nyt Dokument", key=f"f_{idx}")
            if f_up: 
                upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
            
            # VIS DOWNLOAD LINK HVIS DOKUMENT FINDES
            if row.get('Fil_Data'):
                st.markdown(f"📄 **{row['Fil_Navn']}**")
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">👉 Hent dokument</a>', unsafe_allow_html=True)

        with col_m2:
            st.markdown("🖼️ **Billedgalleri**")
            gal_up = st.file_uploader("Upload til Galleri", accept_multiple_files=True, key=f"g_{idx}")
            if gal_up:
                gal_list = []
                for f in gal_up: gal_list.append(base64.b64encode(f.read()).decode())
                upd['Gallery_Data'] = json.dumps(gal_list)
            
            # VIS GALLERI BILLEDER VISUELT
            if row.get('Gallery_Data'):
                images = json.loads(row['Gallery_Data'])
                st.write(f"Der er {len(images)} billeder i galleriet:")
                cols = st.columns(3)
                for i, img_b64 in enumerate(images):
                    cols[i % 3].image(f"data:image/png;base64,{img_b64}", use_container_width=True)

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 8. SIDEBAR & DASHBOARD ---
with st.sidebar:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    if st.session_state.user_role == "admin":
        with st.expander("🛠️ Admin Panel"):
            cat = st.selectbox("Edit List:", ["agents", "status", "membership", "towns"])
            v_new = st.text_input("New Value:")
            if st.button("Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat, "v":v_new})
                st.rerun()
            if st.button("🚨 Reset Database"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nums = pd.to_numeric(st.session_state.df_leads['Client ID'], errors='coerce').dropna()
        nid = int(nums.max() + 1) if not nums.empty else 1001
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = nid
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()

st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(f"Antal: {len(df_v)}")
sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    lead_popup(df_v.index[sel.selection.rows[0]])
