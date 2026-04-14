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
st.set_page_config(page_title="Enterprise CRM Master AI", layout="wide", page_icon="🎯")

# --- 2. DATABASE MOTOR & STRUKTURERING ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return None
    if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.begin() as conn:
            # Hovedtabeller
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, client_id INTEGER, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            
            # Relationelle hjælpetabeller
            conn.execute(text("CREATE TABLE IF NOT EXISTS regions (id TEXT PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS cities (id TEXT PRIMARY KEY, name TEXT, region_id TEXT, area TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS categories (id TEXT PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS subcategories (id SERIAL PRIMARY KEY, name TEXT, category_id TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))

            # SEED DATA (Indsæt din JSON struktur hvis tabellerne er tomme)
            check = conn.execute(text("SELECT COUNT(*) FROM regions")).fetchone()[0]
            if check == 0:
                # Regioner & Byer
                geo_data = {
                    "andalusia": ("Andalusia", [("marbella", "Marbella", "Costa del Sol West"), ("estepona", "Estepona", "Costa del Sol West"), ("malaga", "Málaga", "Costa del Sol East"), ("fuengirola", "Fuengirola", "Costa del Sol Central")]),
                    "valencian": ("Valencian Community", [("alicante", "Alicante", ""), ("valencia", "Valencia", "")]),
                    "madrid": ("Madrid", [("madrid_city", "Madrid", "")]),
                }
                for rid, (rname, cities) in geo_data.items():
                    conn.execute(text("INSERT INTO regions (id, name) VALUES (:id, :n)"), {"id": rid, "n": rname})
                    for cid, cname, area in cities:
                        conn.execute(text("INSERT INTO cities (id, name, region_id, area) VALUES (:id, :n, :rid, :a)"), {"id": cid, "n": cname, "rid": rid, "a": area})
                
                # Kategorier
                cat_data = {
                    "real_estate": ("Ejendom", ["Køb bolig", "Nybyggeri", "Investering"]),
                    "tourism": ("Turisme & ferie", ["Hoteller", "Oplevelser"]),
                    "lifestyle": ("Lifestyle", ["Restauranter", "Golf"])
                }
                for cid, (cname, subs) in cat_data.items():
                    conn.execute(text("INSERT INTO categories (id, name) VALUES (:id, :n)"), {"id": cid, "n": cname})
                    for s in subs:
                        conn.execute(text("INSERT INTO subcategories (name, category_id) VALUES (:n, :cid)"), {"n": s, "cid": cid})
            
            # Admin setup
            res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
            if res[0] == 0:
                conn.execute(text("INSERT INTO users VALUES ('admin', 'mgm2024', 'admin')"))
        return engine
    except Exception as e:
        st.error(f"DB Error: {e}")
        return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 3. SPROG-DATABASE ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "login_title": "Enterprise CRM Login", "login_btn": "LOG IND", "search": "Søg i database...",
        "sidebar_scan": "📸 AI Visitkort Scanner", "sidebar_filter": "🎯 Filtre", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "tab1": "📞 Kontakt", "tab2": "🌍 Geografi & Branche",
        "labels": {"field_id": "Klient ID", "field_name": "Virksomhed", "field_reg": "Region", "field_town": "By", "field_cat": "Hovedkategori"}
    },
    "🇬🇧 English": {
        "login_title": "Enterprise CRM Login", "login_btn": "LOG IN", "search": "Search database...",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Filters", "btn_create": "➕ CREATE MANUAL",
        "btn_save": "💾 SAVE ALL CHANGES", "tab1": "📞 Contact", "tab2": "🌍 Geography",
        "labels": {"field_id": "Client ID", "field_name": "Company", "field_reg": "Region", "field_town": "City", "field_cat": "Category"}
    }
}

# --- 4. LOGIN LOGIK ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False
if "lang" not in st.session_state: st.session_state.lang = "🇩🇰 Dansk"

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    if db_engine:
        with db_engine.connect() as conn:
            res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
            if res and res[0] == p:
                st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
                st.rerun()

if not st.session_state.authenticated:
    st.session_state.lang = st.selectbox("🌐 Language", list(TRANSLATIONS.keys()))
    L = TRANSLATIONS[st.session_state.lang]
    st.title(L['login_title'])
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("User", key="l_u")
        st.text_input("Password", type="password", key="l_p")
        st.button(L['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang]

# --- 5. DATA LOGIK ---
MASTER_COLS = [
    'Client ID', 'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data'
]

def load_options():
    with db_engine.connect() as conn:
        regions = pd.read_sql("SELECT * FROM regions", conn)
        cities = pd.read_sql("SELECT * FROM cities", conn)
        categories = pd.read_sql("SELECT * FROM categories", conn)
        subcategories = pd.read_sql("SELECT * FROM subcategories", conn)
        
        # Brugerdefinerede valg (Agenter, Status osv)
        configs = pd.read_sql("SELECT * FROM crm_configs", conn)
        
    return {"regions": regions, "cities": cities, "categories": categories, "subcategories": subcategories, "configs": configs}

def save_leads(df):
    if db_engine:
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

# Load Leads
if 'df_leads' not in st.session_state:
    try: 
        st.session_state.df_leads = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
    except: 
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

opts = load_options()

# --- 6. POPUP KORT (RELATIONEL LOGIK) ---
@st.dialog("🎯 Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"ID: {row.get('Client ID')} | {row.get('Company Name') or 'New'}")
    
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], "⚙️ Pipeline", "📝 Desc", "📁 Media"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        upd['Company Name'] = c1.text_input("Legal Name", value=row.get('Company Name'))
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
            # RELATIONEL GEOGRAFI
            reg_names = opts['regions']['name'].tolist()
            sel_reg = st.selectbox("Region", reg_names, index=reg_names.index(row['Region']) if row['Region'] in reg_names else 0)
            upd['Region'] = sel_reg
            
            # Filter byer baseret på region
            region_id = opts['regions'][opts['regions']['name'] == sel_reg]['id'].values[0]
            filtered_cities = opts['cities'][opts['cities']['region_id'] == region_id]
            upd['Town'] = st.selectbox("City", filtered_cities['name'].tolist())
            upd['Area'] = st.text_input("Area", value=row.get('Area'))
        
        with c2:
            # RELATIONEL BRANCHE
            cat_names = opts['categories']['name'].tolist()
            sel_cat = st.selectbox("Hovedbranche", cat_names, index=cat_names.index(row['Brancher']) if row['Brancher'] in cat_names else 0)
            upd['Brancher'] = sel_cat
            
            cat_id = opts['categories'][opts['categories']['name'] == sel_cat]['id'].values[0]
            filtered_subs = opts['subcategories'][opts['subcategories']['category_id'] == cat_id]
            upd['Underbrancher'] = ", ".join(st.multiselect("Underservices", filtered_subs['name'].tolist()))

    with t3:
        c1, c2 = st.columns(2)
        upd['Status on lead'] = c1.selectbox("Status", ["Ny", "Dialog", "Vundet", "Tabt"], index=0)
        upd['Agent'] = c1.selectbox("Agent", ["Brian", "Olga"], index=0)
        upd['Date for follow up'] = c2.date_input("Follow up", value=date.today()).strftime('%d/%m/%Y')

    with t5:
        upd['Noter'] = st.text_area("Notes", value=row.get('Noter'), height=200)

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_leads(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("💼 CRM Master")
    
    # AI SCANNER (GPT-4o)
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan")
        if cam:
            with st.spinner("AI Analysis..."):
                # OpenAI Logic Here
                nr = {c: "" for c in MASTER_COLS}
                nr['Company Name'] = "AI Scanned Company"
                nr['Client ID'] = int(st.session_state.df_leads['Client ID'].max() or 1000) + 1
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_leads(st.session_state.df_leads); st.rerun()

    if st.button(L['btn_create'], type="primary", use_container_width=True):
        new_id = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = new_id
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_leads(st.session_state.df_leads); st.rerun()

    if st.button("🚨 Reset Database"):
        with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. DASHBOARD ---
st.title("💼 Business Master Workspace")
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    lead_popup(df_v.index[sel.selection.rows[0]])
