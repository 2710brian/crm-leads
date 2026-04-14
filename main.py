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

# --- 2. SPROG-DATABASE (ALLE FELTER OVERSAT) ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i alt data...", "total_leads": "Viste leads: {n}", "click_info": "💡 Klik på rækken til venstre for at åbne kortet.",
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
        "field_follow": "Opfølgning", "field_last": "Kontakt dato", "field_pitch": "Kort Pitch", "field_desc": "Lang Beskrivelse", 
        "field_qr": "QR Tracking URL", "field_notes": "Interne CRM Noter"
    },
    "🇬🇧 English": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search data...", "total_leads": "Leads: {n}", "click_info": "💡 Click row on the left to open.",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Filters", "sidebar_admin": "🛠️ Admin",
        "sidebar_user": "👤 Users", "sidebar_export": "📤 Export", "btn_create": "➕ CREATE MANUAL",
        "btn_save": "💾 SAVE ALL", "btn_delete": "🗑️ DELETE", "tab1": "📞 Contact & Social",
        "tab2": "🌍 Geo & Industry", "tab3": "⚙️ Sales & Pipeline", "tab4": "📝 Desc", "tab5": "📁 Media",
        "field_id": "Client ID", "field_name": "Company Name", "field_cif": "CIF / VAT", "field_person": "Contact Person",
        "field_title": "Title", "field_mail": "Email", "field_phone": "Office Phone", "field_mobile": "Mobile", "field_wa": "WhatsApp",
        "field_tg": "Telegram", "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Website", "field_reg": "Region",
        "field_area": "Area", "field_town": "City", "field_addr": "Address", "field_zip": "Zip", "field_loc": "Google Maps",
        "field_br": "Industries", "field_ubr": "Sub-industries", "field_lang": "Languages", "field_work": "Work time",
        "field_st": "Status", "field_mem": "Membership", "field_adv": "Ad Profile", "field_agent": "Agent", "field_src": "Source",
        "field_created": "Created", "field_follow": "Follow up", "field_last": "Contact date", "field_pitch": "Short Pitch",
        "field_desc": "Long Description", "field_qr": "QR URL", "field_notes": "Internal Notes"
    },
    "🇪🇸 Español": {
        "title": "CRM Maestro AI", "login_title": "Iniciar Sesión", "login_btn": "ENTRAR", "logout": "🚪 Salir",
        "search": "🔍 Buscar...", "total_leads": "Leads: {n}", "click_info": "💡 Click en la fila para abrir.",
        "sidebar_scan": "📸 Escáner IA", "sidebar_filter": "🎯 Filtros", "sidebar_admin": "🛠️ Admin",
        "sidebar_user": "👤 Usuarios", "sidebar_export": "📤 Exportar", "btn_create": "➕ CREAR MANUAL",
        "btn_save": "💾 GUARDAR", "btn_delete": "🗑️ ELIMINAR", "tab1": "📞 Contacto",
        "tab2": "🌍 Geo y Sector", "tab3": "⚙️ Ventas", "tab4": "📝 Descripción", "tab5": "📁 Medios",
        "field_id": "ID Cliente", "field_name": "Empresa", "field_cif": "CIF / IVA", "field_person": "Contacto",
        "field_title": "Cargo", "field_mail": "Email", "field_phone": "Teléfono", "field_mobile": "Móvil", "field_wa": "WhatsApp",
        "field_tg": "Telegram", "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Sitio Web", "field_reg": "Región",
        "field_area": "Zona", "field_town": "Ciudad", "field_addr": "Dirección", "field_zip": "CP", "field_loc": "Google Maps",
        "field_br": "Sectores", "field_ubr": "Subsectores", "field_lang": "Idiomas", "field_work": "Horario",
        "field_st": "Estado", "field_mem": "Membresía", "field_adv": "Perfil Anuncio", "field_agent": "Agente", "field_src": "Origen",
        "field_created": "Creado", "field_follow": "Seguimiento", "field_last": "Fecha contacto", "field_pitch": "Resumen",
        "field_desc": "Descripción", "field_qr": "URL QR", "field_notes": "Notas"
    }
}

# --- 3. DATABASE MOTOR & RELATIONEL STRUKTUR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return None
    if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, client_id INTEGER, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS regions (id TEXT PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS cities (id TEXT PRIMARY KEY, name TEXT, region_id TEXT, area TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS categories (id TEXT PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS subcategories (id SERIAL PRIMARY KEY, name TEXT, category_id TEXT)"))
            
            # SEED DATA FRA DIN JSON
            if conn.execute(text("SELECT COUNT(*) FROM regions")).fetchone()[0] == 0:
                # Regioner & Byer
                geo = {
                    "andalusia": ("Andalusia", [("marbella", "Marbella", "Costa del Sol West"), ("estepona", "Estepona", "Costa del Sol West"), ("malaga", "Málaga", "Costa del Sol East"), ("fuengirola", "Fuengirola", "Costa del Sol Central")]),
                    "valencian": ("Valencian Community", [("alicante", "Alicante", ""), ("valencia", "Valencia", "")]),
                    "balearic": ("Balearic Islands", [("palma", "Palma de Mallorca", ""), ("ibiza", "Ibiza", "")])
                }
                for rid, (rname, cities) in geo.items():
                    conn.execute(text("INSERT INTO regions VALUES (:id, :n)"), {"id": rid, "n": rname})
                    for cid, cname, area in cities:
                        conn.execute(text("INSERT INTO cities VALUES (:id, :n, :rid, :a)"), {"id": cid, "n": cname, "rid": rid, "a": area})
                
                # Kategorier & Underkategorier
                cats = {
                    "real_estate": ("Ejendom", ["Køb bolig", "Nybyggeri", "Investering"]),
                    "tourism": ("Turisme & ferie", ["Hoteller", "Oplevelser"]),
                    "construction": ("Bolig & renovation", ["Byggefirma", "Interiør", "Have"])
                }
                for cid, (cname, subs) in cats.items():
                    conn.execute(text("INSERT INTO categories VALUES (:id, :n)"), {"id": cid, "n": cname})
                    for s in subs:
                        conn.execute(text("INSERT INTO subcategories (name, category_id) VALUES (:n, :cid)"), {"n": s, "cid": cid})
        return engine
    except: return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 4. LOGIN ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False
if "lang_choice" not in st.session_state: st.session_state.lang_choice = "🇩🇰 Dansk"

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    r_u, r_p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
    if u == r_u and p == r_p:
        st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, "admin", u
    elif db_engine:
        with db_engine.connect() as conn:
            res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
            if res and res[0] == p:
                st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
                st.rerun()

if not st.session_state.authenticated:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    l_login = TRANSLATIONS[st.session_state.lang_choice]
    st.title(l_login['login_title'])
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("User", key="l_u")
        st.text_input("Password", type="password", key="l_p")
        st.button(l_login['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang_choice]

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

# --- 6. POPUP KORT (RELATIONEL LOGIK) ---
@st.dialog("🎯 Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"{L['labels']['field_id']}: {row.get('Client ID')} | {row.get('Company Name') or 'New'}")
    
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}

    with t1:
        st.markdown(f"##### {L['tab1']}")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input(L['field_name'], value=row.get('Company Name'))
        upd['CIF Number VAT'] = ct2.text_input(L['field_cif'], value=row.get('CIF Number VAT'))
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            for f, lab in [('Kontaktperson', 'field_person'), ('Titel', 'field_title'), ('Email', 'field_mail'), ('Mobilnr', 'field_mobile')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''))
        with col2:
            for f, lab in [('WhatsApp', 'field_wa'), ('Telegram', 'field_tg'), ('Facebook', 'field_fb'), ('Instagram', 'field_ig'), ('Website', 'field_web')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''))

    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            r_names = opts['regions']['name'].tolist()
            sel_r = st.selectbox(L['field_reg'], r_names, index=r_names.index(row['Region']) if row['Region'] in r_names else 0)
            upd['Region'] = sel_r
            rid = opts['regions'][opts['regions']['name'] == sel_r]['id'].values[0]
            c_names = opts['cities'][opts['cities']['region_id'] == rid]['name'].tolist()
            upd['Town'] = st.selectbox(L['field_town'], c_names, index=c_names.index(row['Town']) if row['Town'] in c_names else 0)
            upd['Area'] = st.text_input(L['field_area'], value=row.get('Area'))
        with c2:
            cat_names = opts['categories']['name'].tolist()
            sel_cat = st.selectbox(L['field_br'], cat_names, index=cat_names.index(row['Brancher']) if row['Brancher'] in cat_names else 0)
            upd['Brancher'] = sel_cat
            cid = opts['categories'][opts['categories']['name'] == sel_cat]['id'].values[0]
            s_names = opts['subcategories'][opts['subcategories']['category_id'] == cid]['name'].tolist()
            curr_s = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(st.multiselect(L['field_ubr'], s_names, default=[x for x in curr_s if x in s_names]))
            upd['Languages'] = st.text_input(L['field_lang'], value=row.get('Languages'))

    with t3:
        st.markdown(f"##### {L['tab3']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['field_st'], ["Ny", "Dialog", "Vundet", "Tabt"], index=0)
            upd['Membership'] = st.selectbox(L['field_mem'], ["Ingen", "BASIC", "VIP", "Premium", "Gold"], index=0)
            upd['Advertising'] = st.selectbox(L['field_adv'], ["Standard", "Proff.", "SoMe", "Google"], index=0)
        with c2:
            upd['Agent'] = st.selectbox(L['field_agent'], ["Brian", "Olga"], index=0)
            for f, lab in [('Date created', 'field_created'), ('Date for follow up', 'field_follow'), ('Kontakt dato', 'field_last')]:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(L[lab], value=d_v, key=f"d_{f}_{idx}").strftime('%d/%m/%Y')

    with t4:
        st.markdown(f"##### {L['tab4']}")
        upd['Business Description'] = st.text_area(L['field_pitch'], value=row.get('Business Description'))
        upd['Description'] = st.text_area(L['field_desc'], value=row.get('Description'), height=200)
        upd['Tracking_URL'] = st.text_input(L['field_qr'], value=row.get('Tracking_URL'))

    with t5:
        st.markdown(f"##### {L['tab5']}")
        upd['Noter'] = st.text_area(L['field_notes'], value=row.get('Noter'), height=200)
        c1, c2 = st.columns(2)
        if l_up := c1.file_uploader(L['field_logo'], type=['png','jpg']):
            upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        if f_up := c2.file_uploader(L['field_docs']):
            upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_leads(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete']):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_leads(st.session_state.df_leads); st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.session_state.lang = st.selectbox("🌐 Language", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan")
        if cam:
            nr = {c: "" for c in MASTER_COLS}
            nr['Company Name'] = f"AI Scan {datetime.now().strftime('%H:%M')}"
            nr['Client ID'] = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
            st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
            save_leads(st.session_state.df_leads); st.rerun()

    st.header(L['sidebar_filter'])
    f_st = st.multiselect(L['field_st'], ["Ny", "Dialog", "Vundet", "Tabt"])
    f_br = st.multiselect(L['field_br'], opts['categories']['name'].tolist())
    f_re = st.multiselect(L['field_reg'], opts['regions']['name'].tolist())

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        new_id = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = new_id
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_leads(st.session_state.df_leads); st.rerun()

# --- 8. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_br: df_v = df_v[df_v['Brancher'].isin(f_br)]
if f_re: df_v = df_v[df_v['Region'].isin(f_re)]
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(L['total_leads'].format(n=len(df_v)))
DISPLAY_ORDER = ['Client ID', 'Date created', 'Company Name', 'Region', 'Town', 'Status on lead']
sel = st.dataframe(df_v[DISPLAY_ORDER], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    lead_popup(df_v.index[sel.selection.rows[0]])
