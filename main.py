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
st.set_page_config(page_title="Business Master CRM AI", layout="wide", page_icon="🎯")

# --- 2. SPROG-DATABASE ---
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
        "field_qr": "QR Tracking URL", "field_notes": "Interne CRM Noter", "field_logo": "Logo", "field_docs": "Dokumenter", "field_gal": "Galleri"
    },
    "🇬🇧 English": {
        "title": "Business CRM AI", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search all data...", "total_leads": "Leads shown: {n}", "click_info": "💡 Click the row on the left to open the card.",
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
        "field_desc": "Long Description", "field_qr": "QR URL", "field_notes": "Internal Notes", "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Gallery"
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
        "field_desc": "Descripción", "field_qr": "URL QR", "field_notes": "Notas", "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Galería"
    }
}

# --- 3. FASTLÅSTE DATA (BYER, REGIONER, BRANCHER) ---
GEOGRAPHY = {
    "Andalusia": ["Estepona", "Marbella", "Benahavís", "San Pedro de Alcántara", "Nueva Andalucía", "Puerto Banús", "Casares", "Manilva", "Sotogrande", "Fuengirola", "Mijas", "Mijas Costa", "Benalmádena", "Torremolinos", "Málaga", "Rincón de la Victoria", "Torre del Mar", "Vélez-Málaga", "Nerja", "Frigiliana", "Granada", "Seville"],
    "Catalonia": ["Barcelona"],
    "Madrid": ["Madrid"],
    "Valencian Community": ["Alicante", "Torrevieja", "Valencia", "Benidorm", "Altea", "Calpe", "Denia", "Javea"],
    "Murcia": ["Murcia", "Cartagena"],
    "Balearic Islands": ["Palma de Mallorca", "Ibiza"],
    "Canary Islands": ["Tenerife", "Gran Canaria"],
    "Basque Country": ["Bilbao"], "Galicia": ["A Coruña"], "Castile and León": ["Valladolid"], "Castile-La Mancha": ["Toledo"], "Aragon": ["Zaragoza"], "Extremadura": ["Mérida"], "Asturias": ["Oviedo"], "Cantabria": ["Santander"], "Navarre": ["Pamplona"], "La Rioja": ["Logroño"]
}

INDUSTRIES = {
    "Ejendom": ["Køb bolig", "Sælge bolig", "Nybyggeri", "Investering", "Udlejning kort", "Udlejning lang"],
    "Turisme & ferie": ["Hoteller", "Ferieboliger", "Resorts", "Fly & transport", "Oplevelser"],
    "Transport": ["Biludlejning", "Luksusbiler", "Lufthavn transfer", "Leasing"],
    "Juridisk & rådgivning": ["Advokat", "Skatterådgivning", "NIE nummer", "Residency"],
    "Finans & bank": ["Boliglån", "Bank", "Valuta exchange", "Forsikring"],
    "Bolig & renovation": ["Byggefirma", "Renovering", "Interiør", "Møbler", "Pool / have"],
    "Service & drift": ["Rengøring", "Property management", "Nøgleservice"],
    "Sundhed & velvære": ["Hospital", "Læge", "Tandlæge", "Wellness"],
    "Uddannelse": ["Internationale skoler", "Sprogskoler"],
    "Lifestyle": ["Restauranter", "Golf", "Fitness", "Beach clubs"],
    "Hverdagsliv": ["Supermarked", "Internet", "El / vand"],
    "Flytning & relocation": ["Flyttefirma", "Import af bil", "Pet relocation"]
}

# --- 4. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return None
    if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, client_id INTEGER, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            if conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()[0] == 0:
                conn.execute(text("INSERT INTO users VALUES ('admin', :p, 'admin')"), {"p": os.getenv("APP_PASSWORD", "mgm2024")})
        return engine
    except: return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 5. LOGIN ---
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
    st.session_state.lang_choice = st.selectbox("🌐 Language", list(TRANSLATIONS.keys()))
    l_login = TRANSLATIONS[st.session_state.lang_choice]
    st.title(l_login['login_title'])
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("User", key="l_u")
        st.text_input("Password", type="password", key="l_p")
        st.button(l_login['login_btn'], type="primary", use_container_width=True, on_click=check_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang_choice]

# --- 6. MASTER LOGIK & TYPE-CHECK ---
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
    opts = {
        "agents": ["Brian", "Olga"], "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"], 
        "memberships": ["BASIC", "VIP", "Premium", "Gold"], 
        "advertising": ["Standard", "Medium", "Proff.", "SoMe", "FB", "Google", "Website", "Webshop", "Landingpage"],
        "lead_types": ["Inbound", "AI Scan", "Andet"], "titles": ["CEO", "Ejer", "Manager", "Marketingchef"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM crm_configs", db_engine)
            for key in ["agents", "status", "memberships", "advertising", "lead_types", "titles"]:
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: opts[key] = sorted(list(set(opts[key] + stored)))
        except: pass
    return opts

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename_map = {'Merchant': 'Company Name', 'Programnavn': 'Company Name'}
    df = df.rename(columns=rename_map)
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    # VIKTIGT: Hvis Client ID mangler i databasen, skaber vi den her for at undgå KeyError
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = ""
    return df[MASTER_COLS]

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

if 'df_leads' not in st.session_state:
    try:
        df = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
        st.session_state.df_leads = force_clean(df)
    except:
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

opts = load_options()

# --- 7. POPUP KORT ---
@st.dialog("🎯 CRM", width="large")
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
        col1, col2 = st.columns(2)
        with col1:
            for f, k in [('Kontaktperson','field_person'), ('Titel','field_title'), ('Email','field_mail'), ('Phone number','field_phone'), ('Mobilnr','field_mobile')]:
                upd[f] = st.text_input(L[k], value=row.get(f,''))
        with col2:
            for f, k in [('WhatsApp','field_wa'), ('Telegram','field_tg'), ('Facebook','field_fb'), ('Instagram','field_ig'), ('Website','field_web')]:
                upd[f] = st.text_input(L[k], value=row.get(f,''))
    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            r_list = sorted(list(GEOGRAPHY.keys()))
            upd['Region'] = c1.selectbox(L['field_reg'], r_list, index=r_list.index(row['Region']) if row['Region'] in r_list else 0)
            town_list = GEOGRAPHY[upd['Region']]
            upd['Town'] = c1.selectbox(L['field_town'], town_list, index=town_list.index(row['Town']) if row['Town'] in town_list else 0)
            for f, k in [('Area','field_area'), ('Address','field_addr')]: upd[f] = c1.text_input(L[k], value=row.get(f,''))
        with c2:
            ind_list = sorted(list(INDUSTRIES.keys()))
            upd['Brancher'] = c2.selectbox(L['field_br'], ind_list, index=ind_list.index(row['Brancher']) if row['Brancher'] in ind_list else 0)
            sub_list = INDUSTRIES[upd['Brancher']]
            curr_s = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(c2.multiselect(L['field_ubr'], sub_list, default=[x for x in curr_s if x in sub_list]))
    with t3:
        st.markdown(f"##### {L['tab3']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['field_st'], opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox(L['field_mem'], opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0)
            upd['Advertising'] = st.selectbox(L['field_adv'], opts['advertising'], index=opts['advertising'].index(row.get('Advertising')) if row.get('Advertising') in opts['advertising'] else 0)
        with c2:
            upd['Agent'] = st.selectbox(L['field_agent'], opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
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
        if save_db(st.session_state.df_leads): st.rerun()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.session_state.lang = st.selectbox("🌐 Sprog", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan")
        if cam:
            nr = {c: "" for c in MASTER_COLS}; nr['Company Name'] = f"AI Scan {datetime.now().strftime('%H:%M')}"
            nr['Date created'] = date.today().strftime('%d/%m/%Y')
            nr['Client ID'] = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
            st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
            save_db(st.session_state.df_leads); st.rerun()

    st.header(L['sidebar_filter'])
    f_st = st.multiselect(L['field_st'], opts['status'])
    f_br = st.multiselect(L['field_br'], list(INDUSTRIES.keys()))
    f_re = st.multiselect(L['field_reg'], list(GEOGRAPHY.keys()))

    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            cat = st.selectbox("Edit List:", ["agents", "status", "memberships", "advertising", "titles"])
            v_new = st.text_input("New Value:")
            if st.button("Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat, "v":v_new})
                st.rerun()
            if st.button("🚨 Reset Database"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

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
# VIKTIGT: Vi tvinger DISPLAY_COLS til at findes i visningen for at undgå KeyError
safe_display = [c for c in DISPLAY_COLS if c in df_v.columns]
sel = st.dataframe(df_v[safe_display], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    lead_popup(df_v.index[sel.selection.rows[0]])
