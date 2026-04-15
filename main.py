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
            res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
            if res[0] == 0:
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

# --- 3. SPROG-DATABASE (ALLE FELTER 1-35) ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i alt data...", "total_leads": "Viste leads: {n}", "click_info": "💡 Klik på rækken til venstre for at åbne kortet.",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Kampagne Filtre", "sidebar_admin": "🛠️ Admin Kontrol",
        "sidebar_user": "👤 Brugerstyring", "sidebar_export": "📤 Eksport", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "btn_delete": "🗑️ SLET LEAD", "tab1": "📞 Kontakt & Social",
        "tab2": "🌍 Geografi & Brancher", "tab3": "⚙️ Salg & Pipeline", "tab4": "📝 Beskrivelser", "tab5": "📁 Medier & Noter",
        "field_id": "Klient ID", "field_name": "Virksomhed (Legal)", "field_cif": "CIF / VAT", "field_person": "Kontaktperson", 
        "field_title": "Titel", "field_mail": "E-mail", "field_phone": "Kontor Tlf", "field_mobile": "Mobil", "field_wa": "WhatsApp", 
        "field_tg": "Telegram", "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Website URL", "field_reg": "Region", 
        "field_area": "Område", "field_town": "By", "field_addr": "Adresse", "field_zip": "Postnr.", "field_loc": "Google Maps Link", 
        "field_br": "Brancher", "field_ubr": "Underbrancher", "field_lang": "Sprog", "field_work": "Åbningstider", "field_st": "Status", 
        "field_mem": "Medlemskab", "field_adv": "Annonceprofil", "field_agent": "Agent", "field_src": "Kilde", "field_created": "Oprettet", 
        "field_follow": "Opfølgning", "field_last": "Sidste kontakt", "field_pitch": "Kort Pitch", "field_desc": "Lang Beskrivelse", 
        "field_qr": "QR Tracking URL", "field_notes": "Interne CRM Noter", "field_logo": "Logo", "field_docs": "Dokumenter", "field_gal": "Galleri", "field_type": "Område Type"
    },
    "🇬🇧 English": {
        "title": "Business CRM AI Pro", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search data...", "total_leads": "Leads: {n}", "click_info": "💡 Click row to open card.",
        "sidebar_scan": "📸 AI Scanner", "sidebar_filter": "🎯 Filters", "sidebar_admin": "🛠️ Admin",
        "sidebar_user": "👤 Users", "sidebar_export": "📤 Export", "btn_create": "➕ CREATE MANUAL",
        "btn_save": "💾 SAVE ALL", "btn_delete": "🗑️ DELETE", "tab1": "📞 Contact", "tab2": "🌍 Geo",
        "tab3": "⚙️ Sales", "tab4": "📝 Desc", "tab5": "📁 Media",
        "field_id": "Client ID", "field_name": "Company Name", "field_cif": "CIF / VAT", "field_person": "Contact Person",
        "field_title": "Title", "field_mail": "Email", "field_phone": "Office", "field_mobile": "Mobile", "field_wa": "WhatsApp",
        "field_tg": "Telegram", "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Website", "field_reg": "Region",
        "field_area": "Area", "field_town": "City", "field_addr": "Address", "field_zip": "Zip", "field_loc": "Maps Link",
        "field_br": "Industries", "field_ubr": "Sub-industries", "field_lang": "Languages", "field_work": "Work Hours",
        "field_st": "Status", "field_mem": "Membership", "field_adv": "Ad Profile", "field_agent": "Agent", "field_src": "Source",
        "field_created": "Created", "field_follow": "Follow up", "field_last": "Last contact", "field_pitch": "Pitch",
        "field_desc": "Description", "field_qr": "QR URL", "field_notes": "Internal Notes", "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Gallery", "field_type": "Area Type"
    },
    "🇪🇸 Español": {
        "title": "CRM Maestro AI Pro", "login_title": "Entrar", "login_btn": "ENTRAR", "logout": "🚪 Salir",
        "search": "🔍 Buscar...", "total_leads": "Leads: {n}", "click_info": "💡 Haga clic en la fila.",
        "sidebar_scan": "📸 Escáner IA", "sidebar_filter": "🎯 Filtros", "sidebar_admin": "🛠️ Admin",
        "sidebar_user": "👤 Usuarios", "sidebar_export": "📤 Exportar", "btn_create": "➕ CREAR MANUAL",
        "btn_save": "💾 GUARDAR", "btn_delete": "🗑️ ELIMINAR", "tab1": "📞 Contacto",
        "tab2": "🌍 Geografía", "tab3": "⚙️ Ventas", "tab4": "📝 Descripción", "tab5": "📁 Medios",
        "field_id": "ID Cliente", "field_name": "Empresa", "field_cif": "CIF / IVA", "field_person": "Contacto",
        "field_title": "Cargo", "field_mail": "Email", "field_phone": "Teléfono", "field_mobile": "Móvil", "field_wa": "WhatsApp",
        "field_tg": "Telegram", "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Web", "field_reg": "Región",
        "field_area": "Zona", "field_town": "Ciudad", "field_addr": "Dirección", "field_zip": "CP", "field_loc": "Maps",
        "field_br": "Sectores", "field_ubr": "Subsectores", "field_lang": "Idiomas", "field_work": "Horario",
        "field_st": "Estado", "field_mem": "Membresía", "field_adv": "Anuncio", "field_agent": "Agente", "field_src": "Origen",
        "field_created": "Creado", "field_follow": "Seguimiento", "field_last": "Fecha contacto", "field_pitch": "Resumen",
        "field_desc": "Descripción", "field_qr": "URL QR", "field_notes": "Notas", "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Galería", "field_type": "Tipo Zona"
    }
}

# --- 4. FASTLÅSTE DATA (SPANIEN & BRANCHER) ---
SPANISH_GEOGRAPHY = {
    "Sydspanien": ["Costa del Sol", "Costa de la Luz", "Costa Tropical", "Costa de Almería", "Andalusia", "Murcia"],
    "Østspanien": ["Costa Blanca", "Costa de Valencia", "Costa del Azahar", "Costa Cálida", "Catalonia", "Valencian Community"],
    "Nordspanien": ["Costa Brava", "Costa Dorada", "Costa del Maresme", "Costa del Garraf", "Costa Verde", "Costa Vasca", "Galicia", "Basque Country", "Asturias", "Cantabria", "Navarre", "La Rioja"],
    "Øer": ["Mallorca", "Ibiza", "Menorca", "Formentera", "Tenerife", "Gran Canaria", "Lanzarote"]
}

AREA_TYPES = ["coast", "island", "inland", "city_area"]

COSTA_DEL_SOL_TOWNS = {
    "Vest": ["Estepona", "Marbella", "Benahavís", "San Pedro de Alcántara", "Nueva Andalucía", "Puerto Banús", "Casares", "Manilva", "Sotogrande"],
    "Midt": ["Fuengirola", "Mijas", "Mijas Costa", "Benalmádena", "Torremolinos"],
    "Øst": ["Málaga", "Rincón de la Victoria", "Torre del Mar", "Vélez-Málaga", "Nerja", "Frigiliana"],
    "Andre": ["Alicante", "Torrevieja", "Valencia", "Barcelona", "Madrid", "Seville", "Murcia", "Benidorm", "Altea", "Calpe", "Denia", "Javea", "Cartagena", "Granada", "Bilbao", "Palma de Mallorca", "Ibiza"]
}

INDUSTRIES = {
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
    "Hverdagsliv": ["Supermarked", "Internet / telecom", "El / vand"],
    "Flytning & relocation": ["Flyttefirma", "Import af bil", "Pet relocation"]
}

# --- 5. SYSTEM MOTOR ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Område Type', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Data', 'Client ID'
]

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

# --- 6. AI SCANNER (GPT-4o) ---
def analyze_image_ai(image_bytes):
    b64 = base64.b64encode(image_bytes).decode('utf-8')
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "Extract: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address in JSON."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
            ]}], response_format={ "type": "json_object" }
        )
        return json.loads(res.choices[0].message.content)
    except: return {}

# --- 7. LOGIN LOGIK ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False
if "lang_choice" not in st.session_state: st.session_state.lang_choice = "🇩🇰 Dansk"

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
    L_log = TRANSLATIONS[st.session_state.lang_choice]
    st.title(L_log['login_title'])
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
    opts = {
        "agents": ["Brian", "Olga"], "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"], 
        "membership": ["BASIC", "VIP", "Premium", "Gold"], 
        "ad_profile": ["Standard", "Medium", "Proff.", "SoMe", "FB", "Google", "Website", "Webshop", "Landingpage"],
        "titles": ["CEO", "Ejer", "Manager", "Marketingchef"], "kilde": ["Inbound", "AI Scan", "Andet"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk"],
        "regions": [item for sublist in SPANISH_GEOGRAPHY.values() for item in sublist],
        "towns": [item for sublist in COSTA_DEL_SOL_TOWNS.values() for item in sublist]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM crm_configs", db_engine)
            for k in opts.keys():
                stored = df_opt[df_opt['type'] == k]['value'].tolist()
                if stored: opts[k] = sorted(list(set(opts[k] + stored)))
        except: pass
    return opts

opts = load_options()

# --- 8. POPUP KORT (DE 5 FANER) ---
@st.dialog("🎯 Klient Detaljer & CRM Board", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name') or 'Lead'}")
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}
    with t1:
        st.markdown(f"##### {L['tab1']}")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input(L['f_name'], value=row.get('Company Name'))
        upd['CIF Number VAT'] = ct2.text_input(L['f_cif'], value=row.get('CIF Number VAT'))
        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            for f, lab in [('Kontaktperson','f_person'), ('Email','f_mail'), ('Phone number','f_phone'), ('Mobilnr','f_mobile')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1_{f}_{idx}")
            upd['Titel'] = st.selectbox(L['f_title'], opts['titles'], index=opts['titles'].index(row['Titel']) if row['Titel'] in opts['titles'] else 0)
        with c2:
            for f, lab in [('WhatsApp','f_wa'), ('Telegram','f_tg'), ('Facebook','f_fb'), ('Instagram','f_ig'), ('Website','f_web')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1b_{f}_{idx}")
    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Område Type'] = c1.selectbox(L['f_type'], AREA_TYPES, index=AREA_TYPES.index(row['Område Type']) if row['Område Type'] in AREA_TYPES else 0)
            upd['Region'] = c1.selectbox(L['f_reg'], opts['regions'], index=opts['regions'].index(row['Region']) if row['Region'] in opts['regions'] else 0)
            upd['Town'] = c1.selectbox(L['f_town'], opts['towns'], index=opts['towns'].index(row['Town']) if row['Town'] in opts['towns'] else 0)
            for f, lab in [('Area','f_area'), ('Address','f_addr'), ('Postal Code','f_zip')]: upd[f] = c1.text_input(L[lab], value=row.get(f,''), key=f"f2_{f}_{idx}")
        with c2:
            upd['Brancher'] = c2.selectbox(L['f_br'], list(INDUSTRIES.keys()))
            sub_list = INDUSTRIES[upd['Brancher']]
            curr_ubr = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(c2.multiselect(L['f_ubr'], sub_list, default=[x for x in curr_ubr if x in sub_list]))
            curr_l = [x.strip() for x in str(row.get('Languages')).split(',')] if row.get('Languages') else []
            upd['Languages'] = ", ".join(c2.multiselect(L['f_lang'], opts['sprog'], default=[x for x in curr_l if x in opts['sprog']]))
            upd['Work time'] = c2.text_input(L['f_work'], value=row.get('Work time'))
    with t3:
        st.markdown(f"##### {L['tab3']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['f_st'], opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox(L['f_mem'], opts['membership'], index=opts['membership'].index(row.get('Membership')) if row.get('Membership') in opts['membership'] else 0)
            upd['Advertising'] = st.selectbox(L['f_adv'], opts['ad_profile'], index=opts['ad_profile'].index(row.get('Advertising')) if row.get('Advertising') in opts['ad_profile'] else 0)
        with c2:
            upd['Agent'] = st.selectbox(L['f_agent'], opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
            upd['Leadtype'] = st.selectbox(L['f_src'], opts['kilde'], index=opts['kilde'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['kilde'] else 0)
            for f, lab in [('Date created','f_created'), ('Date for follow up','f_follow'), ('Kontakt dato','f_last')]:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(L[lab], value=d_v, key=f"f3d_{f}_{idx}").strftime('%d/%m/%Y')
    with t4:
        st.markdown(f"##### {L['tab4']}")
        upd['Business Description'] = st.text_area(L['f_pitch'], value=row.get('Business Description'), height=100)
        upd['Description'] = st.text_area(L['f_desc'], value=row.get('Description'), height=250)
        upd['Tracking_URL'] = st.text_input(L['f_qr'], value=row.get('Tracking_URL'))
    with t5:
        st.markdown(f"##### {L['tab5']}")
        upd['Noter'] = st.text_area(L['f_notes'], value=row.get('Noter'), height=200)
        c1, c2 = st.columns(2)
        if l_up := c1.file_uploader(L['field_logo'], type=['png','jpg'], key=f"lu_{idx}"):
            upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        if f_up := c2.file_uploader(L['field_docs'], key=f"fu_{idx}"):
            upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
        st.file_uploader(L['field_gal'], accept_multiple_files=True)

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete'], type="secondary"):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 9. SIDEBAR ---
with st.sidebar:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    with st.expander(L['sidebar_scan']):
        if cam := st.camera_input("Scan Card"):
            with st.spinner("AI analyzing..."):
                ai_data = analyze_image_ai(cam.read())
                nr = {c: "" for c in MASTER_COLS}; nr.update(ai_data)
                nr['Date created'] = date.today().strftime('%d/%m/%Y')
                nr['Client ID'] = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()

    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            cat_list = ["agents", "status", "membership", "ad_profile", "kilde", "towns", "regions"]
            cat_ed = st.selectbox("Edit List:", cat_list)
            v_new = st.text_input("New Value:")
            if st.button("💾 Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat_ed, "v":v_new})
                st.rerun()
            st.divider()
            nu, np = st.text_input("New User"), st.text_input("New Pass", type="password")
            if st.button("Create Agent"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO users VALUES (:u,:p,'agent')"), {"u":nu,"p":np})
                st.success("OK")
            if st.button("🚨 Reset Database"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    st.header(L['sidebar_filter'])
    f_st = st.multiselect(L['f_st'], opts['status'])
    f_br = st.multiselect(L['f_br'], list(INDUSTRIES.keys()))
    f_re = st.multiselect(L['f_reg'], opts['regions'])

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nid = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = nid
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    st.download_button(L['sidebar_export'], st.session_state.df_leads.to_csv(index=False), "master.csv", use_container_width=True)
    if st.button(L['logout']): st.session_state.authenticated = False; st.rerun()

# --- 10. DASHBOARD ---
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
