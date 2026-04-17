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

def db_execute(query, params=None):
    try:
        with db_engine.begin() as conn: conn.execute(text(query), params or {})
        return True
    except: return False

# --- 3. SPROG-DATABASE ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i alt data...", "total_leads": "Viste leads: {n}", "sidebar_scan": "📸 AI Card Scanner",
        "sidebar_filter": "🎯 Kampagne Filtre", "sidebar_admin": "🛠️ Admin Kontrol", "sidebar_user": "👤 Brugerstyring",
        "sidebar_export": "📤 Eksport", "sidebar_import": "📥 Import / Flet", "sidebar_master": "📄 Hent Master Skabelon", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "btn_delete": "🗑️ SLET LEAD",
        "tab1": "📞 Kontakt & Social", "tab2": "🌍 Geografi & Brancher", "tab3": "⚙️ Salg & Pipeline",
        "tab4": "📝 Beskrivelser", "tab5": "📁 Medier & Noter",
        "f_id": "Client ID", "f_name": "Virksomhed", "f_cif": "CIF / VAT", "f_person": "Kontaktperson", "f_title": "Titel",
        "f_mail": "E-mail", "f_phone": "Tlf", "f_mobile": "Mobil", "f_wa": "WhatsApp", "f_tg": "Telegram",
        "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Website", "f_reg": "Region", "f_area": "Område",
        "f_town": "By", "f_addr": "Adresse", "f_zip": "Postnr", "f_loc": "Maps Link", "f_br": "Branche",
        "f_ubr": "Underbranche", "f_lang": "Sprog", "f_work": "Timer", "f_st": "Status", "f_mem": "Medlem",
        "f_adv": "Annonce", "f_agent": "Agent", "f_src": "Kilde", "f_created": "Oprettet", "f_follow": "Opfølgning",
        "f_last": "Sidste kontakt", "f_pitch": "Kort Pitch", "f_desc": "Tekst", "f_qr": "QR URL", "f_notes": "Noter",
        "field_logo": "Logo", "field_docs": "Dokumenter", "field_gal": "Galleri", "f_type": "Område Type"
    },
    "🇬🇧 English": {
        "title": "Business CRM AI Pro", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search...", "total_leads": "Leads: {n}", "sidebar_scan": "📸 AI Scanner",
        "sidebar_filter": "🎯 Filters", "sidebar_admin": "🛠️ Admin", "sidebar_user": "👤 Users",
        "sidebar_export": "📤 Export", "sidebar_import": "📥 Import", "sidebar_master": "📄 Get Template", "btn_create": "➕ CREATE MANUAL",
        "btn_save": "💾 SAVE ALL", "btn_delete": "🗑️ DELETE",
        "tab1": "📞 Contact", "tab2": "🌍 Geo", "tab3": "⚙️ Sales", "tab4": "📝 Desc", "tab5": "📁 Media",
        "f_id": "Client ID", "f_name": "Company", "f_cif": "CIF / VAT", "f_person": "Contact", "f_title": "Title",
        "f_mail": "Email", "f_phone": "Phone", "f_mobile": "Mobile", "f_wa": "WhatsApp", "f_tg": "Telegram",
        "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Website", "f_reg": "Region", "f_area": "Area",
        "f_town": "City", "f_addr": "Address", "f_zip": "Zip", "f_loc": "Maps", "f_br": "Industry",
        "f_ubr": "Sub-industry", "f_lang": "Lang", "f_work": "Hours", "f_st": "Status", "f_mem": "Member",
        "f_adv": "Ads", "f_agent": "Agent", "f_src": "Source", "f_created": "Created", "f_follow": "Follow up",
        "f_last": "Last contact", "f_pitch": "Pitch", "f_desc": "Description", "f_qr": "QR URL", "f_notes": "Notes",
        "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Gallery", "f_type": "Area Type"
    },
    "🇪🇸 Español": {
        "title": "Business CRM Maestro AI", "login_title": "CRM Acceso", "login_btn": "INICIAR SESIÓN", "logout": "🚪 Cerrar sesión",
        "search": "🔍 Buscar en todos los datos...", "total_leads": "Leads mostrados: {n}", "sidebar_scan": "📸 Escáner de Tarjetas AI",
        "sidebar_filter": "🎯 Filtros de Campaña", "sidebar_admin": "🛠️ Control de Admin", "sidebar_user": "👤 Gestión de Usuarios",
        "sidebar_export": "📤 Exportar", "sidebar_import": "📥 Importar / Fusionar", "sidebar_master": "📄 Obtener Plantilla Maestra", "btn_create": "➕ CREAR MANUALMENTE",
        "btn_save": "💾 GUARDAR TODO EN CLIENTE", "btn_delete": "🗑️ ELIMINAR LEAD",
        "tab1": "📞 Contacto y Social", "tab2": "🌍 Geografía e Industrias", "tab3": "⚙️ Ventas y Pipeline",
        "tab4": "📝 Descripciones", "tab5": "📁 Medios y Notas",
        "f_id": "ID de Cliente", "f_name": "Empresa", "f_cif": "CIF / IVA", "f_person": "Persona de Contacto", "f_title": "Título",
        "f_mail": "Correo electrónico", "f_phone": "Teléfono", "f_mobile": "Móvil", "f_wa": "WhatsApp", "f_tg": "Telegram",
        "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Sitio web", "f_reg": "Región", "f_area": "Área",
        "f_town": "Ciudad", "f_addr": "Dirección", "f_zip": "Código Postal", "f_loc": "Enlace de Mapas", "f_br": "Industria",
        "f_ubr": "Subindustria", "f_lang": "Idioma", "f_work": "Horas", "f_st": "Estado", "f_mem": "Miembro",
        "f_adv": "Anuncio", "f_agent": "Agente", "f_src": "Fuente", "f_created": "Creado", "f_follow": "Seguimiento",
        "f_last": "Último contacto", "f_pitch": "Pitch Corto", "f_desc": "Texto", "f_qr": "URL QR", "f_notes": "Notas",
        "field_logo": "Logo", "field_docs": "Documentos", "field_gal": "Galería", "f_type": "Tipo de Área"
    }
}

# --- 4. MASTER LISTER ---
GEOGRAPHY = {
    "Andalusia": ["Estepona", "Marbella", "Benahavís", "San Pedro de Alcántara", "Nueva Andalucía", "Puerto Banús", "Casares", "Manilva", "Sotogrande", "Fuengirola", "Mijas", "Mijas Costa", "Benalmádena", "Torremolinos", "Málaga", "Rincón de la Victoria", "Torre del Mar", "Vélez-Málaga", "Nerja", "Frigiliana", "Granada", "Seville"],
    "Catalonia": ["Barcelona"], "Madrid": ["Madrid"], "Valencian Community": ["Alicante", "Torrevieja", "Valencia", "Benidorm", "Altea", "Calpe", "Denia", "Javea"],
    "Murcia": ["Murcia", "Cartagena"], "Balearic Islands": ["Palma de Mallorca", "Ibiza"], "Canary Islands": ["Tenerife", "Gran Canaria"]
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
AREA_TYPES = ["coast", "island", "inland", "city_area"]

# --- 5. LOGIK MOTOR ---
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
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Data', 'Gallery_Data', 'Client ID'
]
DISPLAY_COLS = ['Client ID', 'Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename_map = {'Merchant': 'Company Name', 'Programnavn': 'Company Name', 'Aff. Status': 'Status on lead'}
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

def analyze_image_ai(image_bytes):
    b64 = base64.b64encode(image_bytes).decode('utf-8')
    try:
        res = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "Extract business info into JSON: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
            ]}], response_format={ "type": "json_object" }
        )
        return json.loads(res.choices[0].message.content)
    except: return {}

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

# Data
if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

def load_options():
    opts = {
        "agents": ["Brian", "Olga"], "status": ["Ny", "Dialog", "Vundet", "Tabt"], 
        "membership": ["BASIC", "VIP", "Premium", "Gold"], 
        "advertising": ["Standard", "Medium", "Proff.", "SoMe", "FB", "Google", "Website", "Webshop", "Landingpage"],
        "kilde": ["Inbound", "AI Scan", "Andet"], "titles": ["CEO", "Ejer", "Manager"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk"],
        "regions": sorted(list(GEOGRAPHY.keys())),
        "towns": sorted([t for sub in GEOGRAPHY.values() for t in sub]),
        "brancher": sorted(list(INDUSTRIES.keys())),
        "underbrancher": sorted([u for sub in INDUSTRIES.values() for u in sub]),
        "area_types": AREA_TYPES
    }
    custom = {k: [] for k in opts.keys()}
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM crm_configs", db_engine)
            for k in opts.keys():
                stored = df_opt[df_opt['type'] == k]['value'].tolist()
                if stored:
                    custom[k] = stored
                    opts[k] = sorted(list(set(opts[k] + stored)))
        except: pass
    return opts, custom

opts, custom_opts = load_options()

# --- 7. KLIENT KORT POPUP ---
@st.dialog("🎯 Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    
    col_l1, col_l2 = st.columns([0.8, 0.2])
    with col_l1: 
        upd_id = st.text_input(L['f_id'], value=row.get('Client ID', ''))
        st.title(f"ID: {upd_id} | {row.get('Company Name') or 'Lead'}")
    with col_l2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=100)

    st.divider()
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {'Client ID': upd_id}
    
    with t1:
        st.markdown(f"##### {L['tab1']}")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input(L['f_name'], value=row.get('Company Name'))
        upd['CIF Number VAT'] = ct2.text_input(L['f_cif'], value=row.get('CIF Number VAT'))
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            for f, lab in [('Kontaktperson', 'f_person'), ('Titel', 'f_title'), ('Email', 'f_mail'), ('Phone number', 'f_phone'), ('Mobilnr', 'f_mobile')]:
                if f == 'Titel': upd[f] = st.selectbox(L[lab], opts['titles'], index=opts['titles'].index(row[f]) if row[f] in opts['titles'] else 0)
                else: upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1_{f}_{idx}")
        with col2:
            for f, lab in [('WhatsApp', 'f_wa'), ('Telegram', 'f_tg'), ('Facebook', 'f_fb'), ('Instagram', 'f_ig'), ('Website', 'f_web')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1b_{f}_{idx}")
    
    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Område Type'] = c1.selectbox(L['f_type'], opts['area_types'], index=opts['area_types'].index(row['Område Type']) if row['Område Type'] in opts['area_types'] else 0)
            upd['Region'] = c1.selectbox(L['f_reg'], opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0)
            upd['Town'] = c1.selectbox(L['f_town'], opts['towns'], index=opts['towns'].index(row.get('Town')) if row.get('Town') in opts['towns'] else 0)
            for f, lab in [('Area','f_area'), ('Address', 'f_addr'), ('Postal Code', 'f_zip')]:
                upd[f] = c1.text_input(L[lab], value=row.get(f,''), key=f"f2_{f}_{idx}")
        with c2:
            upd['Brancher'] = c2.selectbox(L['f_br'], opts['brancher'], index=opts['brancher'].index(row.get('Brancher')) if row.get('Brancher') in opts['brancher'] else 0)
            curr_ubr = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(c2.multiselect(L['f_ubr'], opts['underbrancher'], default=[x for x in curr_ubr if x in opts['underbrancher']]))
            curr_l = [x.strip() for x in str(row.get('Languages')).split(',')] if row.get('Languages') else []
            upd['Languages'] = ", ".join(c2.multiselect(L['f_lang'], opts['sprog'], default=[x for x in curr_l if x in opts['sprog']]))
            upd['Work time'] = c2.text_input(L['f_work'], value=row.get('Work time'), key=f"f2w_{idx}")
    
    with t3:
        st.markdown(f"##### {L['tab3']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['f_st'], opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox(L['f_mem'], opts['membership'], index=opts['membership'].index(row.get('Membership')) if row.get('Membership') in opts['membership'] else 0)
            upd['Advertising'] = st.selectbox(L['f_adv'], opts['advertising'], index=opts['advertising'].index(row.get('Advertising')) if row.get('Advertising') in opts['advertising'] else 0)
            upd['Tracking_URL'] = st.text_input(L['f_qr'], value=row.get('Tracking_URL'))
        with c2:
            upd['Agent'] = st.selectbox(L['f_agent'], opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
            upd['Leadtype'] = st.selectbox(L['f_src'], opts['kilde'], index=opts['kilde'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['kilde'] else 0)
            for f, lab in [('Date created','f_created'), ('Date for follow up','f_follow'), ('Kontakt dato','f_last')]:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(L[lab], value=d_v, key=f"f3d_{f}_{idx}").strftime('%d/%m/%Y')
    
    with t4:
        st.markdown(f"##### {L['tab4']}")
        upd['Business Description'] = st.text_area(L['f_pitch'], value=row.get('Business Description'), height=100)
        upd['Description'] = st.text_area(L['f_desc'], height=250, value=row.get('Description'))

    with t5:
        st.markdown(f"##### {L['tab5']}")
        upd['Noter'] = st.text_area(L['f_notes'], value=row.get('Noter'), height=150)
        st.divider()
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown(f"##### {L['field_logo']}")
            if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=150)
            l_up = st.file_uploader(L['field_logo'], type=['png','jpg'], key=f"lu_{idx}")
            if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        with col_m2:
            st.markdown(f"##### {L['field_docs']}")
            if row.get('Fil_Data'):
                st.markdown(f"📄 **{row['Fil_Navn']}**")
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">👉 Hent fil</a>', unsafe_allow_html=True)
            f_up = st.file_uploader(L['field_docs'], key=f"fu_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
        st.divider()
        st.markdown(f"##### {L['field_gal']}")
        gal_up = st.file_uploader("Upload Galleri", accept_multiple_files=True, key=f"ga_{idx}")
        if gal_up:
            gal_list = []
            for f in gal_up: gal_list.append(base64.b64encode(f.read()).decode())
            upd['Gallery_Data'] = json.dumps(gal_list)
        if row.get('Gallery_Data'):
            imgs = json.loads(row['Gallery_Data'])
            g_cols = st.columns(3)
            for i, img in enumerate(imgs): g_cols[i % 3].image(f"data:image/png;base64,{img}", use_container_width=True)

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete'], type="secondary", use_container_width=True):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    # Import
    uploaded_file = st.file_uploader(L['sidebar_import'], type=['csv', 'xlsx'])
    if uploaded_file:
        new_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, force_clean(new_df)], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    
    st.download_button(L['sidebar_master'], pd.DataFrame(columns=MASTER_COLS).to_csv(index=False), "master_skabelon.csv", use_container_width=True)
    
    # AI SCANNER
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan Card")
        if cam:
            with st.spinner("AI scan..."):
                ai_data = analyze_image_ai(cam.read())
                nr = {c: "" for c in MASTER_COLS}; nr.update(ai_data)
                nr['Date created'] = date.today().strftime('%d/%m/%Y')
                nums = pd.to_numeric(st.session_state.df_leads['Client ID'], errors='coerce').dropna()
                nr['Client ID'] = int(nums.max() + 1) if not nums.empty else 1001
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()

    # ADMIN
    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            st.markdown("##### 👤 Agenter")
            nu, np = st.text_input("Name"), st.text_input("Pass", type="password")
            if st.button("Add Agent"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO users VALUES (:u,:p,'agent')"), {"u":nu,"p":np})
                st.success("OK")
            st.divider()
            cat_list = ["agents", "status", "membership", "advertising", "kilde", "titles", "sprog", "towns", "regions", "brancher", "underbrancher", "area_types"]
            cat_ed = st.selectbox("Rediger lister:", cat_list)
            v_new = st.text_input("Ny værdi:")
            if st.button("💾 Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat_ed, "v":v_new})
                st.rerun()
            options_to_show = opts[cat_ed]
            v_del = st.selectbox("Slet fra database:", ["Vælg..."] + options_to_show)
            if v_del != "Vælg..." and st.button("🗑️ Slet"):
                with db_engine.begin() as conn: conn.execute(text("DELETE FROM crm_configs WHERE type=:t AND value=:v"), {"t":cat_ed, "v":v_del})
                st.rerun()
            if st.button("🚨 Reset Database"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    st.header(L['sidebar_filter'])
    f_st = st.multiselect(L['f_st'], opts['status'])
    f_br = st.multiselect(L['f_br'], opts['brancher'])
    f_re = st.multiselect(L['f_reg'], opts['regions'])

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nums = pd.to_numeric(st.session_state.df_leads['Client ID'], errors='coerce').dropna()
        nid = int(nums.max() + 1) if not nums.empty else 1001
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = nid
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    if st.button(L['logout'] if 'logout' in L else "Log ud"): st.session_state.authenticated = False; st.rerun()

# --- 9. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_br: df_v = df_v[df_v['Brancher'].isin(f_br)]
if f_re: df_v = df_v[df_v['Region'].isin(f_re)]
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(L['total_leads'].format(n=len(df_v)))

# Knapper til bulk-handlinger
col_b1, col_b2, _ = st.columns([0.5, 0.5, 9])

# Forbered visning
df_display = df_v[DISPLAY_COLS].copy()
df_display.insert(0, "Select", False)

# Brug st.data_editor
# Vi aktiverer 'selection_mode="single"' for at fange klik på rækken
edited_df = st.data_editor(
    df_display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Select": st.column_config.CheckboxColumn("Vælg", help="Vælg til bulk slet/download"),
        "Company Name": st.column_config.TextColumn(f"🔗 {L['f_name']}", help="Klik på navnet for at åbne lead-kortet")
    },
    disabled=[c for c in DISPLAY_COLS],
    key="data_editor_v9"
)

# 1. Find rækker valgt til bulk-handlinger (Select)
selected_bulk = edited_df.index[edited_df["Select"]].tolist()

# 2. Find rækken der skal åbnes (Vi bruger 'selection' i session_state)
# Dette fanger når brugeren klikker på en række
rows_clicked = []
if 'data_editor_v9' in st.session_state and 'selection' in st.session_state.data_editor_v9:
    rows_clicked = st.session_state.data_editor_v9['selection'].get('rows', [])

# Bulk Slet
if col_b1.button("🗑️") and selected_bulk:
    st.session_state.df_leads = st.session_state.df_leads.drop(df_v.iloc[selected_bulk].index)
    save_db(st.session_state.df_leads); st.rerun()

# Bulk Download
if selected_bulk:
    csv_data = df_v.iloc[selected_bulk].to_csv(index=False).encode('utf-8')
    col_b2.download_button("📥", data=csv_data, file_name=f"valgte_leads_{date.today()}.csv", mime="text/csv")
else:
    col_b2.button("📥", disabled=True)

# Åbn popup hvis en række er klikket på
if rows_clicked:
    # Vi åbner lead-kortet for den klikkede række
    idx_to_open = df_v.index[rows_clicked[0]]
    lead_popup(idx_to_open)
