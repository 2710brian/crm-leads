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

# --- 3. SPROG-DATABASE (KOMPLET OVERSÆTTELSE) ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i alt data...", "total_leads": "Viste leads: {n}", "click_info": "💡 Klik på rækken for at åbne klient-kortet.",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Kampagne Filtre", "sidebar_admin": "🛠️ Admin Kontrol",
        "sidebar_user": "👤 Brugerstyring", "sidebar_export": "📤 Eksport", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "btn_delete": "🗑️ SLET LEAD",
        "tab1": "📞 Kontakt & Social", "tab2": "🌍 Geografi & Brancher", "tab3": "⚙️ Salg & Pipeline",
        "tab4": "📝 Beskrivelser", "tab5": "📁 Medier & Noter",
        "f_id": "Client ID", "f_name": "Virksomhed (Legal)", "f_cif": "CIF / VAT", "f_person": "Kontaktperson", 
        "f_title": "Titel", "f_mail": "E-mail", "f_phone": "Kontor Tlf", "f_mobile": "Mobil", "f_wa": "WhatsApp", 
        "f_tg": "Telegram", "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Website URL", "f_reg": "Region", 
        "f_area": "Område", "f_town": "By", "f_addr": "Adresse", "f_zip": "Postnr.", "f_loc": "Google Maps Link", 
        "f_br": "Brancher", "f_ubr": "Underbrancher", "f_lang": "Sprog", "f_work": "Åbningstider", "f_st": "Aff. status", 
        "f_mem": "Medlemskab", "f_adv": "Annonceprofil", "f_agent": "Agent", "f_src": "Kilde", "f_created": "Dato oprettet", 
        "f_follow": "Opfølgningsdato", "f_last": "Kontakt dato", "f_pitch": "Kort Pitch", "f_desc": "Annoncetekst", 
        "f_qr": "QR Tracking URL", "f_notes": "Interne CRM Noter", "field_logo": "Logo", "field_docs": "Dokumenter", "field_gal": "Galleri", "f_type": "Område Type"
    },
    "🇬🇧 English": {
        "title": "Business Master CRM", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search...", "total_leads": "Total leads: {n}", "click_info": "💡 Click a row to open.",
        "sidebar_scan": "📸 AI Scanner", "sidebar_filter": "🎯 Filters", "sidebar_admin": "🛠️ Admin",
        "sidebar_user": "👤 Users", "sidebar_export": "📤 Export", "btn_create": "➕ CREATE MANUAL",
        "btn_save": "💾 SAVE CLIENT", "btn_delete": "🗑️ DELETE",
        "tab1": "📞 Contact", "tab2": "🌍 Geo", "tab3": "⚙️ Sales", "tab4": "📝 Desc", "tab5": "📁 Media",
        "f_id": "Client ID", "f_name": "Company", "f_cif": "CIF / VAT", "f_person": "Contact", 
        "f_title": "Title", "f_mail": "Email", "f_phone": "Office", "f_mobile": "Mobile", "f_wa": "WhatsApp", 
        "f_tg": "Telegram", "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Website", "f_reg": "Region", 
        "f_area": "Area", "f_town": "City", "f_addr": "Address", "f_zip": "Zip", "f_loc": "Maps", 
        "f_br": "Industry", "f_ubr": "Sub-industry", "f_lang": "Lang", "f_work": "Hours", "f_st": "Aff. Status", 
        "f_mem": "Membership", "f_adv": "Ad Profile", "f_agent": "Agent", "f_src": "Source", "f_created": "Created", 
        "f_follow": "Follow up", "f_last": "Contact date", "f_pitch": "Pitch", "f_desc": "Text", 
        "f_qr": "QR URL", "f_notes": "Notes", "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Gallery", "f_type": "Area Type"
    }
}

# --- 4. DATA LISTER (DE SPANSKE DATA DU HAR SENDT) ---
GEOGRAPHY_DATA = {
    "Sydspanien": ["Costa del Sol", "Costa de la Luz", "Costa Tropical", "Costa de Almería", "Andalusia", "Murcia"],
    "Østspanien": ["Costa Blanca", "Costa de Valencia", "Costa del Azahar", "Costa Cálida", "Catalonia", "Valencian Community"],
    "Nordspanien": ["Costa Brava", "Costa Dorada", "Costa del Maresme", "Costa del Garraf", "Costa Verde", "Costa Vasca", "Galicia", "Basque Country", "Asturias", "Cantabria", "Navarre", "La Rioja"],
    "Øer": ["Mallorca", "Ibiza", "Menorca", "Formentera", "Tenerife", "Gran Canaria", "Lanzarote"]
}

TOWNS_BY_COAST = {
    "Costa del Sol Vest": ["Estepona", "Marbella", "Benahavís", "San Pedro de Alcántara", "Nueva Andalucía", "Puerto Banús", "Casares", "Manilva", "Sotogrande"],
    "Costa del Sol Midt": ["Fuengirola", "Mijas", "Mijas Costa", "Benalmádena", "Torremolinos"],
    "Costa del Sol Øst": ["Málaga", "Rincón de la Victoria", "Torre del Mar", "Vélez-Málaga", "Nerja", "Frigiliana"],
    "Andre": ["Alicante", "Torrevieja", "Valencia", "Barcelona", "Madrid", "Seville", "Granada", "Ibiza", "Palma de Mallorca"]
}

INDUSTRIES = {
    "Ejendom": ["Køb bolig", "Sælge bolig", "Nybyggeri", "Investering", "Udlejning kort", "Udlejning lang"],
    "Turisme & ferie": ["Hoteller", "Ferieboliger", "Resorts", "Oplevelser", "Guidede ture"],
    "Transport": ["Biludlejning", "Luksusbiler", "Lufthavn transfer", "Leasing"],
    "Juridisk & rådgivning": ["Advokat", "Skatterådgivning", "NIE nummer", "Residency"],
    "Finans & bank": ["Boliglån", "Bank", "Valuta exchange", "Forsikring"],
    "Bolig & renovation": ["Byggefirma", "Renovering", "Interiør", "Møbler", "Pool & have"],
    "Service & drift": ["Rengøring", "Property management", "Nøgleservice", "Udlejning management"],
    "Sundhed & velvære": ["Hospital", "Læge", "Tandlæge", "Wellness"],
    "Uddannelse": ["Internationale skoler", "Sprogskoler"],
    "Lifestyle": ["Restauranter", "Golf", "Fitness", "Beach clubs"],
    "Hverdagsliv": ["Supermarked", "Internet", "El & vand"],
    "Flytning & relocation": ["Flyttefirma", "Bilimport", "Pet relocation"]
}

# --- 5. LOGIK MOTOR ---
MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Område Type', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Data', 'Gallery_Data', 'Client ID'
]

def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # Rens for NaT/None
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00', 'nan.0'], '')
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

# --- 6. AI & IMPORT MAPPING ---
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

def map_vcf_import(df):
    # Logik til at læse filen du sendte (contacts - Export VCF)
    mapping = {
        'Company': 'Company Name',
        'Email1': 'Email',
        'Telephone1': 'Phone number',
        'Mobile1': 'Mobilnr',
        'Website': 'Website',
        'Address1City1': 'Town',
        'Address1Street1': 'Address',
        'Address1Zip': 'Postal Code',
        'Note': 'Noter',
        'Name': 'Kontaktperson'
    }
    return df.rename(columns=mapping)

# --- 7. LOGIN ---
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
    opts = {
        "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"],
        "membership": ["BASIC", "VIP", "Premium", "Gold"],
        "advertising": ["Standard", "Medium", "Proff.", "SoMe", "FB", "Google", "Website", "Webshop", "Landingpage"],
        "kilde": ["Inbound", "AI Scan", "VCF Import"],
        "agents": ["Brian", "Olga"],
        "titles": ["CEO", "Ejer", "Manager"],
        "sprog": ["Dansk", "Engelsk", "Spansk"],
        "area_types": ["coast", "island", "inland", "city_area"],
        "regions": sorted([r for sub in GEOGRAPHY_DATA.values() for r in sub]),
        "towns": sorted([t for sub in TOWNS_BY_COAST.values() for t in sub])
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
@st.dialog("🎯 Client Details", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    
    # VIS KUNDE LOGO
    c_logo1, c_logo2 = st.columns([0.8, 0.2])
    with c_logo1: st.title(f"ID: {row.get('Client ID')} | {row.get('Company Name') or 'Lead'}")
    with c_logo2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=100)

    st.divider()
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}

    with t1:
        st.markdown("##### 🏢 Identification")
        c_i1, c_i2 = st.columns(2)
        upd['Company Name'] = c_i1.text_input(L['f_name'], value=row.get('Company Name'))
        upd['CIF Number VAT'] = c_i2.text_input(L['f_cif'], value=row.get('CIF Number VAT'))
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            for f, lab in [('Kontaktperson','f_person'), ('Titel','f_title'), ('Email','f_mail'), ('Phone number','f_phone'), ('Mobilnr','f_mobile')]:
                if f == 'Titel': upd[f] = st.selectbox(L[lab], opts['titles'], index=0)
                else: upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1_{f}_{idx}")
        with col2:
            for f, lab in [('WhatsApp','f_wa'), ('Telegram','f_tg'), ('Facebook','f_fb'), ('Instagram','f_ig'), ('Website','f_web')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1b_{f}_{idx}")

    with t2:
        st.markdown("##### 🌍 Geography & Industry")
        c1, c2 = st.columns(2)
        with c1:
            upd['Område Type'] = c1.selectbox(L['f_type'], opts['area_types'], index=0)
            upd['Region'] = c1.selectbox(L['f_reg'], opts['regions'], index=0)
            upd['Town'] = c1.selectbox(L['f_town'], opts['towns'], index=0)
            for f, lab in [('Address', 'f_addr'), ('Postal Code', 'f_zip')]: upd[f] = c1.text_input(L[lab], value=row.get(f,''), key=f"f2_{f}_{idx}")
        with c2:
            upd['Brancher'] = c2.selectbox(L['f_br'], sorted(list(INDUSTRIES.keys())))
            sub_l = INDUSTRIES.get(upd['Brancher'], [])
            curr_ubr = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(c2.multiselect(L['f_ubr'], sub_l, default=[x for x in curr_ubr if x in sub_l]))
            curr_l = [x.strip() for x in str(row.get('Languages')).split(',')] if row.get('Languages') else []
            upd['Languages'] = ", ".join(c2.multiselect(L['f_lang'], opts['sprog'], default=[x for x in curr_l if x in opts['sprog']]))
            upd['Work time'] = c2.text_input(L['f_work'], value=row.get('Work time'), key=f"f2w_{idx}")

    with t3:
        st.markdown("##### ⚙️ Pipeline")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['f_st'], opts['status'], index=0)
            upd['Membership'] = st.selectbox(L['f_mem'], opts['membership'], index=0)
            upd['Advertising'] = st.selectbox(L['f_adv'], opts['advertising'], index=0)
            upd['Tracking_URL'] = st.text_input(L['f_qr'], value=row.get('Tracking_URL'))
        with c2:
            upd['Agent'] = st.selectbox(L['f_agent'], opts['agents'], index=0)
            upd['Leadtype'] = st.selectbox(L['f_src'], opts['kilde'], index=0)
            for f, lab in [('Date created','f_created'), ('Date for follow up','f_follow'), ('Kontakt dato','f_last')]:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(L[lab], value=d_v, key=f"f3d_{f}_{idx}").strftime('%d/%m/%Y')

    with t4:
        upd['Business Description'] = st.text_area(L['f_pitch'], value=row.get('Business Description'), height=100)
        upd['Description'] = st.text_area(L['f_desc'], value=row.get('Description'), height=250)

    with t5:
        upd['Noter'] = st.text_area(L['f_notes'], value=row.get('Noter'), height=150)
        st.divider()
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            l_up = st.file_uploader(L['field_logo'], type=['png','jpg'], key=f"lu_{idx}")
            if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">👉 Hent dokument</a>', unsafe_allow_html=True)
        with col_m2:
            f_up = st.file_uploader(L['field_docs'], key=f"fu_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
        st.file_uploader(L['field_gal'], accept_multiple_files=True, key=f"ga_{idx}")

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete'], type="secondary", use_container_width=True):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 9. SIDEBAR ---
with st.sidebar:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan Card")
        if cam:
            with st.spinner("AI scan..."):
                ai_d = analyze_image_ai(cam.read())
                nr = {c: "" for c in MASTER_COLS}; nr.update(ai_d)
                nr['Date created'] = date.today().strftime('%d/%m/%Y')
                ids = pd.to_numeric(st.session_state.df_leads['Client ID'], errors='coerce').dropna()
                nr['Client ID'] = int(ids.max() + 1) if not ids.empty else 1001
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()

    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            cat_list = ["agents", "status", "membership", "advertising", "kilde", "titles", "sprog", "towns", "regions", "brancher", "underbrancher", "area_types"]
            cat_ed = st.selectbox("Rediger lister:", cat_list)
            v_new = st.text_input("Ny værdi:")
            if st.button("💾 Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat_ed, "v":v_new})
                st.rerun()
            if opts[cat_ed]:
                v_del = st.selectbox("Slet:", ["Vælg..."] + opts[cat_ed])
                if v_del != "Vælg..." and st.button("🗑️ Slet"):
                    with db_engine.begin() as conn: conn.execute(text("DELETE FROM crm_configs WHERE type=:t AND value=:v"), {"t":cat_ed, "v":v_del})
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
    
    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "master.csv", use_container_width=True)
    
    # DOWNLOAD SKABELON KNAP
    template = pd.DataFrame(columns=MASTER_COLS)
    st.download_button("📊 Download Skabelon (CSV)", template.to_csv(index=False), "crm_skabelon.csv", use_container_width=True)

    f_up = st.file_uploader("Flet ny fil (VCF Export understøttet)")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        # Tjek om det er VCF filen og map automatisk
        if 'Company' in nd.columns: nd = map_vcf_import(nd)
        st.session_state.df_leads = force_clean(pd.concat([st.session_state.df_leads, nd], ignore_index=True))
        save_db(st.session_state.df_leads); st.rerun()

# --- 10. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

# TABEL MED AUTO-OPEN
DISPLAY_COLS_WITH_ID = ['Client ID'] + DISPLAY_COLS[1:]
sel = st.dataframe(df_v[DISPLAY_COLS_WITH_ID], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
