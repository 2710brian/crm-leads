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
            # Sikrer admin bruger
            res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
            if res[0] == 0:
                u, p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
                conn.execute(text("INSERT INTO users VALUES (:u, :p, 'admin')"), {"u": u, "p": p})
        return engine
    except: return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# SIKKER DATABASE-FUNKTION TIL ADMIN
def db_execute(query, params=None):
    try:
        with db_engine.begin() as conn: conn.execute(text(query), params or {})
        return True
    except: return False

# --- 3. SPROG-DATABASE (ALLE 35+ FELTER) ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "title": "Business CRM Master AI", "login_title": "CRM Login", "login_btn": "LOG IND", "logout": "🚪 Log ud",
        "search": "🔍 Søg i alt data...", "total_leads": "Viste leads: {n}", "click_info": "💡 Klik på ✏️ for at åbne kortet. Marker for bulk handlinger.",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Kampagne Filtre", "sidebar_admin": "🛠️ Admin Kontrol",
        "sidebar_user": "👤 Brugerstyring", "sidebar_export": "📤 Eksport", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "btn_delete": "🗑️ SLET LEAD", "btn_bulk_delete": "🗑️ SLET VALGTE",
        "tab1": "📞 Kontakt & Social", "tab2": "🌍 Geografi & Brancher", "tab3": "⚙️ Salg & Pipeline",
        "tab4": "📝 Beskrivelser", "tab5": "📁 Medier & Noter",
        "f_id": "Klient ID", "f_name": "Virksomhed (Legal)", "f_cif": "CIF / VAT", "f_person": "Kontaktperson", 
        "f_title": "Titel", "f_mail": "E-mail", "f_phone": "Kontor Tlf", "f_mobile": "Mobil", "f_wa": "WhatsApp", 
        "f_tg": "Telegram", "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Website URL", "f_reg": "Region", 
        "f_area": "Område", "f_town": "By", "f_addr": "Adresse", "f_zip": "Postnr.", "f_loc": "Google Maps Link", 
        "f_br": "Brancher", "f_ubr": "Underbrancher", "f_lang": "Sprog", "f_work": "Åbningstider", "f_st": "Status", 
        "f_mem": "Medlemskab", "f_adv": "Annonceprofil", "f_agent": "Agent", "f_src": "Kilde", "f_created": "Oprettet", 
        "f_follow": "Opfølgning", "f_last": "Sidste kontakt", "f_pitch": "Kort Pitch", "f_desc": "Lang Beskrivelse", 
        "f_qr": "QR Tracking URL", "f_notes": "Interne CRM Noter", "field_logo": "Logo", "field_docs": "Dokumenter", "field_gal": "Galleri"
    },
    "🇬🇧 English": {
        "title": "Business CRM AI Pro", "login_title": "CRM Login", "login_btn": "LOG IN", "logout": "🚪 Log out",
        "search": "🔍 Search...", "total_leads": "Leads: {n}", "click_info": "💡 Click ✏️ to open card.",
        "sidebar_scan": "📸 AI Scanner", "sidebar_filter": "🎯 Filters", "sidebar_admin": "🛠️ Admin",
        "sidebar_user": "👤 Users", "sidebar_export": "📤 Export", "btn_create": "➕ CREATE MANUAL",
        "btn_save": "💾 SAVE ALL", "btn_delete": "🗑️ DELETE", "btn_bulk_delete": "🗑️ DELETE SELECTED",
        "tab1": "📞 Contact", "tab2": "🌍 Geo", "tab3": "⚙️ Sales", "tab4": "📝 Desc", "tab5": "📁 Media",
        "f_id": "Client ID", "f_name": "Company", "f_cif": "CIF / VAT", "f_person": "Contact", 
        "f_title": "Title", "f_mail": "Email", "f_phone": "Office", "f_mobile": "Mobile", "f_wa": "WhatsApp", 
        "f_tg": "Telegram", "f_fb": "Facebook", "f_ig": "Instagram", "f_web": "Website", "f_reg": "Region", 
        "f_area": "Area", "f_town": "City", "f_addr": "Address", "f_zip": "Zip", "f_loc": "Maps Link", 
        "f_br": "Industry", "f_ubr": "Sub-industry", "f_lang": "Lang", "f_work": "Hours", "f_st": "Status", 
        "f_mem": "Member", "f_adv": "Ads", "f_agent": "Agent", "f_src": "Source", "f_created": "Created", 
        "f_follow": "Follow up", "f_last": "Last contact", "f_pitch": "Pitch", "f_desc": "Long Desc", 
        "f_qr": "QR URL", "f_notes": "Notes", "field_logo": "Logo", "field_docs": "Docs", "field_gal": "Gallery"
    }
}

# --- 4. FASTLÅSTE DATA LISTER (GEOGRAFI & BRANCHER) ---
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

AREAS = ["Costa del Sol Vest", "Costa del Sol Midt", "Costa del Sol Øst", "Costa Blanca", "Costa Brava", "Madrid City", "Barcelona City", "Inland"]

# --- 5. SYSTEM FUNKTIONER ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename_map = {'Merchant': 'Company Name', 'Programnavn': 'Company Name'}
    df = df.rename(columns=rename_map)
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = False if c == '✏️' else ""
    return df[MASTER_COLS]

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Company Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

def load_options():
    opts = {
        "agents": ["Brian", "Olga"], "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"], 
        "membership": ["BASIC", "VIP", "Premium", "Gold"], 
        "ad_profile": ["Standard", "Medium", "Proff.", "SoMe", "FB", "Google", "Website", "Webshop", "Landingpage"],
        "titles": ["CEO", "Ejer", "Manager", "Marketingchef"], "kilde": ["Inbound", "AI Scan", "Andet"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk"]
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

# --- 6. AI SCANNER ---
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

# --- 8. MASTER DATA ---
MASTER_COLS = [
    '✏️', 'Client ID', 'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Data'
]
DISPLAY_COLS = ['✏️', 'Client ID', 'Date created', 'Company Name', 'Brancher', 'Region', 'Town', 'Status on lead', 'Agent']

if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts, custom_opts = load_options()

# --- 9. KLIENT KORT POPUP (ALT ER GENOPRETTET) ---
@st.dialog("🎯 Client Card", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"ID: {row.get('Client ID')} | {row.get('Company Name') or 'Lead'}")
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}
    with t1:
        c_id1, c_id2 = st.columns([1, 3])
        upd['Client ID'] = c_id1.text_input(L['f_id'], value=row.get('Client ID'))
        upd['CIF Number VAT'] = c_id2.text_input(L['f_cif'], value=row.get('CIF Number VAT'))
        st.divider()
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input(L['f_name'], value=row.get('Company Name'))
        upd['Email'] = ct2.text_input(L['f_mail'], value=row.get('Email'))
        col1, col2 = st.columns(2)
        with col1:
            for f, lab in [('Kontaktperson','f_person'), ('Phone number','f_phone'), ('Mobilnr','f_mobile')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1_{f}_{idx}")
            upd['Titel'] = st.selectbox(L['f_title'], opts['titles'], index=opts['titles'].index(row['Titel']) if row['Titel'] in opts['titles'] else 0)
        with col2:
            for f, lab in [('WhatsApp','f_wa'), ('Telegram','f_tg'), ('Facebook','f_fb'), ('Instagram','f_ig'), ('Website','f_web')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"f1b_{f}_{idx}")
    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            r_list = sorted(list(GEOGRAPHY.keys()))
            upd['Region'] = c1.selectbox(L['f_reg'], r_list, index=r_list.index(row['Region']) if row['Region'] in r_list else 0)
            town_list = GEOGRAPHY[upd['Region']]
            upd['Town'] = c1.selectbox(L['f_town'], town_list, index=town_list.index(row['Town']) if row['Town'] in town_list else 0)
            upd['Area'] = c1.selectbox(L['f_area'], AREAS, index=AREAS.index(row['Area']) if row['Area'] in AREAS else 0)
            for f, lab in [('Address','f_addr'), ('Postal Code','f_zip'), ('Exact Location','f_loc')]:
                upd[f] = c1.text_input(L[lab], value=row.get(f,''), key=f"f2_{f}_{idx}")
        with c2:
            ind_list = sorted(list(INDUSTRIES.keys()))
            upd['Brancher'] = c2.selectbox(L['f_br'], ind_list, index=ind_list.index(row['Brancher']) if row['Brancher'] in ind_list else 0)
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
                upd[f] = st.date_input(L[lab], value=d_v, key=f"d_{f}_{idx}").strftime('%d/%m/%Y')
    with t4:
        upd['Business Description'] = st.text_area(L['f_pitch'], value=row.get('Business Description'))
        upd['Description'] = st.text_area(L['f_desc'], value=row.get('Description'), height=200)
        upd['Tracking_URL'] = st.text_input(L['f_qr'], value=row.get('Tracking_URL'))
    with t5:
        upd['Noter'] = st.text_area(L['f_notes'], value=row.get('Noter'), height=200)
        c1, c2 = st.columns(2)
        if l_up := c1.file_uploader(L['field_logo'], type=['png','jpg'], key=f"lu_{idx}"):
            upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        if f_up := c2.file_uploader(L['field_docs'], key=f"fu_{idx}"):
            upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
        st.file_uploader(L['field_gal'], accept_multiple_files=True, key=f"ga_{idx}")

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        upd['✏️'] = False
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['btn_delete'], type="secondary", use_container_width=True):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 10. SIDEBAR ---
with st.sidebar:
    st.session_state.lang_choice = st.selectbox("🌐 Sprog", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    # AI SCANNER
    with st.expander(L['sidebar_scan']):
        cam = st.camera_input("Scan Card")
        if cam:
            nr = {c: "" for c in MASTER_COLS}; nr['Company Name'] = f"AI Scan {datetime.now().strftime('%H:%M')}"
            nr['Date created'] = date.today().strftime('%d/%m/%Y')
            nr['Client ID'] = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
            st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
            save_db(st.session_state.df_leads); st.rerun()

    # ADMIN
    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            # BRUGERSTYRING
            st.markdown("##### Opret Agent")
            nu, np = st.text_input("Navn", key="nu"), st.text_input("Kode", type="password", key="np")
            if st.button("Opret Agent"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO users VALUES (:u,:p,'agent')"), {"u":nu,"p":np})
                st.success("OK")
            st.divider()
            # LISTER
            cat_list = ["agents", "status", "membership", "ad_profile", "kilde", "titles", "sprog"]
            cat_ed = st.selectbox("Rediger lister:", cat_list)
            v_new = st.text_input("New Value:")
            if st.button("💾 Add"):
                with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t,:v)"), {"t":cat_ed, "v":v_new})
                st.rerun()
            if custom_opts[cat_ed]:
                v_del = st.selectbox("Delete from DB:", ["Select..."] + custom_opts[cat_ed])
                if v_del != "Select..." and st.button("🗑️ Delete"):
                    with db_engine.begin() as conn: conn.execute(text("DELETE FROM crm_configs WHERE type=:t AND value=:v"), {"t":cat_ed, "v":v_del})
                    st.rerun()
            if st.button("🚨 Reset Database"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    # FILTRE
    st.header(L['sidebar_filter'])
    f_st = st.multiselect(L['f_st'], opts['status'])
    f_br = st.multiselect(L['f_br'], list(INDUSTRIES.keys()))
    f_re = st.multiselect(L['f_reg'], list(GEOGRAPHY.keys()))

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nid = int(st.session_state.df_leads['Client ID'].replace('',0).astype(float).max() or 1000) + 1
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Client ID'] = nid
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    st.download_button(L['sidebar_export'], st.session_state.df_leads.to_csv(index=False), "master.csv", use_container_width=True)
    if st.button(L['logout']): st.session_state.authenticated = False; st.rerun()

# --- 11. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()

if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_br: df_v = df_v[df_v['Brancher'].isin(f_br)]
if f_re: df_v = df_v[df_v['Region'].isin(f_re)]
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

# ACTION BAR
if 'sel_rows' in st.session_state and len(st.session_state.sel_rows) > 0:
    c_btn1, c_btn2 = st.columns([1, 4])
    if st.session_state.user_role == "admin":
        if c_btn1.button(L['btn_bulk_delete'], type="secondary", use_container_width=True):
            st.session_state.df_leads = st.session_state.df_leads.drop(df_v.index[st.session_state.sel_rows])
            save_db(st.session_state.df_leads); st.rerun()
    sel_data = df_v.iloc[st.session_state.sel_rows]
    c_btn2.download_button(f"📥 Download ({len(st.session_state.sel_rows)})", sel_data.to_csv(index=False), "selected.csv")

# TABEL MED BLYANT (✏️)
st.write(L['total_leads'].format(n=len(df_v)))
# Tvinger DISPLAY_COLS til at eksistere
safe_display = [c for c in DISPLAY_COLS if c in df_v.columns]
edited_df = st.data_editor(
    df_v[safe_display], 
    column_config={"✏️": st.column_config.CheckboxColumn("Open", default=False)},
    use_container_width=True, height=600, key="editor"
)

# TJEK OM BLYANT KLIKKES
clicked = edited_df[edited_df['✏️'] == True]
if not clicked.empty:
    real_idx = clicked.index[0]
    st.session_state.df_leads.at[real_idx, '✏️'] = False
    lead_popup(real_idx)

# BULK SELECTION (FLUEBEN TIL VENSTRE)
st.info("💡 Bulk actions:")
sel_bulk = st.dataframe(df_v[safe_display].drop(columns=['✏️']), use_container_width=True, selection_mode="multi-row", on_select="rerun", height=200)
st.session_state.sel_rows = sel_bulk.selection.rows
