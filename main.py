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

# --- 2. SPROG-DATABASE ---
TRANSLATIONS = {
    "🇩🇰 Dansk": {
        "login_title": "CRM Master Login", "login_btn": "LOG IND", "search": "🔍 Søg i alt data...",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Kampagne Filtre", "sidebar_admin": "🛠️ Admin Kontrol",
        "sidebar_user": "👤 Brugerstyring", "sidebar_export": "📤 Eksport", "btn_create": "➕ OPRET MANUELT",
        "btn_save": "💾 GEM ALT PÅ KLIENT", "btn_delete": "🗑️ SLET", "tab1": "📞 Kontakt & Social",
        "tab2": "🌍 Geografi & Brancher", "tab3": "⚙️ Salg & Pipeline", "tab4": "📝 Beskrivelser", "tab5": "📁 Medier & Noter",
        "field_name": "Virksomhed (Legal)", "field_cif": "CIF / VAT", "field_person": "Kontaktperson", "field_title": "Titel",
        "field_mail": "E-mail", "field_phone": "Kontor Tlf", "field_mobile": "Mobil", "field_wa": "WhatsApp", "field_tg": "Telegram",
        "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Website URL", "field_reg": "Region", "field_area": "Område",
        "field_town": "By", "field_addr": "Adresse", "field_zip": "Postnr.", "field_loc": "Google Maps Link", "field_br": "Brancher",
        "field_ubr": "Underbrancher", "field_lang": "Sprog", "field_work": "Åbningstider", "field_st": "Status", "field_mem": "Medlemskab",
        "field_adv": "Annonceprofil", "field_agent": "Agent", "field_src": "Kilde", "field_created": "Oprettet", "field_follow": "Opfølgning",
        "field_last": "Kontakt dato", "field_pitch": "Kort Pitch", "field_desc": "Lang Beskrivelse", "field_qr": "QR Tracking URL",
        "field_notes": "Interne CRM Noter"
    },
    "🇬🇧 English": {
        "login_title": "CRM Master Login", "login_btn": "LOG IN", "search": "🔍 Search all data...",
        "sidebar_scan": "📸 AI Card Scanner", "sidebar_filter": "🎯 Campaign Filters", "sidebar_admin": "🛠️ Admin Control",
        "sidebar_user": "👤 User Management", "sidebar_export": "📤 Export", "btn_create": "➕ CREATE MANUALLY",
        "btn_save": "💾 SAVE ALL CHANGES", "btn_delete": "🗑️ DELETE", "tab1": "📞 Contact & Social",
        "tab2": "🌍 Geography & Industry", "tab3": "⚙️ Sales & Pipeline", "tab4": "📝 Descriptions", "tab5": "📁 Media & Notes",
        "field_name": "Company Name", "field_cif": "CIF / VAT", "field_person": "Contact Person", "field_title": "Title",
        "field_mail": "Email", "field_phone": "Office Phone", "field_mobile": "Mobile", "field_wa": "WhatsApp", "field_tg": "Telegram",
        "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Website URL", "field_reg": "Region", "field_area": "Area",
        "field_town": "City", "field_addr": "Address", "field_zip": "Postal Code", "field_loc": "Google Maps Link", "field_br": "Industries",
        "field_ubr": "Sub-industries", "field_lang": "Languages", "field_work": "Opening Hours", "field_st": "Status", "field_mem": "Membership",
        "field_adv": "Ad Profile", "field_agent": "Agent", "field_src": "Source", "field_created": "Created", "field_follow": "Follow up",
        "field_last": "Contact date", "field_pitch": "Short Pitch", "field_desc": "Long Description", "field_qr": "QR Tracking URL",
        "field_notes": "Internal CRM Notes"
    },
    "🇪🇸 Español": {
        "login_title": "Inicio de Sesión", "login_btn": "ENTRAR", "search": "🔍 Buscar...",
        "sidebar_scan": "📸 Escáner IA", "sidebar_filter": "🎯 Filtros", "sidebar_admin": "🛠️ Administración",
        "sidebar_user": "👤 Usuarios", "sidebar_export": "📤 Exportar", "btn_create": "➕ CREAR MANUAL",
        "btn_save": "💾 GUARDAR", "btn_delete": "🗑️ ELIMINAR", "tab1": "📞 Contacto y Social",
        "tab2": "🌍 Geografía y Sector", "tab3": "⚙️ Ventas", "tab4": "📝 Descripciones", "tab5": "📁 Medios y Notas",
        "field_name": "Empresa", "field_cif": "CIF / IVA", "field_person": "Contacto", "field_title": "Cargo",
        "field_mail": "Email", "field_phone": "Teléfono", "field_mobile": "Móvil", "field_wa": "WhatsApp", "field_tg": "Telegram",
        "field_fb": "Facebook", "field_ig": "Instagram", "field_web": "Sitio Web", "field_reg": "Región", "field_area": "Zona",
        "field_town": "Ciudad", "field_addr": "Dirección", "field_zip": "CP", "field_loc": "Google Maps", "field_br": "Sectores",
        "field_ubr": "Subsectores", "field_lang": "Idiomas", "field_work": "Horario", "field_st": "Estado", "field_mem": "Membresía",
        "field_adv": "Perfil Anuncio", "field_agent": "Agente", "field_src": "Origen", "field_created": "Creado", "field_follow": "Seguimiento",
        "field_last": "Fecha contacto", "field_pitch": "Resumen", "field_desc": "Descripción", "field_qr": "URL QR",
        "field_notes": "Notas Internas"
    }
}

# --- 3. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, data JSONB)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            return engine
        except: return None
    return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 4. LOGIN LOGIK (FORBEDRET) ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False
if "lang_choice" not in st.session_state: st.session_state.lang_choice = "🇩🇰 Dansk"

def process_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    # 1. Tjek altid Railway Variables først (Nødindgang)
    rail_u, rail_p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
    if u == rail_u and p == rail_p:
        st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, "admin", u
    # 2. Tjek Database
    elif db_engine:
        try:
            with db_engine.connect() as conn:
                res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
                if res and res[0] == p:
                    st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
                else: st.error("❌ Login fejlede")
        except: st.error("Database forbindelse driller - prøv admin koden fra Railway")

if not st.session_state.authenticated:
    st.session_state.lang_choice = st.selectbox("🌐 Choose Language", list(TRANSLATIONS.keys()))
    l = TRANSLATIONS[st.session_state.lang_choice]
    st.title(l['login_title'])
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("Brugernavn", key="l_u")
        st.text_input("Adgangskode", type="password", key="l_p")
        st.button(l['login_btn'], type="primary", use_container_width=True, on_click=process_login)
    st.stop()

L = TRANSLATIONS[st.session_state.lang_choice]

# --- 5. MASTER STRUKTUR ---
MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data', 'Gallery_Data'
]
DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

def load_options():
    defaults = {
        "agents": ["Brian", "Olga"], "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Turisme", "Andet"],
        "underbrancher": ["Boligsalg", "Take-away", "VVS", "El", "Tømrer", "Murer", "Bogføring", "Rengøring"], "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk", "Tysk"], "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia", "Canarias", "Baleares"],
        "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Mallorca"], "titles": ["CEO", "Ejer", "Manager", "Marketingchef"],
        "memberships": ["Ingen", "Gratis", "Basis", "Premium", "VIP"], "advertising": ["Ingen", "Standard", "Premium"], "lead_types": ["Inbound", "Outbound", "AI Scan"]
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
    rename_map = {'Merchant': 'Company Name', 'Programnavn': 'Company Name'}
    df = df.rename(columns=rename_map)
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Company Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 6. KLIENT KORT ---
@st.dialog("🎯 CRM", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    c1, c2 = st.columns([0.8, 0.2])
    with c1: st.title(f"🏢 {row.get('Company Name') or 'Lead'}")
    with c2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=80)
    st.divider()
    t1, t2, t3, t4, t5 = st.tabs([L['tab1'], L['tab2'], L['tab3'], L['tab4'], L['tab5']])
    upd = {}
    with t1:
        st.markdown(f"##### {L['tab1']}")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input(L['field_name'], value=row.get('Company Name'), key=f"n_{idx}")
        upd['CIF Number VAT'] = ct2.text_input(L['field_cif'], value=row.get('CIF Number VAT'), key=f"c_{idx}")
        col1, col2 = st.columns(2)
        with col1:
            for f, lab in [('Kontaktperson', 'field_person'), ('Email', 'field_mail'), ('Phone number', 'field_phone'), ('Mobilnr', 'field_mobile')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"{f}_{idx}")
            upd['Titel'] = st.selectbox(L['field_title'], opts['titles'], index=opts['titles'].index(row.get('Titel')) if row.get('Titel') in opts['titles'] else 0, key=f"t_{idx}")
        with col2:
            for f, lab in [('WhatsApp', 'field_wa'), ('Telegram', 'field_tg'), ('Facebook', 'field_fb'), ('Instagram', 'field_ig'), ('Website', 'field_web')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"{f}_{idx}")
    with t2:
        st.markdown(f"##### {L['tab2']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Region'] = st.selectbox(L['field_reg'], opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0, key=f"r_{idx}")
            upd['Area'] = st.selectbox(L['field_area'], opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0, key=f"a_{idx}")
            for f, lab in [('Town', 'field_town'), ('Address', 'field_addr'), ('Postal Code', 'field_zip'), ('Exact Location', 'field_loc')]:
                upd[f] = st.text_input(L[lab], value=row.get(f,''), key=f"{f}_{idx}")
        with c2:
            for f, o, lab in [('Brancher', 'brancher', 'field_br'), ('Underbrancher', 'underbrancher', 'field_ubr'), ('Languages', 'sprog', 'field_lang')]:
                curr = [x.strip() for x in str(row.get(f)).split(',')] if row.get(f) else []
                upd[f] = ", ".join(st.multiselect(L[lab], opts[o], default=[x for x in curr if x in opts[o]], key=f"{f}_{idx}"))
            upd['Work time'] = st.text_input(L['field_work'], value=row.get('Work time'), key=f"w_{idx}")
    with t3:
        st.markdown(f"##### {L['tab3']}")
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox(L['field_st'], opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0, key=f"s_{idx}")
            upd['Membership'] = st.selectbox(L['field_mem'], opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0, key=f"m_{idx}")
            upd['Advertising'] = st.selectbox(L['field_adv'], opts['advertising'], index=opts['advertising'].index(row.get('Advertising')) if row.get('Advertising') in opts['advertising'] else 0, key=f"ad_{idx}")
        with c2:
            upd['Agent'] = st.selectbox(L['field_agent'], opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0, key=f"ag_{idx}")
            upd['Leadtype'] = st.selectbox(L['field_src'], opts['lead_types'], index=opts['lead_types'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['lead_types'] else 0, key=f"lt_{idx}")
            for f, lab in [('Date created', 'field_created'), ('Date for follow up', 'field_follow'), ('Kontakt dato', 'field_last')]:
                d_val = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(L[lab], value=d_val, key=f"{f}_{idx}").strftime('%d/%m/%Y')
    with t4:
        st.markdown(f"##### {L['tab4']}")
        upd['Business Description'] = st.text_area(L['field_pitch'], value=row.get('Business Description'), height=100, key=f"bd_{idx}")
        upd['Description'] = st.text_area(L['field_desc'], value=row.get('Description'), height=200, key=f"de_{idx}")
        upd['Tracking_URL'] = st.text_input(L['field_qr'], value=row.get('Tracking_URL'), key=f"tr_{idx}")
    with t5:
        st.markdown(f"##### {L['tab5']}")
        upd['Noter'] = st.text_area(L['field_notes'], value=row.get('Noter'), height=200, key=f"no_{idx}")
        c1, c2 = st.columns(2)
        if l_up := st.file_uploader("Logo", type=['png','jpg'], key=f"lu_{idx}"):
            upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        if f_up := st.file_uploader("Document", key=f"fu_{idx}"):
            upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
        st.file_uploader("Gallery", accept_multiple_files=True, key=f"ga_{idx}")

    if st.button(L['btn_save'], type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and st.button(L['field_name'] + " " + L['btn_delete']):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.session_state.lang_choice = st.selectbox("🌐 Language", list(TRANSLATIONS.keys()))
    st.header(f"👤 {st.session_state.username}")
    
    # AI SCANNER
    with st.expander(L['sidebar_scan']):
        if cam := st.camera_input("Scan"):
            with st.spinner("AI..."):
                response = openai_client.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "user", "content": [
                        {"type": "text", "text": "Extract business info into JSON: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address."},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ]}], response_format={ "type": "json_object" }
                )
                nr = {c: "" for c in MASTER_COLS}; nr.update(json.loads(response.choices[0].message.content))
                nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Leadtype'] = "AI Scan"
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()

    if st.session_state.user_role == "admin":
        with st.expander(L['sidebar_admin']):
            labels = [("Agent", "agents"), ("Branche", "brancher"), ("Status", "status"), ("Sprog", "sprog"), ("Region", "regions"), ("Area", "areas")]
            for lab, key in labels:
                v_new = st.text_input(f"Add {lab}:", key=f"ad_{key}")
                if st.button(f"Save {lab}", key=f"bt_{key}"):
                    with db_engine.begin() as conn: conn.execute(text("INSERT INTO crm_configs (type, value) VALUES (:t, :v)"), {"t": key, "v": v_new})
                    st.rerun()
            if st.button("🚨 RESET DB"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    st.header(L['sidebar_filter'])
    f_ag = st.multiselect(L['agent_label'], opts['agents'])
    f_st = st.multiselect(L['field_st'], opts['status'])
    f_br = st.multiselect(L['field_br'], opts['brancher'])
    f_to = st.multiselect(L['field_town'], sorted([t for t in st.session_state.df_leads['Town'].unique() if t]))

    st.divider()
    if st.button(L['btn_create'], type="primary", use_container_width=True):
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Agent'] = st.session_state.username
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    st.download_button(L['sidebar_export'], st.session_state.df_leads.to_csv(index=False), "master.csv", use_container_width=True)
    if st.button(L['logout']): st.session_state.authenticated = False; st.rerun()

# --- 8. DASHBOARD ---
st.title(L['title'])
search = st.text_input(L['search'])
df_v = st.session_state.df_leads.copy()
if f_ag: df_v = df_v[df_v['Agent'].isin(f_ag)]
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_br: df_v = df_v[df_v['Brancher'].apply(lambda x: any(b in x for b in f_br))]
if f_to: df_v = df_v[df_v['Town'].isin(f_to)]
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(L['total_leads'].format(n=len(df_v)))
sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
