import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Business Master CRM", layout="wide", page_icon="🎯")

# --- 2. DATABASE & BILLED-LOGIK ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, data JSONB)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS pg_settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

def get_base64_image(file_path):
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return None

# --- 3. MASTER STRUKTUR (SAMTLIGE 35 FELTER) ---
MASTER_COLS = [
    'Date created', 'Company Name', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data'
]

DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Region', 'Town', 'Status on lead', 'Agent']

# --- 4. DROPDOWN ADMINISTRATION ---
def load_options():
    defaults = {
        "networks": ["Partner-ads", "Awin", "Adtraction"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia", "Castilla y León", "País Vasco", "Canarias", "Murcia", "Aragón", "Baleares"],
        "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Costa de la Luz", "Mallorca", "Ibiza"],
        "titles": ["Ejer", "Manager", "Marketingchef", "E-commerce Manager", "Salgschef", "Andet"],
        "agents": ["Brian", "Agent 1", "Agent 2"],
        "lead_types": ["Inbound (Form)", "Outbound (Cold)", "SoMe Lead", "Reference", "Google"],
        "memberships": ["Ingen", "Gratis", "Basis", "Premium", "VIP"],
        "advertising": ["Ingen", "Standard Profil", "Premium Eksponering", "Banner kampagne"],
        "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning", "Pause"],
        "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Service", "Turisme", "Andet"],
        "underbrancher": ["Boligsalg", "Udlejning", "Tapas", "Take-away", "VVS", "El", "Tømrer", "Skønhed"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk", "Tysk"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM pg_settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def add_option(opt_type, value):
    if db_engine and value:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO pg_settings (type, value) VALUES (:t, :v)"), {"t": opt_type, "v": value})
            conn.commit()

# --- 5. RENSE-MOTOR ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
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

# Initialisering
if 'df_leads' not in st.session_state:
    try:
        df = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
        st.session_state.df_leads = force_clean(df)
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 6. DET KOMPLETTE KLIENT KORT ---
@st.dialog("🎯 Lead Administration & CRM Board", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    
    # Header med Logo
    c_h1, c_h2 = st.columns([0.8, 0.2])
    with c_h1: st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    with c_h2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=80)

    st.divider()
    t1, t2, t3, t4, t5 = st.tabs(["📞 Kontakt & Social", "🌍 Geografi & Brancher", "⚙️ Salg & Pipeline", "📝 Beskrivelser", "📁 Medier & Noter"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 👤 Personlig kontakt")
            upd['Kontaktperson'] = st.text_input("Navn", value=row.get('Kontaktperson'))
            upd['Titel'] = st.selectbox("Titel", opts['titles'], index=opts['titles'].index(row.get('Titel')) if row.get('Titel') in opts['titles'] else 0)
            upd['Email'] = st.text_input("E-mail", value=row.get('Email'))
            upd['Phone number'] = st.text_input("Kontor Tlf", value=row.get('Phone number'))
            upd['Mobilnr'] = st.text_input("Mobil (Vigtig)", value=row.get('Mobilnr'))
        with c2:
            st.markdown("##### 📱 Sociale Medier & Chat")
            upd['WhatsApp'] = st.text_input("WhatsApp", value=row.get('WhatsApp'))
            upd['Telegram'] = st.text_input("Telegram", value=row.get('Telegram'))
            upd['Facebook'] = st.text_input("Facebook Page", value=row.get('Facebook'))
            upd['Instagram'] = st.text_input("Instagram Profil", value=row.get('Instagram'))
            upd['Website'] = st.text_input("Website URL", value=row.get('Website'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 📍 Lokation")
            upd['Region'] = st.selectbox("Region", opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0)
            upd['Area'] = st.selectbox("Område/Kyst", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0)
            upd['Town'] = st.text_input("By", value=row.get('Town'))
            upd['Address'] = st.text_input("Adresse", value=row.get('Address'))
            upd['Postal Code'] = st.text_input("Postnr.", value=row.get('Postal Code'))
            upd['Exact Location'] = st.text_input("Google Maps Link", value=row.get('Exact Location'))
        with c2:
            st.markdown("##### 🏷️ Klassificering")
            cur_br = [x.strip() for x in str(row.get('Brancher')).split(',')] if row.get('Brancher') else []
            upd['Brancher'] = ", ".join(st.multiselect("Brancher", opts['brancher'], default=[x for x in cur_br if x in opts['brancher']]))
            cur_ubr = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(st.multiselect("Underbrancher", opts['underbrancher'], default=[x for x in cur_ubr if x in opts['underbrancher']]))
            cur_lang = [x.strip() for x in str(row.get('Languages')).split(',')] if row.get('Languages') else []
            upd['Languages'] = ", ".join(st.multiselect("Sprog", opts['sprog'], default=[x for x in cur_lang if x in opts['sprog']]))
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'))

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🚀 Salgsstatus")
            upd['Status on lead'] = st.selectbox("Pipeline Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox("Medlemskab", opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0)
            upd['Advertising'] = st.selectbox("Annonceringsprofil", opts['advertising'], index=opts['advertising'].index(row.get('Advertising')) if row.get('Advertising') in opts['advertising'] else 0)
        with c2:
            st.markdown("##### 📋 Kilde & Ansvarlig")
            upd['Leadtype'] = st.selectbox("Lead Kilde", opts['lead_types'], index=opts['lead_types'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['lead_types'] else 0)
            upd['Agent'] = st.selectbox("Ansvarlig Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
            upd['Date created'] = st.date_input("Oprettet", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
            upd['Date for follow up'] = st.date_input("Opfølgning", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')

    with t4:
        st.markdown("##### 📢 Marketing Tekster")
        upd['Business Description'] = st.text_area("Kort Pitch / Introduktion", value=row.get('Business Description'), height=100)
        upd['Description'] = st.text_area("Lang Beskrivelse (Annoncetekst)", value=row.get('Description'), height=200)
        st.divider()
        upd['Tracking_URL'] = st.text_input("🔗 QR Tracking URL (Eget system)", value=row.get('Tracking_URL'))

    with t5:
        st.markdown("##### 📓 Interne CRM Noter")
        upd['Noter'] = st.text_area("Logbog", value=row.get('Noter'), height=200)
        c_m1, c_m2 = st.columns(2)
        with c_m1:
            st.markdown("🖼️ **Kundens Logo**")
            if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=150)
            l_up = st.file_uploader("Upload Logo", type=['png','jpg'], key=f"l_{idx}")
            if l_up: upd['Logo_Navn'], upd['Logo_Data'] = l_up.name, base64.b64encode(l_up.read()).decode()
        with c_m2:
            st.markdown("📎 **Dokumenter (PDF/Excel)**")
            if row.get('Fil_Navn'):
                st.info(f"Fil: {row['Fil_Navn']}")
                if row.get('Fil_Data'): st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Hent fil</a>', unsafe_allow_html=True)
            f_up = st.file_uploader("Upload dokument", key=f"f_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()

    if st.button("💾 GEM ALT PÅ KLIENT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR (LOGO & FILTER) ---
with st.sidebar:
    l_mgm = get_base64_image("applogo.png")
    if l_mgm: st.markdown(f'<div style="text-align:center"><img src="data:image/png;base64,{l_mgm}" width="180"></div>', unsafe_allow_html=True)
    
    st.header("🎯 Kampagne Filter")
    f_br = st.multiselect("Branche:", opts['brancher'])
    f_reg = st.multiselect("Region:", opts['regions'])
    f_town = st.multiselect("By:", sorted([t for t in st.session_state.df_leads['Town'].unique() if t]))
    f_st = st.multiselect("Status:", opts['status'])
    f_ag = st.multiselect("Agent:", opts['agents'])

    with st.expander("🛠️ Administrer Dropdowns"):
        t_sel = st.selectbox("Vælg:", ["brancher", "underbrancher", "regions", "areas", "titles", "agents", "lead_types", "memberships", "advertising", "status", "sprog"])
        v_new = st.text_input("Nyt navn:")
        if st.button("Tilføj") and v_new: add_option(t_sel, v_new); st.rerun()

    st.divider()
    if st.button("➕ OPRET NYT LEAD", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()

    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "leads_master.csv", use_container_width=True)
    if st.button("🚨 Nulstil DB"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground")); conn.commit()
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. DASHBOARD ---
st.title("💼 Business CRM Pro")
df_v = st.session_state.df_leads.copy()

if f_br: df_v = df_v[df_v['Brancher'].apply(lambda x: any(b in x for b in f_br))]
if f_reg: df_v = df_v[df_v['Region'].isin(f_reg)]
if f_town: df_v = df_v[df_v['Town'].isin(f_town)]
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_ag: df_v = df_v[df_v['Agent'].isin(f_ag)]

search = st.text_input("🔍 Hurtig søg i alt data...")
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(f"Viste leads: **{len(df_v)}**")
sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
