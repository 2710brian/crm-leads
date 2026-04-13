import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Lead Database Master", layout="wide", page_icon="🎯")

# --- 2. DATABASE FORBINDELSE ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS leads_data (id SERIAL PRIMARY KEY, data JSONB)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS lead_settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. NY MASTER STRUKTUR (DINE FELTER) ---
MASTER_COLS = [
    'Company Name', 'CIF Number VAT', 'Business Category', 'Business Description',
    'Area', 'Town', 'Postal Code', 'Exact Location', 'Website', 'Email', 
    'Phone number', 'Address', 'Work time', 'Languages', 'Description', 
    'Date created', 'Date for follow up', 'Status on lead', 'Advertising', 
    'Pricelist', 'Agent', 'Membership', 'Leadtype', 'Logo_File', 'Gallery_Data'
]

# --- 4. DROPDOWN LOGIK ---
def load_options():
    defaults = {
        "categories": ["Håndværker", "Restaurant", "Butik", "Service", "Andet"],
        "areas": ["Costa del Sol", "Malaga", "Marbella", "Fuengirola", "Estepona"],
        "languages": ["Dansk", "Engelsk", "Spansk", "Tysk", "Svensk", "Norsk"],
        "lead_status": ["Ny", "Dialog i gang", "Oplæg sendt", "Vundet", "Tabt", "Pause"],
        "memberships": ["Gratis", "Basis", "Premium", "VIP"],
        "lead_types": ["Inbound (Form)", "Outbound (Cold)", "Reference", "Messe"],
        "agents": ["Brian", "Agent 1", "Agent 2"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM lead_settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

# --- 5. RENSE- OG MERGE MOTOR ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean_leads(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_leads_db(df):
    if db_engine:
        df = force_clean_leads(df)
        df['MATCH_KEY'] = df['Company Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False) # Vi genbruger tabelnavn for nemhed
        return True
    return False

# --- INITIALISERING ---
if 'df_leads' not in st.session_state:
    if db_engine:
        try: 
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            st.session_state.df_leads = force_clean_leads(df)
        except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
    else: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

opts = load_options()

# --- 6. DET NYE KLIENT KORT (POP-UP) ---
@st.dialog("🎯 Lead Detaljer & Administration", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name', 'Nyt Lead')}")
    st.divider()
    
    t1, t2, t3, t4 = st.tabs(["📌 Basis & Kontakt", "🏢 Forretning", "📊 CRM & Salg", "🖼️ Medier"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        with c1:
            upd['Company Name'] = st.text_input("Virksomhedsnavn (Legal)", value=row.get('Company Name'))
            upd['CIF Number VAT'] = st.text_input("CIF / VAT Nummer", value=row.get('CIF Number VAT'))
            upd['Email'] = st.text_input("E-mail", value=row.get('Email'))
            upd['Phone number'] = st.text_input("Telefon", value=row.get('Phone number'))
        with c2:
            upd['Website'] = st.text_input("Website", value=row.get('Website'))
            upd['Address'] = st.text_input("Adresse", value=row.get('Address'))
            upd['Town'] = st.text_input("By", value=row.get('Town'))
            upd['Postal Code'] = st.text_input("Postnummer", value=row.get('Postal Code'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            # Multi-select til Kategorier
            current_cats = [x.strip() for x in str(row.get('Business Category')).split(',')] if row.get('Business Category') else []
            upd['Business Category'] = ", ".join(st.multiselect("Kategorier", opts['categories'], default=[c for c in current_cats if c in opts['categories']]))
            
            upd['Area'] = st.selectbox("Område", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0)
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'))
        with c2:
            current_langs = [x.strip() for x in str(row.get('Languages')).split(',')] if row.get('Languages') else []
            upd['Languages'] = ", ".join(st.multiselect("Sprog", opts['languages'], default=[l for l in current_langs if l in opts['languages']]))
            upd['Exact Location'] = st.text_input("Kort lokation (URL/GPS)", value=row.get('Exact Location'))
        
        upd['Business Description'] = st.text_area("Kort beskrivelse", value=row.get('Business Description'))
        upd['Description'] = st.text_area("Lang beskrivelse (til annoncering)", value=row.get('Description'), height=150)

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Status", opts['lead_status'], index=opts['lead_status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['lead_status'] else 0)
            upd['Leadtype'] = st.selectbox("Lead type", opts['lead_types'], index=opts['lead_types'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['lead_types'] else 0)
            upd['Agent'] = st.selectbox("Ansvarlig Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
        with c2:
            upd['Membership'] = st.selectbox("Medlemskab", opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0)
            upd['Date created'] = st.date_input("Oprettet dato", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
            upd['Date for follow up'] = st.date_input("Opfølgningsdato", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')
        
        upd['Noter'] = st.text_area("Interne CRM Noter", value=row.get('Noter'), height=150)

    with t4:
        st.subheader("Logo & Galleri")
        st.file_uploader("Upload Logo", key=f"logo_{idx}")
        st.file_uploader("Upload Galleri Billeder", accept_multiple_files=True, key=f"gal_{idx}")

    if st.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_leads_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR (NYT: MANUEL OPRETTELSE) ---
with st.sidebar:
    st.title("⚙️ CRM Kontrol")
    
    if st.button("➕ OPRET NYT LEAD MANUELT", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        # Gem med det samme så vi får et index
        save_leads_db(st.session_state.df_leads)
        st.rerun()

    st.divider()
    st.header("📥 Import Excel/CSV")
    f_up = st.file_uploader("Vælg fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df_leads = force_clean_leads(pd.concat([st.session_state.df_leads, nd], ignore_index=True))
        save_leads_db(st.session_state.df_leads); st.rerun()

    st.divider()
    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants"))
                conn.commit()
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. HOVEDVISNING ---
st.title("💼 Lead Database")
search = st.text_input("🔍 Hurtig søgning i leads...", "")

df_v = st.session_state.df_leads.copy()
if search:
    df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(
    df_v, 
    use_container_width=True, 
    selection_mode="single-row", 
    on_select="rerun", 
    height=600,
    column_config={"Website": st.column_config.LinkColumn("Website")}
)

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
