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
            return create_engine(db_url, pool_pre_ping=True)
        except: return None
    return None

db_engine = get_engine()

def get_base64_image(file_path):
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return None

# --- 3. MASTER STRUKTUR (INKL. KUNDE LOGO) ---
MASTER_COLS = [
    'Date created', 'Company Name', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Kontaktperson', 'Titel', 'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 
    'Email', 'Website', 'Facebook', 'Instagram', 'Status on lead', 'Membership', 
    'Leadtype', 'Agent', 'Date for follow up', 'Address', 'Postal Code', 'Exact Location', 
    'Work time', 'Languages', 'Business Description', 'Description', 'Tracking_URL', 
    'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data'
]

DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Kontaktperson']

# --- 4. DROPDOWN VALGMULIGHEDER ---
DEFAULTS = {
    "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia", "Castilla y León", "País Vasco", "Canarias", "Murcia", "Aragón", "Baleares"],
    "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Costa de la Luz", "Mallorca", "Ibiza"],
    "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Service", "Turisme", "Andet"],
    "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning", "Pause"],
    "agents": ["Brian", "Agent 1", "Agent 2"]
}

# --- 5. HJÆLPEFUNKTIONER ---
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
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

if 'df_leads' not in st.session_state:
    try:
        df = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
        st.session_state.df_leads = force_clean(df)
    except:
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

# --- 6. KLIENT KORT POPUP ---
@st.dialog("🎯 Lead Administration", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    
    # Header med lille logo preview hvis det findes
    col_h1, col_h2 = st.columns([0.8, 0.2])
    with col_h1:
        st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    with col_h2:
        if row.get('Logo_Data'):
            st.image(f"data:image/png;base64,{row['Logo_Data']}", width=80)

    st.divider()
    t1, t2, t3, t4 = st.tabs(["📞 Kontakt", "🌍 Område & Branche", "⚙️ Salg", "📁 Medier & Noter"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        with c1:
            upd['Company Name'] = st.text_input("Virksomhed", value=row.get('Company Name'))
            upd['Kontaktperson'] = st.text_input("Kontaktperson", value=row.get('Kontaktperson'))
            upd['Titel'] = st.text_input("Titel", value=row.get('Titel'))
            upd['Email'] = st.text_input("E-mail", value=row.get('Email'))
        with c2:
            upd['Phone number'] = st.text_input("Kontor Tlf", value=row.get('Phone number'))
            upd['Mobilnr'] = st.text_input("Mobil (Vigtig)", value=row.get('Mobilnr'))
            upd['WhatsApp'] = st.text_input("WhatsApp", value=row.get('WhatsApp'))
            upd['Website'] = st.text_input("Website URL", value=row.get('Website'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            upd['Region'] = st.selectbox("Region", DEFAULTS['regions'], index=DEFAULTS['regions'].index(row.get('Region')) if row.get('Region') in DEFAULTS['regions'] else 0)
            upd['Town'] = st.text_input("By", value=row.get('Town'))
            upd['Area'] = st.selectbox("Område/Kyst", DEFAULTS['areas'], index=DEFAULTS['areas'].index(row.get('Area')) if row.get('Area') in DEFAULTS['areas'] else 0)
        with c2:
            current_br = [x.strip() for x in str(row.get('Brancher')).split(',')] if row.get('Brancher') else []
            upd['Brancher'] = ", ".join(st.multiselect("Brancher", DEFAULTS['brancher'], default=[x for x in current_br if x in DEFAULTS['brancher']]))
            upd['Languages'] = st.text_input("Sprog", value=row.get('Languages'))

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Status", DEFAULTS['status'], index=DEFAULTS['status'].index(row.get('Status on lead')) if row.get('Status on lead') in DEFAULTS['status'] else 0)
            upd['Agent'] = st.selectbox("Agent", DEFAULTS['agents'], index=DEFAULTS['agents'].index(row.get('Agent')) if row.get('Agent') in DEFAULTS['agents'] else 0)
        with c2:
            upd['Date created'] = st.date_input("Oprettet", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
            upd['Date for follow up'] = st.date_input("Opfølgning", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')

    with t4:
        st.markdown("##### 📓 Noter & Arkiv")
        upd['Noter'] = st.text_area("Logbog", value=row.get('Noter'), height=100)
        
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown("🖼️ **Kundens Logo**")
            if row.get('Logo_Data'):
                st.image(f"data:image/png;base64,{row['Logo_Data']}", width=150)
            
            logo_up = st.file_uploader("Upload nyt Logo", type=['png', 'jpg', 'jpeg'], key=f"logo_{idx}")
            if logo_up:
                upd['Logo_Navn'] = logo_up.name
                upd['Logo_Data'] = base64.b64encode(logo_up.read()).decode()

        with col_m2:
            st.markdown("📎 **Dokumenter (PDF/Excel)**")
            if row.get('Fil_Navn'):
                st.info(f"Fil: {row['Fil_Navn']}")
                if row.get('Fil_Data'):
                    st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Hent dokument</a>', unsafe_allow_html=True)
            
            file_up = st.file_uploader("Upload nyt dokument", key=f"file_{idx}")
            if file_up:
                upd['Fil_Navn'] = file_up.name
                upd['Fil_Data'] = base64.b64encode(file_up.read()).decode()

    if st.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    logo_mgm = get_base64_image("applogo.png")
    if logo_mgm:
        st.markdown(f'<div style="text-align:center"><img src="data:image/png;base64,{logo_mgm}" width="180"></div>', unsafe_allow_html=True)
    
    st.header("🎯 Kampagne Filter")
    f_br = st.multiselect("Branche:", DEFAULTS['brancher'])
    f_reg = st.multiselect("Region:", DEFAULTS['regions'])
    f_town = st.multiselect("By:", sorted([t for t in st.session_state.df_leads['Town'].unique() if t]))
    f_st = st.multiselect("Status:", DEFAULTS['status'])

    st.divider()
    if st.button("➕ OPRET NYT LEAD", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()

    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "leads_master.csv", use_container_width=True)
    
    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                conn.commit()
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. DASHBOARD ---
st.title("💼 Business CRM Master")
df_v = st.session_state.df_leads.copy()

if f_br: df_v = df_v[df_v['Brancher'].apply(lambda x: any(b in x for b in f_br))]
if f_reg: df_v = df_v[df_v['Region'].isin(f_reg)]
if f_town: df_v = df_v[df_v['Town'].isin(f_town)]
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]

search = st.text_input("🔍 Søg i alt...")
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(f"Viste leads: **{len(df_v)}**")

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
