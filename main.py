import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Lead Master Playground", layout="wide", page_icon="🎯")

# --- 2. DATABASE ---
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

# --- 3. MASTER STRUKTUR (ALT INDHOLD) ---
MASTER_COLS = [
    'Date created', 'Company Name', 'Brancher', 'Underbrancher', 'Area', 'Town', 
    'Status on lead', 'Membership', 'CIF Number VAT', 'Business Description', 
    'Postal Code', 'Exact Location', 'Website', 'Email', 'Phone number', 
    'Address', 'Work time', 'Languages', 'Description', 'Date for follow up', 
    'Advertising', 'Pricelist', 'Agent', 'Leadtype', 'Tracking_URL', 'Noter'
]

# Oversigt på forsiden
DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Area', 'Status on lead', 'Membership']

# --- 4. DROPDOWN VALGMULIGHEDER (MULTI-CHOICE) ---
DEFAULTS = {
    "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Service", "Turisme", "Andet"],
    "underbrancher": ["Boligsalg", "Udlejning", "Tapas", "Take-away", "VVS", "El", "Tømrer", "Skønhed", "IT"],
    "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk", "Tysk", "Fransk", "Hollandsk"],
    "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning", "Pause"],
    "membership": ["Ingen", "Gratis", "Basis", "Premium", "VIP"]
}

# --- 5. HJÆLPEFUNKTIONER ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']:
        return date.today()
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        return dt.date() if not pd.isna(dt) else date.today()
    except:
        return date.today()

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

# Indlæs data
if 'df_leads' not in st.session_state:
    try:
        df = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
        st.session_state.df_leads = force_clean(df)
    except:
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

# --- 6. DET NYE POPUP KORT (4 FANER) ---
@st.dialog("🎯 Lead Administration", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    st.divider()
    
    t1, t2, t3, t4 = st.tabs(["📌 Basis & Kontakt", "📂 Brancher & Sprog", "⚙️ Salg & Pipeline", "📝 Beskrivelse & Noter"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        with c1:
            upd['Company Name'] = st.text_input("Virksomhedsnavn (Legal)", value=row.get('Company Name'))
            upd['CIF Number VAT'] = st.text_input("CIF / VAT Nummer", value=row.get('CIF Number VAT'))
            upd['Email'] = st.text_input("E-mail", value=row.get('Email'))
            upd['Phone number'] = st.text_input("Telefon", value=row.get('Phone number'))
        with c2:
            upd['Website'] = st.text_input("Hjemmeside URL", value=row.get('Website'))
            upd['Tracking_URL'] = st.text_input("🔗 QR / Tracking Link", value=row.get('Tracking_URL'))
            upd['Address'] = st.text_input("Adresse", value=row.get('Address'))
            upd['Town'] = st.text_input("By / Postnr", value=row.get('Town'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            # MULTI-SELECT BRANCHER
            current_br = [x.strip() for x in str(row.get('Brancher')).split(',')] if row.get('Brancher') else []
            upd['Brancher'] = ", ".join(st.multiselect("Vælg Branche(r)", DEFAULTS['brancher'], default=[x for x in current_br if x in DEFAULTS['brancher']]))
            
            # MULTI-SELECT UNDERBRANCHER
            current_ubr = [x.strip() for x in str(row.get('Underbrancher')).split(',')] if row.get('Underbrancher') else []
            upd['Underbrancher'] = ", ".join(st.multiselect("Vælg Underbranche(r)", DEFAULTS['underbrancher'], default=[x for x in current_ubr if x in DEFAULTS['underbrancher']]))
        
        with c2:
            # MULTI-SELECT SPROG
            current_lang = [x.strip() for x in str(row.get('Languages')).split(',')] if row.get('Languages') else []
            upd['Languages'] = ", ".join(st.multiselect("Sprog talt i virksomheden", DEFAULTS['sprog'], default=[x for x in current_lang if x in DEFAULTS['sprog']]))
            
            upd['Area'] = st.text_input("Område (f.eks. Costa del Sol)", value=row.get('Area'))
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'))

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Pipeline Status", DEFAULTS['status'], index=DEFAULTS['status'].index(row.get('Status on lead')) if row.get('Status on lead') in DEFAULTS['status'] else 0)
            upd['Membership'] = st.selectbox("Medlemskab", DEFAULTS['membership'], index=DEFAULTS['membership'].index(row.get('Membership')) if row.get('Membership') in DEFAULTS['membership'] else 0)
            upd['Agent'] = st.text_input("Ansvarlig Agent", value=row.get('Agent'))
        with c2:
            # KALENDER VÆLGERE
            upd['Date created'] = st.date_input("Oprettet den", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
            upd['Date for follow up'] = st.date_input("Næste opfølgning", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')
            upd['Leadtype'] = st.text_input("Kilde / Leadtype", value=row.get('Leadtype'))

    with t4:
        upd['Business Description'] = st.text_input("Kort Pitch (Slogan)", value=row.get('Business Description'))
        upd['Description'] = st.text_area("Lang beskrivelse (Annoncetekst)", value=row.get('Description'), height=150)
        upd['Noter'] = st.text_area("Interne noter / Logbog", value=row.get('Noter'), height=150)

    if st.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.header("🎯 CRM Menu")
    
    if st.button("➕ OPRET NYT LEAD", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y') # Sætter automatisk dags dato
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads)
        st.rerun()

    st.divider()
    # KATEGORI FILTER
    st.subheader("Filter")
    filter_brancher = st.multiselect("Vis kun Brancher:", DEFAULTS['brancher'])

    st.divider()
    f_up = st.file_uploader("Importér Excel/CSV")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        # Sæt dags dato på nye leads der ikke har en dato
        if 'Date created' not in nd.columns: nd['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = force_clean(pd.concat([st.session_state.df_leads, nd], ignore_index=True))
        save_db(st.session_state.df_leads); st.rerun()

    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                conn.commit()
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. HOVEDVISNING ---
st.title("💼 CRM Playground")

df_display = st.session_state.df_leads.copy()

# Anvend sidebar filtre
if filter_brancher:
    df_display = df_display[df_display['Brancher'].apply(lambda x: any(b in x for b in filter_brancher))]

search = st.text_input("🔍 Søg i navne, byer, noter...", "")
if search:
    df_display = df_display[df_display.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(f"Viser **{len(df_display)}** leads")

# Tabellen viser kun DISPLAY_COLS
sel = st.dataframe(
    df_display[DISPLAY_COLS], 
    use_container_width=True, 
    selection_mode="single-row", 
    on_select="rerun", 
    height=600
)

if sel.selection.rows:
    real_idx = df_display.index[sel.selection.rows[0]]
    lead_popup(real_idx)
