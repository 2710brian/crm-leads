import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Business & Lead Master", layout="wide", page_icon="🎯")

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

# --- 3. MASTER STRUKTUR (UDVIDET) ---
MASTER_COLS = [
    'Date created', 'Company Name', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Kontaktperson', 'Titel', 'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 
    'Email', 'Website', 'Facebook', 'Instagram', 'Status on lead', 'Membership', 
    'Leadtype', 'Agent', 'Date for follow up', 'Address', 'Postal Code', 'Exact Location', 
    'Work time', 'Languages', 'Business Description', 'Description', 'Tracking_URL', 'Noter'
]

# Oversigt på forsiden
DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Region', 'Area', 'Status on lead', 'Kontaktperson']

# --- 4. DROPDOWN VALGMULIGHEDER ---
DEFAULTS = {
    "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia", "Castilla y León", "País Vasco", "Canarias", "Castilla-La Mancha", "Murcia", "Aragón", "Extremadura", "Baleares", "Asturias", "Navarra", "Cantabria", "La Rioja"],
    "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Costa de la Luz", "Costa Almería", "Mallorca", "Ibiza", "Madrid City", "Barcelona City"],
    "lead_types": ["Inbound (Form)", "Outbound (Cold)", "SoMe Lead", "Google Search", "Reference", "Messe", "Andet"],
    "agents": ["Brian", "Agent 1", "Agent 2", "Partner"],
    "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Service", "Turisme", "Andet"],
    "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk", "Tysk", "Fransk", "Hollandsk"],
    "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning", "Pause"]
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

# --- 6. DET STORE KLIENT KORT (5 FANER) ---
@st.dialog("🎯 Business Lead Administration", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    st.divider()
    
    t1, t2, t3, t4, t5 = st.tabs(["📞 Kontaktperson", "🌍 Område & Branche", "⚙️ Salg & Pipeline", "📱 Web & SoMe", "📝 Noter"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        with c1:
            upd['Company Name'] = st.text_input("Virksomhedsnavn (Legal)", value=row.get('Company Name'))
            upd['Kontaktperson'] = st.text_input("Kontaktperson", value=row.get('Kontaktperson'))
            upd['Titel'] = st.text_input("Titel (Ejer, Manager, etc.)", value=row.get('Titel'))
            upd['Email'] = st.text_input("Hoved E-mail", value=row.get('Email'))
        with c2:
            upd['Phone number'] = st.text_input("Telefon (Kontor)", value=row.get('Phone number'))
            upd['Mobilnr'] = st.text_input("Mobilnummer", value=row.get('Mobilnr'))
            upd['WhatsApp'] = st.text_input("WhatsApp (Link eller nr)", value=row.get('WhatsApp'))
            upd['Telegram'] = st.text_input("Telegram (Brugernavn)", value=row.get('Telegram'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            # Region & Område Dropdowns
            upd['Region'] = st.selectbox("Region (Spanien)", DEFAULTS['regions'], index=DEFAULTS['regions'].index(row.get('Region')) if row.get('Region') in DEFAULTS['regions'] else 0)
            upd['Area'] = st.selectbox("Område / Kyst", DEFAULTS['areas'], index=DEFAULTS['areas'].index(row.get('Area')) if row.get('Area') in DEFAULTS['areas'] else 0)
            upd['Town'] = st.text_input("By", value=row.get('Town'))
            upd['Postal Code'] = st.text_input("Postnummer", value=row.get('Postal Code'))
        with c2:
            current_br = [x.strip() for x in str(row.get('Brancher')).split(',')] if row.get('Brancher') else []
            upd['Brancher'] = ", ".join(st.multiselect("Hovedbranche(r)", DEFAULTS['brancher'], default=[x for x in current_br if x in DEFAULTS['brancher']]))
            upd['Address'] = st.text_input("Vejnavn & Nr.", value=row.get('Address'))
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'))

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Pipeline Status", DEFAULTS['status'], index=DEFAULTS['status'].index(row.get('Status on lead')) if row.get('Status on lead') in DEFAULTS['status'] else 0)
            upd['Leadtype'] = st.selectbox("Kilde / Leadtype", DEFAULTS['lead_types'], index=DEFAULTS['lead_types'].index(row.get('Leadtype')) if row.get('Leadtype') in DEFAULTS['lead_types'] else 0)
            upd['Agent'] = st.selectbox("Ansvarlig Agent", DEFAULTS['agents'], index=DEFAULTS['agents'].index(row.get('Agent')) if row.get('Agent') in DEFAULTS['agents'] else 0)
        with c2:
            upd['Date created'] = st.date_input("Oprettet", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
            upd['Date for follow up'] = st.date_input("Opfølgning", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')
            upd['Membership'] = st.selectbox("Medlemskab", ["Ingen", "Gratis", "Basis", "Premium", "VIP"], index=0)

    with t4:
        c1, c2 = st.columns(2)
        with c1:
            upd['Website'] = st.text_input("Website URL", value=row.get('Website'))
            upd['Facebook'] = st.text_input("Facebook Page", value=row.get('Facebook'))
            upd['Instagram'] = st.text_input("Instagram Profil", value=row.get('Instagram'))
        with c2:
            upd['Tracking_URL'] = st.text_input("🔗 QR / Tracking Link", value=row.get('Tracking_URL'))
            upd['Exact Location'] = st.text_input("Google Maps Link", value=row.get('Exact Location'))

    with t5:
        upd['Business Description'] = st.text_input("Kort Pitch", value=row.get('Business Description'))
        upd['Description'] = st.text_area("Annoncetekst (Lang)", value=row.get('Description'), height=100)
        upd['Noter'] = st.text_area("Interne CRM Noter", value=row.get('Noter'), height=200)

    if st.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.header("🎯 CRM Menu")
    
    if st.button("➕ OPRET NYT LEAD", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads)
        st.rerun()

    st.divider()
    filter_region = st.multiselect("Region Filter:", DEFAULTS['regions'])
    filter_status = st.multiselect("Status Filter:", DEFAULTS['status'])

    st.divider()
    f_up = st.file_uploader("Importér Leads (CSV/Excel)")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df_leads = force_clean(pd.concat([st.session_state.df_leads, nd], ignore_index=True))
        save_db(st.session_state.df_leads); st.rerun()

    if st.button("🚨 Nulstil Playground DB"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                conn.commit()
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. DASHBOARD ---
st.title("💼 Business Leads Playground")

df_display = st.session_state.df_leads.copy()

# Filtre
if filter_region:
    df_display = df_display[df_display['Region'].isin(filter_region)]
if filter_status:
    df_display = df_display[df_display['Status on lead'].isin(filter_status)]

search = st.text_input("🔍 Søg i alt (Navn, Person, By, Noter...)", "")
if search:
    df_display = df_display[df_display.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(f"Antal leads i view: **{len(df_display)}**")

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
