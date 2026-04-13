import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Lead Master Pro", layout="wide", page_icon="🎯")

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

# --- 3. STRUKTUR ---
MASTER_COLS = [
    'Date created', 'Company Name', 'Business Category', 'Area', 'Town', 'Status on lead', 'Membership',
    'CIF Number VAT', 'Business Description', 'Postal Code', 'Exact Location', 'Website', 'Email', 
    'Phone number', 'Address', 'Work time', 'Languages', 'Description', 'Date for follow up', 
    'Advertising', 'Pricelist', 'Agent', 'Leadtype', 'Tracking_URL', 'Noter'
]

# Kolonner der vises på forsiden
DISPLAY_COLS = ['Date created', 'Company Name', 'Business Category', 'Area', 'Status on lead', 'Membership']

# --- 4. RENSE-MOTOR ---
def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # Rens for tekniske mærkelige værdier
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    # Tving kolonne-rækkefølge
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# Indlæs data
if 'df_leads' not in st.session_state:
    try:
        df = pd.read_sql("SELECT * FROM merchants", db_engine)
        st.session_state.df_leads = force_clean(df)
    except:
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)

# --- 5. DROPDOWN VALGMULIGHEDER ---
# Disse kan senere gøres dynamiske i sidebaren ligesom i dit andet CRM
DEFAULT_CATEGORIES = ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Service", "Andet"]
DEFAULT_STATUS = ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"]

# --- 6. POPUP KORT ---
@st.dialog("🎯 Lead Administration", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name', 'Nyt Lead')}")
    
    t1, t2, t3 = st.tabs(["📌 Basis & Type", "⚙️ Salg & CRM", "📝 Beskrivelse & Noter"])
    upd = {}

    with t1:
        c1, c2 = st.columns(2)
        with c1:
            upd['Company Name'] = st.text_input("Virksomhedsnavn (Legal)", value=row.get('Company Name'))
            # Branche / Type Dropdown
            current_cat = row.get('Business Category', '')
            upd['Business Category'] = st.selectbox("Branche (Type)", DEFAULT_CATEGORIES, 
                                                   index=DEFAULT_CATEGORIES.index(current_cat) if current_cat in DEFAULT_CATEGORIES else 0)
            upd['Email'] = st.text_input("Email", value=row.get('Email'))
            upd['Phone number'] = st.text_input("Telefon", value=row.get('Phone number'))
        with c2:
            upd['Website'] = st.text_input("Website", value=row.get('Website'))
            upd['Tracking_URL'] = st.text_input("🔗 Din QR / Tracking URL", value=row.get('Tracking_URL'), help="Indsæt linket fra dit QR system her")
            upd['Town'] = st.text_input("By", value=row.get('Town'))
            upd['Area'] = st.text_input("Område", value=row.get('Area'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Pipeline Status", DEFAULT_STATUS, 
                                                index=DEFAULT_STATUS.index(row.get('Status on lead')) if row.get('Status on lead') in DEFAULT_STATUS else 0)
            upd['Membership'] = st.selectbox("Medlemskab", ["Ingen", "Gratis", "Basis", "Premium", "VIP"], 
                                            index=0)
        with c2:
            upd['Date created'] = st.text_input("Oprettet dato", value=row.get('Date created'))
            upd['Date for follow up'] = st.text_input("Opfølgningsdato", value=row.get('Date for follow up'))

    with t3:
        upd['Business Description'] = st.text_input("Kort pitch", value=row.get('Business Description'))
        upd['Noter'] = st.text_area("Logbog / Interne noter", value=row.get('Noter'), height=200)

    if st.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR KONTROL & FILTER ---
with st.sidebar:
    st.header("🎯 Kampagne Styring")
    
    # KATEGORI FILTER (VIGTIGT FOR DIT ØNSKE)
    all_cats = sorted(list(st.session_state.df_leads['Business Category'].unique()))
    if '' in all_cats: all_cats.remove('')
    
    selected_type = st.multiselect("Filtrer på Branche (Type):", ["Alle"] + DEFAULT_CATEGORIES, default="Alle")
    
    st.divider()
    
    if st.button("➕ OPRET NYT LEAD", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads)
        st.rerun()
    
    st.divider()
    st.subheader("Import")
    f_up = st.file_uploader("Upload Excel/CSV")
    if f_up and st.button("Flet"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df_leads = force_clean(pd.concat([st.session_state.df_leads, nd], ignore_index=True))
        save_db(st.session_state.df_leads); st.rerun()

# --- 8. DASHBOARD ---
st.title("💼 Lead Workspace")

# Anvend filter
df_view = st.session_state.df_leads.copy()

# Branche filtrering
if selected_type and "Alle" not in selected_type:
    df_view = df_view[df_view['Business Category'].isin(selected_type)]

# Søgefelt
search = st.text_input("🔍 Søg i navne eller byer...", "")
if search:
    df_view = df_view[df_view.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

st.write(f"Antal rækker fundet: **{len(df_view)}**")

# Vis tabellen
sel = st.dataframe(
    df_view[DISPLAY_COLS], 
    use_container_width=True, 
    selection_mode="single-row", 
    on_select="rerun", 
    height=600
)

if sel.selection.rows:
    real_idx = df_view.index[sel.selection.rows[0]]
    lead_popup(real_idx)
