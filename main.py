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
st.set_page_config(page_title="CRM Master Pro AI", layout="wide", page_icon="🎯")

# --- 2. DATABASE MOTOR (HÆRDET VERSION) ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            # Tilføjet pool_size og max_overflow for at forhindre hængende forbindelser
            return create_engine(db_url, pool_size=10, max_overflow=20, pool_pre_ping=True)
        except Exception as e:
            st.error(f"Database engine fejl: {e}")
    return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# SIKKER SKRIVE-FUNKTION (Forhindrer frys)
def db_execute(query, params=None):
    if not db_engine: return False
    try:
        with db_engine.connect() as conn:
            conn.execute(text(query), params or {})
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Fejl ved database-skrivning: {e}")
        return False

# Initialiser tabeller
if db_engine:
    with db_engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, data JSONB)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS pg_settings (type TEXT, value TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
        # Opret admin hvis den ikke findes
        res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
        if res[0] == 0:
            u, p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
            conn.execute(text("INSERT INTO users VALUES (:u, :p, 'admin')"), {"u": u, "p": p})

# --- 3. AI SCANNER ---
def analyze_image_with_ai(image_bytes):
    if not os.getenv("OPENAI_API_KEY"): return {"Company Name": "Mangler API Key"}
    b64_image = base64.b64encode(image_bytes).decode('utf-8')
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "Extract business card info into JSON. Keys: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address. Return ONLY raw JSON."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
            ]}],
            response_format={ "type": "json_object" }
        )
        return json.loads(response.choices[0].message.content)
    except: return {"Company Name": "AI Scanning Fejl"}

# --- 4. LOGIN SYSTEM ---
if "authenticated" not in st.session_state: st.session_state.authenticated = False

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    rail_u, rail_p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
    if u == rail_u and p == rail_p:
        st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, "admin", u
    elif db_engine:
        with db_engine.connect() as conn:
            res = conn.execute(text("SELECT password, role FROM users WHERE username = :u"), {"u": u}).fetchone()
            if res and res[0] == p:
                st.session_state.authenticated, st.session_state.user_role, st.session_state.username = True, res[1], u
            else: st.error("❌ Login fejlede")

if not st.session_state.authenticated:
    st.title("💼 Business CRM Master Login")
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("Brugernavn", key="l_u")
        st.text_input("Adgangskode", type="password", key="l_p")
        st.button("LOG IND", type="primary", use_container_width=True, on_click=check_login)
    st.stop()

# --- 5. MASTER DATA DEFINITIONER ---
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
        "agents": ["Brian", "Olga"], "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker"],
        "underbrancher": ["Boligsalg", "Udlejning", "Tapas"], "status": ["Ny", "Dialog", "Vundet", "Tabt"],
        "sprog": ["Dansk", "Engelsk", "Spansk"], "regions": ["Andalucía", "Madrid"],
        "areas": ["Costa del Sol"], "titles": ["CEO", "Manager", "Ejer"],
        "memberships": ["Ingen", "Basis", "VIP"], "advertising": ["Ingen", "Standard"],
        "lead_types": ["Inbound", "Outbound", "AI Scan"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM pg_settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

# --- 6. RENSE MOTOR ---
def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename_map = {'Merchant': 'Company Name', 'Programnavn': 'Company Name'}
    df = df.rename(columns=rename_map)
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00', 'nan.0'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Company Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants_playground', db_engine, if_exists='replace', index=False)
        return True
    return False

# Initialiser CRM Data
if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 7. KLIENT KORT POPUP ---
@st.dialog("🎯 Lead Detaljer & CRM", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    c1, c2 = st.columns([0.8, 0.2])
    with c1: st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    with c2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=80)
    st.divider()
    t1, t2, t3, t4, t5 = st.tabs(["📞 Kontakt", "🌍 Geografi", "⚙️ Salg", "📝 Beskrivelser", "📁 Medier & Noter"])
    upd = {}
    with t1:
        st.markdown("##### 🏢 Virksomheds Identifikation")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input("Legal Name", value=row.get('Company Name'), key=f"name_{idx}")
        upd['CIF Number VAT'] = ct2.text_input("CIF / VAT Nummer", value=row.get('CIF Number VAT'), key=f"cif_{idx}")
        c1, c2 = st.columns(2)
        with c1:
            for f in ['Kontaktperson', 'Email', 'Phone number', 'Mobilnr']: upd[f] = st.text_input(f, value=row.get(f,''), key=f"{f}_{idx}")
            upd['Titel'] = st.selectbox("Titel", opts['titles'], index=opts['titles'].index(row.get('Titel')) if row.get('Titel') in opts['titles'] else 0, key=f"tit_{idx}")
        with c2:
            for f in ['WhatsApp', 'Telegram', 'Facebook', 'Instagram', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''), key=f"{f}_{idx}")
    with t2:
        c1, c2 = st.columns(2)
        with c1:
            upd['Region'] = st.selectbox("Region", opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0, key=f"reg_{idx}")
            upd['Area'] = st.selectbox("Område", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0, key=f"area_{idx}")
            for f in ['Town', 'Address', 'Postal Code', 'Exact Location']: upd[f] = st.text_input(f, value=row.get(f,''), key=f"{f}_{idx}")
        with c2:
            for f, o in [('Brancher', 'brancher'), ('Underbrancher', 'underbrancher'), ('Languages', 'sprog')]:
                curr = [x.strip() for x in str(row.get(f)).split(',')] if row.get(f) else []
                upd[f] = ", ".join(st.multiselect(f, opts[o], default=[x for x in curr if x in opts[o]], key=f"{f}_{idx}"))
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'), key=f"work_{idx}")
    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Pipeline Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0, key=f"st_{idx}")
            upd['Membership'] = st.selectbox("Medlemskab", opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0, key=f"mem_{idx}")
            upd['Advertising'] = st.selectbox("Annonceprofil", opts['advertising'], index=opts['advertising'].index(row.get('Advertising')) if row.get('Advertising') in opts['advertising'] else 0, key=f"adv_{idx}")
        with c2:
            upd['Agent'] = st.selectbox("Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0, key=f"ag_{idx}")
            for f in ['Date created', 'Date for follow up', 'Kontakt dato']:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(f, value=d_v, key=f"{f}_{idx}").strftime('%d/%m/%Y')
    with t4:
        upd['Business Description'] = st.text_area("Kort Pitch", value=row.get('Business Description'), height=100, key=f"bd_{idx}")
        upd['Description'] = st.text_area("Annoncetekst", value=row.get('Description'), height=200, key=f"desc_{idx}")
        upd['Tracking_URL'] = st.text_input("QR Tracking URL", value=row.get('Tracking_URL'), key=f"qr_{idx}")
    with t5:
        upd['Noter'] = st.text_area("Interne CRM Noter", value=row.get('Noter'), height=200, key=f"note_{idx}")
        c1, c2 = st.columns(2)
        with c1:
            l_up = st.file_uploader("Logo", type=['png','jpg'], key=f"lu_{idx}")
            if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        with c2:
            f_up = st.file_uploader("Dokument (PDF/Excel)", key=f"fu_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()

    if st.button("💾 GEM ALT PÅ KLIENT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.header(f"👤 {st.session_state.username}")
    
    # AI SCANNER
    with st.expander("📸 AI Card Scanner"):
        cam = st.camera_input("Tag billede af visitkort")
        if cam:
            with st.spinner("Analyse i gang..."):
                ai_data = analyze_image_with_ai(cam.read())
                nr = {c: "" for c in MASTER_COLS}
                nr.update(ai_data); nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Leadtype'] = "AI Scan"
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()

    st.divider()
    # KATEGORI ADMINISTRATION (FORBEDRET)
    if st.session_state.user_role == "admin":
        with st.expander("🛠️ Dropdown Administration"):
            for label, key in [("Agent", "agents"), ("Branche", "brancher"), ("Status", "status"), ("Sprog", "sprog")]:
                st.markdown(f"**{label}**")
                v_new = st.text_input(f"Tilføj til {label}:", key=f"side_add_{key}")
                if st.button(f"Tilføj {label}", key=f"side_btn_{key}"):
                    if db_execute("INSERT INTO pg_settings (type, value) VALUES (:t, :v)", {"t": key, "v": v_new}):
                        st.success("Tilføjet!"); st.rerun()
            
            st.divider()
            nu, np = st.text_input("Nyt brugernavn:"), st.text_input("Kode:", type="password")
            if st.button("Opret Agent"):
                if db_execute("INSERT INTO users VALUES (:u,:p,'agent')", {"u":nu,"p":np}):
                    st.success("Agent oprettet!"); st.rerun()

    st.divider()
    # FILTRE
    st.subheader("🎯 Kampagne Filtre")
    f_br = st.multiselect("Branche:", opts['brancher'])
    f_st = st.multiselect("Status:", opts['status'])
    f_town = st.multiselect("By:", sorted([t for t in st.session_state.df_leads['Town'].unique() if t]))

    st.divider()
    if st.button("➕ OPRET MANUELT", use_container_width=True, type="primary"):
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Agent'] = st.session_state.username
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    
    if st.button("🚪 Log ud", use_container_width=True):
        st.session_state.authenticated = False; st.rerun()

# --- 9. DASHBOARD ---
st.title("💼 Business CRM Master AI")
df_v = st.session_state.df_leads.copy()

# Anvend filtre
if f_br: df_v = df_v[df_v['Brancher'].apply(lambda x: any(b in x for b in f_br))]
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_town: df_v = df_v[df_v['Town'].isin(f_town)]

search = st.text_input("🔍 Søg efter alt...")
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
