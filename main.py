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
st.set_page_config(page_title="Business Master CRM AI", layout="wide", page_icon="🎯")

# --- 2. DATABASE MOTOR ---
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
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
                
                # Hvis ingen brugere findes, opret admin fra Railway variabler eller fallback
                res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
                if res[0] == 0:
                    u = os.getenv("APP_USER", "admin")
                    p = os.getenv("APP_PASSWORD", "mgm2024")
                    conn.execute(text("INSERT INTO users VALUES (:u, :p, 'admin')"), {"u": u, "p": p})
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- 3. AI SCANNER FUNKTION ---
def analyze_image_with_ai(image_bytes):
    if not os.getenv("OPENAI_API_KEY"):
        return {"Company Name": "FEJL: Mangler OpenAI Key i Railway"}
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract info into JSON: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address. Return ONLY JSON."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }],
            response_format={ "type": "json_object" }
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"Company Name": f"AI Fejl: {str(e)}"}

# --- 4. LOGIN LOGIK ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def check_login():
    u, p = st.session_state.get("l_u"), st.session_state.get("l_p")
    rail_u = os.getenv("APP_USER", "admin")
    rail_p = os.getenv("APP_PASSWORD", "mgm2024")
    
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

# --- 5. MASTER DEFINITIONER ---
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
        "regions": ["Andalucía", "Madrid", "Valenciana"],
        "areas": ["Costa del Sol", "Costa Blanca"],
        "titles": ["Ejer", "Manager", "Marketingchef"],
        "agents": ["Brian", "Agent 1"],
        "lead_types": ["Inbound", "Outbound", "AI Scan 🤖"],
        "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"],
        "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat"],
        "underbrancher": ["Boligsalg", "Udlejning", "Tapas"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM pg_settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def add_option(t, v):
    if db_engine and v:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO pg_settings (type, value) VALUES (:t, :v)"), {"t": t, "v": v})
            conn.commit()

# --- 6. RENSE-MOTOR ---
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

if 'df_leads' not in st.session_state:
    try: st.session_state.df_leads = force_clean(pd.read_sql("SELECT * FROM merchants_playground", db_engine))
    except: st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 7. POPUP KORT ---
@st.dialog("🎯 Lead Administration", width="large")
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
        st.markdown("##### 🏢 Virksomheds Info")
        c_t1, c_t2 = st.columns(2)
        upd['Company Name'] = c_t1.text_input("Legal Name", value=row.get('Company Name'))
        upd['CIF Number VAT'] = c_t2.text_input("CIF / VAT Nummer", value=row.get('CIF Number VAT'))
        col1, col2 = st.columns(2)
        with col1:
            for f in ['Kontaktperson', 'Email', 'Phone number', 'Mobilnr']: upd[f] = st.text_input(f, value=row.get(f,''))
            upd['Titel'] = st.selectbox("Titel", opts['titles'], index=opts['titles'].index(row.get('Titel')) if row.get('Titel') in opts['titles'] else 0)
        with col2:
            for f in ['WhatsApp', 'Telegram', 'Facebook', 'Instagram', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
    with t2:
        c1, c2 = st.columns(2)
        with c1:
            upd['Region'] = st.selectbox("Region", opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0)
            upd['Area'] = st.selectbox("Område", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0)
            for f in ['Town', 'Address', 'Postal Code', 'Exact Location']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            for f, o in [('Brancher', 'brancher'), ('Underbrancher', 'underbrancher'), ('Languages', 'sprog')]:
                curr = [x.strip() for x in str(row.get(f)).split(',')] if row.get(f) else []
                upd[f] = ", ".join(st.multiselect(f, opts[o], default=[x for x in curr if x in opts[o]]))
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'))
    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Pipeline Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox("Medlemskab", ["Ingen", "Gratis", "Basis", "Premium", "VIP"], index=0)
            upd['Agent'] = st.selectbox("Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
        with c2:
            for f in ['Date created', 'Date for follow up', 'Kontakt dato']:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(f, value=d_v).strftime('%d/%m/%Y')
    with t4:
        upd['Business Description'] = st.text_area("Kort Pitch", value=row.get('Business Description'), height=100)
        upd['Description'] = st.text_area("Lang Annoncetekst", value=row.get('Description'), height=200)
        upd['Tracking_URL'] = st.text_input("QR Tracking URL", value=row.get('Tracking_URL'))
    with t5:
        upd['Noter'] = st.text_area("Interne CRM Noter", value=row.get('Noter'), height=200)
        c1, c2 = st.columns(2)
        with c1:
            l_up = st.file_uploader("Upload Kunde Logo", type=['png','jpg'], key=f"l_{idx}")
            if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        with c2:
            f_up = st.file_uploader("Upload Dokument (PDF)", key=f"f_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()

    c_s, c_d = st.columns([4, 1])
    if c_s.button("💾 GEM ALT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    if st.session_state.user_role == "admin" and c_d.button("🗑️ SLET", type="secondary", use_container_width=True):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads); st.rerun()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.header(f"👤 {st.session_state.username}")
    with st.expander("📸 AI Card Scanner"):
        cam = st.camera_input("Tag billede")
        if cam:
            with st.spinner("AI tænker..."):
                ai_data = analyze_image_with_ai(cam.read())
                nr = {c: "" for c in MASTER_COLS}
                nr.update(ai_data); nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Leadtype'] = "AI Scan 🤖"
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()
    
    with st.expander("🛠️ Admin Menu"):
        t_sel = st.selectbox("Ret Dropdown:", ["brancher", "underbrancher", "status", "agents", "sprog"])
        v_new = st.text_input("Nyt valg:")
        if st.button("Tilføj"): add_option(t_sel, v_new); st.rerun()
        if st.button("🚨 NULSTIL DB"):
            if db_engine:
                with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground")); conn.commit()
            st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    st.divider()
    if st.button("➕ OPRET MANUELT", use_container_width=True, type="primary"):
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Agent'] = st.session_state.username
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "leads.csv", use_container_width=True)
    if st.button("🚪 Log ud"): st.session_state.authenticated = False; st.rerun()

# --- 9. DASHBOARD ---
st.title("💼 Business CRM Master AI")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df_leads.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
