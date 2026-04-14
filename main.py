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
    if not db_url: return None
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    try:
        engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS merchants_playground (id SERIAL PRIMARY KEY, data JSONB)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS crm_configs (id SERIAL PRIMARY KEY, type TEXT, value TEXT)"))
            res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
            if res[0] == 0:
                u, p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "mgm2024")
                conn.execute(text("INSERT INTO users VALUES (:u, :p, 'admin')"), {"u": u, "p": p})
        return engine
    except: return None

db_engine = get_engine()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def db_execute(query, params=None):
    try:
        with db_engine.begin() as conn:
            conn.execute(text(query), params or {})
        return True
    except Exception as e:
        st.error(f"Databasefejl: {e}")
        return False

# --- 3. AI & LOGIN (Kortet ned for overblik) ---
def analyze_image_with_ai(image_bytes):
    if not os.getenv("OPENAI_API_KEY"): return {"Company Name": "Mangler API Key"}
    b64_image = base64.b64encode(image_bytes).decode('utf-8')
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "Extract info into JSON: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
            ]}],
            response_format={ "type": "json_object" }
        )
        return json.loads(response.choices[0].message.content)
    except: return {"Company Name": "AI Fejl"}

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
    st.title("💼 CRM Master Login")
    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.text_input("Brugernavn", key="l_u")
        st.text_input("Adgangskode", type="password", key="l_p")
        st.button("LOG IND", type="primary", use_container_width=True, on_click=check_login)
    st.stop()

# --- 4. DATA STRUKTUR ---
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
        "agents": ["Brian", "Olga"],
        "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Turisme", "Andet"],
        "underbrancher": ["Boligsalg", "Udlejning", "Tapas", "Take-away", "VVS", "El", "Tømrer", "Murer", "Rengøring"],
        "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia", "Castilla y León", "País Vasco", "Canarias", "Murcia", "Aragón", "Extremadura", "Baleares", "Asturias", "Navarra", "Cantabria", "La Rioja"],
        "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Costa de la Luz", "Mallorca", "Ibiza", "Madrid City", "Barcelona City"],
        "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning", "Pause"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk", "Tysk"],
        "titles": ["CEO", "Ejer", "Manager", "Marketingchef"],
        "memberships": ["Ingen", "Gratis", "Basis", "Premium", "VIP"],
        "advertising": ["Ingen", "Standard Profil", "Premium Eksponering", "Banner kampagne"],
        "lead_types": ["Inbound", "Outbound", "AI Scan", "Reference"]
    }
    custom_only = {k: [] for k in defaults.keys()}
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM crm_configs", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored:
                    custom_only[key] = stored # Gemmer kun dem fra DB til slette-listen
                    defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults, custom_only

# --- 5. CRM MOTOR ---
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
opts, custom_opts = load_options()

# --- 6. POPUP ---
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
        st.markdown("##### 🏢 Identifikation")
        ct1, ct2 = st.columns(2)
        upd['Company Name'] = ct1.text_input("Legal Name", value=row.get('Company Name'), key=f"n_{idx}")
        upd['CIF Number VAT'] = ct2.text_input("CIF / VAT", value=row.get('CIF Number VAT'), key=f"c_{idx}")
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            for f in ['Kontaktperson', 'Email', 'Phone number', 'Mobilnr']: upd[f] = st.text_input(f, value=row.get(f,''), key=f"{f}_{idx}")
            upd['Titel'] = st.selectbox("Titel", opts['titles'], index=opts['titles'].index(row.get('Titel')) if row.get('Titel') in opts['titles'] else 0, key=f"t_{idx}")
        with col2:
            for f in ['WhatsApp', 'Telegram', 'Facebook', 'Instagram', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''), key=f"{f}_{idx}")
    with t2:
        c1, c2 = st.columns(2)
        with c1:
            upd['Region'] = st.selectbox("Region", opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0, key=f"r_{idx}")
            upd['Area'] = st.selectbox("Område", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0, key=f"a_{idx}")
            for f in ['Town', 'Address', 'Postal Code', 'Exact Location']: upd[f] = st.text_input(f, value=row.get(f,''), key=f"{f}_{idx}")
        with c2:
            for f, o in [('Brancher', 'brancher'), ('Underbrancher', 'underbrancher'), ('Languages', 'sprog')]:
                curr = [x.strip() for x in str(row.get(f)).split(',')] if row.get(f) else []
                upd[f] = ", ".join(st.multiselect(f, opts[o], default=[x for x in curr if x in opts[o]], key=f"{f}_{idx}"))
            upd['Work time'] = st.text_input("Åbningstider", value=row.get('Work time'), key=f"w_{idx}")
    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Pipeline Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0, key=f"s_{idx}")
            upd['Membership'] = st.selectbox("Medlemskab", opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0, key=f"m_{idx}")
            upd['Advertising'] = st.selectbox("Annonceprofil", opts['advertising'], index=opts['advertising'].index(row.get('Advertising')) if row.get('Advertising') in opts['advertising'] else 0, key=f"ad_{idx}")
        with c2:
            upd['Agent'] = st.selectbox("Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0, key=f"ag_{idx}")
            upd['Leadtype'] = st.selectbox("Kilde", opts['lead_types'], index=opts['lead_types'].index(row.get('Leadtype')) if row.get('Leadtype') in opts['lead_types'] else 0, key=f"lt_{idx}")
            for f in ['Date created', 'Date for follow up', 'Kontakt dato']:
                d_v = date.today() if not row.get(f) else pd.to_datetime(row.get(f), dayfirst=True, errors='coerce').date() or date.today()
                upd[f] = st.date_input(f, value=d_v, key=f"{f}_{idx}").strftime('%d/%m/%Y')
    with t4:
        upd['Business Description'] = st.text_area("Kort Pitch", value=row.get('Business Description'), height=100, key=f"bd_{idx}")
        upd['Description'] = st.text_area("Lang Annoncetekst", value=row.get('Description'), height=200, key=f"de_{idx}")
        upd['Tracking_URL'] = st.text_input("QR Tracking URL", value=row.get('Tracking_URL'), key=f"tr_{idx}")
    with t5:
        upd['Noter'] = st.text_area("CRM Logbog", value=row.get('Noter'), height=200, key=f"no_{idx}")
        c1, c2 = st.columns(2)
        if l_up := st.file_uploader("Logo", type=['png','jpg'], key=f"lu_{idx}"):
            upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        if f_up := st.file_uploader("Fil", key=f"fu_{idx}"):
            upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()
        st.file_uploader("Galleri", accept_multiple_files=True, key=f"ga_{idx}")

    if st.button("💾 GEM ALT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.header(f"👤 {st.session_state.username}")
    with st.expander("📸 AI Card Scanner"):
        if cam := st.camera_input("Tag billede"):
            with st.spinner("AI tænker..."):
                ai = analyze_image_with_ai(cam.read())
                nr = {c: "" for c in MASTER_COLS}
                nr.update(ai); nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Leadtype'] = "AI Scan"
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
                save_db(st.session_state.df_leads); st.rerun()

    if st.session_state.user_role == "admin":
        st.divider()
        with st.expander("🛠️ Dropdown Administration"):
            labels = [("Agent", "agents"), ("Branche", "brancher"), ("Status", "status"), ("Sprog", "sprog"), ("Medlemskab", "memberships"), ("Region", "regions"), ("Område", "areas")]
            for label, key in labels:
                st.markdown(f"**{label}**")
                v_new = st.text_input(f"Tilføj {label}:", key=f"add_{key}")
                if st.button(f"Tilføj", key=f"btn_a_{key}"):
                    if db_execute("INSERT INTO crm_configs (type, value) VALUES (:t, :v)", {"t": key, "v": v_new}):
                        st.success("Tilføjet!"); st.rerun()
                
                # SLET FUNKTION
                if custom_opts[key]:
                    v_del = st.selectbox(f"Slet fra {label}:", ["Vælg..."] + custom_opts[key], key=f"del_{key}")
                    if v_del != "Vælg..." and st.button(f"Slet valgte", key=f"btn_d_{key}"):
                        if db_execute("DELETE FROM crm_configs WHERE type = :t AND value = :v", {"t": key, "v": v_del}):
                            st.success("Slettet!"); st.rerun()
            
            st.divider()
            if st.button("🚨 NULSTIL DB", type="secondary"):
                with db_engine.begin() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants_playground"))
                st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS); st.rerun()

    st.divider()
    st.subheader("🎯 Kampagne Filtre")
    f_ag = st.multiselect("Agent:", opts['agents'])
    f_st = st.multiselect("Status:", opts['status'])
    f_br = st.multiselect("Branche:", opts['brancher'])
    f_re = st.multiselect("Region:", opts['regions'])
    f_to = st.multiselect("By:", sorted([t for t in st.session_state.df_leads['Town'].unique() if t]))

    if st.button("➕ OPRET MANUELT", type="primary", use_container_width=True):
        nr = {c: "" for c in MASTER_COLS}; nr['Date created'] = date.today().strftime('%d/%m/%Y'); nr['Agent'] = st.session_state.username
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([nr])], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "master.csv", use_container_width=True)
    if st.button("🚪 Log ud"): st.session_state.authenticated = False; st.rerun()

# --- 8. DASHBOARD ---
st.title("💼 Business CRM Master AI")
df_v = st.session_state.df_leads.copy()
if f_ag: df_v = df_v[df_v['Agent'].isin(f_ag)]
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]
if f_re: df_v = df_v[df_v['Region'].isin(f_re)]
if f_br: df_v = df_v[df_v['Brancher'].apply(lambda x: any(b in x for b in f_br))]
if f_to: df_v = df_v[df_v['Town'].isin(f_to)]

search = st.text_input("🔍 Søg...")
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx)
