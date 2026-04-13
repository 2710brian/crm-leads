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

# --- 2. DATABASE & AI MOTOR ---
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
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# AI Funktion til visitkort
def analyze_card_ai(image_bytes):
    if not os.getenv("OPENAI_API_KEY"):
        return {"Company Name": "FEJL: Mangler API Key i Railway"}
    b64_image = base64.b64encode(image_bytes).decode('utf-8')
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract business card data into JSON. Use these keys exactly: Company Name, Kontaktperson, Email, Phone number, Website, Town, Address. Return ONLY raw JSON."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64_image}"}}
                ]
            }],
            response_format={ "type": "json_object" }
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"Company Name": f"AI Fejl: {str(e)}"}

# --- 3. MASTER DEFINITIONER ---
MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data'
]

DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

# --- 4. DROPDOWNS ---
def load_options():
    defaults = {
        "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia"],
        "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Mallorca"],
        "titles": ["Ejer", "Manager", "Marketingchef", "Andet"],
        "agents": ["Brian", "Agent 1", "Agent 2"],
        "lead_types": ["Inbound", "Outbound", "AI Scan 🤖", "Reference"],
        "memberships": ["Ingen", "Gratis", "Basis", "Premium", "VIP"],
        "advertising": ["Ingen", "Standard Profil", "Premium Eksponering"],
        "status": ["Ny", "Dialog", "Vundet", "Tabt", "Opfølgning"],
        "brancher": ["Ejendomsmægler", "Restaurant", "Håndværker", "Advokat", "Butik", "Turisme", "Andet"],
        "underbrancher": ["Boligsalg", "Udlejning", "Tapas", "Take-away", "VVS", "El"],
        "sprog": ["Dansk", "Engelsk", "Spansk", "Svensk", "Norsk"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM pg_settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

# --- 5. DATA MANAGEMENT ---
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

if 'df_leads' not in st.session_state:
    try:
        df = pd.read_sql("SELECT * FROM merchants_playground", db_engine)
        st.session_state.df_leads = force_clean(df)
    except:
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 6. POPUP KORT ---
@st.dialog("🎯 Lead Administration", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    t1, t2, t3, t4, t5 = st.tabs(["📞 Kontakt", "🌍 Geografi", "⚙️ Salg", "📝 Beskrivelser", "📁 Medier"])
    upd = {}

    with t1:
        st.markdown("##### 🏢 Virksomheds Identifikation")
        c_top1, c_top2 = st.columns(2)
        upd['Company Name'] = c_top1.text_input("Virksomhedsnavn", value=row.get('Company Name'))
        upd['CIF Number VAT'] = c_top2.text_input("CIF / VAT", value=row.get('CIF Number VAT'))
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            upd['Kontaktperson'] = st.text_input("Navn", value=row.get('Kontaktperson'))
            upd['Titel'] = st.selectbox("Titel", opts['titles'], index=opts['titles'].index(row.get('Titel')) if row.get('Titel') in opts['titles'] else 0)
            upd['Email'] = st.text_input("E-mail", value=row.get('Email'))
            upd['Mobilnr'] = st.text_input("Mobil", value=row.get('Mobilnr'))
        with c2:
            upd['WhatsApp'] = st.text_input("WhatsApp", value=row.get('WhatsApp'))
            upd['Telegram'] = st.text_input("Telegram", value=row.get('Telegram'))
            upd['Website'] = st.text_input("Website URL", value=row.get('Website'))

    with t2:
        c1, c2 = st.columns(2)
        upd['Region'] = c1.selectbox("Region", opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0)
        upd['Area'] = c1.selectbox("Område", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0)
        upd['Town'] = c2.text_input("By", value=row.get('Town'))
        upd['Address'] = c2.text_input("Vej & Nr.", value=row.get('Address'))
        cur_br = [x.strip() for x in str(row.get('Brancher')).split(',')] if row.get('Brancher') else []
        upd['Brancher'] = ", ".join(st.multiselect("Brancher", opts['brancher'], default=[x for x in cur_br if x in opts['brancher']]))

    with t3:
        c1, c2 = st.columns(2)
        upd['Status on lead'] = c1.selectbox("Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
        upd['Agent'] = c1.selectbox("Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
        upd['Date created'] = c2.date_input("Oprettet", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
        upd['Date for follow up'] = c2.date_input("Opfølgning", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')

    with t5:
        upd['Noter'] = st.text_area("Logbog", value=row.get('Noter'), height=200)
        c_m1, c_m2 = st.columns(2)
        with c_m1:
            if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=150)
            l_up = st.file_uploader("Upload Logo", type=['png','jpg'], key=f"l_{idx}")
            if l_up: upd['Logo_Data'] = base64.b64encode(l_up.read()).decode()
        with c_m2:
            f_up = st.file_uploader("Upload dokument", key=f"f_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()

    col_btn1, col_btn2 = st.columns([4, 1])
    if col_btn1.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()
    
    # SLET KNAP INDE PÅ KORTET
    if col_btn2.button("🗑️ SLET", type="secondary", use_container_width=True):
        st.session_state.df_leads = st.session_state.df_leads.drop(idx)
        save_db(st.session_state.df_leads)
        st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.header("📸 AI Scanner")
    cam = st.camera_input("Scan visitkort")
    if cam:
        with st.spinner("GPT-4o analyserer..."):
            ai_results = analyze_card_ai(cam.read())
            new_r = {c: "" for c in MASTER_COLS}
            new_r.update(ai_results)
            new_r['Date created'] = date.today().strftime('%d/%m/%Y')
            new_r['Leadtype'] = "AI Scan 🤖"
            st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([new_r])], ignore_index=True)
            save_db(st.session_state.df_leads); st.rerun()

    st.divider()
    if st.button("➕ OPRET MANUELT", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()
    
    # SLET KNAP I SIDEBAR
    if 'sel_row' in st.session_state and st.session_state.sel_row is not None:
        if st.button("🗑️ SLET VALGTE LEAD", use_container_width=True):
            st.session_state.df_leads = st.session_state.df_leads.drop(st.session_state.sel_row)
            save_db(st.session_state.df_leads)
            st.session_state.sel_row = None
            st.rerun()

    st.divider()
    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "leads.csv", use_container_width=True)

# --- 8. DASHBOARD ---
st.title("💼 CRM Playground AI")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df_leads.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    st.session_state.sel_row = real_idx
    if st.button(f"✏️ Åbn kort for {df_v.loc[real_idx, 'Company Name']}", type="primary"):
        lead_popup(real_idx)
else:
    st.session_state.sel_row = None
