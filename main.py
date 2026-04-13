import st as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date
from openai import OpenAI

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
            return create_engine(db_url, pool_pre_ping=True)
        except: return None
    return None

db_engine = get_engine()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Funktion til at analysere billede med OpenAI
def analyze_image_with_ai(image_bytes):
    if not os.getenv("OPENAI_API_KEY"):
        return {"Company Name": "FEJL: Ingen API Key fundet i Railway"}
    
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Extract business card info into JSON. Use these keys: Company Name, CIF Number VAT, Kontaktperson, Email, Phone number, Website, Town, Address. Return ONLY JSON."},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ],
                }
            ],
            response_format={ "type": "json_object" }
        )
        import json
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"Company Name": f"AI Fejl: {str(e)}"}

def get_base64_image(file_path):
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return None

# --- 3. MASTER STRUKTUR ---
MASTER_COLS = [
    'Date created', 'Company Name', 'CIF Number VAT', 'Brancher', 'Underbrancher', 'Region', 'Area', 'Town', 
    'Postal Code', 'Address', 'Exact Location', 'Kontaktperson', 'Titel', 'Email', 
    'Phone number', 'Mobilnr', 'WhatsApp', 'Telegram', 'Facebook', 'Instagram', 
    'Languages', 'Business Description', 'Description', 'Status on lead', 'Leadtype', 
    'Agent', 'Membership', 'Advertising', 'Date for follow up', 'Kontakt dato', 'Work time', 
    'Tracking_URL', 'Noter', 'Fil_Navn', 'Fil_Data', 'Logo_Navn', 'Logo_Data'
]

DISPLAY_COLS = ['Date created', 'Company Name', 'Brancher', 'Town', 'Status on lead', 'Agent']

# --- 4. DROPDOWN ADMINISTRATION ---
def load_options():
    defaults = {
        "regions": ["Andalucía", "Cataluña", "Madrid", "Valenciana", "Galicia", "Canarias", "Baleares"],
        "areas": ["Costa del Sol", "Costa Blanca", "Costa Brava", "Mallorca", "Ibiza"],
        "titles": ["Ejer", "Manager", "Marketingchef", "Andet"],
        "agents": ["Brian", "Agent 1", "Agent 2"],
        "lead_types": ["Inbound", "Outbound", "AI Card Scan 🤖", "Reference"],
        "memberships": ["Ingen", "Gratis", "Basis", "Premium", "VIP"],
        "advertising": ["Ingen", "Standard", "Premium"],
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

def add_option(opt_type, value):
    if db_engine and value:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO pg_settings (type, value) VALUES (:t, :v)"), {"t": opt_type, "v": value})
            conn.commit()

# --- 5. RENSE- OG DATO-MOTOR ---
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
    except:
        st.session_state.df_leads = pd.DataFrame(columns=MASTER_COLS)
opts = load_options()

# --- 6. POPUP KORT ---
@st.dialog("🎯 Lead Administration & CRM Board", width="large")
def lead_popup(idx):
    row = st.session_state.df_leads.loc[idx].to_dict()
    
    c_h1, c_h2 = st.columns([0.8, 0.2])
    with c_h1: st.title(f"🏢 {row.get('Company Name') or 'Nyt Lead'}")
    with c_h2: 
        if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=80)

    st.divider()
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
        with c2:
            upd['Phone number'] = st.text_input("Kontor Tlf", value=row.get('Phone number'))
            upd['Mobilnr'] = st.text_input("Mobil", value=row.get('Mobilnr'))
            upd['WhatsApp'] = st.text_input("WhatsApp", value=row.get('WhatsApp'))
            upd['Website'] = st.text_input("Website URL", value=row.get('Website'))

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            upd['Region'] = st.selectbox("Region", opts['regions'], index=opts['regions'].index(row.get('Region')) if row.get('Region') in opts['regions'] else 0)
            upd['Area'] = st.selectbox("Område/Kyst", opts['areas'], index=opts['areas'].index(row.get('Area')) if row.get('Area') in opts['areas'] else 0)
            upd['Town'] = st.text_input("By", value=row.get('Town'))
        with c2:
            cur_br = [x.strip() for x in str(row.get('Brancher')).split(',')] if row.get('Brancher') else []
            upd['Brancher'] = ", ".join(st.multiselect("Brancher", opts['brancher'], default=[x for x in cur_br if x in opts['brancher']]))
            upd['Address'] = st.text_input("Vej & Nr.", value=row.get('Address'))

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            upd['Status on lead'] = st.selectbox("Status", opts['status'], index=opts['status'].index(row.get('Status on lead')) if row.get('Status on lead') in opts['status'] else 0)
            upd['Membership'] = st.selectbox("Medlemskab", opts['memberships'], index=opts['memberships'].index(row.get('Membership')) if row.get('Membership') in opts['memberships'] else 0)
        with c2:
            upd['Agent'] = st.selectbox("Agent", opts['agents'], index=opts['agents'].index(row.get('Agent')) if row.get('Agent') in opts['agents'] else 0)
            upd['Date created'] = st.date_input("Oprettet", value=get_safe_date(row.get('Date created'))).strftime('%d/%m/%Y')
            upd['Date for follow up'] = st.date_input("Opfølgning", value=get_safe_date(row.get('Date for follow up'))).strftime('%d/%m/%Y')

    with t4:
        upd['Business Description'] = st.text_area("Kort Pitch", value=row.get('Business Description'), height=100)
        upd['Description'] = st.text_area("Annoncetekst", value=row.get('Description'), height=200)

    with t5:
        upd['Noter'] = st.text_area("Logbog", value=row.get('Noter'), height=200)
        c_m1, c_m2 = st.columns(2)
        with c_m1:
            if row.get('Logo_Data'): st.image(f"data:image/png;base64,{row['Logo_Data']}", width=150)
            l_up = st.file_uploader("Upload Logo", type=['png','jpg'], key=f"l_{idx}")
            if l_up: upd['Logo_Navn'], upd['Logo_Data'] = l_up.name, base64.b64encode(l_up.read()).decode()
        with c_m2:
            if row.get('Fil_Navn'):
                if row.get('Fil_Data'): st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Hent fil</a>', unsafe_allow_html=True)
            f_up = st.file_uploader("Upload dokument", key=f"f_{idx}")
            if f_up: upd['Fil_Navn'], upd['Fil_Data'] = f_up.name, base64.b64encode(f_up.read()).decode()

    if st.button("💾 GEM LEAD DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df_leads.at[idx, k] = v
        if save_db(st.session_state.df_leads): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    l_mgm = get_base64_image("applogo.png")
    if l_mgm: st.markdown(f'<div style="text-align:center"><img src="data:image/png;base64,{l_mgm}" width="180"></div>', unsafe_allow_html=True)
    
    st.header("📸 AI Card Scanner")
    with st.expander("Scan visitkort med AI"):
        cam = st.camera_input("Tag billede")
        if cam:
            with st.spinner("AI analyserer billedet..."):
                ai_data = analyze_image_with_ai(cam.read())
                new_r = {c: "" for c in MASTER_COLS}
                new_r.update(ai_data)
                new_r['Date created'] = date.today().strftime('%d/%m/%Y')
                new_r['Leadtype'] = "AI Card Scan 🤖"
                st.session_state.df_leads = pd.concat([st.session_state.df_leads, pd.DataFrame([new_r])], ignore_index=True)
                save_db(st.session_state.df_leads)
                st.success("Lead oprettet fra AI scanning!")
                st.rerun()

    st.header("🎯 Kampagne Filter")
    f_br = st.multiselect("Branche:", opts['brancher'])
    f_reg = st.multiselect("Region:", opts['regions'])
    f_town = st.multiselect("By:", sorted([t for t in st.session_state.df_leads['Town'].unique() if t]))
    f_st = st.multiselect("Status:", opts['status'])

    with st.expander("🛠️ Dropdowns"):
        t_sel = st.selectbox("Type:", ["brancher", "regions", "areas", "titles", "agents", "lead_types", "memberships", "advertising", "status", "sprog"])
        v_new = st.text_input("Nyt navn:")
        if st.button("Tilføj") and v_new: add_option(t_sel, v_new); st.rerun()

    st.divider()
    if st.button("➕ OPRET MANUELT", use_container_width=True, type="primary"):
        new_row = pd.DataFrame([{c: "" for c in MASTER_COLS}])
        new_row['Date created'] = date.today().strftime('%d/%m/%Y')
        st.session_state.df_leads = pd.concat([st.session_state.df_leads, new_row], ignore_index=True)
        save_db(st.session_state.df_leads); st.rerun()

    st.download_button("📥 Master Export", st.session_state.df_leads.to_csv(index=False), "leads_master.csv", use_container_width=True)

# --- 8. DASHBOARD ---
st.title("💼 CRM Playground")
df_v = st.session_state.df_leads.copy()
if f_br: df_v = df_v[df_v['Brancher'].apply(lambda x: any(b in x for b in f_br))]
if f_reg: df_v = df_v[df_v['Region'].isin(f_reg)]
if f_town: df_v = df_v[df_v['Town'].isin(f_town)]
if f_st: df_v = df_v[df_v['Status on lead'].isin(f_st)]

search = st.text_input("🔍 Søg i alt data...")
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[DISPLAY_COLS], use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    lead_popup(real_idx) 
