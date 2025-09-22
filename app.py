# app.py — Styrekandidat-screener (raskere + enklere filtre)
import io
import re
import requests
import pandas as pd
import streamlit as st

ENHETS_API = "https://data.brreg.no/enhetsregisteret/api/enheter"
PAGE_SIZE = 200
TIMEOUT = 30

st.set_page_config(page_title="Styrekandidat-screener", layout="wide")
st.title("Styrekandidat-screener")

# --- NACE segmenter ---
TECH = ["62", "63"]
ENERGI = ["35", "38", "39"]
FINANS = ["64", "65", "66"]
BYGG = ["41", "42", "43"]
INDUSTRI = [f"{i:02d}" for i in range(10, 34)]
TRANSPORT = ["49", "50", "51", "52", "53"]
HELSE = ["86", "87", "88"]
UTDANNING = ["85"]

SEGMENTS = {
    "Tech": TECH, "Energi": ENERGI, "Finans": FINANS, "Bygg/Anlegg": BYGG,
    "Industri": INDUSTRI, "Transport": TRANSPORT, "Helse": HELSE, "Utdanning": UTDANNING,
}
PUBLIC_ORGFORM = {"KOMM", "FYLKE", "KF", "FKF", "IKS", "STAT", "SF", "ORGL"}

# --- Fylke-prefiks ---
FYLKE_PREFIKS = {
    "Oslo": "03", "Viken": "30", "Innlandet": "34", "Vestfold og Telemark": "38",
    "Agder": "42", "Rogaland": "11", "Vestland": "46", "Møre og Romsdal": "15",
    "Trøndelag": "50", "Nordland": "18", "Troms": "19", "Finnmark": "20", "Buskerud": "33",
}

# --- Sidebar ---
with st.sidebar:
    st.header("Selskapsfiltre")
    fylke = st.selectbox("Fylke (prefiks-filter lokalt)", options=["(ingen)"] + list(FYLKE_PREFIKS.keys()))
    kommunenr_raw = st.text_input("Manuelle kommunenummer (komma-separert)")

    st.subheader("Industri/segment (NACE-grupper)")
    seg_flags = {name: st.checkbox(name, value=False) for name in SEGMENTS.keys()}

    st.subheader("Sektor")
    sektor_priv = st.checkbox("Privat", value=True)
    sektor_off = st.checkbox("Offentlig", value=True)

    top_n = st.number_input("Topp N selskaper (sortert på ansatte)", min_value=1, value=10, step=1)

    st.divider()
    st.header("Personfiltre")
    role_map = {
        "Daglig leder": ["DAGL"],
        "Styreleder": ["LEDER"],
        "Styremedlem": ["STYRMEDL"],
        "Varamedlem": ["VARA"],
        "Signaturberettiget": ["SIGN"],
        "Prokurist": ["PROK"],
    }
    role_flags = {name: st.checkbox(name, value=(name in ["Daglig leder", "Styreleder"])) for name in role_map.keys()}
    active_only = st.checkbox("Kun aktive roller", value=True)

    # Kjønn: valg skrur estimering automatisk på
    gender_filter = st.selectbox("Filtrer kjønn (estimat fra fornavn)", options=["Alle", "Kvinne", "Mann", "Ukjent"], index=0)

    st.divider()
    run = st.button("Kjør søk", type="primary")

# --- Helpers ---
def kommunenummer_list_from_text(txt:str):
    if not txt.strip():
        return []
    return [k.strip() for k in txt.split(",") if re.fullmatch(r"\d{4}", k.strip())]

def nace_hits(nace_codes):
    if not nace_codes:
        return []
    labels = []
    for name, prefixes in SEGMENTS.items():
        if any(code.startswith(p) for p in prefixes for code in nace_codes):
            labels.append(name)
    return labels or ["Annet"]

def infer_sector(enhet:dict) -> str:
    sekt = (enhet.get("institusjonellSektorkode") or {}).get("kode")
    if sekt and str(sekt).startswith("6"):
        return "Offentlig"
    orgform_kode = ((enhet.get("organisasjonsform") or {}).get("kode") or "").upper()
    if orgform_kode in PUBLIC_ORGFORM:
        return "Offentlig"
    return "Privat"

def pass_segment_filter_row(row, seg_flags:dict):
    if not any(seg_flags.values()):
        return True
    segs = set((row.get("segmenter") or "").split(", "))
    for name, on in seg_flags.items():
        if on and name in segs:
            return True
    return False

def pass_sector_filter_row(row, priv, off):
    if priv and off:
        return True
    if not (priv or off):
        return True
    return (row["sektor"] == "Privat" and priv) or (row["sektor"] == "Offentlig" and off)

@st.cache_data(show_spinner=False)
def fetch_enheter(page:int, size:int, kommunenummer_list):
    # prøv (sort+size), så (uten sort), så med size=200
    attempts = [
        {"sort": "antallAnsatte,desc", "size": size},
        {"sort": None,                   "size": size},
        {"sort": "antallAnsatte,desc",  "size": 200},
        {"sort": None,                  "size": 200},
    ]
    last_err = None
    for a in attempts:
        params = {"page": page, "size": a["size"]}
        if kommunenummer_list:
            params["kommunenummer"] = ",".join(kommunenummer_list)
        if a["sort"]:
            params["sort"] = a["sort"]
        try:
            r = requests.get(ENHETS_API, params=params, timeout=TIMEOUT)
            if r.status_code == 200 and "application/json" in r.headers.get("content-type",""):
                return r.json()
            last_err = (r.status_code, r.url, r.text[:800])
        except requests.RequestException as e:
            last_err = (str(e), None, None)

    # vis nyttig feilmelding i UI og stopp
    code, url, body = last_err if isinstance(last_err, tuple) else (None, None, None)
    st.error("Brreg-API avviste kallene. Sjekk detaljer under.")
    if url: st.code(f"URL: {url}")
    if code: st.code(f"Status: {code}")
    if body: st.code(body)
    st.stop()


def normalize_enheter(payload):
    out = []
    for e in payload.get("_embedded", {}).get("enheter", []):
        addr = e.get("forretningsadresse") or {}
        orgf = e.get("organisasjonsform") or {}
        nk = [ (e.get("naeringskode1") or {}).get("kode"),
               (e.get("naeringskode2") or {}).get("kode"),
               (e.get("naeringskode3") or {}).get("kode") ]
        nace_codes = [c for c in nk if c]
        out.append({
            "orgnr": e.get("organisasjonsnummer"),
            "navn": e.get("navn"),
            "hjemmeside": e.get("hjemmeside"),
            "kommune": addr.get("kommune"),
            "kommunenr": addr.get("kommunenummer"),
            "ansatte": e.get("antallAnsatte"),
            "orgform": orgf.get("kode"),
            "nace_codes": nace_codes,
            "segmenter": ", ".join(nace_hits(nace_codes)),
            "sektor": infer_sector(e),
        })
    return pd.DataFrame(out)

# --- Roller ---
ROLE_ENDPOINTS = [
    "https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller",
    "https://data.brreg.no/enhetsregisteret/api/roller/organisasjonsnummer/{orgnr}",
    "https://data.brreg.no/enhetsregisteret/api/roller/enheter/{orgnr}",
]

@st.cache_data(show_spinner=False)
def fetch_roles(orgnr:str):
    for url in ROLE_ENDPOINTS:
        try:
            r = requests.get(url.format(orgnr=orgnr), timeout=TIMEOUT)
            if r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
                return r.json()
        except requests.RequestException:
            pass
    return None

def parse_roles(payload):
    rows = []
    if not payload:
        return rows
    def walk(obj):
        if isinstance(obj, dict):
            role_type = obj.get("type") or obj.get("rolletype")
            person = obj.get("person") or {}
            navn = person.get("navn") or obj.get("navn")
            fradato = obj.get("fradato") or obj.get("registrertDato")
            tildato = obj.get("tildato") or obj.get("avregistrertDato")
            if navn and (role_type or obj.get("rolle")):
                rows.append({
                    "navn": navn,
                    "rolle": (obj.get("rolle") or role_type or "").upper(),
                    "fradato": fradato,
                    "tildato": tildato,
                })
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
    walk(payload)
    seen, out = set(), []
    for r in rows:
        key = (r["navn"], r["rolle"], r.get("fradato"), r.get("tildato"))
        if key not in seen:
            seen.add(key); out.append(r)
    return out

# --- Kjønn (automatisk når filter != Alle) ---
@st.cache_data(show_spinner=False)
def genderize(first_name:str):
    try:
        resp = requests.get("https://api.genderize.io", params={"name": first_name}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            gen = data.get("gender"); prob = data.get("probability"); count = data.get("count")
            if gen in ("male", "female"):
                return ("Mann" if gen == "male" else "Kvinne", prob, count)
    except requests.RequestException:
        pass
    return ("Ukjent", None, None)

def first_name_from_full(name:str):
    if not name: return ""
    return re.split(r"\s+", name.strip())[0]

# --- Run ---
if run:
    kommunenr_manual = kommunenummer_list_from_text(kommunenr_raw)
    fylkepref = FYLKE_PREFIKS.get(fylke) if fylke != "(ingen)" else None

    all_pages = []
    page = 0
    total = None
    try:
        with st.spinner("Henter selskaper fra Enhetsregisteret..."):
            while True:
                payload = fetch_enheter(page, PAGE_SIZE, kommunenr_manual)
                if total is None:
                    total = (payload.get("page") or {}).get("totalElements", 0)
                df_page = normalize_enheter(payload)

                # Lokal fylkefilter (prefiks på kommunenr)
                if fylkepref:
                    df_page = df_page[df_page["kommunenr"].fillna("").astype(str).str.startswith(fylkepref)]
                # Segment/sektor lokalt
                df_page = df_page[df_page.apply(lambda r: pass_segment_filter_row(r, seg_flags), axis=1)]
                df_page = df_page[df_page.apply(lambda r: pass_sector_filter_row(r, sektor_priv, sektor_off), axis=1)]

                all_pages.append(df_page)
                page += 1
                total_pages = (payload.get("page") or {}).get("totalPages", 1)

                # Siden API-et allerede er sortert etter ansatte desc kan vi stoppe
                # så snart vi har > top_n (med litt margin)
                have = pd.concat(all_pages).shape[0]
                if have >= max(top_n * 2, top_n + 50) or page >= total_pages:
                    break
    except requests.HTTPError as e:
        st.error("Feil fra Brreg-API (HTTPError). Prøv å justere filtre eller prøv igjen.")
        st.stop()

    companies_df = pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()
    if not companies_df.empty:
        companies_df["ansatte"] = pd.to_numeric(companies_df["ansatte"], errors="coerce")
        companies_top = companies_df.sort_values("ansatte", ascending=False, na_position="last").head(top_n).copy()
    else:
        companies_top = pd.DataFrame()

    # Roller -> personer
    people_rows = []
    if not companies_top.empty:
        with st.spinner("Henter roller for topp-selskap..."):
            for _, row in companies_top.iterrows():
                orgnr = str(row["orgnr"])
                rp = fetch_roles(orgnr)
                roles = parse_roles(rp)
                for rr in roles:
                    rolle_upper = (rr["rolle"] or "").upper()
                    active = not bool(rr.get("tildato"))

                    # Rollefilter
                    keep = False
                    for ui_label, codes in role_map.items():
                        if role_flags[ui_label] and any(rolle_upper.startswith(c) for c in codes):
                            keep = True; chosen_label = ui_label; break
                    if not any(role_flags.values()):
                        keep = True; chosen_label = rr["rolle"]
                    if not keep or (active_only and not active):
                        continue

                    # Kjønn-estimering slås kun på om filter != Alle
                    est_gender = ""; gender_prob = None
                    if gender_filter != "Alle":
                        first = first_name_from_full(rr["navn"])
                        kjonn, prob, _ = genderize(first)
                        est_gender, gender_prob = kjonn, prob
                        # filtrer iht. valgt kjønn
                        target = {"Kvinne":"Kvinne","Mann":"Mann","Ukjent":"Ukjent"}[gender_filter]
                        if kjonn != target:
                            continue

                    people_rows.append({
                        "Navn": rr["navn"],
                        "Estimert kjønn": est_gender,
                        "Kjønn sannsynlighet": gender_prob,
                        "Rolle": chosen_label,
                        "Selskap": row["navn"],
                        "Orgnr": orgnr,
                        "Ansatte": row["ansatte"],
                        "Industri": row["segmenter"],
                        "Sektor": row["sektor"],
                        "Start": rr.get("fradato"),
                        "Slutt": rr.get("tildato"),
                        "Brreg-lenke": f"https://w2.brreg.no/enhet/sok/detalj.jsp?orgnr={orgnr}",
                    })

    people_df = pd.DataFrame(people_rows)

    # --- Output: Selskaper ---
    st.subheader("Selskaper (topp N)")
    if not companies_top.empty:
        out_companies = companies_top[["navn","orgnr","kommune","ansatte","segmenter","sektor","hjemmeside"]].rename(columns={
            "navn":"Selskapsnavn","orgnr":"Orgnr","kommune":"Kommune","ansatte":"Ansatte",
            "segmenter":"Industri","sektor":"Sektor","hjemmeside":"Nettside",
        })
        st.dataframe(out_companies, width="stretch", hide_index=True)
        st.download_button("⬇️ Last ned selskaper (CSV)", data=out_companies.to_csv(index=False).encode("utf-8"),
                           file_name="companies_top.csv", mime="text/csv")
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
            out_companies.to_excel(w, index=False, sheet_name="Selskaper")
        st.download_button("⬇️ Last ned selskaper (XLSX)", data=xbuf.getvalue(),
                           file_name="companies_top.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Ingen selskaper funnet med gjeldende filtre.")

    # --- Output: Personer ---
    st.subheader("Personer (roller i topp-selskap)")
    if not people_df.empty:
        if gender_filter != "Alle":
            st.caption("Merk: Kjønn er estimert fra fornavn (usikkert).")
        st.dataframe(people_df, width="stretch", hide_index=True)
        st.download_button("⬇️ Last ned personer (CSV)", data=people_df.to_csv(index=False).encode("utf-8"),
                           file_name="people_roles.csv", mime="text/csv")
        xbuf2 = io.BytesIO()
        with pd.ExcelWriter(xbuf2, engine="xlsxwriter") as w:
            people_df.to_excel(w, index=False, sheet_name="Personer")
        st.download_button("⬇️ Last ned personer (XLSX)", data=xbuf2.getvalue(),
                           file_name="people_roles.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Ingen personer/roller funnet i topp-selskapene.")

    # --- Status ---
    st.markdown(
        f"**Topp N:** {top_n} • **Sektorfilter:** "
        f"{'Privat' if sektor_priv else ''}{'/' if (sektor_priv and sektor_off) else ''}{'Offentlig' if sektor_off else ''} • "
        f"**Kjønnsfilter:** {gender_filter}"
    )

st.caption("Kilde: Enhetsregisteret (åpne data). Roller hentes best-effort fra Brreg. Kjønn estimeres kun ved aktivt filter og er usikkert.")
