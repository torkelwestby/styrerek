# app.py — Styrekandidat-screener (persistens + profil + medieomtale, historikk-fix)
import io
import os
import re
import html
from datetime import datetime, date
import requests
import pandas as pd
import streamlit as st

try:
    import feedparser  # for RSS fallback
except Exception:
    feedparser = None

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

# --- Session state init ---
defaults = {
    "companies_top": None,
    "people_df": None,
    "last_filters": {},
    "selected_person": "(ingen)",
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

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
        "Daglig leder": ["DAGL", "DAGLIG LEDER", "DAGLIG_LEDER"],
        "Styreleder":   ["LEDE", "STYRELEDER", "LEDER"],
        "Styremedlem":  ["MEDL", "STYREMEDLEM", "STYRMEDL"],
        "Varamedlem":   ["VARA", "VARAMEDLEM"],
        "Signaturberettiget": ["SIGNATUR", "SIGN"],
        "Prokurist":    ["PROKURIST", "PROK"],
    }
    role_flags = {name: st.checkbox(name, value=(name in ["Daglig leder", "Styreleder"])) for name in role_map.keys()}
    active_only = st.checkbox("Kun aktive roller", value=True)

    gender_filter = st.radio("Kjønnsfilter (estimat fra fornavn)", options=["Alle", "Dame", "Mann"], index=0, horizontal=True)

    st.divider()
    col_a, col_b = st.columns(2)
    with col_a:
        run = st.button("Kjør søk", type="primary")
    with col_b:
        if st.button("Tøm cache"):
            st.cache_data.clear()
            st.experimental_rerun()

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
    attempts = [
        {"sort": "antallAnsatte,desc", "size": size},
        {"sort": None,                  "size": size},
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
            r = requests.get(ENHETS_API, params=params, timeout=TIMEOUT, headers={"Accept":"application/json"})
            if r.status_code == 200 and "application/json" in r.headers.get("content-type",""):
                return r.json()
            last_err = (r.status_code, r.url, r.text[:800])
        except requests.RequestException as e:
            last_err = (str(e), None, None)
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

# --- Roller (robust + historikk) ---
@st.cache_data(show_spinner=False)
def fetch_roles(orgnr: str):
    attempts = [
        ("https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller",
         {"includeHistorikk": "true"}),
        ("https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller",
         {"inkluderHistorikk": "true"}),
        ("https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller",
         {"historikk": "true"}),
        ("https://data.brreg.no/enhetsregisteret/api/roller/organisasjonsnummer/{orgnr}",
         {"inkluderHistorikk": "true"}),
        ("https://data.brreg.no/enhetsregisteret/api/roller/enheter/{orgnr}",
         {"inkluderHistorikk": "true"}),
    ]
    headers = {"Accept": "application/json", "Accept-Language": "nb-NO"}
    for url, params in attempts:
        try:
            r = requests.get(url.format(orgnr=orgnr), params=params, timeout=TIMEOUT, headers=headers)
            if r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
                return r.json()
        except requests.RequestException:
            pass
    return None

def is_now_active(role_dict: dict) -> bool:
    """Aktiv hvis ingen tildato OG ikke fratrådt."""
    tildato = role_dict.get("tildato")
    fratraadt = role_dict.get("fratraadt")
    return (tildato in (None, "",)) and (fratraadt in (None, False))

def _join_name(n):
    if isinstance(n, str):
        return n.strip()
    if isinstance(n, dict):
        parts = [n.get("fornavn"), n.get("mellomnavn"), n.get("etternavn")]
        return " ".join([p for p in parts if p]).strip()
    return None

def _role_to_text_and_code(r):
    raw = r.get("rolle") or r.get("type") or r.get("rolletype")
    if isinstance(raw, str):
        return raw, raw
    if isinstance(raw, dict):
        return raw.get("beskrivelse") or raw.get("kode") or "", raw.get("kode") or ""
    return "", ""

def parse_roles(payload):
    """Trekk ut både aktive og historiske roller uansett hvor i JSON de ligger."""
    rows = []
    if not isinstance(payload, (dict, list)):
        return rows

    def add_from_r(r):
        navn = _join_name(((r.get("person") or {}).get("navn")) or r.get("navn"))
        tekst, kode = _role_to_text_and_code(r)
        fradato = r.get("fradato") or r.get("registrertDato")
        tildato = r.get("tildato") or r.get("avregistrertDato")
        fratraadt = r.get("fratraadt")
        if navn and (tekst or kode):
            rows.append({
                "navn": navn,
                "rolle_tekst": (tekst or kode).upper(),
                "rolle_kode": (kode or tekst).upper(),
                "fradato": fradato,
                "tildato": tildato,
                "fratraadt": fratraadt,
            })

    # 1) Les eksplisitte rollegrupper (typisk aktive)
    if isinstance(payload, dict) and "rollegrupper" in payload:
        for g in payload.get("rollegrupper") or []:
            for r in g.get("roller") or []:
                add_from_r(r)

    # 2) Skann hele payload for alt som *kan* se ut som roller (inkl. historikkfelter)
    def walk(obj):
        if isinstance(obj, dict):
            # nøkler som ofte rommer historikk
            for key in ("historikk", "historiskeRoller", "tidligereRoller", "roller", "rolle", "rolletype", "type"):
                if key in obj:
                    add_from_r(obj)
                    break
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for v in obj:
                if isinstance(v, (dict, list)):
                    walk(v)
    walk(payload)

    # dedup
    seen, out = set(), []
    for r in rows:
        key = (r["navn"], r["rolle_kode"], r.get("fradato"), r.get("tildato"))
        if key not in seen:
            seen.add(key); out.append(r)
    return out

# --- Kjønn (alltid vist; filter valgfritt) ---
@st.cache_data(show_spinner=False)
def genderize(first_name:str):
    if not first_name:
        return "Ukjent"
    try:
        resp = requests.get("https://api.genderize.io", params={"name": first_name}, timeout=10)
        if resp.status_code == 200 and "application/json" in resp.headers.get("content-type",""):
            data = resp.json()
            gen = data.get("gender")
            if gen == "male":
                return "Mann"
            if gen == "female":
                return "Dame"
    except requests.RequestException:
        pass
    return "Ukjent"

def first_name_from_full(name:str):
    if not name: return ""
    return re.split(r"\s+", name.strip())[0]

def _parse_date(s):
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except Exception:
            continue
    return None

def _duration_human(start:date|None, end:date|None):
    if not start: return ""
    end = end or date.today()
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if months < 0: months = 0
    years = months // 12
    rem_m = months % 12
    if years and rem_m:
        return f"{years} år {rem_m} mnd"
    if years:
        return f"{years} år"
    return f"{rem_m} mnd"

# --- Media mentions: NewsAPI -> GDELT -> RSS (samler 5 nyeste) ---
FEEDS = [
    "https://www.nrk.no/toppsaker.rss",
    "https://e24.no/rss",
    "https://www.dn.no/rss",
    "https://www.hegnar.no/rss",
]

def _try_parse_dt(s: str):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%d", "%Y%m%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z","").replace("+00:00",""))
    except Exception:
        return None

def search_mentions_newsapi(person_name: str, page_size: int = 50):
    key = st.secrets.get("NEWSAPI_KEY", "")
    if not key:
        return []
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": person_name,
        "language": "no",
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "apiKey": key,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        out = []
        for a in data.get("articles", []):
            dt = _try_parse_dt(a.get("publishedAt"))
            out.append({
                "title": a.get("title"),
                "url": a.get("url"),
                "source": (a.get("source") or {}).get("name"),
                "date": dt,
                "source_type": "NewsAPI",
            })
        return out
    except Exception:
        return []

def search_mentions_gdelt(person_name: str, maxrecords: int = 50):
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {"query": f'{person_name} sourceCountry:NO', "mode": "ArtList", "maxrecords": maxrecords, "format": "json"}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        out = []
        for a in data.get("articles", []):
            dt = _try_parse_dt(a.get("seendate"))
            out.append({
                "title": a.get("title"),
                "url": a.get("url"),
                "source": a.get("sourceCommonName"),
                "date": dt,
                "source_type": "GDELT",
            })
        return out
    except Exception:
        return []

def search_mentions_rss(person_name: str, limit_per_feed: int = 50):
    if not feedparser:
        return []
    out = []
    q = (person_name or "").lower()
    for f in FEEDS:
        try:
            d = feedparser.parse(f)
            src = d.feed.get("title", "")
            for e in d.entries[:limit_per_feed]:
                text = " ".join([e.get("title",""), html.unescape(e.get("summary",""))]).lower()
                if q and q in text:
                    dt = _try_parse_dt(e.get("published") or e.get("updated") or "")
                    out.append({
                        "title": e.get("title",""),
                        "url": e.get("link",""),
                        "source": src,
                        "date": dt,
                        "source_type": "RSS",
                    })
        except Exception:
            continue
    return out

def search_mentions(person_name: str) -> list[dict]:
    items = []
    items += search_mentions_newsapi(person_name) or []
    items += search_mentions_gdelt(person_name) or []
    items += search_mentions_rss(person_name) or []
    items.sort(key=lambda x: (x.get("date") or datetime.min), reverse=True)
    return items[:5]

# --- RUN SEARCH ---
if run:
    kommunenr_manual = kommunenummer_list_from_text(kommunenr_raw)
    fylkepref = FYLKE_PREFIKS.get(fylke) if fylke != "(ingen)" else None

    # Hent selskaper
    all_pages = []
    page = 0
    with st.spinner("Henter selskaper fra Enhetsregisteret..."):
        while True:
            payload = fetch_enheter(page, PAGE_SIZE, kommunenr_manual)
            df_page = normalize_enheter(payload)

            if fylkepref:
                df_page = df_page[df_page["kommunenr"].fillna("").astype(str).str.startswith(fylkepref)]
            df_page = df_page[df_page.apply(lambda r: pass_segment_filter_row(r, seg_flags), axis=1)]
            df_page = df_page[df_page.apply(lambda r: pass_sector_filter_row(r, sektor_priv, sektor_off), axis=1)]

            all_pages.append(df_page)
            page += 1
            total_pages = (payload.get("page") or {}).get("totalPages", 1)

            have = pd.concat(all_pages).shape[0]
            if have >= max(top_n * 2, top_n + 50) or page >= total_pages:
                break

    companies_df = pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()
    if not companies_df.empty:
        companies_df["ansatte"] = pd.to_numeric(companies_df["ansatte"], errors="coerce")
        companies_top = companies_df.sort_values("ansatte", ascending=False, na_position="last").head(top_n).copy()
    else:
        companies_top = pd.DataFrame()

    # Roller -> personer (ALLTID hent alle roller; filtrer 'aktive' etterpå basert på active_only)
    people_rows = []
    if not companies_top.empty:
        with st.spinner("Henter roller for topp-selskap..."):
            for _, row in companies_top.iterrows():
                payload = fetch_roles(str(row["orgnr"]))
                roles = parse_roles(payload)
                for rr in roles:
                    rolle_code = (rr.get("rolle_kode") or "").upper()
                    rolle_text = (rr.get("rolle_tekst") or rolle_code)
                    now_active = is_now_active(rr)

                    # Rollefilter (inkluder ALLE hvis ingen er huket)
                    chosen_label = rolle_text
                    keep = False
                    for ui_label, codes in role_map.items():
                        if role_flags[ui_label] and any(rolle_code.startswith(c) for c in codes):
                            keep = True; chosen_label = ui_label; break
                    if not any(role_flags.values()):
                        keep = True
                    if not keep:
                        continue

                    # Aktiv-filter (GJØRES NÅ, etter at alle roller er hentet)
                    if active_only and not now_active:
                        continue

                    kjonn = genderize(first_name_from_full(rr.get("navn", "")))
                    people_rows.append({
                        "Navn": rr.get("navn"),
                        "Kjønn": kjonn,
                        "Rolle": chosen_label,
                        "Selskap": row["navn"],
                        "Ansatte": row["ansatte"],
                        "Industri": row["segmenter"],
                        "Sektor": row["sektor"],
                        "Nåværende": bool(now_active),
                        "_start": rr.get("fradato"),
                        "_slutt": rr.get("tildato"),
                        "Brreg-lenke": f"https://w2.brreg.no/enhet/sok/detalj.jsp?orgnr={row['orgnr']}",
                    })

    people_df_all = pd.DataFrame(people_rows)

    # Lagre i session_state og reset valgt person
    st.session_state.companies_top = companies_top
    st.session_state.people_df = people_df_all
    st.session_state.last_filters = {
        "sektor_priv": sektor_priv,
        "sektor_off": sektor_off,
        "gender_filter": gender_filter,
        "active_only": bool(active_only),
        "top_n": int(top_n),
    }
    st.session_state.selected_person = "(ingen)"

# --- RENDER (bruk session_state hvis data finnes) ---
companies_top = st.session_state.companies_top
people_df_all = st.session_state.people_df
last = st.session_state.last_filters

def build_full_profile_for_person(name: str, companies_top_df: pd.DataFrame) -> pd.DataFrame:
    """Hent alle roller personen har i topp-selskapene (alltid komplett historikk)."""
    if not isinstance(companies_top_df, pd.DataFrame) or companies_top_df.empty:
        return pd.DataFrame()

    rows = []
    for _, c in companies_top_df.iterrows():
        payload = fetch_roles(str(c["orgnr"]))
        roles = parse_roles(payload)
        for rr in roles:
            n = (rr.get("navn") or "").strip()
            if n.lower() == name.strip().lower():
                now_active = is_now_active(rr)
                rows.append({
                    "Rolle": rr.get("rolle_tekst") or rr.get("rolle_kode"),
                    "Selskap": c["navn"],
                    "Industri": c["segmenter"],
                    "Sektor": c["sektor"],
                    "Nåværende": bool(now_active),
                    "_start": rr.get("fradato"),
                    "_slutt": rr.get("tildato"),
                    "Brreg-lenke": f"https://w2.brreg.no/enhet/sok/detalj.jsp?orgnr={c['orgnr']}",
                })
    return pd.DataFrame(rows)

if companies_top is not None and people_df_all is not None:
    # Selskaper
    st.subheader("Selskaper")
    if not companies_top.empty:
        out_companies = companies_top[
            ["navn","orgnr","kommune","ansatte","segmenter","sektor","hjemmeside"]
        ].rename(columns={
            "navn":"Selskapsnavn","orgnr":"Orgnr","kommune":"Kommune","ansatte":"Ansatte",
            "segmenter":"Industri","sektor":"Sektor","hjemmeside":"Nettside",
        })
        st.dataframe(out_companies, width="stretch", hide_index=True)
        st.download_button("⬇️ Last ned selskaper (CSV)",
                           data=out_companies.to_csv(index=False).encode("utf-8"),
                           file_name="companies_top.csv", mime="text/csv")
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
            out_companies.to_excel(w, index=False, sheet_name="Selskaper")
        st.download_button("⬇️ Last ned selskaper (XLSX)", data=xbuf.getvalue(),
                           file_name="companies_top.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Ingen selskaper funnet med gjeldende filtre.")

    # Personer (apply kjønnsfilter ved visning)
    st.subheader("Personer (roller i topp-selskap)")
    people_df = people_df_all.copy()
    gf = last.get("gender_filter", "Alle")
    if gf != "Alle":
        people_df = people_df[people_df["Kjønn"] == gf]

    if not people_df.empty:
        visible_people = people_df.drop(columns=["_start","_slutt"], errors="ignore")
        st.dataframe(visible_people, width="stretch", hide_index=True)
        st.download_button("⬇️ Last ned personer (CSV)",
                           data=visible_people.to_csv(index=False).encode("utf-8"),
                           file_name="people_roles.csv", mime="text/csv")
        xbuf2 = io.BytesIO()
        with pd.ExcelWriter(xbuf2, engine="xlsxwriter") as w:
            visible_people.to_excel(w, index=False, sheet_name="Personer")
        st.download_button("⬇️ Last ned personer (XLSX)", data=xbuf2.getvalue(),
                           file_name="people_roles.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Mini-profil (full historikk uansett filtre)
        uniq = sorted(people_df["Navn"].dropna().unique().tolist())
        if "selected_person" not in st.session_state:
            st.session_state.selected_person = "(ingen)"
        pick = st.selectbox("Vis profil for person:", ["(ingen)"] + uniq, key="selected_person")

        if pick != "(ingen)":
            prof = build_full_profile_for_person(pick, companies_top)
            if prof.empty:
                prof = people_df_all[people_df_all["Navn"] == pick][[
                    "Rolle","Selskap","Industri","Sektor","Nåværende","_start","_slutt","Brreg-lenke"
                ]].copy()

            # dato/ansiennitet
            prof["Start_d"] = prof["_start"].apply(_parse_date)
            prof["Slutt_d"] = prof["_slutt"].apply(_parse_date)
            prof["Ansiennitet"] = prof.apply(lambda r: _duration_human(r["Start_d"], r["Slutt_d"]), axis=1)

            ant_roller = len(prof)
            ant_aktive = int(prof["Nåværende"].sum())
            rollelabels = sorted(prof["Rolle"].dropna().unique().tolist())
            bransjer = sorted({b for s in prof["Industri"].fillna("").tolist() for b in (s.split(", ") if s else []) if b})
            sektorer = sorted(prof["Sektor"].dropna().unique().tolist())

            st.markdown(f"**Profil – {pick}**")
            bits = [f"{pick} har {ant_roller} registrerte roller i dette uttrekket ({ant_aktive} aktive)."]
            if rollelabels: bits.append(f"Typiske roller: {', '.join(rollelabels)}.")
            if bransjer: bits.append(f"Bransjer: {', '.join(bransjer)}.")
            if sektorer: bits.append(f"Sektor: {', '.join(sektorer)}.")
            st.write(" ".join(bits))

            show_prof = prof[[
                "Rolle","Selskap","Industri","Sektor","Nåværende","_start","_slutt","Ansiennitet","Brreg-lenke"
            ]].rename(columns={"_start":"Start","_slutt":"Slutt"})
            st.dataframe(show_prof, width="stretch", hide_index=True)

            # Medieomtale (5 nyeste samlet)
            mentions = search_mentions(pick)
            if mentions:
                st.markdown("**Nevnt i media (nyeste først):**")
                for m in mentions:
                    title = m.get("title") or "(uten tittel)"
                    url = m.get("url") or ""
                    source = m.get("source") or ""
                    dt = m.get("date")
                    date_str = dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else ""
                    src_tag = m.get("source_type")
                    st.markdown(
                        f"- [{title}]({url}) — {source}{(' • ' + date_str) if date_str else ''}  \n"
                        f"  <sub>via {src_tag}</sub>",
                        unsafe_allow_html=True
                    )
            else:
                st.caption("Ingen medietreff funnet (NewsAPI/GDELT/RSS).")
    else:
        st.info("Ingen personer/roller funnet i topp-selskapene.")

    # Status (faktiske tall)
    st.markdown(
        f"**Antall selskaper vist:** {0 if companies_top is None else len(companies_top):,} • "
        f"**Antall personer vist:** {len(people_df) if 'people_df' in locals() else 0:,} • "
        f"**Sektorfilter:** "
        f"{'Privat' if last.get('sektor_priv', True) else ''}{'/' if (last.get('sektor_priv', True) and last.get('sektor_off', True)) else ''}{'Offentlig' if last.get('sektor_off', True) else ''} • "
        f"**Kjønnsfilter:** {last.get('gender_filter','Alle')} • "
        f"**Kun aktive roller:** {'Ja' if last.get('active_only', True) else 'Nei'}"
    )
else:
    st.info("Kjør et søk fra venstremenyen for å vise resultater.")

st.caption("Kilde: Enhetsregisteret (åpne data). Roller hentes best-effort fra Brreg (inkl. historikk). Kjønn estimeres fra fornavn og er usikkert. Medieomtale er valgfri (NewsAPI/GDELT/RSS).")
