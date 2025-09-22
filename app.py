# app.py — Styrekandidat-finner (Firmify People)
import io
import math
import re
import requests
import pandas as pd
import streamlit as st

# ---------------- Basics ----------------
ENHETS_API = "https://data.brreg.no/enhetsregisteret/api/enheter"
PAGE_SIZE = 200  # fast mot API
TIMEOUT = 30

st.set_page_config(page_title="Styrekandidat-screener", layout="wide")
st.title("Styrekandidat-screener")

# ---------------- NACE segmenter ----------------
TECH = ["62", "63"]
ENERGI = ["35", "38", "39"]
FINANS = ["64", "65", "66"]
BYGG = ["41", "42", "43"]
INDUSTRI = [f"{i:02d}" for i in range(10, 34)]
TRANSPORT = ["49", "50", "51", "52", "53"]
HELSE = ["86", "87", "88"]
UTDANNING = ["85"]

SEGMENTS = {
    "Tech": TECH,
    "Energi": ENERGI,
    "Finans": FINANS,
    "Bygg/Anlegg": BYGG,
    "Industri": INDUSTRI,
    "Transport": TRANSPORT,
    "Helse": HELSE,
    "Utdanning": UTDANNING,
}

PUBLIC_ORGFORM = {"KOMM", "FYLKE", "KF", "FKF", "IKS", "STAT", "SF", "ORGL"}

# ---------------- Fylke (prefiks) ----------------
# Vi bruker kommunenummer-prefiks (første to siffer). Ny fylkesstruktur er litt i flux,
# men dette dekker vanlig bruk. Man kan alltid legge inn eksakte kommunenummer manuelt.
FYLKE_PREFIKS = {
    "Oslo": "03",
    "Viken": "30",
    "Innlandet": "34",
    "Vestfold og Telemark": "38",
    "Agder": "42",
    "Rogaland": "11",
    "Vestland": "46",
    "Møre og Romsdal": "15",
    "Trøndelag": "50",
    "Nordland": "18",
    "Troms": "19",
    "Finnmark": "20",
    "Buskerud": "33",  # nytt fra 2024/25, praktisk prefiks-filtrering
}

# ---------------- Sidebar: filtre ----------------
with st.sidebar:
    st.header("Selskapsfiltre (område, bransje, sektor)")

    fylke = st.selectbox("Fylke (prefiks-filter lokalt)", options=["(ingen)"] + list(FYLKE_PREFIKS.keys()))
    kommunenr_raw = st.text_input("Manuelle kommunenummer (komma-separert)")

    col1, col2 = st.columns(2)
    with col1:
        min_ans = st.number_input("Min ansatte", min_value=0, value=0, step=1)
    with col2:
        max_ans = st.number_input("Maks ansatte", min_value=0, value=999_999, step=10)

    st.subheader("Industri/segment (NACE-grupper)")
    seg_flags = {name: st.checkbox(name, value=False) for name in SEGMENTS.keys()}

    st.subheader("Sektor")
    sektor_priv = st.checkbox("Privat", value=True)
    sektor_off = st.checkbox("Offentlig", value=True)

    only_with_site = st.checkbox("Kun selskaper med nettside", value=True)
    top_n = st.number_input("Topp N selskaper (sortert på ansatte)", min_value=1, value=10, step=1)

    st.divider()
    st.header("Personfiltre (roller, kjønn)")

    role_map = {
        "Daglig leder": ["DAGL"],         # daglig leder
        "Styreleder": ["LEDER"],          # styrets leder
        "Styremedlem": ["STYRMEDL"],      # styremedlem
        "Varamedlem": ["VARA"],           # varamedlem
        "Signaturberettiget": ["SIGN"],   # signatur
        "Prokurist": ["PROK"],            # prokura
    }
    role_flags = {name: st.checkbox(name, value=(name in ["Daglig leder", "Styreleder"])) for name in role_map.keys()}
    active_only = st.checkbox("Kun aktive roller", value=True)

    st.subheader("Kjønn (valgfritt, estimert via navn – usikkert)")
    use_gender = st.checkbox("Skru på kjønn-estimering", value=False)
    gender_filter = st.selectbox("Filtrer kjønn", options=["Alle", "Kvinne", "Mann", "Ukjent"], index=0)

    st.divider()
    run = st.button("Kjør søk", type="primary")

# ---------------- Helpers ----------------
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

def has_site(url):
    return bool(url and url.strip() and len(url.strip()) > 3)

def kommunenummer_list_from_text(txt:str):
    if not txt.strip():
        return []
    return [k.strip() for k in txt.split(",") if re.fullmatch(r"\d{4}", k.strip())]

@st.cache_data(show_spinner=False)
def fetch_enheter(page:int, size:int, kommunenummer_list, min_ans, max_ans):
    params = {"page": page, "size": size}
    if kommunenummer_list:
        params["kommunenummer"] = ",".join(kommunenummer_list)
    if min_ans is not None:
        params["fraAntallAnsatte"] = min_ans
    if max_ans is not None:
        params["tilAntallAnsatte"] = max_ans
    r = requests.get(ENHETS_API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

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

def pass_segment_filter_row(row, seg_flags:dict):
    # Ingen segment valgt = gjennom
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

# ---------------- Roller (best-effort API) ----------------
ROLE_ENDPOINTS = [
    # Vi prøver flere varianter (API har variert over tid). Returnerer første som svarer med JSON.
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
    """Prøver å trekke ut (navn, rolle_kode/tekst, fradato, tildato). Strukturen kan variere; vi leser defensivt."""
    rows = []
    if not payload:
        return rows

    # Vanlige mønstre vi har sett: {'rollegrupper':[{'type':'STYRE','roller':[{'navn':'...','person':{'navn':..},'fradato':..,'tildato':..,'type':'STYRELEDER'}]}]}
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
    # Dedup på (navn, rolle, fradato, tildato)
    seen = set()
    out = []
    for r in rows:
        key = (r["navn"], r["rolle"], r.get("fradato"), r.get("tildato"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# ---------------- Kjønn (valgfritt/estimert) ----------------
@st.cache_data(show_spinner=False)
def genderize(first_name:str):
    try:
        resp = requests.get("https://api.genderize.io", params={"name": first_name}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            gen = data.get("gender")
            prob = data.get("probability")
            count = data.get("count")
            if gen in ("male", "female"):
                return ("Mann" if gen == "male" else "Kvinne", prob, count)
    except requests.RequestException:
        pass
    return ("Ukjent", None, None)

def first_name_from_full(name:str):
    if not name:
        return ""
    return re.split(r"\s+", name.strip())[0]

# ---------------- Kjør ----------------
if run:
    # 1) Sett opp kommune-filter: manuelle kommunenr → API; fylke → lokal prefiksfiltrering
    kommunenr_manual = kommunenummer_list_from_text(kommunenr_raw)
    fylkepref = FYLKE_PREFIKS.get(fylke) if fylke != "(ingen)" else None

    # 2) Hent selskaper side for side til vi har nok for top-N sortering
    all_rows = []
    page = 0
    total = None
    with st.spinner("Henter selskaper fra Enhetsregisteret..."):
        while True:
            payload = fetch_enheter(page, PAGE_SIZE, kommunenr_manual, min_ans, max_ans)
            if total is None:
                total = (payload.get("page") or {}).get("totalElements", 0)
            df_page = normalize_enheter(payload)
            # Lokal fylkefilter på kommunenummer-prefiks
            if fylkepref:
                df_page = df_page[df_page["kommunenr"].fillna("").astype(str).str.startswith(fylkepref)]
            # Segmentfilter lokalt
            df_page = df_page[df_page.apply(lambda r: pass_segment_filter_row(r, seg_flags), axis=1)]
            # Sektorfilter lokalt
            df_page = df_page[df_page.apply(lambda r: pass_sector_filter_row(r, sektor_priv, sektor_off), axis=1)]
            # Nettside
            if only_with_site:
                df_page = df_page[df_page["hjemmeside"].apply(has_site)]

            all_rows.append(df_page)
            page += 1
            # Break når vi har scannet alle sider (eller vi har minst top_n kandidater å rangere)
            total_pages = (payload.get("page") or {}).get("totalPages", 1)
            if page >= total_pages:
                break
            # Hvis vi allerede har betydelig mer enn top_n, kan vi stoppe tidlig
            if pd.concat(all_rows).shape[0] >= max(top_n * 3, top_n + 100):
                # litt margin for å sortere på ansatte
                break

    companies_df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    # Velg topp N på ansatte
    if not companies_df.empty:
        companies_df["ansatte"] = pd.to_numeric(companies_df["ansatte"], errors="coerce")
        companies_df = companies_df.sort_values("ansatte", ascending=False, na_position="last")
        companies_top = companies_df.head(top_n).copy()
    else:
        companies_top = pd.DataFrame()

    # 3) Hent roller for topp-selskap og bygg personliste
    people_rows = []
    if not companies_top.empty:
        with st.spinner("Henter roller for topp-selskap..."):
            for _, row in companies_top.iterrows():
                orgnr = str(row["orgnr"])
                rp = fetch_roles(orgnr)
                roles = parse_roles(rp)
                for rr in roles:
                    # mappe rolle til "våre" koder (vi bruker startswith mot vår liste)
                    rolle_upper = (rr["rolle"] or "").upper()
                    # Aktiv?
                    active = not bool(rr.get("tildato"))
                    # Behold kun roller som passer user-valg
                    keep = False
                    for ui_label, codes in role_map.items():
                        if role_flags[ui_label] and any(rolle_upper.startswith(c) for c in codes):
                            keep = True
                            break
                    if not any(role_flags.values()):
                        keep = True  # ingen rollefilter valgt -> ta alt
                    if not keep:
                        continue
                    if active_only and not active:
                        continue

                    # kjønn (valgfritt)
                    kjonn, kj_prob, kj_count = ("Ukjent", None, None)
                    if use_gender:
                        first = first_name_from_full(rr["navn"])
                        kjonn, kj_prob, kj_count = genderize(first)

                    people_rows.append({
                        "Navn": rr["navn"],
                        "Estimert kjønn": kjonn if use_gender else "",
                        "Kjønn sannsynlighet": kj_prob if use_gender else None,
                        "Rolle": ui_label if keep else rr["rolle"],
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

    # 4) Filtrer på kjønn hvis valgt
    if use_gender and not people_df.empty and gender_filter != "Alle":
        if gender_filter == "Ukjent":
            people_df = people_df[people_df["Estimert kjønn"].isin(["", "Ukjent"])]
        else:
            people_df = people_df[people_df["Estimert kjønn"] == gender_filter]

    # ---------------- Output: Selskaper ----------------
    st.subheader("Selskaper (topp N)")
    cols_company = ["navn", "orgnr", "kommune", "ansatte", "segmenter", "sektor", "hjemmeside"]
    if not companies_top.empty:
        out_companies = companies_top[cols_company].rename(columns={
            "navn": "Selskapsnavn",
            "orgnr": "Orgnr",
            "kommune": "Kommune",
            "ansatte": "Ansatte",
            "segmenter": "Industri",
            "sektor": "Sektor",
            "hjemmeside": "Nettside",
        })
        st.dataframe(out_companies, width="stretch", hide_index=True)
        # Nedlasting
        csv_bytes = out_companies.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Last ned selskaper (CSV)", data=csv_bytes, file_name="companies_top.csv", mime="text/csv")
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
            out_companies.to_excel(w, index=False, sheet_name="Selskaper")
        st.download_button("⬇️ Last ned selskaper (XLSX)", data=xbuf.getvalue(),
                           file_name="companies_top.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Ingen selskaper funnet med gjeldende filtre.")

    # ---------------- Output: Personer ----------------
    st.subheader("Personer (roller i topp-selskap)")
    if not people_df.empty:
        # Tydelig merk kjønn-estimat
        if use_gender:
            st.caption("Merk: Kjønn er estimert fra fornavn (usikkert).")

        st.dataframe(people_df, width="stretch", hide_index=True)
        # Nedlasting
        csv_bytes = people_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Last ned personer (CSV)", data=csv_bytes, file_name="people_roles.csv", mime="text/csv")
        xbuf2 = io.BytesIO()
        with pd.ExcelWriter(xbuf2, engine="xlsxwriter") as w:
            people_df.to_excel(w, index=False, sheet_name="Personer")
        st.download_button("⬇️ Last ned personer (XLSX)", data=xbuf2.getvalue(),
                           file_name="people_roles.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Valgfri "profil": velg person for enkel rolle-oversikt (i dette søket)
        uniq_names = sorted(people_df["Navn"].dropna().unique().tolist())
        sel = st.selectbox("Vis enkel rolleprofil (i dette uttrekket):", options=["(ingen)"] + uniq_names)
        if sel != "(ingen)":
            prof = people_df[people_df["Navn"] == sel].sort_values(["Slutt", "Start"], na_position="first")
            st.write(f"Roller for **{sel}** i dette uttrekket:")
            st.dataframe(prof[["Rolle", "Selskap", "Orgnr", "Ansatte", "Industri", "Sektor", "Start", "Slutt", "Brreg-lenke"]],
                         width="stretch", hide_index=True)
    else:
        st.info("Ingen personer/roller funnet i topp-selskapene.")

    # ---------------- Status ----------------
    st.markdown(
        f"**Total råtreff (API):** {total:,} • **Topp N:** {top_n} • "
        f"**Sektorfilter:** "
        f"{'Privat' if sektor_priv else ''}{'/' if (sektor_priv and sektor_off) else ''}{'Offentlig' if sektor_off else ''} • "
        f"**Kjønn-estimering:** {'På' if use_gender else 'Av'}"
    )

st.caption("Kilde: Enhetsregisteret (åpne data). Roller hentes best-effort fra Brreg sine rolle-endepunkt. Kjønn er valgfritt og estimert (usikkert).")
