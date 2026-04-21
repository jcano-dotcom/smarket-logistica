"""
Smarket TMS — Optimizador de Ruteo
Desarrollado con Streamlit
"""

import streamlit as st
import pandas as pd
import numpy as np
import math
import json
import io
from typing import Optional

# ── Dependencias opcionales ────────────────────────────────
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_OK = True
except ImportError:
    GSHEETS_OK = False

try:
    import plotly.graph_objects as go
    PLOTLY_OK = True
except ImportError:
    PLOTLY_OK = False

# ══════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Smarket TMS",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Estilos globales
st.markdown("""
<style>
/* Tarjetas de ruta */
.ruta-card {
    background: #fff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 12px;
    box-shadow: 0 1px 4px rgba(0,0,0,.06);
}
.ruta-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
    flex-wrap: wrap;
    gap: 6px;
}
.ruta-title { font-size: 15px; font-weight: 600; color: #111; }
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 99px;
    font-size: 11px;
    font-weight: 600;
}
.badge-green  { background: #dcfce7; color: #166534; }
.badge-yellow { background: #fef9c3; color: #854d0e; }
.badge-red    { background: #fee2e2; color: #991b1b; }
.badge-blue   { background: #dbeafe; color: #1e40af; }
.badge-gray   { background: #f3f4f6; color: #374151; }

.stat-row {
    display: flex;
    gap: 16px;
    font-size: 12px;
    color: #6b7280;
    margin-bottom: 8px;
    flex-wrap: wrap;
}
.stat-val { font-weight: 600; color: #111; }

.ped-row {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    padding: 6px 0;
    border-top: 1px solid #f3f4f6;
    font-size: 12px;
    gap: 8px;
}
.ped-dir { color: #111; font-weight: 500; flex: 1; }
.ped-client { color: #6b7280; font-size: 11px; }
.ped-imp-ok   { color: #166534; font-weight: 700; }
.ped-imp-warn { color: #b45309; font-weight: 700; }
.ped-imp-bad  { color: #991b1b; font-weight: 700; }

.progress-wrap {
    background: #f3f4f6;
    border-radius: 4px;
    height: 6px;
    overflow: hidden;
    margin-top: 4px;
}
.progress-fill { height: 100%; border-radius: 4px; }

.dep-card {
    background: #fafaf9;
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    padding: 12px 16px;
    margin-top: 12px;
}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# CONSTANTES / DATOS MUESTRA
# ══════════════════════════════════════════════════════════
SAMPLE_PEDIDOS = pd.DataFrame([
    {"id":"35268","cliente":"Sebastian Duarte (1)","direccion":"Av. S. Martín 7300","zona":"Sur-Oeste","cp":1419,"kilos":114,"valor":570137,"bultos":74},
    {"id":"35269","cliente":"Sebastian Duarte (2)","direccion":"Av. S. Martín 7300","zona":"Sur-Oeste","cp":1419,"kilos":5,"valor":60077,"bultos":1},
    {"id":"35250","cliente":"Buenos Alimentos BSM1644","direccion":"Av. General Paz 12301","zona":"Sur-Oeste","cp":1752,"kilos":125,"valor":755344,"bultos":10},
    {"id":"35178","cliente":"Servicios en Gastronomia SRL","direccion":"Lisandro de la Torre 1609","zona":"Sur-Oeste","cp":1752,"kilos":55,"valor":507580,"bultos":6},
    {"id":"35280","cliente":"Pulvirenti Pablo Leonel","direccion":"Tuyuti 1387","zona":"Sur-Oeste","cp":1770,"kilos":43,"valor":231361,"bultos":3},
    {"id":"35197","cliente":"Paez Saura German Ezequiel","direccion":"Av. Boulogne Sur Mer 1193","zona":"Sur-Oeste","cp":1770,"kilos":25,"valor":251506,"bultos":2},
    {"id":"35224","cliente":"Schenone Santiago Gabriel","direccion":"Soberanía Nacional 7070","zona":"Sur-Oeste","cp":1759,"kilos":40,"valor":206107,"bultos":6},
    {"id":"35241","cliente":"Outletbar Misiones - Lamera","direccion":"Perdriel 1151","zona":"CABA","cp":1279,"kilos":48,"valor":224813,"bultos":28},
    {"id":"35265","cliente":"Fuerzas del Interior SRL","direccion":"Av. Garmendia 4813","zona":"CABA","cp":1427,"kilos":175,"valor":388527,"bultos":13},
    {"id":"35225","cliente":"Luviam S.R.L.","direccion":"Morlote 966","zona":"CABA","cp":1427,"kilos":54,"valor":170974,"bultos":4},
    {"id":"35282","cliente":"Cincunegui Jorge Santiago","direccion":"Montevideo 274","zona":"Norte","cp":1648,"kilos":27,"valor":180113,"bultos":3},
    {"id":"35278","cliente":"Outletbar Tigre","direccion":"Sarmiento 585","zona":"Norte","cp":1648,"kilos":62,"valor":613049,"bultos":32},
    {"id":"35141","cliente":"Francisco Orlando Chávez","direccion":"Cordero 3821","zona":"Norte","cp":1645,"kilos":782,"valor":4176564,"bultos":64},
    {"id":"35190","cliente":"María Elena Zorrilla","direccion":"25 de Agosto 1987","zona":"Norte","cp":1619,"kilos":131,"valor":604242,"bultos":57},
    {"id":"35220","cliente":"La Vineria Wine Bar","direccion":"Tucumán 388","zona":"Norte","cp":1629,"kilos":119,"valor":496445,"bultos":12},
    {"id":"35152","cliente":"Novo Eventos S.R.L.","direccion":"Manuel L. de Oliden 8753","zona":"Norte","cp":1669,"kilos":79,"valor":303962,"bultos":9},
    {"id":"35232","cliente":"Dam Eventos S.R.L.","direccion":"Eduardo Wilde 1561","zona":"Acceso Oeste","cp":1746,"kilos":289,"valor":1371475,"bultos":34},
    {"id":"35188","cliente":"Ramirez Gustavo Esteban","direccion":"San Martín esq. Italia","zona":"Acceso Oeste","cp":6700,"kilos":118,"valor":1049708,"bultos":12},
])

SAMPLE_TRANSPORTES = pd.DataFrame([
    {"nombre":"Gaby","capacidad_kg":600,"costo_fijo":0,"costo_hora":22000,"ayudante":False,"origen":"Vicente López"},
    {"nombre":"Juan","capacidad_kg":600,"costo_fijo":0,"costo_hora":22000,"ayudante":False,"origen":"Vicente López"},
    {"nombre":"Greco - Utilitario","capacidad_kg":600,"costo_fijo":0,"costo_hora":18000,"ayudante":False,"origen":"Béccar"},
    {"nombre":"Greco - Sprinter","capacidad_kg":2800,"costo_fijo":50000,"costo_hora":35000,"ayudante":True,"origen":"Béccar"},
    {"nombre":"Greco - MB 608","capacidad_kg":3500,"costo_fijo":50000,"costo_hora":48000,"ayudante":True,"origen":"Béccar"},
    {"nombre":"Greco - MB 1114","capacidad_kg":10000,"costo_fijo":0,"costo_hora":60000,"ayudante":False,"origen":"Béccar"},
])

ZONAS_CORREDOR = {
    "Sur-Oeste":    {"transportista":"Gaby","orden_geo":{"Villa Devoto":1,"Lomas del Mirador":2,"Tapiales":3,"González Catán":4}},
    "CABA":         {"transportista":"Juan","orden_geo":{"CABA":1,"Villa Soldati":2,"Chacarita":3}},
    "Norte":        {"transportista":"Greco - Sprinter","orden_geo":{"Munro":1,"Tigre":2,"San Fernando":3,"Garín":4,"Pilar":5,"Del Viso":6}},
    "Acceso Oeste": {"transportista":"Greco - Utilitario","orden_geo":{"Ramos Mejía":1,"Haedo":2,"Morón":3,"Castelar":4,"Ituzaingó":5,"Paso del Rey":6,"Moreno":7,"Francisco Álvarez":8,"General Rodríguez":9,"Luján":10}},
}

AYUDANTE_COSTO = 50000
MAX_PARADAS    = 8

# ══════════════════════════════════════════════════════════
# UTILIDADES
# ══════════════════════════════════════════════════════════
def fmt_peso(n: float) -> str:
    return f"{n:,.1f} kg"

def fmt_money(n: float) -> str:
    return f"${n:,.0f}".replace(",", ".")

def fmt_pct(n: float) -> str:
    return f"{n:.2f}%"

def impact_color_class(pct: float) -> str:
    if pct <= 3:   return "badge-green"
    if pct <= 5:   return "badge-yellow"
    return "badge-red"

def impact_row_class(pct: float) -> str:
    if pct <= 3:   return "ped-imp-ok"
    if pct <= 5:   return "ped-imp-warn"
    return "ped-imp-bad"

def carga_color(pct_carga: float) -> str:
    if pct_carga >= 75: return "#16a34a"
    if pct_carga >= 45: return "#d97706"
    return "#dc2626"

def calcular_horas(km: float, n_paradas: int, vel: float = 35.0, min_por_parada: float = 20.0) -> float:
    h = km / vel + n_paradas * (min_por_parada / 60)
    return math.ceil(h * 2) / 2  # redondeo a 0.5h

def costo_ruta(horas: float, tarifa_hora: float, costo_fijo: float = 0) -> float:
    return horas * tarifa_hora + costo_fijo

def impacto_ruta(flete: float, valor_merc: float) -> float:
    if valor_merc <= 0: return 0.0
    return flete / valor_merc * 100

def impacto_pedido_por_peso(kg_ped: float, val_ped: float, kg_total: float, flete_total: float) -> float:
    """Part. x Peso = (flete × kg_ped / kg_total) / val_ped"""
    if kg_total <= 0 or val_ped <= 0: return 0.0
    return (flete_total * kg_ped / kg_total) / val_ped * 100

def asignar_vehiculo_greco(kg: float, transportes_df: pd.DataFrame):
    greco = transportes_df[transportes_df["nombre"].str.startswith("Greco")].copy()
    greco = greco[greco["capacidad_kg"] >= kg].sort_values("capacidad_kg")
    if greco.empty:
        return transportes_df[transportes_df["nombre"].str.startswith("Greco")].sort_values("capacidad_kg").iloc[-1]
    return greco.iloc[0]


# ══════════════════════════════════════════════════════════
# MOTOR DE RUTEO
# ══════════════════════════════════════════════════════════
def optimizar_rutas(
    pedidos_df: pd.DataFrame,
    transportes_df: pd.DataFrame,
    fletes_manuales: dict,   # {ruta_id: costo_override}
    horas_estimadas: dict,   # {ruta_id: horas_estimadas_manual}
) -> list[dict]:
    """
    Devuelve lista de dicts con info completa de cada ruta.
    """
    # Guardia: DataFrame inválido o sin columna zona
    if pedidos_df is None or not isinstance(pedidos_df, pd.DataFrame):
        return []
    if "zona" not in pedidos_df.columns:
        st.error(
            "**Error: No se encontró la columna `zona` en el archivo.**\n\n"
            "Por favor verificá el nombre de las columnas en tu archivo."
        )
        return []
    if pedidos_df.empty:
        st.warning("El archivo no contiene pedidos válidos para rutear.")
        return []

    rutas = []
    zonas = pedidos_df["zona"].unique()

    for zona in zonas:
        peds_zona = pedidos_df[pedidos_df["zona"] == zona].copy()

        # Ordenar por CP
        peds_zona = peds_zona.sort_values("cp")

        kg_zona = peds_zona["kilos"].sum()

        # Decidir transportista
        corr_info = ZONAS_CORREDOR.get(zona, {})
        trans_nombre = corr_info.get("transportista", "Greco - Utilitario")

        if trans_nombre.startswith("Greco"):
            trans_row = asignar_vehiculo_greco(kg_zona, transportes_df)
            trans_nombre = trans_row["nombre"]
        else:
            match = transportes_df[transportes_df["nombre"] == trans_nombre]
            if match.empty:
                trans_row = transportes_df.iloc[0]
            else:
                trans_row = match.iloc[0]

        cap_kg     = float(trans_row["capacidad_kg"])
        tarifa     = float(trans_row["costo_hora"])
        costo_fijo = float(trans_row.get("costo_fijo", 0))
        ayudante   = bool(trans_row.get("ayudante", False))

        # Dividir si excede capacidad o MAX_PARADAS
        chunks = []
        chunk, kg_chunk = [], 0.0
        for _, ped in peds_zona.iterrows():
            if (kg_chunk + ped["kilos"] > cap_kg or len(chunk) >= MAX_PARADAS) and chunk:
                chunks.append(chunk)
                chunk, kg_chunk = [], 0.0
            chunk.append(ped)
            kg_chunk += ped["kilos"]
        if chunk:
            chunks.append(chunk)

        for ci, chunk in enumerate(chunks):
            ruta_id = f"{zona}_{ci}"
            kg_t  = sum(p["kilos"] for p in chunk)
            val_t = sum(p["valor"] for p in chunk)
            bul_t = sum(p["bultos"] for p in chunk)

            # Horas (se puede sobrescribir manualmente)
            h_auto = calcular_horas(km=80, n_paradas=len(chunk))  # km placeholder
            h_ruta = horas_estimadas.get(ruta_id, h_auto)

            # Costo
            costo_auto = costo_ruta(h_ruta, tarifa, costo_fijo) + (AYUDANTE_COSTO if ayudante else 0)
            flete = fletes_manuales.get(ruta_id, costo_auto)

            imp = impacto_ruta(flete, val_t)
            pct_carga = kg_t / cap_kg * 100 if cap_kg > 0 else 0

            # Sub-impacto por pedido
            peds_data = []
            for p in chunk:
                imp_p = impacto_pedido_por_peso(p["kilos"], p["valor"], kg_t, flete)
                peds_data.append({
                    "id":       str(p.get("id", "")),
                    "cliente":  str(p.get("cliente", "")),
                    "direccion":str(p.get("direccion", "")),
                    "zona":     str(p.get("zona", "")),
                    "cp":       int(p.get("cp", 0)),
                    "kilos":    float(p["kilos"]),
                    "valor":    float(p["valor"]),
                    "bultos":   int(p.get("bultos", 0)),
                    "imp_ped":  imp_p,
                    "flete_ped":flete * p["kilos"] / kg_t if kg_t > 0 else 0,
                })

            rutas.append({
                "id":           ruta_id,
                "zona":         zona + (f" ({ci+1})" if len(chunks) > 1 else ""),
                "transportista":trans_nombre,
                "vehiculo":     trans_nombre,
                "cap_kg":       cap_kg,
                "kg_total":     kg_t,
                "val_total":    val_t,
                "bultos_total": bul_t,
                "n_paradas":    len(chunk),
                "horas":        h_ruta,
                "tarifa_hora":  tarifa,
                "costo_fijo":   costo_fijo,
                "ayudante":     ayudante,
                "flete":        flete,
                "flete_auto":   costo_auto,
                "impacto":      imp,
                "pct_carga":    pct_carga,
                "peds":         peds_data,
            })

    return rutas


# ══════════════════════════════════════════════════════════
# CARGA DE DATOS
# ══════════════════════════════════════════════════════════
def load_from_file(uploaded) -> Optional[pd.DataFrame]:
    try:
        name = uploaded.name.lower()
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded)
        else:
            df = pd.read_excel(uploaded)
        return normalizar_columnas(df)
    except Exception as e:
        st.error(f"Error al leer archivo: {e}")
        return None

def normalizar_columnas(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Normaliza nombres de columnas al esquema interno.
    Paso 1: convierte todas las columnas a minusculas.
    Paso 2: renombra variantes conocidas al nombre interno.
    Paso 3: valida que exista la columna critica "zona" y muestra error amigable si no.
    """
    # Paso 1 — minusculas y sin espacios extremos
    df.columns = [c.strip().lower() for c in df.columns]

    # Paso 2 — mapa ampliado de sinonimos
    MAP = {
        "n° de pedido": "id", "n° pedido": "id", "nro pedido": "id",
        "pedido": "id", "n_pedido": "id", "id pedido": "id",
        "descripcion cliente": "cliente", "descripción cliente": "cliente",
        "descripcion_cliente": "cliente", "nombre cliente": "cliente",
        "razon social": "cliente",
        "direccion de entrega": "direccion", "dirección de entrega": "direccion",
        "direccion_entrega": "direccion", "dirección": "direccion",
        "domicilio": "direccion",
        "zona": "zona", "localidad": "zona", "localidad de entrega": "zona",
        "barrio/expreso": "zona", "barrio": "zona", "ciudad": "zona",
        "region": "zona", "región": "zona", "partido": "zona",
        "cp": "cp", "codigo postal": "cp", "código postal": "cp",
        "cod postal": "cp", "cod. postal": "cp",
        "kilos": "kilos", "kg": "kilos", "peso": "kilos",
        "peso kg": "kilos", "peso (kg)": "kilos",
        "valor": "valor", "importe del pedido": "valor",
        "valor venta": "valor", "importe": "valor", "monto": "valor",
        "bultos": "bultos", "cajas": "bultos", "cant. bultos": "bultos",
    }
    df = df.rename(columns={k: v for k, v in MAP.items() if k in df.columns})

    # Paso 3 — validar columna critica "zona"
    if "zona" not in df.columns:
        cols = ", ".join(f"'{c}'" for c in df.columns.tolist())
        st.error(
            "**Error: No se encontró la columna `zona` en el archivo.**\n\n"
            "Verificá que tu archivo tenga una columna con alguno de estos nombres: "
            "`zona`, `localidad`, `localidad de entrega`, `barrio`, `ciudad`, `región`.\n\n"
            f"Columnas encontradas en tu archivo: {cols}"
        )
        return None

    # Paso 4 — columnas numericas opcionales
    for col, default in [("cp", 0), ("kilos", 0), ("valor", 0), ("bultos", 0)]:
        if col not in df.columns:
            df[col] = default

    # Paso 5 — columnas de texto opcionales
    for col in ("id", "cliente", "direccion"):
        if col not in df.columns:
            df[col] = ""

    # Paso 6 — parseo numerico seguro
    df["kilos"] = pd.to_numeric(df["kilos"], errors="coerce").fillna(0)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
    df["cp"]    = pd.to_numeric(df["cp"],    errors="coerce").fillna(0).astype(int)

    # Paso 7 — filtrar pedidos de deposito
    df = df[~df["direccion"].str.lower().str.contains("maestro santana|beccar", na=False)]
    df = df[df["kilos"] > 0]

    if df.empty:
        st.warning("No quedaron pedidos luego del filtro. Verificá que haya filas con kilos > 0.")
        return None

    return df.reset_index(drop=True)

def load_from_gsheet(url_or_id: str, creds_json: Optional[str] = None) -> Optional[pd.DataFrame]:
    if not GSHEETS_OK:
        st.error("gspread no instalado. Corré: pip install gspread google-auth")
        return None
    try:
        if creds_json:
            info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(
                info,
                scopes=["https://spreadsheets.google.com/feeds",
                        "https://www.googleapis.com/auth/drive"]
            )
            gc = gspread.authorize(creds)
        else:
            gc = gspread.oauth()

        if "spreadsheets/d/" in url_or_id:
            parts = url_or_id.split("/d/")[1]
            sheet_id = parts.split("/")[0]
        else:
            sheet_id = url_or_id

        sh = gc.open_by_key(sheet_id)
        ws = sh.get_worksheet(0)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        return normalizar_columnas(df)
    except Exception as e:
        st.error(f"Error Google Sheets: {e}")
        return None


# ══════════════════════════════════════════════════════════
# RENDER TARJETA DE RUTA
# ══════════════════════════════════════════════════════════
def render_ruta_card(r: dict, idx: int):
    imp     = r["impacto"]
    pct_c   = r["pct_carga"]
    badge_c = impact_color_class(imp)
    carga_c = carga_color(pct_c)
    ay_str  = f" <span class='badge badge-yellow'>+ Ayudante ${AYUDANTE_COSTO:,.0f}</span>" if r["ayudante"] else ""

    # Generar filas de pedidos
    peds_html = ""
    for p in r["peds"]:
        ic = impact_row_class(p["imp_ped"])
        peds_html += f"""
        <div class='ped-row'>
            <div style='flex:1;min-width:0'>
                <div class='ped-dir'>{p['direccion']}</div>
                <div class='ped-client'>{p['cliente']} · #{p['id']} · CP {p['cp']}</div>
                {f"<div class='ped-client' style='color:#b45309'>{p.get('obs','')}</div>" if p.get('obs') else ''}
            </div>
            <div style='text-align:right;flex-shrink:0;padding-left:8px'>
                <div style='font-weight:500;font-size:12px'>{p['kilos']:.0f} kg · {p['bultos']} btos</div>
                <div style='color:#6b7280;font-size:11px'>${p['valor']:,.0f}</div>
                <div class='{ic}'>{p['imp_ped']:.2f}%</div>
            </div>
        </div>"""

    merc_min = r["flete"] / 0.03 if r["flete"] > 0 else 0
    falta    = max(0, merc_min - r["val_total"])

    html = f"""
    <div class='ruta-card'>
        <div class='ruta-header'>
            <div>
                <span class='ruta-title'>{r['transportista']}</span>
                <span class='badge badge-blue' style='margin-left:8px'>{r['zona']}</span>
                <span class='badge badge-gray' style='margin-left:4px'>{r['vehiculo'].replace('Greco - ','')}</span>
                {ay_str}
            </div>
            <span class='badge {badge_c}' style='font-size:13px'>{imp:.2f}%</span>
        </div>
        <div class='stat-row'>
            <span>{r['n_paradas']} paradas</span>
            <span><span class='stat-val'>{r['kg_total']:.0f} kg</span> / {r['cap_kg']:.0f} kg cap.</span>
            <span><span class='stat-val' style='color:{carga_c}'>{pct_c:.0f}% carga</span></span>
            <span>{r['horas']:.1f}h</span>
            <span>Flete <span class='stat-val'>${r['flete']:,.0f}</span></span>
            <span>Merc. <span class='stat-val'>${r['val_total']:,.0f}</span></span>
        </div>
        <div class='progress-wrap'>
            <div class='progress-fill' style='width:{min(pct_c,100):.1f}%;background:{carga_c}'></div>
        </div>
        <div class='progress-wrap' style='margin-top:4px'>
            <div class='progress-fill' style='width:{min(imp/8*100,100):.1f}%;background:{"#16a34a" if imp<=3 else "#d97706" if imp<=5 else "#dc2626"}'></div>
        </div>
        <div style='display:flex;justify-content:space-between;font-size:9px;color:#9ca3af;margin-bottom:6px'>
            <span>impacto</span><span>objetivo 3%</span><span>8%</span>
        </div>
        {f"<div style='font-size:11px;color:#dc2626;margin-bottom:6px'>Para llegar al 3% necesitás ${merc_min:,.0f} de mercadería (faltan ${falta:,.0f})</div>" if imp > 3 else ""}
        <details>
            <summary style='cursor:pointer;font-size:12px;color:#6b7280;margin-top:4px'>
                Ver {r['n_paradas']} pedidos ▾
            </summary>
            {peds_html}
        </details>
    </div>"""
    st.markdown(html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════
def sidebar_config():
    with st.sidebar:
        st.image("https://via.placeholder.com/200x60?text=🚛+Smarket+TMS", width=200)
        st.markdown("---")

        st.subheader("📂 Cargar pedidos")
        fuente = st.radio("Fuente de datos", ["Datos de muestra", "Subir archivo", "Google Sheets"], index=0)

        pedidos_df = None

        if fuente == "Datos de muestra":
            pedidos_df = SAMPLE_PEDIDOS.copy()
            st.success(f"✓ {len(pedidos_df)} pedidos de muestra cargados")

        elif fuente == "Subir archivo":
            uploaded = st.file_uploader(
                "CSV o Excel",
                type=["csv","xlsx","xls"],
                help="El archivo debe tener columnas: cliente, dirección, zona/localidad, CP, kilos, valor, bultos"
            )
            if uploaded:
                pedidos_df = load_from_file(uploaded)
                if pedidos_df is not None and not pedidos_df.empty:
                    st.success(f"✓ {len(pedidos_df)} pedidos cargados")

        elif fuente == "Google Sheets":
            st.info("ℹ Necesitás credenciales de Service Account")
            gsheet_url = st.text_input(
                "URL o ID de la Spreadsheet",
                placeholder="https://docs.google.com/spreadsheets/d/..."
            )
            with st.expander("🔑 Credenciales Service Account (JSON)"):
                creds_json = st.text_area(
                    "Pegá el JSON de la Service Account",
                    height=150,
                    help="Descargá el JSON desde Google Cloud Console > IAM > Service Accounts"
                )
            if gsheet_url and st.button("Conectar Google Sheets"):
                with st.spinner("Conectando..."):
                    pedidos_df = load_from_gsheet(gsheet_url, creds_json if creds_json else None)
                    if pedidos_df is not None:
                        st.success(f"✓ {len(pedidos_df)} pedidos cargados desde Google Sheets")

        st.markdown("---")
        st.subheader("🚛 Transportistas")
        transportes_df = st.data_editor(
            SAMPLE_TRANSPORTES,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "nombre": st.column_config.TextColumn("Nombre"),
                "capacidad_kg": st.column_config.NumberColumn("Cap. (kg)", min_value=0),
                "costo_hora": st.column_config.NumberColumn("$/hora", min_value=0),
                "costo_fijo": st.column_config.NumberColumn("Costo fijo ($)", min_value=0),
                "ayudante": st.column_config.CheckboxColumn("Ayudante"),
                "origen": st.column_config.TextColumn("Origen"),
            },
            hide_index=True,
            key="transportes_editor",
        )

        st.markdown("---")
        st.caption("Smarket TMS v1.0 · Desarrollado con Streamlit")

    return pedidos_df, transportes_df


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    # Estado
    if "fletes_manuales" not in st.session_state:
        st.session_state["fletes_manuales"] = {}
    if "horas_estimadas" not in st.session_state:
        st.session_state["horas_estimadas"] = {}

    pedidos_df, transportes_df = sidebar_config()

    if pedidos_df is None:
        st.info("← Cargá pedidos desde la barra lateral para comenzar.")
        return

    # Header
    st.title("🚛 Smarket TMS — Optimizador de Ruteo")

    # Tabs
    tab_panel, tab_pedidos, tab_config, tab_export, tab_gsheets = st.tabs([
        "📋 Panel de Rutas",
        "📦 Pedidos",
        "⚙️ Configuración de Fletes",
        "⬇️ Exportar Excel",
        "🔗 Google Sheets",
    ])

    # ── Optimizar ──────────────────────────────────────────
    # pedidos_df puede ser None si normalizar_columnas falló
    rutas = optimizar_rutas(
        pedidos_df if pedidos_df is not None else pd.DataFrame(),
        transportes_df,
        st.session_state["fletes_manuales"],
        st.session_state["horas_estimadas"],
    )

    # ── TAB PANEL ──────────────────────────────────────────
    with tab_panel:
        # KPIs globales
        total_peds  = sum(r["n_paradas"] for r in rutas)
        total_costo = sum(r["flete"]     for r in rutas)
        total_val   = sum(r["val_total"] for r in rutas)
        total_kg    = sum(r["kg_total"]  for r in rutas)
        imp_global  = total_costo / total_val * 100 if total_val > 0 else 0

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Rutas activas",   len(rutas))
        k2.metric("Pedidos",         total_peds)
        k3.metric("Kg total",        f"{total_kg:,.0f}")
        k4.metric("Costo flete",     f"${total_costo:,.0f}")
        k5.metric("Impacto global",  f"{imp_global:.2f}%",
                  delta=f"{imp_global-3:.2f}% vs obj 3%",
                  delta_color="inverse")

        st.markdown("---")

        # Alertas
        criticas = [r for r in rutas if r["impacto"] > 3]
        if criticas:
            st.warning(f"⚠️ **{len(criticas)} ruta(s) superan el 3% de impacto.** Ajustá los fletes en la pestaña ⚙️ Configuración.")
        else:
            st.success("✅ Todas las rutas están por debajo del 3% de impacto.")

        # Grilla de rutas — 2 columnas
        n = len(rutas)
        if n == 0:
            st.info("No hay rutas generadas.")
        elif n == 1:
            render_ruta_card(rutas[0], 0)
        else:
            pairs = [(rutas[i], rutas[i+1] if i+1 < n else None) for i in range(0, n, 2)]
            for r1, r2 in pairs:
                col1, col2 = st.columns(2)
                with col1:
                    render_ruta_card(r1, 0)
                with col2:
                    if r2:
                        render_ruta_card(r2, 1)

    # ── TAB PEDIDOS ────────────────────────────────────────
    with tab_pedidos:
        st.subheader("Pedidos cargados")
        st.dataframe(pedidos_df, use_container_width=True, hide_index=True)
        st.caption(f"{len(pedidos_df)} pedidos · {pedidos_df['kilos'].sum():,.0f} kg · ${pedidos_df['valor'].sum():,.0f}")

    # ── TAB CONFIGURACIÓN DE FLETES ────────────────────────
    with tab_config:
        st.subheader("⚙️ Editar flete y horas por ruta")
        st.info("Modificá el flete o las horas de cualquier ruta para ver el impacto en tiempo real.")

        for r in rutas:
            with st.expander(f"**{r['transportista']}** — {r['zona']}  |  Impacto actual: {r['impacto']:.2f}%"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    flete_new = st.number_input(
                        "Flete total ($)",
                        min_value=0,
                        value=int(r["flete"]),
                        step=1000,
                        key=f"flete_{r['id']}",
                        help=f"Automático: ${r['flete_auto']:,.0f}"
                    )
                with c2:
                    horas_new = st.number_input(
                        "Horas estimadas",
                        min_value=0.5,
                        max_value=24.0,
                        value=float(r["horas"]),
                        step=0.5,
                        key=f"horas_{r['id']}",
                    )
                with c3:
                    imp_preview = flete_new / r["val_total"] * 100 if r["val_total"] > 0 else 0
                    color = "green" if imp_preview <= 3 else "orange" if imp_preview <= 5 else "red"
                    st.markdown(f"**Impacto con este flete:**")
                    st.markdown(f"<h2 style='color:{color}'>{imp_preview:.2f}%</h2>", unsafe_allow_html=True)

                # Guardar overrides
                if flete_new != int(r["flete_auto"]):
                    st.session_state["fletes_manuales"][r["id"]] = flete_new
                elif r["id"] in st.session_state["fletes_manuales"]:
                    del st.session_state["fletes_manuales"][r["id"]]

                st.session_state["horas_estimadas"][r["id"]] = horas_new

                merc_min = flete_new / 0.03 if flete_new > 0 else 0
                st.caption(f"Para llegar al 3%: necesitás ${merc_min:,.0f} de mercadería (tenés ${r['val_total']:,.0f})")

    # ── TAB EXPORTAR ───────────────────────────────────────
    with tab_export:
        st.subheader("⬇️ Exportar Hojas de Ruta")
        if st.button("📊 Generar Excel", type="primary", use_container_width=True):
            try:
                from openpyxl import Workbook
                from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
                from openpyxl.utils import get_column_letter

                def fill(h): return PatternFill("solid", fgColor=h)
                def th():
                    s = Side(style="thin", color="CCCCCC")
                    return Border(left=s, right=s, top=s, bottom=s)
                def ic(v): return "EAF3DE" if v<=3 else "FAEEDA" if v<=5 else "FCEBEB"

                wb = Workbook()
                wb.remove(wb.active)

                # Resumen
                ws = wb.create_sheet("Resumen", 0)
                for i, w in enumerate([14,22,16,8,8,8,10,14,12,18], 1):
                    ws.column_dimensions[get_column_letter(i)].width = w
                ws.merge_cells("A1:J1")
                ws["A1"] = "SMARKET — RESUMEN HOJAS DE RUTA"
                ws["A1"].font = Font(name="Arial", size=12, bold=True, color="FFFFFF")
                ws["A1"].fill = fill("1F3864")
                ws["A1"].alignment = Alignment(horizontal="center")

                hdrs = ["Transportista","Zona","Vehículo","Km","Horas","Kg","Paradas","Flete","Impacto %","Valor"]
                for ci, h in enumerate(hdrs, 1):
                    c = ws.cell(2, ci, h)
                    c.font = Font(name="Arial", size=9, bold=True, color="FFFFFF")
                    c.fill = fill("2E75B6"); c.border = th()
                    c.alignment = Alignment(horizontal="center")

                for ri, r in enumerate(rutas):
                    row = 3 + ri
                    bg = "E1F5EE" if "Gaby" in r["transportista"] or "Juan" in r["transportista"] else "EEEDFE"
                    vals = [r["transportista"], r["zona"], r["vehiculo"],
                            "—", r["horas"], f'{r["kg_total"]:.0f}',
                            r["n_paradas"], r["flete"], f'{r["impacto"]:.2f}%', r["val_total"]]
                    for ci, v in enumerate(vals, 1):
                        c = ws.cell(row, ci, v)
                        c.font = Font(name="Arial", size=9)
                        c.fill = fill(ic(r["impacto"]) if ci==9 else bg)
                        c.border = th(); c.alignment = Alignment(horizontal="center" if ci>2 else "left")
                        if ci in (8,10): c.number_format = '$#,##0'

                # Una hoja por ruta
                for r in rutas:
                    sname = f"{r['transportista'][:12]} - {r['zona'][:14]}"
                    ws2 = wb.create_sheet(sname)
                    for i, w in enumerate([5,10,28,38,16,7,7,7,12,10,22,14,13], 1):
                        ws2.column_dimensions[get_column_letter(i)].width = w

                    ws2.merge_cells("A1:M1")
                    ws2["A1"] = f"SMARKET — {r['transportista']} — {r['zona']}"
                    ws2["A1"].font = Font(name="Arial", size=11, bold=True, color="FFFFFF")
                    ws2["A1"].fill = fill("1F3864")
                    ws2["A1"].alignment = Alignment(horizontal="center")

                    ay_str = " + Ayudante" if r["ayudante"] else ""
                    ws2.merge_cells("A2:M2")
                    ws2["A2"] = (f"Transp: {r['transportista']} | Veh: {r['vehiculo']}{ay_str} | "
                                 f"Horas: {r['horas']}h | Flete: ${r['flete']:,.0f} | Impacto: {r['impacto']:.2f}%")
                    ws2["A2"].font = Font(name="Arial", size=8, bold=True)
                    ws2["A2"].fill = fill("E1F5EE" if "Gaby" in r["transportista"] or "Juan" in r["transportista"] else "EEEDFE")

                    hdrs2 = ["Ord.","N° Ped.","Cliente","Dirección","Localidad","CP","Kg","Btos","Valor","Horario","Obs","Flete $","Part.%"]
                    for ci, h in enumerate(hdrs2, 1):
                        c = ws2.cell(4, ci, h)
                        c.font = Font(name="Arial", size=8, bold=True, color="FFFFFF")
                        c.fill = fill("2E75B6"); c.border = th()
                        c.alignment = Alignment(horizontal="center", wrap_text=True)

                    for ni, p in enumerate(r["peds"]):
                        row = 5 + ni
                        bg2 = "FFFFFF" if ni%2==0 else "EBF3FB"
                        vals = [ni+1, p["id"], p["cliente"], p["direccion"], p["zona"],
                                p["cp"], p["kilos"], p["bultos"], p["valor"],
                                p.get("h",""), p.get("obs",""),
                                p["flete_ped"], f'{p["imp_ped"]:.2f}%']
                        for ci, v in enumerate(vals, 1):
                            c = ws2.cell(row, ci, v)
                            c.font = Font(name="Arial", size=8)
                            c.fill = fill(ic(p["imp_ped"]) if ci==13 else bg2)
                            c.border = th()
                            c.alignment = Alignment(
                                horizontal="left" if ci in (3,4,11) else "center",
                                wrap_text=ci in (3,4,11)
                            )
                            if ci in (9,12): c.number_format = '$#,##0'
                        ws2.row_dimensions[row].height = 28

                    ws2.freeze_panes = "A5"
                    ws2.page_setup.orientation = "landscape"
                    ws2.page_setup.fitToPage = True

                buf = io.BytesIO()
                wb.save(buf)
                buf.seek(0)

                st.download_button(
                    label="⬇️ Descargar Smarket_HDR.xlsx",
                    data=buf,
                    file_name="Smarket_HDR.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                st.success("✅ Excel generado. Hacé click en Descargar.")

            except Exception as e:
                st.error(f"Error generando Excel: {e}")

    # ── TAB GOOGLE SHEETS ──────────────────────────────────
    with tab_gsheets:
        st.subheader("🔗 Configurar Google Sheets")

        st.markdown("""
### Cómo conectar Google Sheets

#### Paso 1 — Crear un proyecto en Google Cloud
1. Ir a [console.cloud.google.com](https://console.cloud.google.com)
2. Crear un nuevo proyecto o seleccionar uno existente
3. Activar las APIs: **Google Sheets API** y **Google Drive API**

#### Paso 2 — Crear una Service Account
1. Ir a **IAM y Admin → Service Accounts**
2. Click en **Crear Service Account**
3. Asignar el rol **Editor** (o al menos Viewer para leer)
4. Crear una clave en formato **JSON** y descargarla

#### Paso 3 — Compartir la Spreadsheet
1. Abrí tu Google Sheet
2. Click en **Compartir**
3. Pegá el email de la Service Account (ejemplo: `smarket@proyecto.iam.gserviceaccount.com`)
4. Dale permisos de **Lector**

#### Paso 4 — Usar en la app
1. Ir a la barra lateral → **Google Sheets**
2. Pegar la URL de la Spreadsheet
3. Pegar el contenido del JSON de la Service Account
4. Click en **Conectar**

#### Formato esperado de la Spreadsheet
| N° de Pedido | Descripción Cliente | Dirección de entrega | Localidad | CP | Kilos | Valor | Bultos |
|---|---|---|---|---|---|---|---|
| 35268 | Sebastian Duarte | Av. S. Martín 7300 | Villa Devoto | 1419 | 114 | 570137 | 74 |
        """)

        with st.expander("🔑 Variables de entorno recomendadas (para producción)"):
            st.code("""
# En .streamlit/secrets.toml
[gsheets]
type = "service_account"
project_id = "tu-proyecto"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\\n..."
client_email = "smarket@tu-proyecto.iam.gserviceaccount.com"
client_id = "..."
            """, language="toml")
            st.code("""
# En app.py — leer desde secrets
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

creds = Credentials.from_service_account_info(
    st.secrets["gsheets"],
    scopes=["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)
            """, language="python")


if __name__ == "__main__":
    main()
