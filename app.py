import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.io as pio
import requests
import streamlit as st
from dotenv import load_dotenv

# ======================
# CONFIGURAÇÕES INICIAIS
# ======================
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_UPN = os.getenv("USER_UPN")

DRIVE_ID = os.getenv("DRIVE_ID")
ITEM_ID = os.getenv("ITEM_ID")

APP_USER = os.getenv("APP_USER")
APP_PASS = os.getenv("APP_PASS")

# ====== ESTILO E CORES ======
pio.templates["ello"] = {
    "layout": {
        "font": {"family": "Segoe UI, Roboto, sans-serif", "color": "#1A1A1A"},
        "paper_bgcolor": "#FFFFFF",
        "plot_bgcolor": "#FFFFFF",
        "title": {"font": {"size": 18, "color": "#123C5E"}},
        "colorway": ["#123C5E", "#4A90E2", "#D84C4C"],
    }
}
pio.templates.default = "ello"

STYLE = """
<style>
body, .stApp { background-color:#F5F7FA; color:#1A1A1A; font-family:"Segoe UI","Roboto",sans-serif; }
h1,h2,h3,h4{color:#123C5E;font-weight:600;}
h1{text-align:center;margin-bottom:0.3em;}
.logo-container{display:flex;justify-content:center;align-items:center;margin-bottom:1em;}
.logo-container img{width:220px;}
.stMetric{background:#FFFFFF;border-radius:14px;padding:12px 18px;box-shadow:0 3px 10px rgba(0,0,0,0.08);text-align:center;}
[data-testid="stMetricValue"]{color:#123C5E;font-weight:700;font-size:1.2rem;}
[data-testid="stMetricLabel"]{color:#3E4C59;font-weight:500;}
input,textarea,select{background:#FFFFFF;color:#1A1A1A;border:1px solid #C7D0D9;border-radius:8px;}
input:focus{border-color:#4A90E2;box-shadow:0 0 0 1px #4A90E2;}
label, .stTextInput label{color:#1A1A1A !important;}
.stButton>button{background:linear-gradient(90deg,#123C5E,#4A90E2);color:#fff;font-weight:600;border:none;border-radius:8px;padding:0.6em 1.4em;box-shadow:0 2px 6px rgba(0,0,0,0.2);}
thead tr th{background:#F0F4F8;color:#123C5E;font-weight:600;white-space:normal;overflow-wrap:anywhere;}
tbody tr td{white-space:normal;overflow-wrap:anywhere;}
.loader-container {
  display:flex;
  flex-direction:column;
  justify-content:flex-start;
  align-items:center;
  margin-top:2em;
  text-align:center;
  color:#123C5E;
}
.loader-icon {
  font-size:3rem;
  animation:spin 1.2s linear infinite;
}
@keyframes spin {from{transform:rotate(0deg);} to{transform:rotate(360deg);}}
</style>
"""

# ========= HELPERS =========


def brl(n: float) -> str:
    if pd.isna(n):
        return "R$ 0,00"
    s = f"{float(n):,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")


def get_graph_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


@st.cache_data(ttl=600, show_spinner=False)
def read_table(name: str) -> pd.DataFrame:
    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    base = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}"
        f"/items/{ITEM_ID}/workbook/tables('{name}')"
    )

    cols_resp = requests.get(f"{base}/columns", headers=headers, timeout=30)
    rows_resp = requests.get(f"{base}/rows", headers=headers, timeout=60)

    try:
        cols = cols_resp.json()
        rows = rows_resp.json()
    except Exception:
        return pd.DataFrame()

    if "error" in cols or "error" in rows:
        return pd.DataFrame()

    columns = [c.get("name", "") for c in cols.get("value", [])]
    values = [r["values"][0] for r in rows.get("value", [])]
    return pd.DataFrame(values, columns=columns)


@st.cache_data(ttl=600, show_spinner=False)
def read_cell(address: str, sheet: str = "src") -> float:
    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}"
        f"/items/{ITEM_ID}/workbook/worksheets('{sheet}')/range(address='{address}')"
    )
    r = requests.get(url, headers=headers, timeout=20)
    j = r.json()
    if "values" not in j:
        return 0.0

    raw = j["values"][0][0]

    if isinstance(raw, (int, float)):
        return float(raw)

    s = str(raw).strip().replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def load_all():
    socios = read_table("SOCIOS")
    movbank = read_table("movbank")
    fornece = read_table("fornece")
    saldo_bancario = read_cell("B7", "src")
    return socios, movbank, fornece, saldo_bancario


# ====== LOGIN ======


def login():
    st.markdown(STYLE, unsafe_allow_html=True)
    st.markdown(
        '<div class="logo-container"><img src="https://www.elloconsultoria.com.br/LOGO-SEM%20FUNDO-PARA%20FUNDO%20CLARO.png"></div>',
        unsafe_allow_html=True,
    )
    st.markdown("<h1>🔐 Acesso ao Painel Financeiro</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if u == APP_USER and p == APP_PASS:
            st.session_state["auth"] = True
            st.rerun()
        else:
            st.error("Credenciais inválidas.")


# ====== DASHBOARD ======


def dashboard():
    st.markdown(STYLE, unsafe_allow_html=True)
    st.markdown(
        '<div class="logo-container"><img src="https://www.elloconsultoria.com.br/LOGO-SEM%20FUNDO-PARA%20FUNDO%20CLARO.png"></div>',
        unsafe_allow_html=True,
    )
    st.markdown("<h1>Painel Financeiro SPE M15B3</h1>", unsafe_allow_html=True)
    st.divider()

    # Loader
    loader_html = """
    <div class="loader-container">
        <div class="loader-icon">🔄</div>
        <h3>Carregando dados...</h3>
    </div>
    """
    loader_placeholder = st.empty()
    loader_placeholder.markdown(loader_html, unsafe_allow_html=True)

    socios, movbank, fornece, saldo_bancario = load_all()

    loader_placeholder.empty()

    # ============ SALDO BANCÁRIO ============
    hoje = date.today()
    st.subheader(f"💳 Saldo bancário em {hoje.strftime('%d/%m/%Y')}:")
    st.write(f"**{brl(saldo_bancario)}**")

    st.divider()

    # ============ SOCIOS ============
    st.header("💰 Integralização dos Sócios")

    if socios is None or socios.empty:
        st.warning("A tabela SOCIOS veio vazia.")
    else:
        # Converter para numérico
        for col in [
            "VALOR SUBSCRITO",
            "VALOR INTEGRALIZADO",
            "VALOR A INTEGRALIZAR/REEMBOLSAR",
        ]:
            if col in socios.columns:
                socios[col] = pd.to_numeric(socios[col], errors="coerce")

        total_int = socios.get("VALOR INTEGRALIZADO", pd.Series(dtype=float)).sum()
        total_a_int = socios.get(
            "VALOR A INTEGRALIZAR/REEMBOLSAR", pd.Series(dtype=float)
        ).sum()
        perc_global = (
            total_int / (total_int + total_a_int)
            if (total_int + total_a_int) > 0
            else 0
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Integralizado", brl(total_int))
        c2.metric("A Integralizar", brl(total_a_int))
        c3.metric("Percentual Global", f"{perc_global*100:.2f}%")

        socios_display = socios.copy()
        if "VALOR A INTEGRALIZAR/REEMBOLSAR" in socios_display.columns:
            socios_display = socios_display.rename(
                columns={"VALOR A INTEGRALIZAR/REEMBOLSAR": "VALOR A INTEGRALIZAR"}
            )

        if "PERCENTUAL COTAS" in socios_display.columns:
            socios_display["PERCENTUAL COTAS"] = pd.to_numeric(
                socios_display["PERCENTUAL COTAS"], errors="coerce"
            ).apply(lambda x: f"{x:.2%}")

        for col_fmt in ["VALOR INTEGRALIZADO", "VALOR A INTEGRALIZAR"]:
            if col_fmt in socios_display.columns:
                socios_display[col_fmt] = socios_display[col_fmt].apply(brl)

        cols_socios = [
            c
            for c in [
                "SÓCIO",
                "QUOTAS",
                "PERCENTUAL COTAS",
                "VALOR INTEGRALIZADO",
                "VALOR A INTEGRALIZAR",
            ]
            if c in socios_display.columns
        ]

        st.dataframe(
            socios_display[cols_socios],
            width="stretch",
            hide_index=True,
        )

        st.subheader("Participação Individual")
        cols_plot = st.columns(2)

        for i, (_, row) in enumerate(socios.iterrows()):
            subscrito = float(row.get("VALOR SUBSCRITO", 0) or 0)
            integralizado = float(row.get("VALOR INTEGRALIZADO", 0) or 0)
            nome = row.get("SÓCIO", f"Sócio {i+1}")

            if subscrito <= 0 and integralizado <= 0:
                continue

            if integralizado > subscrito:
                excedente = integralizado - subscrito
                perc_excedente = (excedente / subscrito) * 100 if subscrito > 0 else 0
                valores = [100, perc_excedente]
                labels = [
                    "Integralizado (100%)",
                    f"Excedente ({perc_excedente:.2f}%)",
                ]
                cores = ["#4A90E2", "#D84C4C"]
            else:
                perc_integral = (integralizado / subscrito) * 100 if subscrito > 0 else 0
                perc_faltante = 100 - perc_integral
                valores = [perc_integral, perc_faltante]
                labels = [
                    f"Integralizado ({perc_integral:.2f}%)",
                    f"A Integralizar ({perc_faltante:.2f}%)",
                ]
                cores = ["#4A90E2", "#123C5E"]

            fig = px.pie(
                values=valores,
                names=labels,
                hole=0.35,
                title=nome,
            )
            fig.update_traces(
                textinfo="label",
                textfont_size=13,
                marker=dict(line=dict(color="#fff", width=2)),
                marker_colors=cores,
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", y=-0.1),
                height=420,
                margin=dict(l=20, r=20, t=60, b=20),
            )
            cols_plot[i % 2].plotly_chart(fig, width="stretch")

            if i % 2 == 1 and i < len(socios) - 1:
                cols_plot = st.columns(2)

    # ========= CONTAS A PAGAR / PAGAS =========
    st.divider()
    st.header("📅 Contas a Pagar e Pagas")

    if movbank is None or movbank.empty:
        st.warning("Nenhum dado encontrado na tabela 'movbank'.")
        return

    # Converter VALOR
    if "VALOR" in movbank.columns:
        movbank["VALOR"] = pd.to_numeric(movbank["VALOR"], errors="coerce")
    else:
        movbank["VALOR"] = 0.0

    # Tratar VECTO como data
    movbank["VECTO_NUM"] = pd.to_numeric(movbank["VECTO"], errors="coerce")
    mask_na = movbank["VECTO_NUM"].isna()
    if mask_na.any():
        dt_txt = pd.to_datetime(
            movbank.loc[mask_na, "VECTO"], dayfirst=True, errors="coerce"
        )
        num_from_txt = (dt_txt - pd.to_datetime("1899-12-30")).dt.days
        movbank.loc[mask_na, "VECTO_NUM"] = num_from_txt

    movbank["VECTO_DT"] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
        movbank["VECTO_NUM"], unit="D"
    )
    movbank["VECTO"] = movbank["VECTO_DT"].dt.strftime("%d/%m/%Y")

    movbank = movbank.sort_values(by="VECTO_DT", ascending=True)

    total_pago = movbank[movbank["STATUS"] == "PAGO"]["VALOR"].sum()
    total_previsto = movbank[
        movbank["STATUS"].isin(["PREVISTO", "ATRASADO", "AGENDADO"])
    ]["VALOR"].sum()

    c4, c5 = st.columns(2)
    c4.metric("💸 Total Pago (Global)", brl(total_pago))
    c5.metric("📅 Previsto / Atrasado / Agendado", brl(total_previsto))

    # Filtros – agora padrão "Semana"
    opcoes = ["A pagar", "Pago", "Hoje", "Semana", "Todos"]
    filtro = st.selectbox("Visualização", opcoes, index=3)

    hoje_dt = hoje

    if filtro == "A pagar":
        df_f = movbank[movbank["STATUS"].isin(["PREVISTO", "ATRASADO", "AGENDADO"])]
    elif filtro == "Pago":
        df_f = movbank[movbank["STATUS"] == "PAGO"]
    elif filtro == "Hoje":
        df_f = movbank[movbank["VECTO_DT"].dt.date == hoje_dt]
    elif filtro == "Semana":
        # do último sábado até a próxima sexta
        # weekday(): segunda=0 ... domingo=6, sábado=5
        dias_ate_sabado = (hoje_dt.weekday() - 5) % 7
        sabado_anterior = hoje_dt - timedelta(days=dias_ate_sabado)
        dias_ate_sexta = (4 - hoje_dt.weekday()) % 7
        sexta_seguinte = hoje_dt + timedelta(days=dias_ate_sexta)
        df_f = movbank[
            (movbank["VECTO_DT"].dt.date >= sabado_anterior)
            & (movbank["VECTO_DT"].dt.date <= sexta_seguinte)
        ]
    else:
        df_f = movbank

    df_f = df_f.sort_values("VECTO_DT", ascending=True)

    st.write(f"**Total filtrado:** {brl(df_f['VALOR'].sum())}")

    df_display = df_f.copy()
    df_display["VALOR"] = df_display["VALOR"].apply(brl)

    cols_mov = [
        c
        for c in ["VECTO", "DESCRIÇÃO", "FORNECEDOR", "VALOR", "STATUS"]
        if c in df_display.columns
    ]

    st.dataframe(
        df_display[cols_mov],
        width="stretch",
        hide_index=True,
    )


# ====== EXECUÇÃO ======

if "auth" not in st.session_state:
    st.session_state["auth"] = False

if not st.session_state["auth"]:
    login()
else:
    dashboard()
