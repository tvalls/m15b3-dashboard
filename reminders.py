import os
import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ========= CONFIG POR VARIÁVEL DE AMBIENTE =========

TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_UPN      = os.getenv("USER_UPN")
ITEM_ID       = os.getenv("ITEM_ID")

SMTP_SERVER   = os.getenv("SMTP_SERVER")     
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER")      
SMTP_PASS     = os.getenv("SMTP_PASS")      
TO_EMAILS     = [e.strip() for e in os.getenv("TO_EMAILS", "").split(",") if e.strip()]


# ========= HELPER – GRAPH =========

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
    token = r.json()["access_token"]
    print("[DEBUG] Token Graph obtido com sucesso.")
    return token


def read_table(name: str) -> pd.DataFrame:
    print(f"[DEBUG] Lendo tabela: {name}")
    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    base = (
        f"https://graph.microsoft.com/v1.0/users/{USER_UPN}"
        f"/drive/items/{ITEM_ID}/workbook/tables('{name}')"
    )

    cols_resp = requests.get(f"{base}/columns", headers=headers, timeout=30)
    rows_resp = requests.get(f"{base}/rows", headers=headers, timeout=60)

    cols = cols_resp.json()
    rows = rows_resp.json()

    if "error" in cols:
        print(f"[DEBUG] Erro ao buscar colunas de {name}: {cols}")
        return pd.DataFrame()
    if "error" in rows:
        print(f"[DEBUG] Erro ao buscar rows de {name}: {rows}")
        return pd.DataFrame()

    columns = [c["name"] for c in cols.get("value", [])]
    values = [r["values"][0] for r in rows.get("value", [])]
    df = pd.DataFrame(values, columns=columns)
    print(f"[DEBUG] Tabela {name} carregada: {df.shape[0]} linhas, colunas: {list(df.columns)}")
    return df


def read_saldo_atual() -> float:
    """
    Lê o saldo atual em src!B7 (já considerando o que a planilha
    subtraiu para contas marcadas como PAGO).
    """
    print("[DEBUG] Lendo saldo atual em src!B7")
    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_UPN}"
        f"/drive/items/{ITEM_ID}/workbook/worksheets('src')/range(address='B7')"
    )
    r = requests.get(url, headers=headers, timeout=30)
    j = r.json()
    if "error" in j:
        print("[DEBUG] Erro ao ler saldo em src!B7:", j)
        return 0.0

    try:
        raw = j["values"][0][0]
    except Exception as e:
        print("[DEBUG] Não foi possível extrair valor de src!B7:", e, j)
        return 0.0

    # tenta converter de forma resiliente
    if isinstance(raw, (int, float)):
        saldo = float(raw)
    else:
        s = str(raw).strip()
        # tenta formato brasileiro
        s = s.replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            # assume . como milhar e , como decimal
            s = s.replace(".", "").replace(",", ".")
        elif "," in s and "." not in s:
            s = s.replace(",", ".")
        try:
            saldo = float(s)
        except Exception as e:
            print("[DEBUG] Falha ao converter saldo, valor bruto:", raw, "erro:", e)
            saldo = 0.0

    print("[DEBUG] Saldo atual lido:", saldo)
    return saldo


# ========= CARREGAR MOVBANK (COM DEBUG DE DATAS) =========

def load_movbank() -> pd.DataFrame:
    df = read_table("movbank").copy()
    if df.empty:
        print("[DEBUG] movbank veio vazio do Graph.")
        return df

    print(f"[DEBUG] movbank bruto: {df.shape[0]} linhas, colunas: {list(df.columns)}")

    df["VALOR"] = pd.to_numeric(df["VALOR"], errors="coerce")

    # VECTO pode vir como número (serial Excel) ou texto (dd/mm/aaaa)
    df["VECTO_NUM"] = pd.to_numeric(df["VECTO"], errors="coerce")

    mask_na = df["VECTO_NUM"].isna()
    if mask_na.any():
        print(f"[DEBUG] {mask_na.sum()} linhas com VECTO não numérico, tentando parse de data texto...")
        dt_txt = pd.to_datetime(df.loc[mask_na, "VECTO"], dayfirst=True, errors="coerce")
        num_from_txt = (dt_txt - pd.to_datetime("1899-12-30")).dt.days
        df.loc[mask_na, "VECTO_NUM"] = num_from_txt

    df["VECTO_DT"] = pd.to_datetime("1899-12-30") + pd.to_timedelta(df["VECTO_NUM"], unit="D")

    try:
        print("[DEBUG] Amostra VECTO / VECTO_NUM / VECTO_DT / STATUS:")
        print(df[["VECTO", "VECTO_NUM", "VECTO_DT", "STATUS"]].head(10))
        print("[DEBUG] Intervalo de VECTO_DT:",
              df["VECTO_DT"].min(), "->", df["VECTO_DT"].max())
    except Exception as e:
        print("[DEBUG] erro ao imprimir amostra de datas:", e)

    return df


# ========= FORMATADOR DE MOEDA =========

def brl(n: float) -> str:
    if pd.isna(n):
        return "R$ 0,00"
    s = f"{n:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")


# ========= ENVIO DE E-MAIL =========

def send_email(subject: str, html: str):
    if not (SMTP_SERVER and SMTP_USER and SMTP_PASS and TO_EMAILS):
        raise RuntimeError("Config SMTP/TO_EMAILS incompleta nas secrets.")

    print(f"[DEBUG] Enviando e-mail para: {TO_EMAILS}")
    msg = MIMEText(html, "html", "utf-8")
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(TO_EMAILS)
    msg["Subject"] = subject

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.sendmail(SMTP_USER, TO_EMAILS, msg.as_string())
    print("[DEBUG] E-mail enviado com sucesso.")


# ========= RENDERIZAÇÃO DE TABELA (ZEBRADA) =========

def render_table(df: pd.DataFrame) -> str:
    """Só a tabela, sem título/subtítulo/rodapé."""
    if df.empty:
        return "<p><strong>Sem contas no período.</strong></p>"

    rows_html = []
    for i, (_, r) in enumerate(df.iterrows()):
        bg = "#ffffff" if i % 2 == 0 else "#f3f4f6"
        data = (
            r["VECTO_DT"].date().strftime("%d/%m/%Y")
            if pd.notna(r.get("VECTO_DT"))
            else ""
        )
        rows_html.append(
            "<tr style='background:{bg}'>"
            "<td style='padding:6px;text-align:left'>{data}</td>"
            "<td style='padding:6px;text-align:left'>{desc}</td>"
            "<td style='padding:6px;text-align:left'>{forn}</td>"
            "<td style='padding:6px;text-align:left'>{valor}</td>"
            "<td style='padding:6px;text-align:left'>{status}</td>"
            "</tr>".format(
                bg=bg,
                data=data,
                desc=r.get("DESCRIÇÃO", ""),
                forn=r.get("FORNECEDOR", ""),
                valor=brl(r.get("VALOR")),
                status=r.get("STATUS", ""),
            )
        )

    tabela = (
        "<table style='border-collapse:collapse;width:100%;font-size:13px'>"
        "<thead>"
        "<tr style='background:#0b2545;color:#fff'>"
        "<th style='padding:8px;text-align:left'>Vencimento</th>"
        "<th style='padding:8px;text-align:left'>Descrição</th>"
        "<th style='padding:8px;text-align:left'>Fornecedor</th>"
        "<th style='padding:8px;text-align:left'>Valor</th>"
        "<th style='padding:8px;text-align:left'>Status</th>"
        "</tr></thead><tbody>"
        + "".join(rows_html) +
        "</tbody></table>"
    )
    return tabela


def html_base(titulo: str, subtitulo: str, corpo_html: str, extra_html: str = "") -> str:
    """Wrapper padrão do e-mail."""
    html = f"""
    <div style="font-family:Segoe UI,Roboto,Arial,sans-serif;max-width:840px;margin:auto">
      <h2 style="color:#0b2545;margin:0 0 4px 0">{titulo}</h2>
      <p style="color:#333;margin:0 0 16px 0">{subtitulo}</p>
      {extra_html}
      {corpo_html}
      <p style="color:#888;font-size:11px;margin-top:18px">
        Enviado automaticamente pelo painel financeiro M15B3 (dados da planilha no OneDrive).
      </p>
    </div>
    """
    return html


# ========= RESUMOS (DIÁRIO / SEMANAL) =========

def resumo_diario_html(saldo_atual_ajustado: float,
                       total_hoje: float,
                       total_atraso: float) -> str:
    """
    saldo_atual_ajustado já inclui de volta o valor das contas de hoje que
    estão marcadas como PAGO na planilha, para o e-mail enxergar essas
    contas como 'a pagar' no dia.
    """
    saldo_pos_dia = saldo_atual_ajustado - total_hoje

    linhas = [
        "<div style='margin:4px 0 22px 0'>",
        f"<p style='margin:2px 0'><strong>Saldo atual:</strong> {brl(saldo_atual_ajustado)}</p>",
        f"<p style='margin:2px 0'><strong>Total contas de hoje:</strong> {brl(total_hoje)}</p>",
        f"<p style='margin:2px 0'><strong>Saldo após pagar contas de hoje:</strong> {brl(saldo_pos_dia)}</p>",
    ]

    if total_atraso > 0:
        saldo_pos_atraso = saldo_atual_ajustado - total_atraso
        saldo_pos_dia_atraso = saldo_atual_ajustado - (total_hoje + total_atraso)
        linhas.extend([
            f"<p style='margin:10px 0 2px 0'><strong>Total contas atrasadas:</strong> {brl(total_atraso)}</p>",
            f"<p style='margin:2px 0'><strong>Saldo após pagar contas atrasadas:</strong> {brl(saldo_pos_atraso)}</p>",
            f"<p style='margin:2px 0'><strong>Saldo após pagar hoje + atrasadas:</strong> {brl(saldo_pos_dia_atraso)}</p>",
        ])

    linhas.append("</div>")
    return "".join(linhas)


def resumo_semanal_html(saldo_atual: float,
                        total_periodo: float,
                        inicio,
                        fim) -> str:
    saldo_final = saldo_atual - total_periodo
    html = f"""
    <div style='margin:4px 0 22px 0'>
      <p style='margin:2px 0'><strong>Saldo atual:</strong> {brl(saldo_atual)}</p>
      <p style='margin:2px 0'><strong>Total contas do período:</strong> {brl(total_periodo)}</p>
      <p style='margin:2px 0'><strong>Saldo após pagar contas do período ({inicio.strftime('%d/%m/%Y')}–{fim.strftime('%d/%m/%Y')}):</strong> {brl(saldo_final)}</p>
    </div>
    """
    return html


# ========= HTML DE CONTEÚDO =========

def html_lista(titulo: str, subtitulo: str, df: pd.DataFrame, extra_html: str = "") -> str:
    return html_base(titulo, subtitulo, render_table(df), extra_html=extra_html)


def html_diario(hoje,
                df_hoje: pd.DataFrame,
                df_atraso: pd.DataFrame,
                resumo_html: str) -> str:

    subt = (
        f"Contas que vencem hoje ({hoje.strftime('%d/%m/%Y')}) consideradas para o fluxo de caixa, "
        f"tratando contas já pagas hoje como agendadas neste resumo."
    )

    partes = []

    partes.append("<h3 style='margin-top:0'>Contas de hoje</h3>")
    partes.append(render_table(df_hoje))

    partes.append("<h3 style='margin-top:18px'>Contas atrasadas (sem informação de data de pagamento)</h3>")
    if df_atraso.empty:
        partes.append("<p><strong>Não há contas atrasadas.</strong></p>")
    else:
        partes.append(render_table(df_atraso))

    corpo = "".join(partes)
    return html_base("Lembrete diário – contas de hoje", subt, corpo_html=corpo, extra_html=resumo_html)


# ========= LEMBRETE DIÁRIO =========

def run_daily():
    tz = ZoneInfo("America/Sao_Paulo")
    hoje = datetime.now(tz).date()
    print(f"[DEBUG] Data de hoje (timezone BR): {hoje}")

    saldo_atual = read_saldo_atual()

    df = load_movbank()
    if df.empty:
        print("[DEBUG] movbank vazio após load_movbank.")
        resumo = resumo_diario_html(saldo_atual, 0.0, 0.0)
        html = html_base(
            "Lembrete diário – contas de hoje",
            f"Hoje ({hoje.strftime('%d/%m/%Y')}) não há contas cadastradas (movbank vazio).",
            "<p><strong>Sem contas no período.</strong></p>",
            extra_html=resumo
        )
        send_email(f"[M15B3] Lembrete diário – {hoje.strftime('%d/%m/%Y')}", html)
        return

    # --- Seleção de hoje e atrasadas ---
    mask_hoje   = df["VECTO_DT"].dt.date == hoje
    mask_atraso = df["VECTO_DT"].dt.date < hoje

    # Hoje: consideramos TODAS as contas do dia, inclusive PAGO,
    # porque para o e-mail elas serão tratadas como "agendadas".
    df_hoje = df[mask_hoje].copy()

    # Atrasadas: apenas as que ainda NÃO estão marcadas como PAGO
    mask_nao_pago = df["STATUS"] != "PAGO"
    df_atraso = df[mask_atraso & mask_nao_pago].copy()

    # Ajustar STATUS de hoje: onde está PAGO, exibimos como AGENDADO
    if not df_hoje.empty and "STATUS" in df_hoje.columns:
        df_hoje.loc[df_hoje["STATUS"] == "PAGO", "STATUS"] = "AGENDADO"

    df_hoje   = df_hoje.sort_values("VECTO_DT", ascending=True)
    df_atraso = df_atraso.sort_values("VECTO_DT", ascending=True)

    print(f"[DEBUG] Linhas com VECTO_DT == hoje (qualquer status): {mask_hoje.sum()}")
    print(f"[DEBUG] Linhas atrasadas (< hoje): {mask_atraso.sum()}")
    print(f"[DEBUG] Linhas atrasadas ainda não pagas: {df_atraso.shape[0]}")
    print(f"[DEBUG] Linhas de hoje (para e-mail): {df_hoje.shape[0]}")

    total_hoje = df_hoje["VALOR"].sum() if not df_hoje.empty else 0.0
    total_atraso = df_atraso["VALOR"].sum() if not df_atraso.empty else 0.0

    # Valor das contas de hoje que já estão marcadas como PAGO na planilha
    # (para devolver esse valor ao saldo e enxergar o pré-pagamento no e-mail)
    total_pago_hoje = df[mask_hoje & (df["STATUS"] == "PAGO")]["VALOR"].sum()
    print(f"[DEBUG] Total contas de hoje (todas): {total_hoje}")
    print(f"[DEBUG] Total contas atrasadas (não pagas): {total_atraso}")
    print(f"[DEBUG] Total de hoje já marcadas como PAGO na planilha: {total_pago_hoje}")

    saldo_ajustado = saldo_atual + total_pago_hoje
    print(f"[DEBUG] Saldo ajustado para o e-mail (saldo_atual + pagos de hoje): {saldo_ajustado}")

    resumo = resumo_diario_html(saldo_ajustado, total_hoje, total_atraso)
    html = html_diario(hoje, df_hoje, df_atraso, resumo_html=resumo)
    send_email(f"[M15B3] Contas de hoje – {hoje.strftime('%d/%m/%Y')}", html)


# ========= LEMBRETE SEMANAL (SEGUNDA) =========

def run_weekly():
    tz = ZoneInfo("America/Sao_Paulo")
    hoje = datetime.now(tz).date()
    print(f"[DEBUG] Rodando semanal. Hoje: {hoje} (weekday={hoje.weekday()})")

    sabado_anterior = hoje - timedelta(days=2)  # segunda - 2 = sábado
    sexta_seguinte  = hoje + timedelta(days=4)  # segunda + 4 = sexta

    saldo_atual = read_saldo_atual()
    df = load_movbank()
    if df.empty:
        resumo = resumo_semanal_html(saldo_atual, 0.0, sabado_anterior, sexta_seguinte)
        html = html_lista(
            "Lembrete semanal – agenda financeira",
            f"Período {sabado_anterior.strftime('%d/%m/%Y')} a {sexta_seguinte.strftime('%d/%m/%Y')}: sem contas.",
            df,
            extra_html=resumo
        )
        send_email(
            f"[M15B3] Lembrete semanal – {sabado_anterior.strftime('%d/%m')}–{sexta_seguinte.strftime('%d/%m')}",
            html
        )
        return

    mask_periodo = (df["VECTO_DT"].dt.date >= sabado_anterior) & (df["VECTO_DT"].dt.date <= sexta_seguinte)
    mask_status  = df["STATUS"] != "PAGO"
    alvo = df[mask_periodo & mask_status].copy()
    alvo = alvo.sort_values("VECTO_DT", ascending=True)

    print(f"[DEBUG] Linhas no período sábado-sexta: {mask_periodo.sum()}")
    print(f"[DEBUG] Linhas com STATUS != 'PAGO': {mask_status.sum()}")
    print(f"[DEBUG] Linhas no alvo semanal: {alvo.shape[0]}")

    total_periodo = alvo["VALOR"].sum() if not alvo.empty else 0.0
    resumo = resumo_semanal_html(saldo_atual, total_periodo, sabado_anterior, sexta_seguinte)

    subt = (
        f"Contas com vencimento de {sabado_anterior.strftime('%d/%m/%Y')} "
        f"a {sexta_seguinte.strftime('%d/%m/%Y')} que ainda não estão marcadas como PAGO."
    )
    html = html_lista("Lembrete semanal – agenda financeira", subt, alvo, extra_html=resumo)
    send_email(
        f"[M15B3] Semana {sabado_anterior.strftime('%d/%m')}–{sexta_seguinte.strftime('%d/%m')}",
        html
    )


# ========= MAIN =========

if __name__ == "__main__":
    tz = ZoneInfo("America/Sao_Paulo")
    agora = datetime.now(tz)
    hoje = agora.date()
    print(f"[DEBUG] Início do script reminders.py em {agora}")

    run_daily()

    if hoje.weekday() == 0:  # segunda
        run_weekly()

    print("[DEBUG] Fim do script reminders.py")
