"""
email_sender.py — Generación de emails HTML y envío via Mailgun
Responsabilidades: plantillas de cards, secciones, email completo, envío
"""

import os

import requests

MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY", "")
MAILGUN_DOMAIN  = os.getenv("MAILGUN_DOMAIN", "metrica.pe")
REMITENTE       = f"monitoreo@{MAILGUN_DOMAIN}"

# ════════════════════════════════════════════════════
# CONSTANTES DE PRESENTACIÓN
# ════════════════════════════════════════════════════

TONO_COLORES = {
    "positivo": {"bg": "#E8F5E9", "border": "#4CAF50", "text": "#1B5E20", "label": "Positivo"},
    "negativo": {"bg": "#FFEBEE", "border": "#F44336", "text": "#B71C1C", "label": "Negativo"},
    "neutro":   {"bg": "#EEF2F7", "border": "#90A4AE", "text": "#37474F", "label": "Neutro"},
    "":         {"bg": "#EEF2F7", "border": "#90A4AE", "text": "#37474F", "label": "—"},
}

TIER_BADGE = {
    "I":              {"bg": "#003366", "color": "#fff",     "label": "Tier I"},
    "II":             {"bg": "#1D4ED8", "color": "#fff",     "label": "Tier II"},
    "III":            {"bg": "#5A6A7E", "color": "#fff",     "label": "Tier III"},
    "Sin clasificar": {"bg": "#E5E7EB", "color": "#374151",  "label": "—"},
}


# ════════════════════════════════════════════════════
# GENERADORES DE HTML
# ════════════════════════════════════════════════════

def card_email(r: dict, es_alerta: bool = False) -> str:
    """Genera el HTML de una card individual para el email."""
    tc = TONO_COLORES.get(r.get("tono", ""), TONO_COLORES[""])
    tb = TIER_BADGE.get(r.get("tier", "Sin clasificar"), TIER_BADGE["Sin clasificar"])

    if r["tipo"] == "youtube":
        tipo_lbl, tipo_bg, tipo_clr = "YouTube",      "#FEE2E2", "#991B1B"
    elif r["tipo"] == "news":
        tipo_lbl, tipo_bg, tipo_clr = "Google News",  "#FEF3C7", "#92400E"
    else:
        tipo_lbl, tipo_bg, tipo_clr = "Google Web",   "#EFF6FF", "#1E40AF"

    alerta_banner = (
        '<p style="margin:0 0 6px;font-size:11px;color:#7C3AED;font-weight:700">⚡ NUEVA MENCIÓN</p>'
        if es_alerta else ""
    )

    tono_row = ""
    if r.get("tono"):
        just = r.get("justificacion", "")
        tono_row = f"""
        <p style="margin:0;padding:5px 10px;background:{tc['bg']};border-radius:4px;
                  font-size:11px;color:{tc['text']}">
          <b>Análisis:</b> {just}
        </p>"""

    tono_badge = (
        f'<span style="background:{tc["bg"]};color:{tc["text"]};font-size:11px;'
        f'font-weight:600;padding:2px 8px;border-radius:3px">{tc["label"]}</span>&nbsp;'
        if r.get("tono") else ""
    )

    ver_label = "Ver video ↗" if r["tipo"] == "youtube" else "Ver nota ↗"
    icono     = "▶" if r["tipo"] == "youtube" else "🌐"
    snippet   = r["snippet"]
    snippet_truncado = snippet[:200] + ("…" if len(snippet) > 200 else "")

    return f"""
    <tr><td style="padding:0 0 10px">
      <table width="100%" style="background:#fff;border:1px solid #D0D9E6;
             border-left:4px solid {tc['border']};border-radius:6px">
        <tr><td style="padding:12px 16px">
          {alerta_banner}
          <table width="100%"><tr>
            <td>
              {tono_badge}
              <span style="background:{tipo_bg};color:{tipo_clr};font-size:11px;
                     padding:2px 8px;border-radius:3px">{tipo_lbl}</span>
              &nbsp;
              <span style="background:{tb['bg']};color:{tb['color']};font-size:10px;
                     font-weight:700;padding:2px 7px;border-radius:3px">{tb['label']}</span>
            </td>
            <td align="right" style="font-size:11px;color:#5A6A7E">{r['fecha']}</td>
          </tr></table>
          <p style="margin:8px 0 4px;font-size:14px;font-weight:700">
            <a href="{r['url']}" style="color:#004080;text-decoration:none">{r['titulo']}</a>
          </p>
          <p style="margin:4px 0 6px;font-size:12px;color:#5A6A7E">{snippet_truncado}</p>
          {tono_row}
          <p style="margin:6px 0 0;font-size:11px;color:#5A6A7E">
            {icono} {r['fuente']} &nbsp;·&nbsp;
            <a href="{r['url']}" style="color:#0066CC">{ver_label}</a></p>
        </td></tr>
      </table>
    </td></tr>"""


def seccion_email(titulo: str, color: str, items: list[dict],
                  es_alerta: bool = False) -> str:
    """Genera una sección completa (encabezado + cards) para el email."""
    if not items:
        return ""
    cards = "".join(card_email(r, es_alerta) for r in items)
    n = len(items)
    return f"""
    <tr><td style="padding:14px 0 6px">
      <p style="margin:0;padding:6px 12px;background:{color};color:#fff;
                font-size:12px;font-weight:700;border-radius:4px">
        {titulo} · {n} resultado{'s' if n != 1 else ''}
      </p>
    </td></tr>
    {cards}"""


def generar_email_html(keyword: str, resultados: list[dict],
                       fecha: str, modo: str = "manual") -> str:
    """
    Genera el HTML completo del email de monitoreo.
    Separa automáticamente medios vs redes sociales.
    """
    medios = [r for r in resultados if not r.get("es_red")]
    redes  = [r for r in resultados if r.get("es_red")]
    n_pos  = sum(1 for r in resultados if r.get("tono") == "positivo")
    n_neg  = sum(1 for r in resultados if r.get("tono") == "negativo")
    n_neu  = sum(1 for r in resultados if r.get("tono") == "neutro")
    es_alerta = (modo == "alerta")
    modo_txt  = {
        "alerta":  "⚡ Alerta inmediata",
        "diario":  "Resumen diario",
        "manual":  "Búsqueda manual",
    }.get(modo, "Manual")

    cuerpo  = seccion_email("📰 Medios y páginas web",        "#003366", medios, es_alerta)
    cuerpo += seccion_email("📱 Redes sociales y YouTube",    "#6D28D9", redes,  es_alerta)

    stats = f"""
    <td align="center"><p style="margin:0;color:#fff;font-size:18px;font-weight:700">{len(resultados)}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">menciones</p></td>
    <td align="center" style="border-left:1px solid rgba(255,255,255,.2)">
      <p style="margin:0;color:#fff;font-size:16px;font-weight:700">{len(medios)}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">medios</p></td>
    <td align="center" style="border-left:1px solid rgba(255,255,255,.2)">
      <p style="margin:0;color:#C4B5FD;font-size:16px;font-weight:700">{len(redes)}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">redes+YT</p></td>"""

    if n_pos or n_neg or n_neu:
        stats += f"""
    <td align="center" style="border-left:1px solid rgba(255,255,255,.2)">
      <p style="margin:0;color:#81C784;font-size:16px;font-weight:700">{n_pos}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">positivas</p></td>
    <td align="center" style="border-left:1px solid rgba(255,255,255,.2)">
      <p style="margin:0;color:#EF9A9A;font-size:16px;font-weight:700">{n_neg}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">negativas</p></td>"""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;background:#F4F6F9;font-family:Arial,sans-serif">
<table width="100%" style="background:#F4F6F9"><tr><td align="center" style="padding:24px 16px">
<table width="620" style="max-width:620px;width:100%">
  <tr><td style="background:#003366;border-radius:8px 8px 0 0;padding:22px 28px">
    <table width="100%"><tr>
      <td><p style="margin:0;color:#fff;opacity:.6;font-size:9px;letter-spacing:2px">
            {modo_txt.upper()} · MONITOREO DE MEDIOS</p>
          <p style="margin:4px 0 0;color:#fff;font-size:20px;font-weight:700">Métrica.</p></td>
      <td align="right">
        <p style="margin:0;color:#fff;opacity:.7;font-size:11px">{fecha}</p>
        <p style="margin:2px 0 0;color:#fff;opacity:.5;font-size:10px">Lima, Perú</p></td>
    </tr></table>
  </td></tr>
  <tr><td style="background:#004080;padding:12px 28px">
    <table width="100%"><tr>{stats}</tr></table>
  </td></tr>
  <tr><td style="background:#F4F6F9;padding:8px 28px 20px">
    <p style="font-size:13px;font-weight:700;color:#003366;
              border-bottom:2px solid #003366;padding-bottom:5px;margin-bottom:0">
      Keyword: {keyword}</p>
    <table width="100%">{cuerpo}</table>
  </td></tr>
  <tr><td style="background:#003366;border-radius:0 0 8px 8px;padding:12px 28px;text-align:center">
    <p style="margin:0;color:#fff;opacity:.4;font-size:10px">
      Métrica Monitor · {modo_txt} · {fecha}</p>
  </td></tr>
</table></td></tr></table></body></html>"""


# ════════════════════════════════════════════════════
# ENVÍO MAILGUN
# ════════════════════════════════════════════════════

def enviar_mailgun(html: str, asunto: str, destinatarios: list[str]) -> bool:
    """
    Envía el email HTML a todos los destinatarios via Mailgun.
    Devuelve True solo si todos los envíos fueron exitosos.
    """
    if not MAILGUN_API_KEY or not destinatarios:
        return False
    try:
        for dest in destinatarios:
            r = requests.post(
                f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
                auth=("api", MAILGUN_API_KEY),
                data={
                    "from":    f"Monitoreo Métrica <{REMITENTE}>",
                    "to":      dest,
                    "subject": asunto,
                    "html":    html,
                },
                timeout=15,
            )
            if r.status_code not in (200, 202):
                print(f"[email] Mailgun error {r.status_code}: {r.text[:200]}")
                return False
        return True
    except Exception as exc:
        print(f"[email] Error Mailgun: {exc}")
        return False
