import os, json, hashlib, time
from datetime import datetime
from contextlib import asynccontextmanager

import requests
import anthropic
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from database import (
    init_db, get_keywords_permanentes, save_keyword_permanente,
    delete_keyword_permanente, get_destinatarios, save_destinatario,
    delete_destinatario, save_historial, get_historial, get_historial_by_id,
    url_ya_enviada, marcar_url_enviada, limpiar_urls_antiguas
)
from tiers import get_tier, es_red_social, orden_tier

# ── CONFIG DESDE VARIABLES DE ENTORNO ────────────────
SERPER_API_KEY    = os.getenv("SERPER_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MAILGUN_API_KEY   = os.getenv("MAILGUN_API_KEY", "")
MAILGUN_DOMAIN    = os.getenv("MAILGUN_DOMAIN", "metrica.pe")
REMITENTE         = f"monitoreo@{MAILGUN_DOMAIN}"

scheduler = BackgroundScheduler(timezone="America/Lima")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.start()
    recargar_jobs()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ════════════════════════════════════════════════════
# MOTOR DE BÚSQUEDA
# ════════════════════════════════════════════════════

def buscar_web(keyword, fecha_inicio=None, fecha_fin=None, num=20):
    payload = {"q": keyword, "gl": "pe", "num": num}
    payload["tbs"] = (
        f"cdr:1,cd_min:{fecha_inicio},cd_max:{fecha_fin}"
        if fecha_inicio and fecha_fin else "qdr:d"
    )
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/search",
                          json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception:
        return []


def buscar_news(keyword, num=20):
    payload = {"q": keyword, "gl": "pe", "num": num}
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/news",
                          json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("news", [])
    except Exception:
        return []


def parsear(item, tipo):
    url = item.get("link", "")
    return {
        "tipo":    tipo,
        "titulo":  item.get("title",   "Sin título").strip(),
        "snippet": item.get("snippet", "").strip(),
        "fuente":  item.get("source",  item.get("displayLink", "Desconocida")).strip(),
        "fecha":   item.get("date",    "Fecha no disponible"),
        "url":     url,
        "tier":    get_tier(url),
        "es_red":  es_red_social(url),
    }


def buscar_keyword(keyword, fecha_inicio=None, fecha_fin=None, num=20):
    """Busca en web + news, deduplica, ordena: medios por tier primero, redes al final."""
    todos = {}
    for item in buscar_web(keyword, fecha_inicio, fecha_fin, num):
        r = parsear(item, "web")
        if r["url"]:
            uid = hashlib.md5(r["url"].encode()).hexdigest()
            if uid not in todos:
                todos[uid] = r
    for item in buscar_news(keyword, num):
        r = parsear(item, "news")
        if r["url"]:
            uid = hashlib.md5(r["url"].encode()).hexdigest()
            if uid not in todos:
                todos[uid] = r
    resultados = list(todos.values())
    resultados.sort(key=lambda x: (int(x["es_red"]), orden_tier(x["tier"])))
    return resultados


# ════════════════════════════════════════════════════
# ANÁLISIS DE TONO (solo para búsqueda manual y resumen diario)
# En alertas NO se usa Claude para ahorrar costo
# ════════════════════════════════════════════════════

PROMPT_TONO = """Eres analista de monitoreo de medios peruano.
Analiza si la mención es positiva, negativa o neutra PARA LA KEYWORD/MARCA.
Responde SOLO JSON válido sin texto extra:
{"tono":"positivo"|"negativo"|"neutro","justificacion":"Una oración.","relevancia":"alta"|"media"|"baja"}"""


def analizar_tono(titulo, snippet, keyword):
    if not ANTHROPIC_API_KEY:
        return {"tono": "neutro", "justificacion": "Sin API Claude configurada.", "relevancia": "media"}
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=120,
            system=PROMPT_TONO,
            messages=[{"role": "user", "content":
                f"Keyword: {keyword}\nTítulo: {titulo}\nSnippet: {snippet[:200]}"}]
        )
        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception:
        return {"tono": "neutro", "justificacion": "Error en análisis.", "relevancia": "media"}


def analizar_resultados(resultados, keyword):
    out = []
    for r in resultados:
        d = analizar_tono(r["titulo"], r["snippet"], keyword)
        r.update(d)
        out.append(r)
        time.sleep(0.2)
    return out


# ════════════════════════════════════════════════════
# GENERADOR DE EMAIL HTML
# ════════════════════════════════════════════════════

TONO_COLORES = {
    "positivo": {"bg":"#E8F5E9","border":"#4CAF50","text":"#1B5E20","label":"Positivo"},
    "negativo": {"bg":"#FFEBEE","border":"#F44336","text":"#B71C1C","label":"Negativo"},
    "neutro":   {"bg":"#EEF2F7","border":"#90A4AE","text":"#37474F","label":"Neutro"},
    "":         {"bg":"#EEF2F7","border":"#90A4AE","text":"#37474F","label":"—"},
}
TIER_BADGE = {
    "I":             {"bg":"#003366","color":"#fff","label":"Tier I"},
    "II":            {"bg":"#1D4ED8","color":"#fff","label":"Tier II"},
    "III":           {"bg":"#5A6A7E","color":"#fff","label":"Tier III"},
    "Sin clasificar":{"bg":"#E5E7EB","color":"#374151","label":"—"},
}


def card_email(r, es_alerta=False):
    tc  = TONO_COLORES.get(r.get("tono",""), TONO_COLORES[""])
    tb  = TIER_BADGE.get(r.get("tier","Sin clasificar"), TIER_BADGE["Sin clasificar"])
    tipo_lbl = "Google News" if r["tipo"] == "news" else "Google Web"
    tipo_bg  = "#FEF3C7" if r["tipo"] == "news" else "#EFF6FF"
    tipo_clr = "#92400E" if r["tipo"] == "news" else "#1E40AF"
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
    return f"""
    <tr><td style="padding:0 0 10px">
      <table width="100%" style="background:#fff;border:1px solid #D0D9E6;
             border-left:4px solid {tc['border']};border-radius:6px">
        <tr><td style="padding:12px 16px">
          {alerta_banner}
          <table width="100%"><tr>
            <td>
              {'<span style="background:'+tc['bg']+';color:'+tc['text']+';font-size:11px;font-weight:600;padding:2px 8px;border-radius:3px">'+tc['label']+'</span>&nbsp;' if r.get('tono') else ''}
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
          <p style="margin:4px 0 6px;font-size:12px;color:#5A6A7E">
            {r['snippet'][:200]}{'…' if len(r['snippet'])>200 else ''}</p>
          {tono_row}
          <p style="margin:6px 0 0;font-size:11px;color:#5A6A7E">
            🌐 {r['fuente']} &nbsp;·&nbsp;
            <a href="{r['url']}" style="color:#0066CC">Ver nota ↗</a></p>
        </td></tr>
      </table>
    </td></tr>"""


def seccion_email(titulo, color, items, es_alerta=False):
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


def generar_email_html(keyword, resultados, fecha, modo="manual"):
    medios = [r for r in resultados if not r.get("es_red")]
    redes  = [r for r in resultados if r.get("es_red")]
    n_pos  = sum(1 for r in resultados if r.get("tono") == "positivo")
    n_neg  = sum(1 for r in resultados if r.get("tono") == "negativo")
    n_neu  = sum(1 for r in resultados if r.get("tono") == "neutro")
    es_alerta = (modo == "alerta")
    modo_txt  = {"alerta":"⚡ Alerta inmediata","diario":"Resumen diario","manual":"Búsqueda manual"}.get(modo, "Manual")

    cuerpo  = seccion_email("📰 Medios y páginas web", "#003366", medios, es_alerta)
    cuerpo += seccion_email("📱 Redes sociales", "#6D28D9", redes, es_alerta)

    stats = f"""
    <td align="center"><p style="margin:0;color:#fff;font-size:18px;font-weight:700">{len(resultados)}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">menciones</p></td>
    <td align="center" style="border-left:1px solid rgba(255,255,255,.2)">
      <p style="margin:0;color:#fff;font-size:16px;font-weight:700">{len(medios)}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">medios</p></td>
    <td align="center" style="border-left:1px solid rgba(255,255,255,.2)">
      <p style="margin:0;color:#C4B5FD;font-size:16px;font-weight:700">{len(redes)}</p>
      <p style="margin:2px 0 0;color:#B8D4F0;font-size:10px">redes</p></td>"""

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
# ENVÍO CON MAILGUN
# ════════════════════════════════════════════════════

def enviar_mailgun(html, asunto, destinatarios):
    if not MAILGUN_API_KEY or not destinatarios:
        return False
    try:
        for dest in destinatarios:
            r = requests.post(
                f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
                auth=("api", MAILGUN_API_KEY),
                data={"from": f"Monitoreo Métrica <{REMITENTE}>",
                      "to": dest, "subject": asunto, "html": html},
                timeout=15
            )
            if r.status_code not in (200, 202):
                print(f"Mailgun error {r.status_code}: {r.text[:200]}")
                return False
        return True
    except Exception as e:
        print(f"Error Mailgun: {e}")
        return False


# ════════════════════════════════════════════════════
# JOBS DEL SCHEDULER
# ════════════════════════════════════════════════════

def job_alerta(kw_id, keyword, freq_minutos):
    """
    Modo alerta: busca cada N minutos.
    - Solo manda email si hay URLs NUEVAS (no enviadas antes).
    - NO usa Claude para ahorrar costo en alertas.
    - Un email por cada noticia nueva.
    """
    print(f"[ALERTA] Verificando: '{keyword}'")
    resultados = buscar_keyword(keyword, num=15)
    dests = [d["email"] for d in get_destinatarios()]
    nuevos = 0

    for r in resultados:
        if not r["url"] or url_ya_enviada(kw_id, r["url"]):
            continue
        # Sin análisis Claude en alerta para ahorrar — solo título + snippet
        r["tono"] = ""
        r["justificacion"] = ""
        fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
        html   = generar_email_html(keyword, [r], fecha, modo="alerta")
        asunto = f"⚡ Alerta Métrica · {keyword} · {r['titulo'][:55]}"
        ok     = enviar_mailgun(html, asunto, dests)
        if ok:
            marcar_url_enviada(kw_id, r["url"])
            save_historial(kw_id, keyword, "alerta", 1, True, html)
            nuevos += 1
            print(f"  → Alerta enviada: {r['titulo'][:60]}")
        time.sleep(0.5)

    if nuevos == 0:
        print(f"[ALERTA] '{keyword}': sin noticias nuevas, no se envió nada")
    else:
        print(f"[ALERTA] '{keyword}': {nuevos} alerta(s) enviada(s)")


def job_diario(kw_id, keyword):
    """
    Modo diario: busca una vez al día a las 12pm.
    - Incluye análisis de tono con Claude.
    - Manda un solo email con todo lo encontrado.
    """
    print(f"[DIARIO] Ejecutando: '{keyword}'")
    resultados = buscar_keyword(keyword, num=20)
    if resultados:
        resultados = analizar_resultados(resultados, keyword)
    fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
    html   = generar_email_html(keyword, resultados, fecha, modo="diario")
    dests  = [d["email"] for d in get_destinatarios()]
    asunto = f"Métrica Monitor · Resumen diario · {keyword} · {fecha}"
    ok     = enviar_mailgun(html, asunto, dests)
    save_historial(kw_id, keyword, "diario", len(resultados), ok, html)
    print(f"[DIARIO] '{keyword}': {len(resultados)} resultados, enviado={ok}")


def recargar_jobs():
    """Recarga todos los jobs del scheduler según las keywords activas en BD."""
    scheduler.remove_all_jobs()
    # Limpieza automática de URLs antiguas cada madrugada
    scheduler.add_job(
        lambda: limpiar_urls_antiguas(30),
        CronTrigger(hour=3, minute=0, timezone="America/Lima"),
        id="limpieza_diaria",
        replace_existing=True
    )
    keywords = get_keywords_permanentes()
    for kw in keywords:
        if not kw["activa"]:
            continue
        freq = kw.get("frecuencia_horas", 24)
        modo = kw.get("modo", "diario")
        if modo == "alerta":
            # freq_horas aquí representa minutos para modo alerta
            # (5, 10 o 15 minutos)
            freq_min = freq  # en modo alerta guardamos minutos directamente
            scheduler.add_job(
                job_alerta,
                IntervalTrigger(minutes=freq_min, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"], freq_min],
                id=f"kw_{kw['id']}",
                replace_existing=True
            )
        else:  # diario
            scheduler.add_job(
                job_diario,
                CronTrigger(hour=12, minute=0, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"]],
                id=f"kw_{kw['id']}",
                replace_existing=True
            )
    total = len(scheduler.get_jobs())
    print(f"[SCHEDULER] {total} jobs activos")


# ════════════════════════════════════════════════════
# RUTAS WEB
# ════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request":       request,
        "keywords":      get_keywords_permanentes(),
        "destinatarios": get_destinatarios(),
        "historial":     get_historial(limit=40),
    })


@app.post("/keywords/agregar")
async def agregar_keyword(
    keyword:          str = Form(...),
    modo:             str = Form("diario"),
    frecuencia_horas: int = Form(15),
):
    save_keyword_permanente(keyword, modo, frecuencia_horas)
    recargar_jobs()
    return RedirectResponse("/", status_code=303)


@app.post("/keywords/eliminar")
async def eliminar_keyword(keyword_id: int = Form(...)):
    delete_keyword_permanente(keyword_id)
    recargar_jobs()
    return RedirectResponse("/", status_code=303)


@app.post("/destinatarios/agregar")
async def agregar_destinatario(email: str = Form(...)):
    save_destinatario(email)
    return RedirectResponse("/", status_code=303)


@app.post("/destinatarios/eliminar")
async def eliminar_destinatario(dest_id: int = Form(...)):
    delete_destinatario(dest_id)
    return RedirectResponse("/", status_code=303)


@app.post("/buscar", response_class=HTMLResponse)
async def buscar_manual(
    request:        Request,
    keyword:        str = Form(...),
    fecha_inicio:   str = Form(""),
    fecha_fin:      str = Form(""),
    num_resultados: int = Form(20),
):
    fi = fecha_inicio or None
    ff = fecha_fin    or None
    resultados = buscar_keyword(keyword, fi, ff, num_resultados)
    if resultados:
        resultados = analizar_resultados(resultados, keyword)
    fecha      = datetime.now().strftime("%d/%m/%Y %H:%M")
    html_email = generar_email_html(keyword, resultados, fecha, modo="manual")
    return templates.TemplateResponse("resultado.html", {
        "request":    request,
        "keyword":    keyword,
        "resultados": resultados,
        "medios":     [r for r in resultados if not r.get("es_red")],
        "redes":      [r for r in resultados if r.get("es_red")],
        "fecha":      fecha,
        "html_email": html_email,
        "total":      len(resultados),
    })


@app.post("/buscar/enviar")
async def enviar_resultado_manual(
    keyword:    str = Form(...),
    html_email: str = Form(...),
):
    dests  = [d["email"] for d in get_destinatarios()]
    fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
    asunto = f"Métrica Monitor · {keyword} · {fecha}"
    ok     = enviar_mailgun(html_email, asunto, dests)
    return JSONResponse({"ok": ok, "destinatarios": dests})


@app.get("/historial/{historial_id}", response_class=HTMLResponse)
async def ver_historial(historial_id: int):
    item = get_historial_by_id(historial_id)
    if not item:
        return HTMLResponse("No encontrado", status_code=404)
    return HTMLResponse(item["html_content"])


@app.get("/health")
async def health():
    return {"status": "ok", "jobs": len(scheduler.get_jobs()),
            "time": datetime.now().isoformat()}
