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
YOUTUBE_API_KEY   = os.getenv("YOUTUBE_API_KEY", "")
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
import os as _os
if _os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ════════════════════════════════════════════════════
# MOTOR DE BÚSQUEDA
# ════════════════════════════════════════════════════

def _fecha_para_serper(fecha_iso):
    """Convierte '2025-04-01' (HTML date) a '4/1/2025' (Serper tbs)."""
    try:
        from datetime import datetime as _dt
        d = _dt.strptime(fecha_iso, "%Y-%m-%d")
        return f"{d.month}/{d.day}/{d.year}"
    except Exception:
        return fecha_iso


def buscar_web(keyword, fecha_inicio=None, fecha_fin=None, num=20):
    payload = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    if fecha_inicio and fecha_fin:
        fi = _fecha_para_serper(fecha_inicio)
        ff = _fecha_para_serper(fecha_fin)
        payload["tbs"] = f"cdr:1,cd_min:{fi},cd_max:{ff}"
    else:
        from datetime import datetime, timedelta
        ayer = (datetime.now() - timedelta(days=1)).strftime("%-m/%-d/%Y")
        hoy  = datetime.now().strftime("%-m/%-d/%Y")
        payload["tbs"] = f"cdr:1,cd_min:{ayer},cd_max:{hoy}"
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/search",
                          json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception:
        return []


def buscar_news(keyword, num=20):
    payload = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/news",
                          json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("news", [])
    except Exception:
        return []


def buscar_youtube(keyword, fecha_inicio=None, fecha_fin=None, num=5):
    """Busca videos en YouTube via Data API v3. Gratis: 100 búsquedas/día."""
    if not YOUTUBE_API_KEY:
        return []
    from datetime import datetime, timedelta, timezone
    params = {
        "part":              "snippet",
        "q":                 keyword,
        "type":              "video",
        "order":             "date",
        "regionCode":        "PE",
        "relevanceLanguage": "es",
        "maxResults":        min(num, 10),
        "key":               YOUTUBE_API_KEY,
    }
    if fecha_inicio and fecha_fin:
        params["publishedAfter"]  = fecha_inicio + "T00:00:00Z"
        params["publishedBefore"] = fecha_fin    + "T23:59:59Z"
    else:
        ayer = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        params["publishedAfter"] = ayer
    try:
        r = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params=params, timeout=15
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        resultados = []
        for item in items:
            s   = item.get("snippet", {})
            vid = item.get("id", {}).get("videoId", "")
            if not vid:
                continue
            url = f"https://www.youtube.com/watch?v={vid}"
            resultados.append({
                "tipo":    "youtube",
                "titulo":  s.get("title",       "Sin título").strip(),
                "snippet": s.get("description", "").strip()[:300],
                "fuente":  s.get("channelTitle", "YouTube"),
                "fecha":   s.get("publishedAt", "")[:10],
                "url":     url,
                "tier":    get_tier(url),
                "es_red":  True,
            })
        return resultados
    except Exception as e:
        print(f"[YouTube] Error: {e}")
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


def generar_variaciones(keyword):
    """Genera variaciones de búsqueda para máxima cobertura."""
    base = keyword.strip()
    variaciones = [base]
    if '"' not in base:
        variaciones.append(f'"{base}"')
    if "peru" not in base.lower() and "perú" not in base.lower():
        variaciones.append(f"{base} Peru")
    return variaciones[:3]


def buscar_keyword(keyword, fecha_inicio=None, fecha_fin=None, num=20):
    """
    Motor completo: Serper (web + news) + YouTube.
    Deduplica por URL, respeta límite num, ordena medios por tier primero.
    """
    todos = {}
    variaciones = generar_variaciones(keyword)
    num_por_variacion = max(5, num // len(variaciones))

    # Serper: web + news
    for kw in variaciones:
        for item in buscar_web(kw, fecha_inicio, fecha_fin, num_por_variacion):
            r = parsear(item, "web")
            if r["url"]:
                uid = hashlib.md5(r["url"].encode()).hexdigest()
                if uid not in todos:
                    todos[uid] = r
        for item in buscar_news(kw, num_por_variacion):
            r = parsear(item, "news")
            if r["url"]:
                uid = hashlib.md5(r["url"].encode()).hexdigest()
                if uid not in todos:
                    todos[uid] = r
        time.sleep(0.2)

    # YouTube: busca solo con keyword principal para no gastar cuota
    num_yt = min(5, max(3, num // 6))
    for r in buscar_youtube(keyword, fecha_inicio, fecha_fin, num_yt):
        if r["url"]:
            uid = hashlib.md5(r["url"].encode()).hexdigest()
            if uid not in todos:
                todos[uid] = r

    resultados = list(todos.values())
    resultados.sort(key=lambda x: (int(x["es_red"]), orden_tier(x["tier"])))
    return resultados[:num]


# ════════════════════════════════════════════════════
# ANÁLISIS DE TONO
# ════════════════════════════════════════════════════

PROMPT_TONO = """Eres un analista senior de relaciones públicas y monitoreo de medios en Perú.
Tu trabajo es determinar si una noticia es POSITIVA, NEGATIVA o NEUTRA para una marca o persona específica (el "cliente").

REGLA FUNDAMENTAL: Analiza el ENCUADRAMIENTO completo, no palabras sueltas.

CONTEXTO DE RIVALES Y COMPETENCIA:
- Si algo negativo le pasa al RIVAL del cliente → es POSITIVO para el cliente
- Si algo positivo le pasa al RIVAL del cliente → puede ser NEGATIVO o NEUTRO para el cliente
- Ejemplos deportivos: expulsión del rival = ventaja para el cliente / derrota del rival = victoria del cliente
- Ejemplos empresariales: crisis en competidor = oportunidad para el cliente / multa al rival = neutro o positivo

REGLA DE DUDA: Si no puedes determinar con certeza cómo afecta al cliente, marca NEUTRO.
Nunca atribuyas negativo por una palabra aislada sin entender a quién pertenece el hecho.

PASOS OBLIGATORIOS:
1. ¿Quién es el protagonista del hecho? ¿Es el cliente o su rival?
2. ¿El hecho narrado beneficia o perjudica al cliente directamente?
3. Si el protagonista es el rival: ¿lo que le pasa al rival beneficia o perjudica al cliente?

CRITERIOS FINALES:
- POSITIVO: el hecho favorece imagen, reputación, resultados o intereses del cliente
- NEGATIVO: el hecho daña directamente la imagen, reputación o intereses del cliente
- NEUTRO: mención informativa, actor secundario, o contexto ambiguo sin certeza

Responde SOLO con JSON válido, sin texto extra, sin markdown:
{"tono":"positivo"|"negativo"|"neutro","justificacion":"Una oración explicando quién es el protagonista y cómo afecta al cliente.","relevancia":"alta"|"media"|"baja"}"""


def analizar_tono(titulo, snippet, keyword):
    if not ANTHROPIC_API_KEY:
        return {"tono": "neutro", "justificacion": "Sin API Claude configurada.", "relevancia": "media"}
    for intento in range(3):
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=PROMPT_TONO,
                messages=[{"role": "user", "content":
                    f"Keyword (cliente): {keyword}\nTítulo: {titulo}\nSnippet: {snippet[:200]}"}]
            )
            text = msg.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            result = json.loads(text)
            # Validar que tiene los campos esperados
            if "tono" not in result or result["tono"] not in ("positivo","negativo","neutro"):
                raise ValueError("JSON inválido")
            return result
        except Exception as e:
            if intento < 2:
                time.sleep(0.5 * (intento + 1))
                continue
            print(f"[Claude] Error después de 3 intentos: {e}")
            return {"tono": "neutro", "justificacion": "Sin análisis disponible.", "relevancia": "media"}


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
    if r["tipo"] == "youtube":
        tipo_lbl, tipo_bg, tipo_clr = "YouTube", "#FEE2E2", "#991B1B"
    elif r["tipo"] == "news":
        tipo_lbl, tipo_bg, tipo_clr = "Google News", "#FEF3C7", "#92400E"
    else:
        tipo_lbl, tipo_bg, tipo_clr = "Google Web", "#EFF6FF", "#1E40AF"
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
            {'▶' if r['tipo']=='youtube' else '🌐'} {r['fuente']} &nbsp;·&nbsp;
            <a href="{r['url']}" style="color:#0066CC">{'Ver video ↗' if r['tipo']=='youtube' else 'Ver nota ↗'}</a></p>
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
    cuerpo += seccion_email("📱 Redes sociales y YouTube", "#6D28D9", redes, es_alerta)

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
    print(f"[ALERTA] Verificando: '{keyword}'")
    resultados = buscar_keyword(keyword, num=15)
    dests = [d["email"] for d in get_destinatarios()]
    nuevos = 0

    for r in resultados:
        if not r["url"] or url_ya_enviada(kw_id, r["url"]):
            continue
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
        print(f"[ALERTA] '{keyword}': sin noticias nuevas")
    else:
        print(f"[ALERTA] '{keyword}': {nuevos} alerta(s) enviada(s)")


def job_diario(kw_id, keyword):
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
    scheduler.remove_all_jobs()
    scheduler.add_job(
        lambda: limpiar_urls_antiguas(30),
        CronTrigger(hour=3, minute=0, timezone="America/Lima"),
        id="limpieza_diaria", replace_existing=True
    )
    keywords = get_keywords_permanentes()
    for kw in keywords:
        if not kw["activa"]:
            continue
        freq = kw.get("frecuencia_horas", 24)
        modo = kw.get("modo", "diario")
        if modo == "alerta":
            scheduler.add_job(
                job_alerta,
                IntervalTrigger(minutes=freq, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"], freq],
                id=f"kw_{kw['id']}", replace_existing=True
            )
        else:
            scheduler.add_job(
                job_diario,
                CronTrigger(hour=12, minute=0, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"]],
                id=f"kw_{kw['id']}", replace_existing=True
            )
    print(f"[SCHEDULER] {len(scheduler.get_jobs())} jobs activos")


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
    total:      int = Form(0),
):
    dests  = [d["email"] for d in get_destinatarios()]
    fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
    asunto = f"Métrica Monitor · {keyword} · {fecha}"
    ok     = enviar_mailgun(html_email, asunto, dests)
    save_historial(None, keyword, "manual", total, ok, html_email)
    return JSONResponse({"ok": ok, "destinatarios": dests})


@app.get("/historial/{historial_id}", response_class=HTMLResponse)
async def ver_historial(historial_id: int):
    item = get_historial_by_id(historial_id)
    if not item:
        return HTMLResponse("No encontrado", status_code=404)
    return HTMLResponse(item["html_content"])


@app.get("/health")
async def health():
    yt = "✅" if YOUTUBE_API_KEY else "⚠️ sin configurar"
    return {
        "status": "ok",
        "jobs":   len(scheduler.get_jobs()),
        "youtube": yt,
        "time":   datetime.now().isoformat()
    }
