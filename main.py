"""
main.py — FastAPI application
Responsabilidades: rutas web, lifespan, nada más.

Toda la lógica de negocio está en:
  motor.py          — búsqueda Serper + YouTube
  tono.py           — análisis Claude
  email_sender.py   — generación HTML + Mailgun
  scheduler_jobs.py — jobs APScheduler
  database.py       — SQLite
  tiers.py          — clasificación de medios
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import (
    delete_destinatario, delete_keyword_permanente,
    get_destinatarios, get_historial, get_historial_by_id,
    get_keywords_permanentes, init_db,
    save_destinatario, save_historial, save_keyword_permanente,
)
from email_sender import enviar_mailgun, generar_email_html
from motor import buscar_keyword
from scheduler_jobs import recargar_jobs, scheduler
from tono import analizar_resultados


# ════════════════════════════════════════════════════
# LIFESPAN
# ════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.start()
    recargar_jobs()
    yield
    scheduler.shutdown()


# ════════════════════════════════════════════════════
# APP
# ════════════════════════════════════════════════════

app = FastAPI(lifespan=lifespan)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


# ════════════════════════════════════════════════════
# RUTAS
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
    yt = "✅" if os.getenv("YOUTUBE_API_KEY") else "⚠️ sin configurar"
    return {
        "status": "ok",
        "jobs":   len(scheduler.get_jobs()),
        "youtube": yt,
        "time":   datetime.now().isoformat(),
    }
