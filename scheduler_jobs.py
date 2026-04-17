"""
scheduler_jobs.py — Jobs del scheduler de monitoreo automatico
"""

import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from database import (
    get_destinatarios, get_keywords_permanentes,
    limpiar_urls_antiguas, marcar_url_enviada,
    save_historial, url_ya_enviada,
)
from email_sender import enviar_mailgun, generar_email_html
from motor import buscar_keyword
from tono import analizar_resultados

scheduler = BackgroundScheduler(timezone="America/Lima")


def job_alerta(kw_id: int, keyword: str, contexto: str, freq_minutos: int) -> None:
    print(f"[ALERTA] Verificando: '{keyword}'")
    resultados = buscar_keyword(keyword, num=15, contexto=contexto)
    dests  = [d["email"] for d in get_destinatarios()]
    nuevos = 0

    for r in resultados:
        if not r["url"] or url_ya_enviada(kw_id, r["url"]):
            continue
        r["tono"] = ""
        r["justificacion"] = ""
        fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
        html   = generar_email_html(keyword, [r], fecha, modo="alerta")
        asunto = f"Alerta Metrica · {keyword} · {r['titulo'][:55]}"
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


def job_diario(kw_id: int, keyword: str, contexto: str) -> None:
    print(f"[DIARIO] Ejecutando: '{keyword}'")
    resultados = buscar_keyword(keyword, num=20, contexto=contexto)
    if resultados:
        resultados = analizar_resultados(resultados, keyword, contexto)
    fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
    html   = generar_email_html(keyword, resultados, fecha, modo="diario")
    dests  = [d["email"] for d in get_destinatarios()]
    asunto = f"Metrica Monitor · Resumen diario · {keyword} · {fecha}"
    ok     = enviar_mailgun(html, asunto, dests)
    save_historial(kw_id, keyword, "diario", len(resultados), ok, html)
    print(f"[DIARIO] '{keyword}': {len(resultados)} resultados, enviado={ok}")


def recargar_jobs() -> None:
    scheduler.remove_all_jobs()
    scheduler.add_job(
        lambda: limpiar_urls_antiguas(30),
        CronTrigger(hour=3, minute=0, timezone="America/Lima"),
        id="limpieza_diaria", replace_existing=True,
    )
    for kw in get_keywords_permanentes():
        if not kw["activa"]:
            continue
        freq     = kw.get("frecuencia_horas", 24)
        modo     = kw.get("modo", "diario")
        contexto = kw.get("contexto", "")
        if modo == "alerta":
            scheduler.add_job(
                job_alerta,
                IntervalTrigger(minutes=freq, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"], contexto, freq],
                id=f"kw_{kw['id']}", replace_existing=True,
            )
        else:
            scheduler.add_job(
                job_diario,
                CronTrigger(hour=12, minute=0, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"], contexto],
                id=f"kw_{kw['id']}", replace_existing=True,
            )
    print(f"[SCHEDULER] {len(scheduler.get_jobs())} jobs activos")
