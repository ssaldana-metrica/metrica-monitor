"""
scheduler_jobs.py — Jobs del scheduler de monitoreo automático
Responsabilidades: job_alerta, job_diario, recargar_jobs, instancia del scheduler
"""

import os
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

# Instancia única del scheduler, importada por main.py
scheduler = BackgroundScheduler(timezone="America/Lima")


# ════════════════════════════════════════════════════
# JOBS
# ════════════════════════════════════════════════════

def job_alerta(kw_id: int, keyword: str, freq_minutos: int) -> None:
    """
    Job de alerta inmediata.
    Se ejecuta cada freq_minutos. Envía un email por cada mención nueva
    (URL no vista antes). No aplica análisis de tono para ser más rápido.
    """
    print(f"[ALERTA] Verificando: '{keyword}'")
    resultados = buscar_keyword(keyword, num=15)
    dests = [d["email"] for d in get_destinatarios()]
    nuevos = 0

    for r in resultados:
        if not r["url"] or url_ya_enviada(kw_id, r["url"]):
            continue

        # Alerta: sin tono para no añadir latencia
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


def job_diario(kw_id: int, keyword: str) -> None:
    """
    Job de resumen diario.
    Se ejecuta a las 12:00 Lima. Incluye análisis de tono con Claude.
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


# ════════════════════════════════════════════════════
# GESTIÓN DE JOBS
# ════════════════════════════════════════════════════

def recargar_jobs() -> None:
    """
    Lee las keywords activas de la BD y recarga todos los jobs del scheduler.
    Se llama al iniciar la app y cada vez que se agrega/elimina una keyword.
    """
    scheduler.remove_all_jobs()

    # Job de limpieza diaria de URLs antiguas (3am Lima)
    scheduler.add_job(
        lambda: limpiar_urls_antiguas(30),
        CronTrigger(hour=3, minute=0, timezone="America/Lima"),
        id="limpieza_diaria",
        replace_existing=True,
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
                id=f"kw_{kw['id']}",
                replace_existing=True,
            )
        else:
            scheduler.add_job(
                job_diario,
                CronTrigger(hour=12, minute=0, timezone="America/Lima"),
                args=[kw["id"], kw["keyword"]],
                id=f"kw_{kw['id']}",
                replace_existing=True,
            )

    total = len(scheduler.get_jobs())
    print(f"[SCHEDULER] {total} jobs activos")
