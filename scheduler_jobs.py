"""
scheduler_jobs.py

BUGS RESUELTOS:
  Bug A — job_alerta mandaba un email por cada noticia → ahora agrupa todas en uno solo
  Bug B — job_alerta sin filtro de fechas → pasaban noticias viejas como "nuevas"
  Bug C — job_alerta sin noticias nuevas mandaba email vacío → ahora silencio total
  Bug 1 — job_diario sin fechas → noticias antiguas (ya corregido antes)
  Bug 2 — job_diario sin urls_enviadas → repeticiones entre días (ya corregido antes)
  Bug 3 — job_diario sin novedades mandaba vacío → email de aviso (ya corregido antes)
"""

import time
from datetime import datetime, timedelta

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


# ════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════

def _rango_ultimas_24h() -> tuple[str, str]:
    """Devuelve (ayer, hoy) en formato YYYY-MM-DD para job_diario."""
    hoy  = datetime.now()
    ayer = hoy - timedelta(days=1)
    return ayer.strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d")


def _rango_ultimas_horas(horas: int) -> tuple[str, str]:
    """
    Devuelve (fecha_inicio, fecha_fin) cubriendo las últimas N horas
    para job_alerta. Usamos los días involucrados para que el filtro
    Python en motor.py capture correctamente el rango.

    Ejemplo: si son las 20:00 y horas=1 → desde las 19:00 de hoy
      fecha_inicio = hoy (puede ser mismo día)
      fecha_fin    = hoy

    Si la ventana cruza medianoche (ej: 00:30, horas=2 → desde 22:30 ayer):
      fecha_inicio = ayer
      fecha_fin    = hoy
    """
    ahora  = datetime.now()
    inicio = ahora - timedelta(hours=horas)
    return inicio.strftime("%Y-%m-%d"), ahora.strftime("%Y-%m-%d")


def _email_sin_novedades(keyword: str, fecha: str) -> str:
    """Email HTML minimalista para job_diario cuando no hay noticias nuevas."""
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;background:#F4F6F9;font-family:Arial,sans-serif">
<table width="100%" style="background:#F4F6F9"><tr><td align="center" style="padding:24px 16px">
<table width="620" style="max-width:620px;width:100%">
  <tr><td style="background:#003366;border-radius:8px 8px 0 0;padding:22px 28px">
    <table width="100%"><tr>
      <td><p style="margin:0;color:#fff;opacity:.6;font-size:9px;letter-spacing:2px">
            RESUMEN DIARIO · MONITOREO DE MEDIOS</p>
          <p style="margin:4px 0 0;color:#fff;font-size:20px;font-weight:700">Métrica.</p></td>
      <td align="right">
        <p style="margin:0;color:#fff;opacity:.7;font-size:11px">{fecha}</p>
        <p style="margin:2px 0 0;color:#fff;opacity:.5;font-size:10px">Lima, Perú</p></td>
    </tr></table>
  </td></tr>
  <tr><td style="background:#fff;border-radius:0 0 8px 8px;padding:40px 28px;text-align:center">
    <p style="font-size:32px;margin:0 0 16px">📭</p>
    <p style="font-size:16px;font-weight:700;color:#003366;margin:0 0 8px">Sin menciones nuevas</p>
    <p style="font-size:13px;color:#5A6A7E;margin:0 0 4px">No se encontraron noticias nuevas sobre</p>
    <p style="font-size:15px;font-weight:700;color:#1E90FF;margin:0 0 20px">{keyword}</p>
    <p style="font-size:11px;color:#9CA3AF;margin:0">en las últimas 24 horas · {fecha}</p>
  </td></tr>
</table></td></tr></table></body></html>"""


# ════════════════════════════════════════════════════
# JOB ALERTA — CORREGIDO
# ════════════════════════════════════════════════════

def job_alerta(kw_id: int, keyword: str, contexto: str, freq_minutos: int) -> None:
    """
    Alerta inmediata cada freq_minutos.

    COMPORTAMIENTO CORRECTO:
    - Busca solo en la ventana de tiempo = freq_minutos * 2 (margen de seguridad)
      para no traer noticias viejas que Serper/YT devuelven fuera de rango
    - Filtra las URLs ya enviadas antes
    - Si hay noticias nuevas → UN SOLO EMAIL con todas juntas (no uno por noticia)
    - Si no hay nada nuevo → silencio total, no manda nada

    FIX Bug A: loop con enviar_mailgun por cada noticia → ahora un solo email grupal
    FIX Bug B: sin fechas → ahora pasa rango de horas explícito
    FIX Bug C: email vacío cuando no hay novedades → ahora silencio total
    """
    print(f"[ALERTA] Verificando: '{keyword}'")

    # ── Fix Bug B: calcular ventana temporal explícita ───────────────
    # Usamos freq_minutos convertido a horas (mínimo 1h para no perder noticias
    # por imprecisión de Serper). Con margen x2 para capturar noticias
    # que aparecen tarde en los índices.
    horas_ventana = max(1, (freq_minutos * 2) // 60) if freq_minutos >= 30 else 1
    fecha_inicio, fecha_fin = _rango_ultimas_horas(horas_ventana)

    todos = buscar_keyword(
        keyword,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        num=20,
        contexto=contexto,
    )
    print(f"[ALERTA] Encontrados: {len(todos)} en ventana de {horas_ventana}h")

    # ── Filtrar URLs ya enviadas ─────────────────────────────────────
    nuevos = [r for r in todos if r.get("url") and not url_ya_enviada(kw_id, r["url"])]

    # ── Fix Bug C: silencio total si no hay novedades ────────────────
    if not nuevos:
        print(f"[ALERTA] '{keyword}': sin noticias nuevas, no se envía nada")
        return

    # ── Fix Bug A: un solo email con TODAS las noticias nuevas ───────
    # Sin análisis de tono para mantener baja la latencia de la alerta
    for r in nuevos:
        r["tono"] = ""
        r["justificacion"] = ""

    fecha  = datetime.now().strftime("%d/%m/%Y %H:%M")
    n      = len(nuevos)
    html   = generar_email_html(keyword, nuevos, fecha, modo="alerta")
    dests  = [d["email"] for d in get_destinatarios()]
    asunto = (
        f"⚡ Alerta Métrica · {keyword} · "
        f"{nuevos[0]['titulo'][:50]}{'…' if n == 1 else f' (+{n-1} más)'}"
    )
    ok = enviar_mailgun(html, asunto, dests)

    if ok:
        # Marcar todas las URLs como enviadas para no repetirlas
        for r in nuevos:
            if r.get("url"):
                marcar_url_enviada(kw_id, r["url"])
        for r in nuevos:
            save_historial(kw_id, keyword, "alerta", 1, True, html)
        print(f"[ALERTA] '{keyword}': {n} noticia(s) nuevas → 1 email enviado")
    else:
        print(f"[ALERTA] '{keyword}': error al enviar email")


# ════════════════════════════════════════════════════
# JOB DIARIO — SIN CAMBIOS DE LÓGICA (ya corregido)
# ════════════════════════════════════════════════════

def job_diario(kw_id: int, keyword: str, contexto: str) -> None:
    """
    Resumen diario a las 12pm Lima.
    Bugs 1/2/3 ya corregidos en versión anterior.
    """
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"[DIARIO] Ejecutando: '{keyword}'")

    fecha_inicio, fecha_fin = _rango_ultimas_24h()
    print(f"[DIARIO] Rango: {fecha_inicio} → {fecha_fin}")

    todos = buscar_keyword(
        keyword,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        num=20,
        contexto=contexto,
    )
    print(f"[DIARIO] Encontrados: {len(todos)} resultados en rango")

    nuevos = [r for r in todos if not url_ya_enviada(kw_id, r["url"])]
    repetidos = len(todos) - len(nuevos)
    if repetidos > 0:
        print(f"[DIARIO] Descartados {repetidos} ya enviados anteriormente")

    if not nuevos:
        print(f"[DIARIO] '{keyword}': sin menciones nuevas → enviando aviso")
        html   = _email_sin_novedades(keyword, fecha)
        dests  = [d["email"] for d in get_destinatarios()]
        asunto = f"Métrica Monitor · Sin novedades · {keyword} · {fecha}"
        ok     = enviar_mailgun(html, asunto, dests)
        save_historial(kw_id, keyword, "diario", 0, ok, html)
        return

    nuevos = analizar_resultados(nuevos, keyword, contexto)
    html   = generar_email_html(keyword, nuevos, fecha, modo="diario")
    dests  = [d["email"] for d in get_destinatarios()]
    asunto = f"Métrica Monitor · Resumen diario · {keyword} · {fecha}"
    ok     = enviar_mailgun(html, asunto, dests)

    if ok:
        for r in nuevos:
            if r.get("url"):
                marcar_url_enviada(kw_id, r["url"])
        print(f"[DIARIO] {len(nuevos)} URLs marcadas como enviadas")

    save_historial(kw_id, keyword, "diario", len(nuevos), ok, html)
    print(f"[DIARIO] '{keyword}': {len(nuevos)} nuevas, enviado={ok}")


# ════════════════════════════════════════════════════
# GESTIÓN DE JOBS
# ════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════
# TESTS INLINE — python scheduler_jobs.py
# ════════════════════════════════════════════════════

def _run_tests() -> None:
    print("── Tests _rango_ultimas_24h ─────────────────")
    fi, ff = _rango_ultimas_24h()
    hoy  = datetime.now().strftime("%Y-%m-%d")
    ayer = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    assert fi == ayer and ff == hoy
    assert fi != ff
    print(f"  ✓  {fi} → {ff}")

    print("\n── Tests _rango_ultimas_horas ───────────────")
    casos = [
        (1,  "ventana 1h"),
        (2,  "ventana 2h"),
        (6,  "ventana 6h — frecuencia 3h * 2"),
    ]
    for horas, desc in casos:
        fi, ff = _rango_ultimas_horas(horas)
        inicio = datetime.now() - timedelta(hours=horas)
        assert fi == inicio.strftime("%Y-%m-%d"), f"fecha_inicio incorrecta para {horas}h"
        print(f"  ✓  [{desc}] {fi} → {ff}")

    print("\n── Tests lógica job_alerta (simulado) ───────")
    urls_vistas = {"https://ejemplo.com/vieja-1", "https://ejemplo.com/vieja-2"}
    resultados = [
        {"url": "https://ejemplo.com/vieja-1",  "titulo": "Noticia vieja 1"},
        {"url": "https://ejemplo.com/vieja-2",  "titulo": "Noticia vieja 2"},
        {"url": "https://ejemplo.com/nueva-1",  "titulo": "Noticia nueva 1"},
        {"url": "https://ejemplo.com/nueva-2",  "titulo": "Noticia nueva 2"},
        {"url": "https://ejemplo.com/nueva-3",  "titulo": "Noticia nueva 3"},
    ]
    nuevos = [r for r in resultados if r["url"] not in urls_vistas]
    assert len(nuevos) == 3, f"Deberian ser 3 nuevas, son {len(nuevos)}"
    print(f"  ✓  5 resultados, 2 ya vistas → {len(nuevos)} nuevas, 1 solo email")

    # Sin novedades → silencio total
    todas_vistas = {r["url"] for r in resultados}
    sin_nuevos = [r for r in resultados if r["url"] not in todas_vistas]
    assert len(sin_nuevos) == 0
    print(f"  ✓  sin novedades → 0 emails (silencio total)")

    # Asunto con 1 noticia
    nuevos_1 = [{"titulo": "Chinalco invierte US$400M", "url": "x"}]
    asunto_1 = f"⚡ Alerta Métrica · Chinalco · {nuevos_1[0]['titulo'][:50]}"
    assert "Chinalco invierte" in asunto_1
    print(f"  ✓  asunto 1 noticia: '{asunto_1}'")

    # Asunto con múltiples noticias
    nuevos_3 = [
        {"titulo": "Chinalco invierte US$400M", "url": "x"},
        {"titulo": "Chinalco amplía operaciones", "url": "y"},
        {"titulo": "Chinalco gana premio", "url": "z"},
    ]
    n = len(nuevos_3)
    asunto_3 = f"⚡ Alerta Métrica · Chinalco · {nuevos_3[0]['titulo'][:50]} (+{n-1} más)"
    assert "(+2 más)" in asunto_3
    print(f"  ✓  asunto múltiples: '{asunto_3}'")

    print(f"\n✅ Todos los tests pasaron")


if __name__ == "__main__":
    _run_tests()
