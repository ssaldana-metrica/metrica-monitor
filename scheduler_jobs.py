"""
scheduler_jobs.py — Jobs del scheduler de monitoreo automatico

BUGS RESUELTOS EN ESTA VERSION:
  Bug 1 — job_diario pasaba fechas=None → filtro Python desactivado → noticias antiguas
  Bug 2 — job_diario no usaba urls_enviadas → noticias repetidas entre dias consecutivos
  Bug 3 — sin noticias nuevas se enviaba email vacio o nada → ahora envia aviso claro
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
    """
    Devuelve (fecha_inicio, fecha_fin) cubriendo las últimas 24 horas
    en formato YYYY-MM-DD, que es lo que espera buscar_keyword().

    Ejemplo a las 12:00 del 20/04:
      fecha_inicio = "2026-04-19"
      fecha_fin    = "2026-04-20"

    Usamos ayer y hoy para que el filtro Python capture tanto
    noticias de ayer tarde como de esta mañana.
    """
    hoy  = datetime.now()
    ayer = hoy - timedelta(days=1)
    return ayer.strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d")


def _email_sin_novedades(keyword: str, fecha: str) -> str:
    """
    Genera un email HTML minimalista para cuando no hay noticias nuevas.
    Es importante enviarlo igual para que el destinatario sepa que
    el sistema funcionó y simplemente no hubo menciones nuevas.
    """
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
    <p style="font-size:16px;font-weight:700;color:#003366;margin:0 0 8px">
      Sin menciones nuevas</p>
    <p style="font-size:13px;color:#5A6A7E;margin:0 0 4px">
      No se encontraron noticias nuevas sobre</p>
    <p style="font-size:15px;font-weight:700;color:#1E90FF;margin:0 0 20px">
      {keyword}</p>
    <p style="font-size:11px;color:#9CA3AF;margin:0">
      en las últimas 24 horas · {fecha}</p>
  </td></tr>
</table></td></tr></table></body></html>"""


# ════════════════════════════════════════════════════
# JOB DIARIO
# ════════════════════════════════════════════════════

def job_diario(kw_id: int, keyword: str, contexto: str) -> None:
    """
    Resumen diario a las 12pm Lima.

    Flujo corregido:
      1. Calcula rango últimas 24h → pasa fechas explícitas a buscar_keyword
         (FIX Bug 1: sin fechas el filtro Python no actúa y pasan noticias viejas)
      2. Filtra resultados ya enviados en días anteriores via urls_enviadas
         (FIX Bug 2: evita repetir noticias de días anteriores)
      3. Si no hay resultados nuevos → envía email de "sin novedades"
         (FIX Bug 3: el destinatario siempre sabe que el sistema corrió)
      4. Analiza tono con Claude solo sobre resultados nuevos (ahorra tokens)
      5. Marca URLs como enviadas para que mañana no se repitan
    """
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"[DIARIO] Ejecutando: '{keyword}'")

    # ── Fix Bug 1: siempre pasar rango de fechas explícito ──────────
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

    # ── Fix Bug 2: filtrar URLs ya enviadas en días anteriores ───────
    nuevos = [r for r in todos if not url_ya_enviada(kw_id, r["url"])]
    repetidos = len(todos) - len(nuevos)
    if repetidos > 0:
        print(f"[DIARIO] Descartados {repetidos} ya enviados en días anteriores")

    # ── Fix Bug 3: email de sin novedades si no hay nada nuevo ───────
    if not nuevos:
        print(f"[DIARIO] '{keyword}': sin menciones nuevas → enviando aviso")
        html  = _email_sin_novedades(keyword, fecha)
        dests = [d["email"] for d in get_destinatarios()]
        asunto = f"Métrica Monitor · Sin novedades · {keyword} · {fecha}"
        ok = enviar_mailgun(html, asunto, dests)
        save_historial(kw_id, keyword, "diario", 0, ok, html)
        return

    # ── Análisis de tono solo sobre resultados nuevos ────────────────
    nuevos = analizar_resultados(nuevos, keyword, contexto)

    # ── Generar y enviar email ────────────────────────────────────────
    html   = generar_email_html(keyword, nuevos, fecha, modo="diario")
    dests  = [d["email"] for d in get_destinatarios()]
    asunto = f"Métrica Monitor · Resumen diario · {keyword} · {fecha}"
    ok     = enviar_mailgun(html, asunto, dests)

    # ── Marcar todas las URLs como enviadas (para no repetir mañana) ─
    if ok:
        for r in nuevos:
            if r.get("url"):
                marcar_url_enviada(kw_id, r["url"])
        print(f"[DIARIO] {len(nuevos)} URLs marcadas como enviadas")

    save_historial(kw_id, keyword, "diario", len(nuevos), ok, html)
    print(f"[DIARIO] '{keyword}': {len(nuevos)} nuevas, enviado={ok}")


# ════════════════════════════════════════════════════
# JOB ALERTA
# ════════════════════════════════════════════════════

def job_alerta(kw_id: int, keyword: str, contexto: str, freq_minutos: int) -> None:
    """
    Alerta inmediata cada freq_minutos.
    Sin análisis de tono para minimizar latencia.
    Ya usaba urls_enviadas correctamente — sin cambios de lógica.
    """
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
    from datetime import datetime, timedelta

    print("── Tests _rango_ultimas_24h ─────────────────")
    fi, ff = _rango_ultimas_24h()
    hoy  = datetime.now().strftime("%Y-%m-%d")
    ayer = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    assert fi == ayer, f"fecha_inicio deberia ser ayer: {fi}"
    assert ff == hoy,  f"fecha_fin deberia ser hoy: {ff}"
    print(f"  ✓  rango correcto: {fi} → {ff}")

    # Verificar que siempre son fechas distintas (no mismo día)
    assert fi != ff, "inicio y fin no pueden ser el mismo dia"
    print(f"  ✓  inicio y fin son dias distintos")

    print("\n── Tests _email_sin_novedades ───────────────")
    html = _email_sin_novedades("La Positiva seguros", "19/04/2026 12:00")
    assert "Sin menciones nuevas" in html, "Falta titulo"
    assert "La Positiva seguros" in html, "Falta keyword"
    assert "19/04/2026 12:00" in html, "Falta fecha"
    assert "24 horas" in html, "Falta referencia a 24 horas"
    print("  ✓  email sin novedades contiene todos los campos")

    # Verificar que es HTML válido (tiene apertura y cierre)
    assert html.startswith("<!DOCTYPE html>"), "Debe ser HTML valido"
    assert "</html>" in html, "Debe cerrar tag html"
    print("  ✓  HTML bien formado")

    print("\n── Tests logica job_diario (simulado) ───────")
    # Simular el flujo con datos mock para verificar la lógica
    urls_vistas = {"https://ejemplo.com/noticia-vieja"}

    resultados_mock = [
        {"url": "https://ejemplo.com/noticia-vieja",  "titulo": "Noticia vieja",  "fecha": "2026-04-18"},
        {"url": "https://ejemplo.com/noticia-nueva-1","titulo": "Noticia nueva 1","fecha": "2026-04-19"},
        {"url": "https://ejemplo.com/noticia-nueva-2","titulo": "Noticia nueva 2","fecha": "2026-04-19"},
    ]

    # Simular filtro de urls_ya_enviadas
    nuevos = [r for r in resultados_mock if r["url"] not in urls_vistas]
    assert len(nuevos) == 2, f"Deberian quedar 2 nuevas, quedaron {len(nuevos)}"
    print(f"  ✓  filtro urls_enviadas: 3 resultados → 2 nuevos (1 ya visto descartado)")

    # Simular caso sin novedades
    todas_vistas = {r["url"] for r in resultados_mock}
    sin_nuevos = [r for r in resultados_mock if r["url"] not in todas_vistas]
    assert len(sin_nuevos) == 0, "No deberia haber nuevos si todo ya fue visto"
    print(f"  ✓  caso sin novedades: 0 nuevos → se enviaria email de aviso")

    print(f"\n✅ Todos los tests pasaron")


if __name__ == "__main__":
    _run_tests()
