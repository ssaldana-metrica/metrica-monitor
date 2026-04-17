"""
motor.py — Motor de búsqueda de medios
Responsabilidades: Serper (web + news) + YouTube Data API + deduplicación + ordenamiento
"""

import hashlib
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from tiers import get_tier, es_red_social, orden_tier

SERPER_API_KEY  = os.getenv("SERPER_API_KEY", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")


# ════════════════════════════════════════════════════
# FIX BUG 1 — CONVERSIÓN DE FECHAS
# ════════════════════════════════════════════════════
#
# Problema original:
#   strftime("%-m/%-d/%Y") usa el modificador "-" de glibc para suprimir
#   el cero inicial (ej: "4/5/2025" en vez de "04/05/2025").
#   En ciertos entornos (Alpine Linux, algunos builds de Render) ese
#   modificador no existe y lanza ValueError, que se traga el except
#   genérico de buscar_web → payload queda sin "tbs" → Serper devuelve
#   resultados sin filtro de fecha → aparecen noticias de semanas atrás.
#
# Fix:
#   Construir el string manualmente con int(), que es portable al 100%.
#   Serper espera M/D/YYYY sin ceros (e.g. "4/5/2025"), lo que logramos
#   con int() sobre los componentes de la fecha.
#
# Test rápido incluido al final del archivo.

def _fecha_serper(fecha_iso: str) -> str:
    """
    Convierte 'YYYY-MM-DD' (HTML date input) → 'M/D/YYYY' (Serper tbs).

    Usa int() para eliminar ceros iniciales de forma portable.
    No usa %-m ni %-d porque fallan en Alpine/musl libc.

    >>> _fecha_serper("2025-04-05")
    '4/5/2025'
    >>> _fecha_serper("2025-12-31")
    '12/31/2025'
    >>> _fecha_serper("2025-01-01")
    '1/1/2025'
    """
    try:
        d = datetime.strptime(fecha_iso, "%Y-%m-%d")
        # int() elimina el cero inicial de forma portable en cualquier OS
        return f"{int(d.month)}/{int(d.day)}/{d.year}"
    except (ValueError, TypeError) as exc:
        # Si el string está malformado, lo registramos y devolvemos "" para
        # que el caller sepa que no debe usar tbs (mejor sin filtro que crash)
        print(f"[motor] _fecha_serper: fecha inválida '{fecha_iso}' → {exc}")
        return ""


def _tbs_rango(fecha_inicio: str | None, fecha_fin: str | None) -> str | None:
    """
    Construye el parámetro tbs para Serper.
    Devuelve None si alguna fecha es inválida para evitar filtros silenciosos.
    """
    if fecha_inicio and fecha_fin:
        fi = _fecha_serper(fecha_inicio)
        ff = _fecha_serper(fecha_fin)
        if not fi or not ff:
            # Una de las fechas estaba malformada → no aplicar filtro y avisar
            print(f"[motor] tbs ignorado por fecha inválida: inicio={fecha_inicio} fin={fecha_fin}")
            return None
        return f"cdr:1,cd_min:{fi},cd_max:{ff}"
    # Sin fechas explícitas → ventana ayer/hoy
    ayer = (datetime.now() - timedelta(days=1))
    hoy  = datetime.now()
    return (
        f"cdr:1,"
        f"cd_min:{int(ayer.month)}/{int(ayer.day)}/{ayer.year},"
        f"cd_max:{int(hoy.month)}/{int(hoy.day)}/{hoy.year}"
    )


# ════════════════════════════════════════════════════
# SERPER — WEB Y NEWS
# ════════════════════════════════════════════════════

def buscar_web(keyword: str,
               fecha_inicio: str | None = None,
               fecha_fin: str | None = None,
               num: int = 20) -> list[dict]:
    """Búsqueda Google Web via Serper. Devuelve lista de resultados orgánicos."""
    tbs = _tbs_rango(fecha_inicio, fecha_fin)
    payload: dict = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    if tbs:
        payload["tbs"] = tbs
    else:
        # tbs inválido: buscamos sin filtro de fecha antes que retornar vacío
        print(f"[motor] buscar_web SIN filtro de fecha para '{keyword}'")

    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            json=payload, headers=headers, timeout=20
        )
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception as exc:
        print(f"[motor] buscar_web error: {exc}")
        return []


def buscar_news(keyword: str, num: int = 20) -> list[dict]:
    """Búsqueda Google News via Serper. News API no acepta tbs de fechas."""
    payload = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post(
            "https://google.serper.dev/news",
            json=payload, headers=headers, timeout=20
        )
        r.raise_for_status()
        return r.json().get("news", [])
    except Exception as exc:
        print(f"[motor] buscar_news error: {exc}")
        return []


# ════════════════════════════════════════════════════
# YOUTUBE DATA API v3
# ════════════════════════════════════════════════════

def buscar_youtube(keyword: str,
                   fecha_inicio: str | None = None,
                   fecha_fin: str | None = None,
                   num: int = 5) -> list[dict]:
    """
    Busca videos en YouTube via Data API v3.
    Cuota: 100 unidades/búsqueda → ~100 búsquedas diarias gratis.
    """
    if not YOUTUBE_API_KEY:
        return []

    params: dict = {
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
        # YouTube espera RFC 3339 completo
        params["publishedAfter"]  = f"{fecha_inicio}T00:00:00Z"
        params["publishedBefore"] = f"{fecha_fin}T23:59:59Z"
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
                "fecha":   s.get("publishedAt",  "")[:10],
                "url":     url,
                "tier":    get_tier(url),
                "es_red":  True,
            })
        return resultados
    except Exception as exc:
        print(f"[motor] buscar_youtube error: {exc}")
        return []


# ════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════

def _parsear(item: dict, tipo: str) -> dict:
    """Normaliza un resultado Serper al formato interno."""
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


def generar_variaciones(keyword: str) -> list[str]:
    """
    Genera hasta 3 variaciones de búsqueda para máxima cobertura.
    Orden: keyword exacta → con comillas → con "Peru".
    """
    base = keyword.strip()
    variaciones = [base]
    if '"' not in base:
        variaciones.append(f'"{base}"')
    if "peru" not in base.lower() and "perú" not in base.lower():
        variaciones.append(f"{base} Peru")
    return variaciones[:3]


# ════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL DE BÚSQUEDA
# ════════════════════════════════════════════════════

def buscar_keyword(keyword: str,
                   fecha_inicio: str | None = None,
                   fecha_fin: str | None = None,
                   num: int = 20) -> list[dict]:
    """
    Motor completo: Serper (web + news) + YouTube.
    - Deduplica por URL (md5)
    - Respeta límite num
    - Ordena: medios primero (Tier I→III), luego redes sociales
    """
    todos: dict[str, dict] = {}
    variaciones = generar_variaciones(keyword)
    num_por_variacion = max(5, num // len(variaciones))

    # ── Serper: web + news ──────────────────────────
    for kw in variaciones:
        for item in buscar_web(kw, fecha_inicio, fecha_fin, num_por_variacion):
            r = _parsear(item, "web")
            if r["url"]:
                uid = hashlib.md5(r["url"].encode()).hexdigest()
                todos.setdefault(uid, r)

        for item in buscar_news(kw, num_por_variacion):
            r = _parsear(item, "news")
            if r["url"]:
                uid = hashlib.md5(r["url"].encode()).hexdigest()
                todos.setdefault(uid, r)

        time.sleep(0.2)   # respetar rate limit Serper

    # ── YouTube: solo keyword principal (cuota limitada) ─
    num_yt = min(5, max(3, num // 6))
    for r in buscar_youtube(keyword, fecha_inicio, fecha_fin, num_yt):
        if r["url"]:
            uid = hashlib.md5(r["url"].encode()).hexdigest()
            todos.setdefault(uid, r)

    resultados = list(todos.values())
    # Medios primero (es_red=False), dentro de cada grupo por tier
    resultados.sort(key=lambda x: (int(x["es_red"]), orden_tier(x["tier"])))
    return resultados[:num]


# ════════════════════════════════════════════════════
# TESTS INLINE — ejecutar con: python motor.py
# ════════════════════════════════════════════════════

def _run_tests() -> None:
    """Tests unitarios para _fecha_serper y _tbs_rango. Sin dependencias externas."""
    print("── Tests _fecha_serper ──────────────────────")

    casos = [
        ("2025-04-05",  "4/5/2025"),
        ("2025-12-31",  "12/31/2025"),
        ("2025-01-01",  "1/1/2025"),
        ("2025-11-09",  "11/9/2025"),
        ("2000-02-29",  "2/29/2000"),   # año bisiesto
    ]
    errores = 0
    for entrada, esperado in casos:
        resultado = _fecha_serper(entrada)
        ok = resultado == esperado
        estado = "✓" if ok else "✗"
        print(f"  {estado}  '{entrada}' → '{resultado}' (esperado: '{esperado}')")
        if not ok:
            errores += 1

    print("\n── Tests _fecha_serper: valores inválidos ───")
    invalidos = ["", "05-04-2025", "2025/04/05", None, "abc"]
    for val in invalidos:
        resultado = _fecha_serper(val)
        ok = resultado == ""
        estado = "✓" if ok else "✗"
        print(f"  {estado}  '{val}' → '{resultado}' (esperado: '')")
        if not ok:
            errores += 1

    print("\n── Tests _tbs_rango ─────────────────────────")
    # Rango válido
    tbs = _tbs_rango("2025-04-01", "2025-04-16")
    assert tbs == "cdr:1,cd_min:4/1/2025,cd_max:4/16/2025", f"Falló: {tbs}"
    print(f"  ✓  rango válido → '{tbs}'")

    # Una fecha inválida → None (no aplicar filtro silencioso)
    tbs_inv = _tbs_rango("2025-04-01", "no-es-fecha")
    assert tbs_inv is None, f"Falló: {tbs_inv}"
    print(f"  ✓  fecha inválida → None (sin filtro silencioso)")

    # Sin fechas → devuelve tbs con ayer/hoy (no None)
    tbs_default = _tbs_rango(None, None)
    assert tbs_default is not None and tbs_default.startswith("cdr:1,"), f"Falló: {tbs_default}"
    print(f"  ✓  sin fechas → '{tbs_default}'")

    print(f"\n{'✅ Todos los tests pasaron' if errores == 0 else f'❌ {errores} test(s) fallaron'}")


if __name__ == "__main__":
    _run_tests()
