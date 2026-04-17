"""
motor.py — Motor de búsqueda de medios
Responsabilidades: Serper (web + news) + YouTube Data API + deduplicación + ordenamiento
"""

import hashlib
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from tiers import get_tier, es_red_social, orden_tier

SERPER_API_KEY  = os.getenv("SERPER_API_KEY", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")


# ════════════════════════════════════════════════════
# CONVERSIÓN DE FECHAS PARA SERPER (tbs param)
# ════════════════════════════════════════════════════

def _fecha_serper(fecha_iso: str) -> str:
    """
    Convierte 'YYYY-MM-DD' → 'M/D/YYYY' para Serper tbs.
    Usa int() — portable en Alpine Linux (Render) donde %-m falla.
    """
    try:
        d = datetime.strptime(fecha_iso, "%Y-%m-%d")
        return f"{int(d.month)}/{int(d.day)}/{d.year}"
    except (ValueError, TypeError) as exc:
        print(f"[motor] _fecha_serper inválida '{fecha_iso}': {exc}")
        return ""


def _tbs_rango(fecha_inicio: str | None, fecha_fin: str | None) -> str | None:
    if fecha_inicio and fecha_fin:
        fi = _fecha_serper(fecha_inicio)
        ff = _fecha_serper(fecha_fin)
        if not fi or not ff:
            print(f"[motor] tbs ignorado: inicio={fecha_inicio} fin={fecha_fin}")
            return None
        return f"cdr:1,cd_min:{fi},cd_max:{ff}"
    ayer = datetime.now() - timedelta(days=1)
    hoy  = datetime.now()
    return (
        f"cdr:1,"
        f"cd_min:{int(ayer.month)}/{int(ayer.day)}/{ayer.year},"
        f"cd_max:{int(hoy.month)}/{int(hoy.day)}/{hoy.year}"
    )


# ════════════════════════════════════════════════════
# FILTRO DE FECHAS EN PYTHON (post-API)
# ════════════════════════════════════════════════════
#
# Problema: tanto Serper como YouTube a veces ignoran el filtro de fechas
# y devuelven resultados fuera del rango pedido.
#
# Solución: parsear el campo "date"/"publishedAt" de cada resultado en
# Python y descartar los que caigan fuera del rango.
#
# Formatos conocidos:
#   Serper News:  "hace N horas/días", "N days ago", "14 ene. 2026", "Jan 6, 2026"
#   Serper Web:   "2026-01-14", "14/01/2026", "January 14, 2026"
#   YouTube:      "2026-04-16" (ISO, ya truncado al guardarlo con [:10])

_MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9,
    "octubre": 10, "noviembre": 11, "diciembre": 12,
}
_MESES_EN = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _parsear_fecha_resultado(fecha_str: str) -> datetime | None:
    """
    Convierte cualquier string de fecha de Serper/YouTube a datetime.
    Devuelve None si no puede parsearlo.
    """
    if not fecha_str or fecha_str.strip() in ("Fecha no disponible", "—", ""):
        return None

    s = fecha_str.strip().lower()
    ahora = datetime.now()

    # Relativos ES/EN: "hace 2 horas", "3 days ago"
    m = re.match(
        r'(?:hace\s+)?(\d+)\s+'
        r'(minuto|minutos|hora|horas|día|dias|días|dia|'
        r'semana|semanas|minute|minutes|hour|hours|day|days|week|weeks)'
        r'(?:\s+ago)?', s
    )
    if m:
        n, uni = int(m.group(1)), m.group(2)
        if uni in ("minuto", "minutos", "minute", "minutes"):
            return ahora - timedelta(minutes=n)
        if uni in ("hora", "horas", "hour", "hours"):
            return ahora - timedelta(hours=n)
        if uni in ("día", "dias", "días", "dia", "day", "days"):
            return ahora - timedelta(days=n)
        if uni in ("semana", "semanas", "week", "weeks"):
            return ahora - timedelta(weeks=n)

    # ISO 8601: "2026-04-16" — también formato YouTube tras [:10]
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # DD/MM/YYYY o DD-MM-YYYY
    m = re.match(r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$', s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # "14 ene. 2026" | "14 enero 2026"
    m = re.match(r'^(\d{1,2})\s+([a-záéíóúü]+\.?)\s+(\d{4})$', s)
    if m:
        dia, mes_s, anio = int(m.group(1)), m.group(2).rstrip("."), int(m.group(3))
        mes = _MESES_ES.get(mes_s) or _MESES_EN.get(mes_s)
        if mes:
            try:
                return datetime(anio, mes, dia)
            except ValueError:
                pass

    # "Jan 14, 2026" | "January 14, 2026"
    m = re.match(r'^([a-z]+\.?)\s+(\d{1,2}),?\s+(\d{4})$', s)
    if m:
        mes_s, dia, anio = m.group(1).rstrip("."), int(m.group(2)), int(m.group(3))
        mes = _MESES_EN.get(mes_s) or _MESES_ES.get(mes_s)
        if mes:
            try:
                return datetime(anio, mes, dia)
            except ValueError:
                pass

    # Último recurso: dateutil si está instalado
    try:
        from dateutil import parser as du
        return du.parse(fecha_str, dayfirst=True)
    except Exception:
        pass

    return None


def _dentro_de_rango(fecha_str: str,
                     fecha_inicio: str | None,
                     fecha_fin: str | None) -> bool:
    """
    True si el resultado debe incluirse.
    - Sin rango → siempre True
    - Con rango y fecha parseable → True solo si está dentro (inclusive)
    - Con rango y fecha NO parseable → False (no colar resultados dudosos)
    """
    if not fecha_inicio or not fecha_fin:
        return True

    dt = _parsear_fecha_resultado(fecha_str)
    if dt is None:
        return False  # fecha desconocida + rango explícito → descartar

    try:
        fi = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        ff = datetime.strptime(fecha_fin, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    except ValueError:
        return True

    return fi <= dt <= ff


# ════════════════════════════════════════════════════
# CONTEXTO EN BÚSQUEDA
# ════════════════════════════════════════════════════

def _enriquecer_query(keyword: str, contexto: str) -> str:
    """
    Si hay contexto definido, genera una query enriquecida para Serper/YouTube
    que ayuda a traer resultados más relevantes.

    Ejemplo:
      keyword="Kallpa", contexto="empresa energética peruana"
      → query = "Kallpa empresa energética"

    Solo toma las primeras 2 palabras clave del contexto para no sobrecargar
    la query y perder resultados válidos.
    """
    if not contexto or not contexto.strip():
        return keyword

    # Extraer tokens significativos del contexto (4+ chars, sin stopwords)
    _STOPWORDS = {"para", "como", "también", "llamada", "conocida", "empresa",
                  "compañía", "grupo", "peruana", "peruano", "perú", "peru"}
    tokens_ctx = [
        t for t in re.split(r'\W+', contexto.lower())
        if len(t) >= 4 and t not in _STOPWORDS
    ][:2]  # máximo 2 tokens extra

    if not tokens_ctx:
        return keyword

    extra = " ".join(tokens_ctx)
    return f"{keyword} {extra}"


# ════════════════════════════════════════════════════
# SERPER — WEB Y NEWS
# ════════════════════════════════════════════════════

def buscar_web(keyword: str,
               fecha_inicio: str | None = None,
               fecha_fin: str | None = None,
               num: int = 20) -> list[dict]:
    tbs = _tbs_rango(fecha_inicio, fecha_fin)
    payload: dict = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    if tbs:
        payload["tbs"] = tbs
    else:
        print(f"[motor] buscar_web SIN tbs para '{keyword}'")
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/search",
                          json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception as exc:
        print(f"[motor] buscar_web error: {exc}")
        return []


def buscar_news(keyword: str, num: int = 20) -> list[dict]:
    payload = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/news",
                          json=payload, headers=headers, timeout=20)
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
    NOTA: YouTube a veces ignora publishedBefore/After, por eso
    aplicamos _dentro_de_rango() sobre los resultados en buscar_keyword().
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
        params["publishedAfter"]  = f"{fecha_inicio}T00:00:00Z"
        params["publishedBefore"] = f"{fecha_fin}T23:59:59Z"
    else:
        ayer = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        params["publishedAfter"] = ayer

    try:
        r = requests.get("https://www.googleapis.com/youtube/v3/search",
                         params=params, timeout=15)
        r.raise_for_status()
        items = r.json().get("items", [])
        resultados = []
        for item in items:
            s   = item.get("snippet", {})
            vid = item.get("id", {}).get("videoId", "")
            if not vid:
                continue
            url = f"https://www.youtube.com/watch?v={vid}"
            # publishedAt viene como "2026-04-16T12:34:56Z"
            # Guardamos YYYY-MM-DD para que _parsear_fecha_resultado lo procese
            fecha_yt = s.get("publishedAt", "")[:10]
            resultados.append({
                "tipo":    "youtube",
                "titulo":  s.get("title",       "Sin título").strip(),
                "snippet": s.get("description", "").strip()[:300],
                "fuente":  s.get("channelTitle", "YouTube"),
                "fecha":   fecha_yt,
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


def generar_variaciones(keyword: str, contexto: str = "") -> list[str]:
    """
    Genera hasta 3 variaciones de búsqueda.
    Si hay contexto, la primera variación ya va enriquecida para
    atraer resultados más relevantes.
    """
    base = keyword.strip()
    query_rica = _enriquecer_query(base, contexto)

    variaciones = [query_rica]  # primera: con contexto si hay

    # Segunda: keyword exacta entre comillas (sin contexto para no perder resultados)
    if '"' not in base:
        variaciones.append(f'"{base}"')

    # Tercera: + Peru si no está ya
    if "peru" not in base.lower() and "perú" not in base.lower():
        variaciones.append(f"{base} Peru")

    # Deduplicar manteniendo orden
    seen = set()
    unicas = []
    for v in variaciones:
        if v not in seen:
            seen.add(v)
            unicas.append(v)

    return unicas[:3]


# ════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL DE BÚSQUEDA
# ════════════════════════════════════════════════════

def buscar_keyword(keyword: str,
                   fecha_inicio: str | None = None,
                   fecha_fin: str | None = None,
                   num: int = 20,
                   contexto: str = "") -> list[dict]:
    """
    Motor completo: Serper (web + news) + YouTube.

    - contexto: descripción del cliente (ej: "empresa energética peruana").
      Se usa para enriquecer queries y reducir resultados irrelevantes.
    - Aplica filtro de fechas en Python sobre TODOS los resultados
      (Serper y YouTube no son confiables con sus filtros propios).
    - Deduplica por URL (md5)
    - Ordena: medios Tier I→III primero, redes sociales al final
    - Respeta límite num DESPUÉS del filtro
    """
    todos: dict[str, dict] = {}
    variaciones = generar_variaciones(keyword, contexto)
    # Pedimos más de lo necesario para compensar los descartados por fechas
    num_por_variacion = max(10, num)

    # ── Serper: web + news ──────────────────────────
    for kw in variaciones:
        for item in buscar_web(kw, fecha_inicio, fecha_fin, num_por_variacion):
            r = _parsear(item, "web")
            if not r["url"]:
                continue
            if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin):
                continue
            uid = hashlib.md5(r["url"].encode()).hexdigest()
            todos.setdefault(uid, r)

        for item in buscar_news(kw, num_por_variacion):
            r = _parsear(item, "news")
            if not r["url"]:
                continue
            if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin):
                continue
            uid = hashlib.md5(r["url"].encode()).hexdigest()
            todos.setdefault(uid, r)

        time.sleep(0.2)

    # ── YouTube ─────────────────────────────────────
    # Usamos query enriquecida también en YouTube para mejor relevancia
    query_yt = _enriquecer_query(keyword, contexto)
    num_yt = min(5, max(3, num // 6))
    for r in buscar_youtube(query_yt, fecha_inicio, fecha_fin, num_yt):
        if not r["url"]:
            continue
        # Filtro post-API: YouTube ignora publishedBefore/After con frecuencia
        if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin):
            print(f"[motor] YouTube descartado por fecha: '{r['titulo'][:50]}' ({r['fecha']})")
            continue
        uid = hashlib.md5(r["url"].encode()).hexdigest()
        todos.setdefault(uid, r)

    resultados = list(todos.values())
    resultados.sort(key=lambda x: (int(x["es_red"]), orden_tier(x["tier"])))

    print(f"[motor] '{keyword}': {len(resultados)} resultado(s) "
          f"({fecha_inicio or 'sin inicio'} → {fecha_fin or 'sin fin'})"
          f"{' [con contexto]' if contexto else ''}")

    return resultados[:num]


# ════════════════════════════════════════════════════
# TESTS INLINE — python motor.py
# ════════════════════════════════════════════════════

def _run_tests() -> None:
    ahora = datetime.now()
    ayer  = ahora - timedelta(days=1)
    hace3 = ahora - timedelta(days=3)

    print("── Tests _parsear_fecha_resultado ───────────")
    casos_parseo = [
        ("hace 2 horas",      "relativo ES horas",           True),
        ("hace 1 día",        "relativo ES día",             True),
        ("hace 3 días",       "relativo ES días",            True),
        ("hace 2 semanas",    "relativo ES semanas",         True),
        ("2 hours ago",       "relativo EN hours",           True),
        ("3 days ago",        "relativo EN days",            True),
        ("1 week ago",        "relativo EN week",            True),
        ("2026-01-14",        "ISO 8601 Serper/YouTube",     True),
        ("2026-04-16",        "ISO 8601 YouTube [:10]",      True),
        ("14/01/2026",        "DD/MM/YYYY",                  True),
        ("14 ene. 2026",      "ES abreviado con punto",      True),
        ("14 enero 2026",     "ES completo",                 True),
        ("Jan 14, 2026",      "EN abreviado",                True),
        ("January 14, 2026",  "EN completo",                 True),
        ("6 ene. 2026",       "ES día sin cero",             True),
        ("Fecha no disponible","sin fecha",                  False),
        ("",                  "vacío",                       False),
    ]
    errores = 0
    for fecha_str, desc, debe_parsear in casos_parseo:
        res = _parsear_fecha_resultado(fecha_str)
        ok  = (res is not None) == debe_parsear
        val = res.strftime("%Y-%m-%d") if res else "None"
        print(f"  {'✓' if ok else '✗'}  [{desc}] '{fecha_str}' → {val}")
        if not ok:
            errores += 1

    print("\n── Tests _dentro_de_rango ───────────────────")
    fi = ayer.strftime("%Y-%m-%d")
    ff = ahora.strftime("%Y-%m-%d")
    f3 = hace3.strftime("%Y-%m-%d")
    casos_rango = [
        ("hace 2 horas",        fi, ff,   True,  "hace 2h dentro de ayer-hoy"),
        ("hace 3 días",         fi, ff,   False, "hace 3d fuera de ayer-hoy"),
        ("hace 2 semanas",      fi, ff,   False, "hace 2sem fuera de ayer-hoy"),
        ("hace 3 días",         f3, ff,   True,  "hace 3d dentro de hace3-hoy"),
        ("14 ene. 2026",        fi, ff,   False, "enero fuera del rango actual"),
        ("2026-04-16",          fi, ff,   True,  "YouTube ISO dentro rango"),
        ("2026-01-06",          fi, ff,   False, "YouTube ISO fuera rango"),
        ("Fecha no disponible", fi, ff,   False, "sin fecha con rango → descartar"),
        ("hace 2 horas",        None,None,True,  "sin rango → siempre True"),
        ("Fecha no disponible", None,None,True,  "sin fecha sin rango → True"),
    ]
    for fecha_str, inicio, fin, esperado, desc in casos_rango:
        res = _dentro_de_rango(fecha_str, inicio, fin)
        ok  = res == esperado
        print(f"  {'✓' if ok else '✗'}  [{desc}] → {res} (esperado: {esperado})")
        if not ok:
            errores += 1

    print("\n── Tests _enriquecer_query ──────────────────")
    casos_query = [
        ("Kallpa",       "empresa energética peruana",    True,  "añade tokens de contexto"),
        ("La Positiva",  "aseguradora peruana seguros",   True,  "añade tokens de contexto"),
        ("Repsol",       "",                              False, "sin contexto → keyword pura"),
        ("Repsol",       "   ",                           False, "contexto vacío → keyword pura"),
        ("Sporting Cristal", "club de fútbol peruano",    True,  "añade 'fútbol' o 'club'"),
    ]
    for kw, ctx, debe_enriquecer, desc in casos_query:
        res = _enriquecer_query(kw, ctx)
        enriquecio = res != kw
        ok = enriquecio == debe_enriquecer
        print(f"  {'✓' if ok else '✗'}  [{desc}] '{kw}' → '{res}'")
        if not ok:
            errores += 1

    print(f"\n{'✅ Todos los tests pasaron' if errores == 0 else f'❌ {errores} test(s) fallaron'}")


if __name__ == "__main__":
    _run_tests()
