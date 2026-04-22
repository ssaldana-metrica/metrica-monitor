"""
motor.py — Motor de busqueda de medios

MEJORAS EN ESTA VERSION:
  Fix 1 — Fuente "Desconocida": extrae dominio de URL si Serper no devuelve source.
  Fix 2 — Links basura: filtro de dominios e-commerce + palabras de producto/compra.
  Fix 3 — Peru: variaciones incluyen "Peru", filtro Python descarta resultados sin conexion.
"""

import hashlib
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from tiers import get_tier, get_dominio, es_red_social, orden_tier

SERPER_API_KEY  = os.getenv("SERPER_API_KEY", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")


# ── Fix 2: dominios basura ────────────────────────────────────────────────────

_DOMINIOS_BASURA = {
    # E-commerce
    "amazon.com","amazon.com.mx","ebay.com","aliexpress.com",
    "mercadolibre.com","mercadolibre.com.pe","falabella.com",
    "ripley.com.pe","oechsle.pe","saga.pe","linio.com.pe",
    "shopee.com","shopify.com","walmart.com","costco.com",
    # Marcas corporativas (no son medios)
    "samsung.com","lg.com","sony.com","apple.com","microsoft.com",
    "ajinomoto.com","nestle.com","unilever.com","pg.com",
    "coca-cola.com","pepsi.com","bimbo.com","gloria.com.pe",
    # Recetas
    "allrecipes.com","food.com","epicurious.com","yummly.com",
    "cookpad.com","recetasnestle.com.pe",
    # Empleo global
    "indeed.com","glassdoor.com","monster.com","ziprecruiter.com",
    # Empleo Peru y Latam
    "computrabajo.com","computrabajo.com.pe","bumeran.com","bumeran.com.pe",
    "aptitus.com","trabajando.com","jooble.org","jobrapido.com",
    "mipleo.com.pe","trabajosperu.com","portaldetrabajo.pe",
    "laboro.com.pe","empleo.com.pe","jobomas.com","multitrabajos.com",
    "opcionempleo.com","kronos.pe","adecco.com.pe","manpower.com.pe",
    "hays.com.pe","michaelpage.com.pe",
    # Directorios y reviews
    "yellowpages.com","yelp.com","tripadvisor.com",
    "trustpilot.com","sitejabber.com",
    # Enciclopedias
    "wikipedia.org","wikimedia.org",
}

_PALABRAS_BASURA = {
    # Compra
    "comprar","buy now","add to cart","agregar al carrito",
    "precio","price","s/.","oferta","descuento","discount",
    "envio gratis","free shipping","stock disponible",
    # Recetas
    "ingredientes","ingredients","preparacion","preparation",
    "cocinar","receta","recipe","porciones","servings","cucharada","tablespoon",
    # Empleo general
    "job description","descripcion del puesto","postular","apply now",
    "vacante","sueldo","salary","oferta de empleo","oferta laboral",
    # Empleo - fichas de trabajo
    "requiere personal","requiere profesional","se busca profesional",
    "requisitos del cargo","perfil del puesto",
    "bachiller en","titulado en","egresado de","licenciado en","requisitos",
    "experiencia minima","experiencia requerida",
    "remuneracion mensual","beneficios de ley",
    "planilla","enviar cv","postula aqui",
    # Producto
    "especificaciones","specifications","garantia","warranty","manual de usuario",
}

import re as _re
_PATRON_EMPLEO = _re.compile(
    r"(en\s+\w+,\s*\w+\s*[-]\s*(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+\d{4})"
    r"|([-]\s*(jobrapido|jooble|bumeran|computrabajo|aptitus|mipleo|indeed|glassdoor|trabajando)\.)",
    _re.IGNORECASE
)


def _es_basura(url, titulo, snippet):
    dominio = get_dominio(url)

    # Dominio en lista negra (e-commerce, empleo, recetas, etc.)
    if dominio in _DOMINIOS_BASURA:
        return True
    for d in _DOMINIOS_BASURA:
        if dominio.endswith("." + d):
            return True

    # Fix A: perfil corporativo de red social (no es contenido periodístico)
    if _es_perfil_social(url):
        return True

    # Fix B: snippet en idioma que no es español ni inglés
    if _es_idioma_extranjero(url, snippet):
        return True

    # Patrón de título de oferta de empleo
    if _PATRON_EMPLEO.search(titulo):
        return True

    # Palabras de producto/empleo/receta (2+ coincidencias)
    texto = (titulo + " " + snippet).lower()
    coincidencias = sum(1 for p in _PALABRAS_BASURA if p in texto)
    return coincidencias >= 2


# ── Fix 3: relevancia Peru ────────────────────────────────────────────────────

_INDICADORES_PERU = {
    "peru","perú","peruana","peruano","peruanas","peruanos",
    "lima","arequipa","trujillo","cusco","piura","chiclayo",
    "iquitos","tacna","juliaca","huancayo","chimbote",
    "minem","osinergmin","osiptel","indecopi","sunat",
    "sunafil","oefa","mef","pcm","congreso peruano",
    "bolsa de valores de lima","bvl","smv",
    "soles","s/.",
}

_DOMINIOS_LATAM_OK = {
    "infobae.com","bloomberglinea.com","americaeconomia.com",
    "bnamericas.com","reuters.com","apnews.com",
    "eleconomista.com","forbes.com","ft.com","wsj.com",
}


def _es_relevante_peru(url, titulo, snippet):
    dominio = get_dominio(url)
    if dominio.endswith(".pe"):
        return True
    for d in _DOMINIOS_LATAM_OK:
        if dominio == d or dominio.endswith("." + d):
            return True
    texto = (url + " " + titulo + " " + snippet).lower()
    return any(ind in texto for ind in _INDICADORES_PERU)


# ── Fix A: perfiles corporativos de redes sociales ────────────────────────────
#
# Diferencia entre contenido útil y fichas de perfil:
#   UTIL:  facebook.com/watch/?v=123  (video/post)
#          facebook.com/permalink/... (post específico)
#   BASURA: facebook.com/pages/Marca/ID  (ficha de página)
#           facebook.com/NombreMarca/    (perfil directo)
#           instagram.com/NombreMarca/   (perfil)
#           linkedin.com/company/Nombre/ (perfil empresa)
#
# Regla: URLs de redes sociales que apuntan a la raíz de un perfil
# (terminan en /pages/..., /company/..., o son perfil sin contenido)
# no tienen valor periodístico.

import re as _re2

_PATRON_PERFIL_SOCIAL = _re2.compile(
    r"facebook\.com/pages/[^/]+/\d+"          # facebook.com/pages/Marca/ID
    r"|facebook\.com/[^/]+/?(\?.*)?$"          # facebook.com/Marca/ (raíz de perfil)
    r"|instagram\.com/[^/]+/?(\?.*)?$"         # instagram.com/Marca/
    r"|linkedin\.com/company/[^/]+/?(\?.*)?$"  # linkedin.com/company/Marca/
    r"|twitter\.com/[^/]+/?(\?.*)?$"           # twitter.com/Marca/
    r"|x\.com/[^/]+/?(\?.*)?$",                # x.com/Marca/
    _re2.IGNORECASE
)

# Excepciones: estas rutas de Facebook/IG SÍ son contenido
_PATRON_CONTENIDO_SOCIAL = _re2.compile(
    r"facebook\.com/(watch|permalink|story|photo|video|reel|posts|groups)"
    r"|instagram\.com/(p|reel|tv)/",
    _re2.IGNORECASE
)


def _es_perfil_social(url):
    """
    True si la URL es la ficha/perfil de una marca en redes sociales,
    no un post o contenido específico.
    """
    if _PATRON_CONTENIDO_SOCIAL.search(url):
        return False  # Es contenido real, no perfil
    return bool(_PATRON_PERFIL_SOCIAL.search(url))


# ── Fix B: detección de idioma extranjero ─────────────────────────────────────
#
# Si el snippet contiene palabras características de un idioma que NO es
# español ni inglés, el resultado no es relevante para monitoreo en Perú.
# Aplicamos solo cuando el dominio NO es .pe (un diario peruano puede
# tener errores de encoding pero igual es válido).
#
# Estrategia: listas de palabras muy frecuentes y únicas de cada idioma
# que raramente aparecen en español o inglés.

_PALABRAS_NO_ES_EN = {
    # Italiano
    "mi piace", "impresa locale", "informazioni", "menzioni", "dettagli",
    "pagina non ufficiale", "ancora nessun post", "accedi", "iscriviti",
    "trasparenza della pagina", "mostra tutto", "persone seguono",
    # Portugués (Brasil/Portugal — diferente del español)
    "curtir", "compartilhar", "comentários", "publicações",
    "sobre nós", "enviar mensagem", "saiba mais", "ver mais",
    # Francés
    "j'aime", "partager", "commentaires", "abonnés", "suivre",
    "en savoir plus", "voir plus", "bonjour", "entreprise locale",
    # Alemán
    "gefällt mir", "teilen", "kommentare", "abonnenten", "folgen",
    "mehr erfahren", "lokales unternehmen", "impressum",
    # Indonesio / Malayo
    "suka", "bagikan", "komentar", "pengikut", "ikuti",
    "pelajari selengkapnya", "bisnis lokal",
    # Croata / Serbio / Bosnio
    "sviđa mi se", "podijeli", "komentari", "pratitelji",
    "lokalno poduzeće", "saznajte više",
    # Hindú / Hindi (transliterado en latin)
    "pasand karo", "share karo", "tippani",
    # Árabe (transliterado)
    "اعجبني", "مشاركة", "تعليقات",
    # Tailandés
    "ถูกใจ", "แชร์", "ความคิดเห็น",
    # Japonés / Chino (caracteres)
    "いいね", "シェア", "コメント", "喜欢", "分享", "评论",
    # Filipino / Tagalo
    "gusto ko ito", "ibahagi", "mga komento",
}


def _es_idioma_extranjero(url, snippet):
    """
    True si el snippet contiene palabras de un idioma que no es
    español ni inglés, Y el dominio no es .pe.

    Los dominios .pe se excluyen porque aunque tengan encoding raro
    siguen siendo medios peruanos válidos.
    """
    dominio = get_dominio(url)
    if dominio.endswith(".pe"):
        return False  # Siempre confiar en medios peruanos

    texto = snippet.lower()
    return any(palabra in texto for palabra in _PALABRAS_NO_ES_EN)


# ── Fix 1: fuente desde URL ───────────────────────────────────────────────────

def _fuente_desde_url(url):
    dominio = get_dominio(url)
    return dominio if dominio else "Desconocida"


# ── Fechas para Serper tbs ────────────────────────────────────────────────────

def _fecha_serper(fecha_iso):
    try:
        d = datetime.strptime(fecha_iso, "%Y-%m-%d")
        return f"{int(d.month)}/{int(d.day)}/{d.year}"
    except (ValueError, TypeError) as exc:
        print(f"[motor] _fecha_serper invalida '{fecha_iso}': {exc}")
        return ""


def _tbs_rango(fecha_inicio, fecha_fin):
    if fecha_inicio and fecha_fin:
        fi = _fecha_serper(fecha_inicio)
        ff = _fecha_serper(fecha_fin)
        if not fi or not ff:
            return None
        return f"cdr:1,cd_min:{fi},cd_max:{ff}"
    ayer = datetime.now() - timedelta(days=1)
    hoy  = datetime.now()
    return f"cdr:1,cd_min:{int(ayer.month)}/{int(ayer.day)}/{ayer.year},cd_max:{int(hoy.month)}/{int(hoy.day)}/{hoy.year}"


# ── Parser de fechas de resultados ────────────────────────────────────────────

_MESES_ES = {
    "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
    "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12,
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,
    "junio":6,"julio":7,"agosto":8,"septiembre":9,
    "octubre":10,"noviembre":11,"diciembre":12,
}
_MESES_EN = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,
    "june":6,"july":7,"august":8,"september":9,
    "october":10,"november":11,"december":12,
}


def _parsear_fecha_resultado(fecha_str):
    if not fecha_str or fecha_str.strip() in ("Fecha no disponible","—",""):
        return None
    s = fecha_str.strip().lower()
    ahora = datetime.now()
    m = re.match(r'(?:hace\s+)?(\d+)\s+(minuto|minutos|hora|horas|dia|dias|día|días|semana|semanas|minute|minutes|hour|hours|day|days|week|weeks)(?:\s+ago)?', s)
    if m:
        n, u = int(m.group(1)), m.group(2)
        if u in ("minuto","minutos","minute","minutes"): return ahora - timedelta(minutes=n)
        if u in ("hora","horas","hour","hours"):         return ahora - timedelta(hours=n)
        if u in ("dia","dias","día","días","day","days"): return ahora - timedelta(days=n)
        if u in ("semana","semanas","week","weeks"):      return ahora - timedelta(weeks=n)
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m:
        try: return datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        except: pass
    m = re.match(r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$', s)
    if m:
        try: return datetime(int(m.group(3)),int(m.group(2)),int(m.group(1)))
        except: pass
    m = re.match(r'^(\d{1,2})\s+([a-zaeiouáéíóú]+\.?)\s+(\d{4})$', s)
    if m:
        dia,mes_s,anio = int(m.group(1)),m.group(2).rstrip("."),int(m.group(3))
        mes = _MESES_ES.get(mes_s) or _MESES_EN.get(mes_s)
        if mes:
            try: return datetime(anio,mes,dia)
            except: pass
    m = re.match(r'^([a-z]+\.?)\s+(\d{1,2}),?\s+(\d{4})$', s)
    if m:
        mes_s,dia,anio = m.group(1).rstrip("."),int(m.group(2)),int(m.group(3))
        mes = _MESES_EN.get(mes_s) or _MESES_ES.get(mes_s)
        if mes:
            try: return datetime(anio,mes,dia)
            except: pass
    try:
        from dateutil import parser as du
        return du.parse(fecha_str, dayfirst=True)
    except: pass
    return None


def _dentro_de_rango(fecha_str, fecha_inicio, fecha_fin):
    if not fecha_inicio or not fecha_fin:
        return True
    dt = _parsear_fecha_resultado(fecha_str)
    if dt is None:
        return False
    try:
        fi = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        ff = datetime.strptime(fecha_fin, "%Y-%m-%d").replace(hour=23,minute=59,second=59)
    except: return True
    return fi <= dt <= ff


# ── Contexto ──────────────────────────────────────────────────────────────────

def _enriquecer_query(keyword, contexto=""):
    if not contexto or not contexto.strip():
        return keyword
    _STOP = {"para","como","también","llamada","conocida","empresa","companía","grupo","peruana","peruano","perú","peru"}
    tokens = [t for t in re.split(r'\W+', contexto.lower()) if len(t)>=4 and t not in _STOP][:2]
    return f"{keyword} {' '.join(tokens)}" if tokens else keyword


# ── Serper ────────────────────────────────────────────────────────────────────

def buscar_web(keyword, fecha_inicio=None, fecha_fin=None, num=20):
    tbs = _tbs_rango(fecha_inicio, fecha_fin)
    payload = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    if tbs: payload["tbs"] = tbs
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/search", json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception as exc:
        print(f"[motor] buscar_web error: {exc}")
        return []


def buscar_news(keyword, num=20):
    payload = {"q": keyword, "gl": "pe", "hl": "es", "num": num}
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    try:
        r = requests.post("https://google.serper.dev/news", json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("news", [])
    except Exception as exc:
        print(f"[motor] buscar_news error: {exc}")
        return []


def buscar_youtube(keyword, fecha_inicio=None, fecha_fin=None, num=5):
    if not YOUTUBE_API_KEY:
        return []
    params = {"part":"snippet","q":keyword,"type":"video","order":"date",
              "regionCode":"PE","relevanceLanguage":"es","maxResults":min(num,10),"key":YOUTUBE_API_KEY}
    if fecha_inicio and fecha_fin:
        params["publishedAfter"]  = f"{fecha_inicio}T00:00:00Z"
        params["publishedBefore"] = f"{fecha_fin}T23:59:59Z"
    else:
        ayer = (datetime.now(timezone.utc)-timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        params["publishedAfter"] = ayer
    try:
        r = requests.get("https://www.googleapis.com/youtube/v3/search", params=params, timeout=15)
        r.raise_for_status()
        resultados = []
        for item in r.json().get("items",[]):
            s   = item.get("snippet",{})
            vid = item.get("id",{}).get("videoId","")
            if not vid: continue
            url = f"https://www.youtube.com/watch?v={vid}"
            resultados.append({
                "tipo":"youtube","titulo":s.get("title","Sin título").strip(),
                "snippet":s.get("description","").strip()[:300],
                "fuente":s.get("channelTitle","YouTube"),
                "fecha":s.get("publishedAt","")[:10],"url":url,
                "tier":get_tier(url),"es_red":True,
            })
        return resultados
    except Exception as exc:
        print(f"[motor] buscar_youtube error: {exc}")
        return []


# ── Parsear resultado Serper ──────────────────────────────────────────────────

def _parsear(item, tipo):
    url     = item.get("link","")
    source  = item.get("source","").strip()
    display = item.get("displayLink","").strip()
    fuente  = source or display or _fuente_desde_url(url)  # Fix 1
    return {
        "tipo":tipo,
        "titulo":item.get("title","Sin título").strip(),
        "snippet":item.get("snippet","").strip(),
        "fuente":fuente,
        "fecha":item.get("date","Fecha no disponible"),
        "url":url,"tier":get_tier(url),"es_red":es_red_social(url),
    }


def generar_variaciones(keyword, contexto=""):
    """
    Fix 3: siempre incluye una variacion con Peru para forzar
    resultados geograficamente relevantes.
    """
    base       = keyword.strip()
    query_rica = _enriquecer_query(base, contexto)
    tiene_peru = "peru" in base.lower() or "peru" in base.lower()

    variaciones = [query_rica]

    if not tiene_peru:
        sin_comillas = base.replace('"','').strip()
        variaciones.append(f'"{sin_comillas}" Peru' if '"' not in base else f"{base} Peru")
    elif '"' not in base:
        variaciones.append(f'"{base}"')

    if base not in variaciones:
        variaciones.append(base)

    seen, unicas = set(), []
    for v in variaciones:
        if v not in seen:
            seen.add(v)
            unicas.append(v)
    return unicas[:3]


# ── Función principal ─────────────────────────────────────────────────────────

def buscar_keyword(keyword, fecha_inicio=None, fecha_fin=None, num=20, contexto=""):
    todos = {}
    variaciones = generar_variaciones(keyword, contexto)
    num_por_variacion = max(10, num)
    desc_basura = desc_peru = 0

    for kw in variaciones:
        for item in buscar_web(kw, fecha_inicio, fecha_fin, num_por_variacion):
            r = _parsear(item, "web")
            if not r["url"]: continue
            if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin): continue
            if _es_basura(r["url"], r["titulo"], r["snippet"]):
                desc_basura += 1; continue
            if not _es_relevante_peru(r["url"], r["titulo"], r["snippet"]):
                desc_peru += 1; continue
            todos.setdefault(hashlib.md5(r["url"].encode()).hexdigest(), r)

        for item in buscar_news(kw, num_por_variacion):
            r = _parsear(item, "news")
            if not r["url"]: continue
            if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin): continue
            if _es_basura(r["url"], r["titulo"], r["snippet"]):
                desc_basura += 1; continue
            if not _es_relevante_peru(r["url"], r["titulo"], r["snippet"]):
                desc_peru += 1; continue
            todos.setdefault(hashlib.md5(r["url"].encode()).hexdigest(), r)

        time.sleep(0.2)

    query_yt = _enriquecer_query(keyword, contexto)
    for r in buscar_youtube(query_yt, fecha_inicio, fecha_fin, min(5,max(3,num//6))):
        if not r["url"]: continue
        if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin): continue
        if not _es_relevante_peru(r["url"], r["titulo"], r["snippet"]):
            desc_peru += 1; continue
        todos.setdefault(hashlib.md5(r["url"].encode()).hexdigest(), r)

    resultados = sorted(todos.values(), key=lambda x: (int(x["es_red"]), orden_tier(x["tier"])))
    print(f"[motor] '{keyword}': {len(resultados)} resultados | basura={desc_basura} fuera-peru={desc_peru}")
    return resultados[:num]


# ── Tests ─────────────────────────────────────────────────────────────────────

def _run_tests():
    errores = 0

    print("── Fix 1: _fuente_desde_url ─────────────────")
    casos = [
        ("https://gestion.pe/economia/nota", "gestion.pe"),
        ("https://www.elcomercio.pe/nota",   "elcomercio.pe"),
        ("https://energiminas.com/chinalco", "energiminas.com"),
        ("https://m.rpp.pe/nota",            "rpp.pe"),
        ("",                                 "Desconocida"),
    ]
    for url, esp in casos:
        res = _fuente_desde_url(url)
        ok  = res == esp
        print(f"  {'OK' if ok else 'FAIL'}  '{url[:45]}' → '{res}'")
        if not ok: errores += 1

    print("\n── Fix 2: _es_basura ────────────────────────")
    casos = [
        ("https://samsung.com/pe/phones",  "Samsung Galaxy A57",          "comprar precio S/. oferta",      True,  "dominio basura"),
        ("https://gestion.pe/nota",        "Samsung lanza Galaxy en Peru", "la empresa presento en Lima",    False, "noticia legitima"),
        ("https://allrecipes.com/recipe",  "Pollo AJI-NO-MOTO",           "ingredientes cucharada cocinar",  True,  "receta basura"),
        ("https://elcomercio.pe/nota",     "Ajinomoto inaugura planta",    "empresa invirtio en Peru",       False, "noticia inversion"),
        ("https://computrabajo.com.pe",    "Vacante LG Peru",              "postular sueldo requisitos",     True,  "empleo basura"),
        ("https://rpp.pe/nota",            "LG gana premio Lima",          "empresa presento resultados",    False, "noticia valida"),
    ]
    for url, tit, snip, esp, desc in casos:
        res = _es_basura(url, tit, snip)
        ok  = res == esp
        print(f"  {'OK' if ok else 'FAIL'}  [{desc}] → {res}")
        if not ok: errores += 1

    print("\n── Fix 3: _es_relevante_peru ────────────────")
    casos = [
        ("https://gestion.pe/nota",       "Repsol Lima",           "",                          True,  "dominio .pe"),
        ("https://infobae.com/peru/nota",  "Chinalco Peru",         "inversion Lima",            True,  "medio latam + Peru"),
        ("https://allrecipes.com/recipe",  "Ajinomoto Ramen Bowl",  "authentic Japanese",        False, "sin Peru"),
        ("https://samsung.com/us/phones",  "Samsung Galaxy launch", "available stores",          False, "EE.UU. sin Peru"),
        ("https://bloomberglinea.com/nota","Kallpa energia Peru",   "Lima inversion",            True,  "medio latam + Peru"),
    ]
    for url, tit, snip, esp, desc in casos:
        res = _es_relevante_peru(url, tit, snip)
        ok  = res == esp
        print(f"  {'OK' if ok else 'FAIL'}  [{desc}] → {res}")
        if not ok: errores += 1

    print("\n── Fix 3: generar_variaciones (Peru forzado) ")
    casos = [
        ("Ajinomoto",    "",   True,  "debe incluir Peru"),
        ("Samsung Peru", "",   True,  "ya tiene Peru"),
        ("Repsol",       "empresa energetica", True, "contexto no quita Peru"),
    ]
    for kw, ctx, debe_peru, desc in casos:
        vars_ = generar_variaciones(kw, ctx)
        tiene = any("peru" in v.lower() for v in vars_)
        ok = tiene == debe_peru
        print(f"  {'OK' if ok else 'FAIL'}  [{desc}] {vars_}")
        if not ok: errores += 1

    print(f"\n{'OK - Todos los tests pasaron' if errores==0 else f'FAIL - {errores} test(s) fallaron'}")


if __name__ == "__main__":
    _run_tests()
