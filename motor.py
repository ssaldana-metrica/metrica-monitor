"""
motor.py — Motor de busqueda de medios
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


# ════════════════════════════════════════════════════
# LISTAS DE FILTRADO
# ════════════════════════════════════════════════════

_DOMINIOS_BASURA = {
    # E-commerce global
    "amazon.com","amazon.com.mx","ebay.com","aliexpress.com",
    "mercadolibre.com","mercadolibre.com.pe","falabella.com",
    "ripley.com.pe","oechsle.pe","saga.pe","linio.com.pe",
    "shopee.com","shopify.com","walmart.com","costco.com",
    "linio.com","juntoz.com","lumingo.com.pe",
    # Supermercados y retail Peru — fichas de producto, no noticias
    "plazavea.com.pe","wong.pe","metro.pe","tottus.com.pe",
    "vivanda.com.pe","makro.com.pe","sodimac.com.pe","promart.com.pe",
    "spsa.com.pe","cencosud.pe","hiperbodega.com.pe",
    "mifarma.com.pe","inkafarma.com.pe","boticas.com.pe",
    # Marcas corporativas (su web oficial no es noticia sobre ellas)
    "samsung.com","lg.com","sony.com","apple.com","microsoft.com",
    "ajinomoto.com","nestle.com","unilever.com","pg.com",
    "coca-cola.com","pepsi.com","bimbo.com","gloria.com.pe",
    # Recetas
    "allrecipes.com","food.com","epicurious.com","yummly.com",
    "cookpad.com","recetasnestle.com.pe",
    # Empleo global
    "indeed.com","glassdoor.com","monster.com","ziprecruiter.com",
    "linkedin.com",                                        # jobs y perfiles
    # Empleo Peru y Latam
    "computrabajo.com","computrabajo.com.pe","bumeran.com","bumeran.com.pe",
    "aptitus.com","trabajando.com","jooble.org","jobrapido.com",
    "mipleo.com.pe","trabajosperu.com","portaldetrabajo.pe",
    "laboro.com.pe","empleo.com.pe","jobomas.com","multitrabajos.com",
    "opcionempleo.com","kronos.pe","adecco.com.pe","manpower.com.pe",
    "hays.com.pe","michaelpage.com.pe",
    # Retail y construccion Peru — fichas de producto, no noticias
    "promart.pe","promart.com.pe",                         # BUG: antes solo .com.pe
    "sodimac.com.pe","maestro.com.pe","casaeideas.com.pe",
    # Portales de capacitacion, cursos y eventos — no noticias periodisticas
    "formate.pe","capacitate.pe","eventbrite.com","eventbrite.pe",
    "udemy.com","coursera.org","platzi.com","crehana.com",
    "meetup.com","ticketmaster.com.pe","joinnus.com",
    # Directorios financieros y perfiles ejecutivos — no son noticias
    "marketscreener.com","marketwatch.com","macrotrends.net",
    "craft.co","dnb.com","zoominfo.com","hoovers.com",
    "crunchbase.com","pitchbook.com","owler.com",
    "manta.com","kompass.com","empresite.eleconomista.es",
    # Portales de practicas universitarias
    "practicas.pe","practicasprofesionales.pe","tecoloco.com",
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
    # Empleo fichas
    "requiere personal","requiere profesional","se busca profesional",
    "requisitos del cargo","perfil del puesto","requisitos",
    "bachiller en","titulado en","egresado de","licenciado en",
    "experiencia minima","experiencia requerida",
    "remuneracion mensual","beneficios de ley",
    "planilla","enviar cv","postula aqui",
    # Producto
    "especificaciones","specifications","garantia","warranty","manual de usuario",
}

# Patron de titulo de oferta de empleo
_PATRON_EMPLEO = re.compile(
    r"(en\s+\w+,\s*\w+\s*[-]\s*(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+\d{4})"
    r"|([-]\s*(jobrapido|jooble|bumeran|computrabajo|aptitus|mipleo|indeed|glassdoor|trabajando)\.)",
    re.IGNORECASE
)

# Patron de titulo de directorio/perfil ejecutivo (no son noticias)
# Ejemplos: "Alexandra Ospina - MarketScreener", "empresas - enfokedirecto.com"
_PATRON_DIRECTORIO = re.compile(
    r"^(empresas|companies|executive|perfil|directorio|ranking|lista de|listado)\s*[-–]"
    r"|[-–]\s*(marketscreener|crunchbase|craft\.co|dnb\.com|zoominfo|hoovers|manta\.com)",
    re.IGNORECASE
)

# Patron de titulo de lista de practicas/pasantias
_PATRON_PRACTICAS = re.compile(
    r"(practicas|pasantias|internship|practicantes).{0,30}(20\d\d|peru|empresas)",
    re.IGNORECASE
)

# Patron de fecha en posts de marca: "Jul 25, 2018"
_PATRON_FECHA_POST = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{1,2},?\s+20\d{2}",
    re.IGNORECASE
)

# Palabras que indican post publicitario de marca propia
_PALABRAS_POST_MARCA = {
    "conoce como se elabora","nueva leche","nuevo producto","nueva version",
    "lonchecito","gloritaza","potencia la nutricion","doble dha",
    "sabor inigualable","disfruta de","descubre nuestra","prueba nuestra",
    "tu producto favorito","nuestra nueva","nuestro nuevo",
    "exclusivo para ti","ahora disponible","ya disponible",
    "nueva formula","nueva presentacion","lanzamiento de",
}

# Palabras en otros idiomas (no espanol ni ingles)
_PALABRAS_IDIOMA_EXTRANJERO = {
    # Italiano
    "mi piace","impresa locale","informazioni","menzioni","dettagli",
    "pagina non ufficiale","ancora nessun post","accedi","iscriviti",
    "trasparenza della pagina","mostra tutto","persone seguono",
    # Portugues (Brasil/Portugal)
    "curtir","compartilhar","sobre nos","enviar mensagem","saiba mais",
    # Frances
    "j aime","partager","abonnes","en savoir plus","entreprise locale",
    # Aleman
    "gefallt mir","teilen","abonnenten","mehr erfahren","lokales unternehmen",
    # Indonesio/Malayo
    "bagikan","pengikut","pelajari selengkapnya","bisnis lokal",
    # Croata/Serbio
    "svidja mi se","podijeli","pratitelji","lokalno poduzece",
    # Japones/Chino (caracteres)
    "いいね","シェア","コメント","喜欢","分享","评论",
    # Arabe
    "اعجبني","مشاركة","تعليقات",
}


# ════════════════════════════════════════════════════
# FUNCIONES DE FILTRADO
# ════════════════════════════════════════════════════

def _es_perfil_social(url):
    """True si la URL es la raiz del perfil de una marca en redes sociales."""
    # Contenido real de redes — dejar pasar
    if re.search(
        r"facebook\.com/(watch|permalink|story|photo|video|reel|posts|groups)"
        r"|instagram\.com/(p|reel|tv)/",
        url, re.IGNORECASE
    ):
        return False
    # Perfil raiz — bloquear
    return bool(re.search(
        r"facebook\.com/pages/[^/]+/\d+"
        r"|facebook\.com/[^/?#]+/?(\?[^/]*)?$"
        r"|instagram\.com/[^/?#]+/?(\?[^/]*)?$"
        r"|linkedin\.com/company/[^/?#]+/?(\?[^/]*)?$"
        r"|twitter\.com/[^/?#]+/?(\?[^/]*)?$"
        r"|x\.com/[^/?#]+/?(\?[^/]*)?$",
        url, re.IGNORECASE
    ))


def _es_idioma_extranjero(url, snippet):
    """True si el snippet esta en un idioma distinto al espanol o ingles."""
    if get_dominio(url).endswith(".pe"):
        return False  # Medios peruanos siempre validos
    texto = snippet.lower()
    return any(p in texto for p in _PALABRAS_IDIOMA_EXTRANJERO)


def _es_post_publicitario(url, titulo, snippet):
    """True si es un post de marca propia en redes (no cobertura periodistica)."""
    dominio = get_dominio(url)
    es_social = any(r in dominio for r in ["facebook","instagram","tiktok","twitter"])
    if not es_social:
        return False
    texto = (titulo + " " + snippet).lower()
    # Fecha antigua en snippet (post viejo resurgiendo) + palabras de producto
    if _PATRON_FECHA_POST.search(snippet) and any(p in texto for p in _PALABRAS_POST_MARCA):
        return True
    # Muchos emojis + palabras de producto
    emoji_count = sum(1 for c in snippet if ord(c) > 127000)
    if emoji_count >= 3 and any(p in texto for p in _PALABRAS_POST_MARCA):
        return True
    return False


# URLs de TikTok que son videos reales vs contenido editorial/perfil
_PATRON_TIKTOK_VALIDA = re.compile(
    r"tiktok\.com/@[^/]+/video/\d+"   # video real: /@usuario/video/ID
    r"|vm\.tiktok\.com/[A-Za-z0-9]+"  # short link vm.tiktok.com
    r"|tiktok\.com/t/[A-Za-z0-9]+",   # short link tiktok.com/t/
    re.IGNORECASE
)


def _es_tiktok_invalido(url):
    """
    True si la URL es de TikTok pero NO es un video reproducible.
    Filtra URLs editoriales (tiktok.com/content/...), perfiles
    (@usuario sin /video/), y páginas discover.
    NO filtra videos reales ni short links.
    """
    if "tiktok.com" not in url.lower() and "vm.tiktok.com" not in url.lower():
        return False  # No es TikTok
    # Si es una URL valida de video → dejar pasar
    if _PATRON_TIKTOK_VALIDA.search(url):
        return False
    # Es TikTok pero no es video — editorial, perfil, discover, etc.
    return True


def _es_basura(url, titulo, snippet):
    """True si el resultado es basura (empleo, producto, perfil, idioma extranero, tiktok invalido)."""
    dominio = get_dominio(url)
    if dominio in _DOMINIOS_BASURA:
        return True
    for d in _DOMINIOS_BASURA:
        if dominio.endswith("." + d):
            return True
    if _es_perfil_social(url):
        return True
    if _es_tiktok_invalido(url):
        return True
    if _es_idioma_extranjero(url, snippet):
        return True
    if _es_post_publicitario(url, titulo, snippet):
        return True
    if _PATRON_EMPLEO.search(titulo):
        return True
    if _PATRON_DIRECTORIO.search(titulo):
        return True
    if _PATRON_PRACTICAS.search(titulo):
        return True
    texto = (titulo + " " + snippet).lower()
    return sum(1 for p in _PALABRAS_BASURA if p in texto) >= 2


# ════════════════════════════════════════════════════
# FILTRO DE RELEVANCIA PERU
# ════════════════════════════════════════════════════

_INDICADORES_PERU = {
    "peru","perú","peruana","peruano","peruanas","peruanos",
    "lima","arequipa","trujillo","cusco","piura","chiclayo",
    "iquitos","tacna","juliaca","huancayo","chimbote",
    "minem","osinergmin","osiptel","indecopi","sunat",
    "sunafil","oefa","mef","bvl","smv",
    "soles","s/.",
}

_DOMINIOS_LATAM_OK = {
    "infobae.com","bloomberglinea.com","americaeconomia.com",
    "bnamericas.com","reuters.com","apnews.com",
    "eleconomista.com","forbes.com","ft.com","wsj.com",
}

# Sufijos de paises que no son Peru
_SUFIJOS_OTROS_PAISES = {
    ".cl",".ar",".mx",".co",".ve",".bo",".ec",".py",".uy",
    ".br",".cr",".gt",".hn",".sv",".ni",".pa",".cu",".do",
}

# Frases que indican que Peru es solo una locacion casual de persona extranjera
_LOCACION_CASUAL = {
    "juega en peru","juega en perú",
    "vive en peru","vive en perú",
    "reside en peru","reside en perú",
    "trabaja en peru","trabaja en perú",
    "paso por peru","paso por perú",
}

# Palabras que confirman que la noticia es sobre una entidad empresarial/institucional
_ENTIDAD_EMPRESARIAL = {
    "empresa","corporacion","grupo","minera","banco","seguro","aseguradora",
    "financiera","holding","conglomerado","industria","fabrica","planta",
    "inversion","proyecto","contrato","licitacion","concesion",
    "indecopi","minem","osinergmin","sunat","smv","bvl",
}


def _es_relevante_peru(url, titulo, snippet, contexto=""):
    """
    True si el resultado tiene conexion real con Peru Y con la marca buscada.

    Cuando hay contexto (ej: "aseguradora peruana"), verifica ademas que
    el resultado sea sobre la entidad correcta y no un homonimo
    (ej: distrito Rimac vs Rimac Seguros, palabra "gloria" vs empresa Gloria).
    """
    dominio = get_dominio(url)
    texto   = (titulo + " " + snippet).lower()

    # ── Filtro semantico por contexto (aplica a TODOS los dominios) ──
    # Si hay contexto, al menos un token del contexto debe aparecer
    # en titulo o snippet para confirmar que es la entidad correcta.
    if contexto and contexto.strip():
        tokens_ctx = _tokens_contexto(contexto, max_tokens=3)
        if tokens_ctx:
            tiene_token_ctx = any(t in texto for t in tokens_ctx)
            if not tiene_token_ctx:
                # El resultado no menciona ningun termino del contexto.
                # Puede ser el homonimo (distrito, nombre comun, etc.)
                # Excepcion: si el dominio es .pe Y la keyword aparece
                # como nombre propio en el titulo → podria ser noticia real
                # En ese caso dejamos que Claude decida con el analisis de tono.
                # Pero si el snippet es claramente sobre otra cosa → descartar.
                if not dominio.endswith(".pe"):
                    return False
                # Para .pe: solo descartar si hay señales claras de homonimo
                # (ej: "distrito", "asentamiento", "vecinos" para Rimac distrito)
                # Señales de que la keyword es un homonimo (lugar, concepto, etc.)
                _HOMONIMOS = {
                    # Rimac como distrito
                    "distrito","asentamiento","vecinos del","barrio","jiron","av.","avenida",
                    "crimen","asesinato","balazos","explosivo","sicario","delincuente",
                    # Gloria/cristal/sol como sustantivos comunes
                    "en busca de","busca la","por la gloria","sin pena","la gloria",
                    "brilla el sol","bajo el sol","tomar el sol","amor y gloria",
                    "cristal de","cristal transparente","copa de cristal",
                    # Rio, cerro, lugar con nombre de marca
                    "rio rimac","cerro","quebrada","margen del",
                }
                if any(h in texto for h in _HOMONIMOS):
                    return False

    # ── Filtro geografico Peru ────────────────────────────────────────
    # Dominio peruano → siempre relevante geograficamente
    if dominio.endswith(".pe"):
        return True

    # Medios internacionales de referencia → verificar mencion de Peru
    for d in _DOMINIOS_LATAM_OK:
        if dominio == d or dominio.endswith("." + d):
            return any(ind in texto for ind in _INDICADORES_PERU)

    # Dominio de otro pais LATAM
    es_otro_pais = any(dominio.endswith(s) for s in _SUFIJOS_OTROS_PAISES)
    if es_otro_pais:
        if not any(ind in texto for ind in _INDICADORES_PERU):
            return False
        es_solo_locacion = any(loc in texto for loc in _LOCACION_CASUAL)
        tiene_entidad = any(e in texto for e in _ENTIDAD_EMPRESARIAL)
        if es_solo_locacion and not tiene_entidad:
            return False
        return True

    # Dominio generico (.com, .net, .org)
    tiene_ind = any(ind in texto for ind in _INDICADORES_PERU)
    if not tiene_ind:
        return False
    es_solo_locacion = any(loc in texto for loc in _LOCACION_CASUAL)
    tiene_entidad    = any(e in texto for e in _ENTIDAD_EMPRESARIAL)
    if es_solo_locacion and not tiene_entidad:
        return False
    return True


# ════════════════════════════════════════════════════
# CONVERSIÓN DE FECHAS PARA SERPER
# ════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════
# FILTRO DE FECHAS EN PYTHON
# ════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════
# CONTEXTO Y VARIACIONES
# ════════════════════════════════════════════════════

def _tokens_contexto(contexto, max_tokens=2):
    """Extrae tokens significativos del contexto para enriquecer queries."""
    if not contexto or not contexto.strip():
        return []
    _STOP = {
        "para","como","también","llamada","conocida","empresa","compania",
        "grupo","peruana","peruano","perú","peru","tambien","también",
        "conocido","conocida","llamado","llamada","sociedad","anonima",
    }
    return [
        t for t in re.split(r'\W+', contexto.lower())
        if len(t) >= 4 and t not in _STOP
    ][:max_tokens]


def _enriquecer_query(keyword, contexto=""):
    """Agrega hasta 2 tokens del contexto a la keyword."""
    tokens = _tokens_contexto(contexto)
    return f"{keyword} {' '.join(tokens)}" if tokens else keyword


def _fuente_desde_url(url):
    dominio = get_dominio(url)
    return dominio if dominio else "Desconocida"


def generar_variaciones(keyword, contexto=""):
    """
    Genera hasta 3 variaciones de busqueda.

    LOGICA CON CONTEXTO (ej: keyword="Rimac", contexto="aseguradora peruana"):
      tokens = ["aseguradora"]   <- extraidos del contexto
      1. "Rimac aseguradora"     <- query rica con contexto
      2. "Rimac aseguradora" Peru <- con Peru para forzar resultados locales
      3. "Rimac" seguros         <- segunda variacion con sinonimo si hay

    LOGICA SIN CONTEXTO (ej: keyword="Chinalco"):
      1. Chinalco                <- keyword directa
      2. "Chinalco" Peru         <- con comillas + Peru
      3. Chinalco Peru           <- sin comillas + Peru

    CLAVE: cuando hay contexto, NUNCA generamos la keyword sola sin
    el termino diferenciador, porque eso trae resultados contaminados
    (distrito Rimac, nombre comun Gloria, etc.)
    """
    base   = keyword.strip()
    tokens = _tokens_contexto(contexto, max_tokens=2)
    tiene_peru = "peru" in base.lower() or "perú" in base.lower()
    sin_comillas = base.replace('"','').strip()

    if tokens:
        # CON CONTEXTO: todas las variaciones llevan el diferenciador
        t1 = tokens[0]
        t2 = tokens[1] if len(tokens) > 1 else t1

        v1 = f"{base} {t1}"                         # "Rimac aseguradora"
        v2 = f"{base} {t1} Peru" if not tiene_peru else f'"{sin_comillas}" {t1}'
        v3 = f'"{sin_comillas}" {t2}' if t2 != t1 else f"{base} {t1} seguros"
    else:
        # SIN CONTEXTO: variaciones normales con Peru forzado
        v1 = base
        v2 = f'"{sin_comillas}" Peru' if not tiene_peru else f'"{sin_comillas}"'
        v3 = f"{base} Peru" if not tiene_peru else base

    # Deduplicar manteniendo orden
    seen, unicas = set(), []
    for v in [v1, v2, v3]:
        if v and v not in seen:
            seen.add(v)
            unicas.append(v)
    return unicas[:3]


# ════════════════════════════════════════════════════
# APIS DE BÚSQUEDA
# ════════════════════════════════════════════════════

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
    params = {
        "part":"snippet","q":keyword,"type":"video","order":"date",
        "regionCode":"PE","relevanceLanguage":"es",
        "maxResults":min(num,10),"key":YOUTUBE_API_KEY,
    }
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


# Parametros de tracking que no forman parte de la identidad de la URL
_PARAMS_TRACKING = re.compile(
    r'[?&](utm_[^&]*|ref=[^&]*|source=[^&]*|fbclid=[^&]*|gclid=[^&]*'
    r'|mc_eid=[^&]*|mc_cid=[^&]*|_ga=[^&]*|yclid=[^&]*)(&|$)',
    re.IGNORECASE
)


def _normalizar_url(url):
    """
    Normaliza una URL para deduplicacion robusta.
    Elimina: www., parametros de tracking (?utm_*, ?ref=*, etc.)
    Mantiene: path, query params relevantes (paginacion, IDs de articulo).

    Ejemplos:
      https://www.promart.pe/producto?utm_source=google  → promart.pe/producto
      https://gestion.pe/nota?ref=serper                 → gestion.pe/nota
      https://gestion.pe/nota?p=12345                    → gestion.pe/nota?p=12345
    """
    if not url:
        return url
    # Quitar schema
    u = url.lower()
    for schema in ("https://","http://"):
        if u.startswith(schema):
            u = url[len(schema):]
            break
    # Quitar www.
    if u.lower().startswith("www."):
        u = u[4:]
    # Quitar parametros de tracking del query string
    if "?" in u:
        base, qs = u.split("?", 1)
        # Filtrar solo params de tracking, mantener otros
        params_limpios = []
        for param in qs.split("&"):
            if param and not re.match(
                r'^(utm_|ref=|source=|fbclid=|gclid=|mc_eid=|mc_cid=|_ga=|yclid=)',
                param, re.IGNORECASE
            ):
                params_limpios.append(param)
        u = base + ("?" + "&".join(params_limpios) if params_limpios else "")
    # Quitar slash final
    u = u.rstrip("/")
    return u


def _url_hash(url):
    """Hash de URL normalizada para deduplicacion robusta."""
    return hashlib.md5(_normalizar_url(url).encode()).hexdigest()


def _parsear(item, tipo):
    url     = item.get("link","")
    source  = item.get("source","").strip()
    display = item.get("displayLink","").strip()
    fuente  = source or display or _fuente_desde_url(url)
    return {
        "tipo":tipo,
        "titulo":item.get("title","Sin título").strip(),
        "snippet":item.get("snippet","").strip(),
        "fuente":fuente,
        "fecha":item.get("date","Fecha no disponible"),
        "url":url,"tier":get_tier(url),"es_red":es_red_social(url),
    }


# ════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ════════════════════════════════════════════════════

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
            if not _es_relevante_peru(r["url"], r["titulo"], r["snippet"], contexto):
                desc_peru += 1; continue
            todos.setdefault(_url_hash(r["url"]), r)

        for item in buscar_news(kw, num_por_variacion):
            r = _parsear(item, "news")
            if not r["url"]: continue
            if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin): continue
            if _es_basura(r["url"], r["titulo"], r["snippet"]):
                desc_basura += 1; continue
            if not _es_relevante_peru(r["url"], r["titulo"], r["snippet"], contexto):
                desc_peru += 1; continue
            todos.setdefault(_url_hash(r["url"]), r)

        time.sleep(0.2)

    query_yt = _enriquecer_query(keyword, contexto)
    for r in buscar_youtube(query_yt, fecha_inicio, fecha_fin, min(5,max(3,num//6))):
        if not r["url"]: continue
        if not _dentro_de_rango(r["fecha"], fecha_inicio, fecha_fin): continue
        if not _es_relevante_peru(r["url"], r["titulo"], r["snippet"], contexto):
            desc_peru += 1; continue
        todos.setdefault(_url_hash(r["url"]), r)

    resultados = sorted(todos.values(), key=lambda x: (int(x["es_red"]), orden_tier(x["tier"])))
    print(f"[motor] '{keyword}': {len(resultados)} resultados | basura={desc_basura} fuera-peru={desc_peru}")
    return resultados[:num]


# ════════════════════════════════════════════════════
# TESTS — python motor.py
# ════════════════════════════════════════════════════

def _run_tests():
    errores = 0

    print("── _es_basura: empleos ──────────────────────")
    casos = [
        ("https://jooble.org/x",       "Corporacion aceros arequipa en Pisco, Ica - Abril 2026",    "requiere personal titulado",                              True),
        ("https://jobrapido.com/x",    "Planeador I/aceros Arequipa Ica (Peru) - Jobrapido.com",     "ACEROS AREQUIPA Requisitos Bachiller",                    True),
        ("https://mipleo.com.pe/x",    "Ingeniero de Confiabilidad /Aceros Arequipa - Pisco, Ica",  "REQUISITOS: Profesional titulado en Ingenieria Mecanica", True),
        ("https://plazavea.com.pe/x",  "Leche Reconstituida Entera GLORIA Lata 390g",               "enriquecida con vitaminas liquido 100g",                  True),
        ("https://wong.pe/x",          "Gloria Leche Evaporada pack 6 unidades",                    "contiene calcio vitaminas proteinas",                     True),
        ("https://gestion.pe/x",       "Gloria invierte 200M en nueva planta en Lima",              "la empresa lactea presento su plan de expansion",         False),
    ]
    for url,tit,snip,esp in casos:
        res = _es_basura(url,tit,snip)
        ok  = res==esp
        print(f"  {'OK' if ok else 'FAIL'}  [{'basura' if esp else 'valido'}] {tit[:55]}")
        if not ok: errores+=1

    print("\n── _es_basura: perfiles y posts sociales ────")
    casos = [
        ("https://facebook.com/pages/-Aceros-Arequipa-/576449?locale=it_IT", '" Aceros Arequipa " - Home', 'Mi piace: 0. Impresa locale. Informazioni.', True),
        ("https://facebook.com/permalink/123", "Conoce como se elabora la Leche Gloria Sin Lactosa", "Gloria Peru. Jul 25, 2018. Nueva Gloria Sin Lactosa. Conoce como se elabora la Leche Gloria Sin Lactosa y su novedoso proceso de ultrafiltracion.", True),
        ("https://facebook.com/permalink/456", "Nueva Leche evaporada Gloria Ninos ahora con DOBLE DHA", "Gloria Peru. Jul 5, 2019. Nueva Leche evaporada Gloria Ninos ahora con DOBLE DHA. Potencia la nutricion de tus hijos!", True),
        ("https://facebook.com/permalink/789", "Gloria S.A. reporta utilidades record en 2026", "Lima. La empresa lactea anuncio sus resultados financieros del primer trimestre", False),
        ("https://gestion.pe/x",               "Indecopi multa a Gloria por practicas anticompetitivas", "grupo Gloria sancionado por 59 millones", False),
    ]
    for url,tit,snip,esp in casos:
        res = _es_basura(url,tit,snip)
        ok  = res==esp
        print(f"  {'OK' if ok else 'FAIL'}  [{'basura' if esp else 'valido'}] {tit[:60]}")
        if not ok: errores+=1

    print("\n── _es_relevante_peru: dominios otros paises ")
    casos = [
        ("https://bolavip.com/chile/x",  "Paso sin pena ni gloria por la U, juega en Peru",    "Ex jugador de Universidad de Chile vive en Peru",                    False),
        ("https://clarin.com.ar/x",      "El gobierno peruano anuncia medidas economicas",      "Lima Peru el presidente anuncio reforma del sector minero",          True),
        ("https://gestion.pe/x",         "Gloria S.A. reporta record de ventas",                "empresa lactea peruana Lima",                                        True),
        ("https://infobae.com/peru/x",   "Indecopi multa a Gloria Peru",                        "empresa sancionada Lima anticompetitivo",                            True),
        ("https://bolavip.com/ar/x",     "Boca Juniors gana la gloria en Copa",                 "el equipo argentino logro el titulo en el estadio monumental",        False),
        ("https://emol.com.cl/x",        "Empresa Gloria invierte en Peru",                     "la corporacion peruana expande operaciones Lima inversion empresa",  True),
    ]
    for url,tit,snip,esp in casos:
        res = _es_relevante_peru(url,tit,snip)
        ok  = res==esp
        print(f"  {'OK' if ok else 'FAIL'}  [{'peru' if esp else 'no-peru'}] {tit[:60]}")
        if not ok: errores+=1

    print(f"\n{'OK - Todos los tests pasaron' if errores==0 else f'FAIL - {errores} test(s) fallaron'}")


if __name__ == "__main__":
    _run_tests()
