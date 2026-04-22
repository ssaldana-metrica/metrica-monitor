"""
tono.py — Analisis de tono con Claude Haiku

MEJORA EN ESTA VERSION:
  Fix 3 (complemento) — el prompt ahora pide explicitamente que si el resultado
  no habla de la marca como entidad en Peru (sino como producto, ingrediente,
  empleo o entidad extranjera sin conexion peruana), marque relevancia "baja".
"""

import json
import os
import re
import time

import anthropic

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

_TONO_NEUTRO_DEFAULT = {"tono":"neutro","justificacion":"La keyword no aparece en el titulo ni en el snippet.","relevancia":"baja"}
_TONO_SIN_API        = {"tono":"neutro","justificacion":"Sin API Claude configurada.","relevancia":"media"}
_TONO_ERROR          = {"tono":"neutro","justificacion":"Sin analisis disponible.","relevancia":"media"}

_PROMPT_BASE = (
    "Eres un analista senior de relaciones publicas y monitoreo de medios en Peru.\n"
    "Tu trabajo es determinar si una noticia es POSITIVA, NEGATIVA o NEUTRA para una marca o persona especifica (el cliente).\n\n"
    "{contexto_bloque}"
    "REGLA FUNDAMENTAL: Analiza el ENCUADRAMIENTO completo, no palabras sueltas.\n\n"
    "PASO 0 — VERIFICACION DE PRESENCIA, RELEVANCIA Y GEOGRAFIA (obligatorio antes de todo):\n"
    "A) Aparece el nombre del cliente en el titulo o snippet?\n"
    "   - Si NO aparece → NEUTRO relevancia baja de inmediato.\n"
    "   - Si aparece como homonimo (otra entidad con mismo nombre) → NEUTRO relevancia baja.\n"
    "B) El resultado habla DE la marca como entidad (empresa, persona, institucion)?\n"
    "   - Si es una receta, producto de venta, oferta de empleo, o ficha tecnica → NEUTRO relevancia baja.\n"
    "   - Si es una noticia, articulo o mencion periodistica sobre la marca → continuar.\n"
    "C) El resultado tiene conexion con Peru o Latinoamerica?\n"
    "   - Si es contenido de otro pais sin relacion con la marca en Peru → NEUTRO relevancia baja.\n"
    "   - Si tiene conexion con Peru, Latinoamerica o la marca opera en Peru → continuar.\n\n"
    "CONTEXTO DE RIVALES:\n"
    "- Algo negativo al RIVAL → POSITIVO para el cliente.\n"
    "- Algo positivo al RIVAL → puede ser NEGATIVO o NEUTRO.\n\n"
    "REGLA DE DUDA: Si no puedes determinar con certeza, marca NEUTRO.\n\n"
    "PASOS (solo si paso el PASO 0 completo):\n"
    "1. Quien es el protagonista? Es el cliente o su rival?\n"
    "2. El hecho beneficia o perjudica al cliente directamente?\n"
    "3. Si el protagonista es el rival: lo que le pasa beneficia o perjudica al cliente?\n\n"
    "CRITERIOS:\n"
    "- POSITIVO: favorece imagen, reputacion, resultados o intereses del cliente.\n"
    "- NEGATIVO: dania directamente imagen, reputacion o intereses del cliente.\n"
    "- NEUTRO: mencion informativa, actor secundario, homonimo, producto/receta/empleo, sin conexion Peru.\n\n"
    'Responde SOLO con JSON valido, sin texto extra, sin markdown:\n'
    '{{"tono":"positivo o negativo o neutro","justificacion":"Una oracion explicando el veredicto.","relevancia":"alta o media o baja"}}'
)

_BLOQUE_CONTEXTO = (
    "IDENTIDAD DEL CLIENTE:\n"
    "El cliente es: {keyword}\n"
    "Contexto: {contexto}\n"
    "Usa esta informacion para distinguir al cliente de otras entidades con nombres similares.\n"
    "Si existe un artista, producto, concepto o empresa extranjera diferente con el mismo nombre, ignora esas menciones.\n\n"
)


def _construir_prompt(keyword, contexto):
    bloque = _BLOQUE_CONTEXTO.format(keyword=keyword, contexto=contexto.strip()) if contexto and contexto.strip() else ""
    return _PROMPT_BASE.format(contexto_bloque=bloque)


def _normalizar(texto):
    """
    Elimina tildes/acentos para comparacion robusta.
    "rímac" == "rimac", "Perú" == "Peru", etc.
    """
    reemplazos = {
        "á":"a","é":"e","í":"i","ó":"o","ú":"u","ü":"u","ñ":"n",
        "à":"a","è":"e","ì":"i","ò":"o","ù":"u",
    }
    t = texto.lower()
    for con_tilde, sin_tilde in reemplazos.items():
        t = t.replace(con_tilde, sin_tilde)
    return t


def _keyword_presente(keyword, titulo, snippet):
    """
    True si al menos un token de la keyword aparece en titulo+snippet.
    Normaliza acentos para que "rimac" matchee "rimac" y viceversa.
    """
    # Normalizar todo para comparacion sin acentos
    texto  = _normalizar(titulo + " " + snippet)
    kw_norm = _normalizar(keyword)
    tokens = [t for t in re.split(r'\W+', kw_norm) if len(t) >= 4]
    if not tokens:
        kw_clean = kw_norm.strip()
        return len(kw_clean) >= 3 and kw_clean in texto
    return any(token in texto for token in tokens)


def analizar_tono(titulo, snippet, keyword, contexto=""):
    if not _keyword_presente(keyword, titulo, snippet):
        return _TONO_NEUTRO_DEFAULT.copy()
    if not ANTHROPIC_API_KEY:
        return _TONO_SIN_API.copy()

    prompt_sistema = _construir_prompt(keyword, contexto)
    prompt_usuario = f"Keyword (cliente): {keyword}\nTitulo: {titulo}\nSnippet: {snippet[:200]}"
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    for intento in range(3):
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=prompt_sistema,
                messages=[{"role":"user","content":prompt_usuario}],
            )
            text = msg.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"): text = text[4:]
            text = text.strip()
            result = json.loads(text)
            if result.get("tono") not in ("positivo","negativo","neutro"):
                raise ValueError(f"tono invalido: {result}")
            result.setdefault("justificacion","")
            result.setdefault("relevancia","media")
            return result
        except Exception as exc:
            if intento < 2:
                time.sleep(0.5*(intento+1))
                continue
            print(f"[tono] Error tras 3 intentos '{titulo[:50]}': {exc}")
            return _TONO_ERROR.copy()
    return _TONO_ERROR.copy()


def analizar_resultados(resultados, keyword, contexto=""):
    out = []
    for r in resultados:
        d = analizar_tono(r["titulo"], r["snippet"], keyword, contexto)
        r.update(d)
        out.append(r)
        time.sleep(0.2)
    return out


def _run_tests():
    print("── Tests _keyword_presente ──────────────────")
    casos = [
        ("Ajinomoto", "Pollo con AJI-NO-MOTO receta",         "ingredientes cucharada",          False, "keyword corta no matchea AJI-NO-MOTO"),
        ("Ajinomoto", "Ajinomoto inaugura planta en Lima",     "empresa japonesa invirtio",       True,  "keyword exacta en titulo"),
        ("Samsung",   "Samsung lanza Galaxy A57 en Peru",      "la empresa presento en Lima",     True,  "keyword en titulo"),
        ("Samsung",   "Cyber Wow celulares descuento",         "comprar Galaxy precio S/.",       False, "keyword no aparece"),
        ("Kallpa",    "Kallpa Generacion amplia capacidad",    "empresa energetica Lima",         True,  "keyword en titulo"),
        ("LG Peru",   "LG Peru lanza nueva linea TV",          "empresa presento en Lima",        True,  "keyword LG Peru en titulo"),
    ]
    errores = 0
    for kw, tit, snip, esp, desc in casos:
        res = _keyword_presente(kw, tit, snip)
        ok  = res == esp
        print(f"  {'OK' if ok else 'FAIL'}  [{desc}] → {res}")
        if not ok: errores += 1

    print("\n── Tests _construir_prompt ───────────────────")
    p_con = _construir_prompt("Kallpa", "empresa energetica peruana")
    assert "IDENTIDAD DEL CLIENTE" in p_con
    assert "GEOGRAFIA" in p_con
    print("  OK  con contexto → bloque IDENTIDAD + GEOGRAFIA presentes")

    p_sin = _construir_prompt("Repsol", "")
    assert "IDENTIDAD DEL CLIENTE" not in p_sin
    assert "GEOGRAFIA" in p_sin
    print("  OK  sin contexto → solo GEOGRAFIA presente")

    print(f"\n{'OK - Todos los tests pasaron' if errores==0 else f'FAIL - {errores} test(s) fallaron'}")


if __name__ == "__main__":
    _run_tests()
