"""
tono.py — Análisis de tono con Claude Haiku
Responsabilidades: prompt, llamada API, retry, guard anti-inferencia
"""

import json
import os
import re
import time

import anthropic

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# ════════════════════════════════════════════════════
# FIX BUG 2 — ANTI-INFERENCIA
# ════════════════════════════════════════════════════
#
# Problema original:
#   Claude recibía título + snippet y a veces deducía tono negativo
#   aunque el cliente (keyword) ni aparecía en el texto. Ejemplo real:
#   keyword="Sporting Cristal", snippet sobre Alianza Lima → Claude
#   infería "negativo para el cliente" por contexto futbolístico.
#
# Fix en dos capas:
#
#   CAPA 1 — Guard Python (antes de llamar a la API):
#     Si ningún token de la keyword aparece en título+snippet
#     (comparación case-insensitive, tokens de 4+ caracteres para
#     evitar falsos positivos con preposiciones), devolvemos neutro
#     directamente sin gastar una llamada a Claude.
#     Umbral: basta con que aparezca UN token de los significativos
#     para dejar pasar al modelo.
#
#   CAPA 2 — Instrucción en el prompt (paso 0 explícito):
#     Aunque el guard Python pase (porque el token sí está en el texto),
#     le pedimos al modelo que verifique él mismo la presencia antes
#     de asignar tono. Esto cubre casos donde el token aparece pero
#     en contexto completamente ajeno al cliente.

_TONO_NEUTRO_DEFAULT = {
    "tono":          "neutro",
    "justificacion": "La keyword no aparece en el título ni en el snippet.",
    "relevancia":    "baja",
}

_TONO_SIN_API = {
    "tono":          "neutro",
    "justificacion": "Sin API Claude configurada.",
    "relevancia":    "media",
}

_TONO_ERROR = {
    "tono":          "neutro",
    "justificacion": "Sin análisis disponible.",
    "relevancia":    "media",
}

# ── PROMPT ───────────────────────────────────────────
# Cambio vs. original: se agrega PASO 0 explícito de presencia.

PROMPT_TONO = """Eres un analista senior de relaciones públicas y monitoreo de medios en Perú.
Tu trabajo es determinar si una noticia es POSITIVA, NEGATIVA o NEUTRA para una marca o persona específica (el "cliente").

REGLA FUNDAMENTAL: Analiza el ENCUADRAMIENTO completo, no palabras sueltas.

PASO 0 — VERIFICACIÓN DE PRESENCIA (obligatorio antes de cualquier otro paso):
¿Aparece el nombre del cliente (o un sinónimo/abreviatura directa) en el título o el snippet?
- Si NO aparece → responde NEUTRO con relevancia "baja" de inmediato. No inferas nada.
- Si SÍ aparece → continúa con los pasos siguientes.

CONTEXTO DE RIVALES Y COMPETENCIA:
- Si algo negativo le pasa al RIVAL del cliente → es POSITIVO para el cliente
- Si algo positivo le pasa al RIVAL del cliente → puede ser NEGATIVO o NEUTRO para el cliente
- Ejemplos deportivos: expulsión del rival = ventaja para el cliente / derrota del rival = victoria del cliente
- Ejemplos empresariales: crisis en competidor = oportunidad para el cliente / multa al rival = neutro o positivo

REGLA DE DUDA: Si no puedes determinar con certeza cómo afecta al cliente, marca NEUTRO.
Nunca atribuyas negativo por una palabra aislada sin entender a quién pertenece el hecho.

PASOS OBLIGATORIOS (solo si el cliente SÍ aparece en el texto):
1. ¿Quién es el protagonista del hecho? ¿Es el cliente o su rival?
2. ¿El hecho narrado beneficia o perjudica al cliente directamente?
3. Si el protagonista es el rival: ¿lo que le pasa al rival beneficia o perjudica al cliente?

CRITERIOS FINALES:
- POSITIVO: el hecho favorece imagen, reputación, resultados o intereses del cliente
- NEGATIVO: el hecho daña directamente la imagen, reputación o intereses del cliente
- NEUTRO: mención informativa, actor secundario, o contexto ambiguo sin certeza

Responde SOLO con JSON válido, sin texto extra, sin markdown:
{"tono":"positivo"|"negativo"|"neutro","justificacion":"Una oración explicando quién es el protagonista y cómo afecta al cliente.","relevancia":"alta"|"media"|"baja"}"""


# ════════════════════════════════════════════════════
# GUARD PYTHON — CAPA 1
# ════════════════════════════════════════════════════

def _keyword_presente(keyword: str, titulo: str, snippet: str) -> bool:
    """
    Devuelve True si al menos un token significativo de la keyword
    aparece en título o snippet (case-insensitive).

    "Token significativo" = 4+ caracteres, para ignorar preposiciones
    y artículos cortos que generarían falsos positivos.

    Ejemplos:
      keyword="Sporting Cristal", titulo="Alianza Lima ganó"  → False
      keyword="Repsol", snippet="derrame de petróleo Repsol"  → True
      keyword="PetroTal", titulo="PetroTal anuncia dividendos" → True
      keyword="la marca", titulo="la empresa anunció"          → False (tokens <4 chars)
    """
    texto = (titulo + " " + snippet).lower()
    # Extraer tokens de 4+ caracteres de la keyword
    tokens = [t for t in re.split(r'\W+', keyword.lower()) if len(t) >= 4]
    if not tokens:
        # Todos los tokens eran cortos (ej: "RPP", "la fe")
        # Usamos la keyword completa en minúsculas como último recurso,
        # pero solo si tiene 3+ caracteres para evitar falsos con "el", "la"
        kw_clean = keyword.lower().strip()
        if len(kw_clean) >= 3:
            tokens = [kw_clean]
        else:
            # Keyword demasiado corta para ser discriminante → no inferir
            return False
    return any(token in texto for token in tokens)


# ════════════════════════════════════════════════════
# ANÁLISIS PRINCIPAL
# ════════════════════════════════════════════════════

def analizar_tono(titulo: str, snippet: str, keyword: str) -> dict:
    """
    Analiza el tono de un resultado para la keyword dada.

    Flujo:
      1. Guard Python: si keyword no está en texto → neutro sin llamar API
      2. Llamada a Claude Haiku con retry (hasta 3 intentos)
      3. Validación del JSON devuelto
      4. Fallback a neutro en cualquier error
    """
    # ── Capa 1: guard de presencia ───────────────────
    if not _keyword_presente(keyword, titulo, snippet):
        return _TONO_NEUTRO_DEFAULT.copy()

    # ── Sin API configurada ──────────────────────────
    if not ANTHROPIC_API_KEY:
        return _TONO_SIN_API.copy()

    # ── Capa 2: llamada a Claude con retry ───────────
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt_usuario = (
        f"Keyword (cliente): {keyword}\n"
        f"Título: {titulo}\n"
        f"Snippet: {snippet[:200]}"
    )

    for intento in range(3):
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=PROMPT_TONO,
                messages=[{"role": "user", "content": prompt_usuario}],
            )
            text = msg.content[0].text.strip()

            # Limpiar posibles backticks de markdown que Claude a veces añade
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()

            result = json.loads(text)

            # Validar estructura mínima antes de retornar
            if result.get("tono") not in ("positivo", "negativo", "neutro"):
                raise ValueError(f"Tono inválido en respuesta: {result}")

            # Asegurar que justificacion y relevancia existan
            result.setdefault("justificacion", "")
            result.setdefault("relevancia", "media")
            return result

        except Exception as exc:
            if intento < 2:
                time.sleep(0.5 * (intento + 1))
                continue
            print(f"[tono] Error tras 3 intentos para '{titulo[:50]}': {exc}")
            return _TONO_ERROR.copy()

    return _TONO_ERROR.copy()   # nunca debería llegar aquí


def analizar_resultados(resultados: list[dict], keyword: str) -> list[dict]:
    """
    Analiza el tono de una lista de resultados.
    Añade in-place: tono, justificacion, relevancia.
    """
    out = []
    for r in resultados:
        d = analizar_tono(r["titulo"], r["snippet"], keyword)
        r.update(d)
        out.append(r)
        time.sleep(0.2)   # respetar rate limit Anthropic
    return out


# ════════════════════════════════════════════════════
# TESTS INLINE — ejecutar con: python tono.py
# ════════════════════════════════════════════════════

def _run_tests() -> None:
    print("── Tests _keyword_presente ──────────────────")

    casos = [
        # (keyword, titulo, snippet, esperado, descripcion)
        ("Sporting Cristal",
         "Alianza Lima gana el clásico",
         "El equipo blanquiazul venció en el estadio",
         False, "rival, keyword ausente"),

        ("Repsol",
         "Derrame de petróleo en el mar",
         "Repsol enfrenta demanda millonaria",
         True,  "keyword en snippet"),

        ("PetroTal",
         "PetroTal anuncia dividendos récord",
         "La empresa reportó ganancias",
         True,  "keyword en titulo"),

        ("Chinalco",
         "Empresa minera amplía operaciones",
         "La compañía Chinalco invierte en Perú",
         True,  "keyword en snippet"),

        ("la fe",
         "la empresa anunció resultados",
         "expertos analizan el mercado energético esta semana",
         False, "keyword 'la fe' ausente del texto"),

        ("RPP",
         "RPP Noticias reportó el caso",
         "",
         True,  "keyword corta, match directo"),

        ("Kallpa",
         "Bolsa de Valores sube 2% esta semana",
         "Acciones del sector energético lideran el alza",
         False, "keyword totalmente ausente"),
    ]

    errores = 0
    for keyword, titulo, snippet, esperado, desc in casos:
        resultado = _keyword_presente(keyword, titulo, snippet)
        ok = resultado == esperado
        estado = "✓" if ok else "✗"
        print(f"  {estado}  [{desc}] → {resultado} (esperado: {esperado})")
        if not ok:
            errores += 1

    print(f"\n{'✅ Todos los tests pasaron' if errores == 0 else f'❌ {errores} test(s) fallaron'}")


if __name__ == "__main__":
    _run_tests()
