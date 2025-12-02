# src/transformers/ai_helpers.py
from __future__ import annotations

# ============================================================
#  BOOTSTRAP RUTAS
# ============================================================
import sys
import os
import json
import re
from pathlib import Path
from difflib import SequenceMatcher
from typing import Optional, Dict, Any, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ============================================================
#  IMPORTS CORE PULSEFORGE
# ============================================================
import google.generativeai as genai
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, EnvConfigError


# ============================================================
#  VARIABLES GLOBALES
# ============================================================
_CFG = None
_IA_ENABLED = False

_GEMINI_MODELS: List[str] = []
_GEMINI_MODEL_PRIMARY: Optional[str] = None
_GEMINI_MODEL_FALLBACKS: List[str] = []


# ============================================================
#  NORMALIZACIÓN LOCAL
# ============================================================
def normalize_text(value: str) -> str:
    """Normaliza texto para comparaciones."""
    if value is None:
        return ""

    text = str(value).strip().lower()

    reemplazos = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ü": "u", "ñ": "n",
        "´": "", "`": "", "’": "'", "“": '"', "”": '"',
    }

    for k, v in reemplazos.items():
        text = text.replace(k, v)

    return re.sub(r"\s+", " ", text)


def _local_similarity(a: str, b: str) -> float:
    """Similitud clásica (fallback)."""
    return float(SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio())


# ============================================================
#  CONFIGURACIÓN GEMINI
# ============================================================
def _init_gemini():
    """Inicializa configuración y modelos Gemini."""
    global _CFG, _IA_ENABLED, _GEMINI_MODELS, _GEMINI_MODEL_PRIMARY, _GEMINI_MODEL_FALLBACKS

    if _CFG is not None:
        return

    try:
        _CFG = get_config()
    except EnvConfigError as e:
        warn(f"AIHelpers: no se pudo cargar config → {e}")
        _CFG = None
        _IA_ENABLED = False
        return

    activar = getattr(_CFG, "activar_ia", False)
    api_key = getattr(_CFG, "api_gemini_key", None)

    if not activar:
        warn("AIHelpers: IA desactivada.")
        _IA_ENABLED = False
        return

    if not api_key:
        warn("AIHelpers: sin API key → IA OFF.")
        _IA_ENABLED = False
        return

    try:
        genai.configure(api_key=api_key)
        ok("Gemini configurado correctamente.")
        _IA_ENABLED = True
    except Exception as e:
        error(f"Error configurando Gemini → {e}")
        _IA_ENABLED = False
        return

    # Modelo principal (prioridad)
    env_model = os.getenv("PULSEFORGE_GEMINI_MODEL", "").strip() or None
    primary = env_model or "models/gemini-2.5-pro"

    fallbacks = [
        "models/gemini-2.5-flash",
        "models/gemini-flash-latest",
        "models/gemini-pro",
        "models/gemini-1.5-flash",
        "models/gemini-1.5-flash-001",
    ]

    _GEMINI_MODEL_PRIMARY = primary
    _GEMINI_MODEL_FALLBACKS = [m for m in fallbacks if m != primary]
    _GEMINI_MODELS = [primary] + _GEMINI_MODEL_FALLBACKS

    info("Modelos IA cargados:")
    for m in _GEMINI_MODELS:
        ok(f" → {m}")


# ============================================================
#  LLAMADA GEMINI
# ============================================================
def _call_gemini(prompt: str) -> Optional[str]:
    """Prueba modelos en orden: primary → fallbacks."""
    _init_gemini()

    if not _IA_ENABLED:
        return None

    for model_name in _GEMINI_MODELS:
        try:
            info(f"Probando modelo IA → {model_name}")
            model = genai.GenerativeModel(model_name)
            resp = model.generate_content(prompt)

            text = getattr(resp, "text", None)

            # Algunas versiones devuelven candidates
            if not text and getattr(resp, "candidates", None):
                parts = resp.candidates[0].content.parts
                text = "".join((getattr(p, "text", "") or "") for p in parts)

            if text:
                ok(f"Gemini OK → {model_name}")
                return text.strip()

        except Exception as e:
            warn(f"Fallo modelo {model_name} → {e}")

    error("Todos los modelos IA fallaron.")
    return None


# ============================================================
#  IA — SIMILITUD
# ============================================================
def ai_similarity(a: str, b: str) -> float:
    """Similitud semántica con fallback garantizado."""
    info(f"Similitud IA → '{a}' vs '{b}'")

    a_norm = normalize_text(a)
    b_norm = normalize_text(b)

    prompt = f"""
Devuelve SOLO un número entre 0 y 1 indicando similitud.
Texto 1: "{a_norm}"
Texto 2: "{b_norm}"
""".strip()

    text = _call_gemini(prompt)

    if text:
        match = re.search(r"\b([01](?:\.\d+)?)\b", text)
        if match:
            try:
                val = float(match.group(1))
                return max(0, min(1, val))
            except:
                pass

    # fallback
    score = _local_similarity(a_norm, b_norm)
    ok(f"Similitud local → {score:.3f}")
    return score


# ============================================================
#  IA — CLASIFICACIÓN DE MOVIMIENTOS
# ============================================================
def ai_classify(description: str) -> Dict[str, Any]:
    """Clasifica textual bancario."""
    desc = description.strip()

    prompt = f"""
Clasifica una descripción bancaria en Perú.
Devuelve SOLO un JSON válido:

{{
  "tipo": "pago_factura" | "detraccion" | "transferencia" | "otro",
  "probabilidad": float,
  "justificacion": "texto corto"
}}

Descripción: "{desc}"
""".strip()

    text = _call_gemini(prompt)

    if text:
        try:
            match = re.search(r"\{.*\}", text, re.S)
            if not match:
                raise ValueError("JSON no encontrado")

            data = json.loads(match.group(0))

            tipo = data.get("tipo", "otro")
            prob = float(data.get("probabilidad", 0.3))
            just = (data.get("justificacion") or "").strip()

            if tipo not in ("pago_factura", "detraccion", "transferencia", "otro"):
                tipo = "otro"

            if prob < 0 or prob > 1:
                prob = 0.3

            if not just:
                just = "Clasificación IA sin detalle."

            return {"tipo": tipo, "probabilidad": prob, "justificacion": just}

        except Exception as e:
            warn(f"IA classify: error procesando JSON → {e}")

    return {"tipo": "otro", "probabilidad": 0.3, "justificacion": "Fallback local"}


# ============================================================
#  IA — DECISIÓN FINAL MATCHER
# ============================================================
def ai_decide_match(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decide MATCH / MATCH_DUDOSO / NO_MATCH en casos ambiguos.
    Usado por Matcher.
    """
    prompt = f"""
Eres un auditor contable experto.
Decide si un pago bancario corresponde a una factura.

Responde SOLO con un JSON válido:

{{
  "decision": "MATCH" | "MATCH_DUDOSO" | "NO_MATCH",
  "justificacion": "texto corto"
}}

Datos:
{json.dumps(payload, indent=2, ensure_ascii=False)}
""".strip()

    text = _call_gemini(prompt)

    if text:
        try:
            match = re.search(r"\{.*\}", text, re.S)
            if not match:
                raise ValueError("JSON no encontrado")

            data = json.loads(match.group(0))

            decision = data.get("decision", "MATCH_DUDOSO")
            just = (data.get("justificacion") or "").strip()

            if decision not in ("MATCH", "MATCH_DUDOSO", "NO_MATCH"):
                decision = "MATCH_DUDOSO"

            if not just:
                just = "Decisión IA sin detalle."

            return {"decision": decision, "justificacion": just}

        except Exception as e:
            warn(f"IA decide_match error → {e}")

    return {
        "decision": "MATCH_DUDOSO",
        "justificacion": "Fallback IA: respuesta inválida"
    }


# ============================================================
#  MATCH CLIENTE DIRECTO
# ============================================================
def match_cliente(nombre_a: str, nombre_b: str) -> float:
    score = ai_similarity(nombre_a, nombre_b)
    ok(f"Match cliente → {score:.3f}")
    return score


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    print("\n===== TEST AI_HELPERS =====\n")

    _init_gemini()

    print("\n-- SIMILITUD --")
    print(ai_similarity("GYTRES S.A.C.", "GYTRES SAC"))

    print("\n-- CLASIFICACIÓN --")
    print(ai_classify("Depósito detracción SUNAT Danper Trujillo"))

    print("\n-- MATCH CLIENTE --")
    print(match_cliente("DANPER TRUJILLO SAC", "Danper Trujillo S.A.C."))

    ok("\nTEST AI_HELPERS OK\n")
