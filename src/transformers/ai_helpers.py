# src/transformers/ai_helpers.py
from __future__ import annotations

import sys
import os
import json
import re
import time
from pathlib import Path
from difflib import SequenceMatcher
from typing import Optional, Dict, Any, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import google.generativeai as genai
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, EnvConfigError


# ============================================================
#  ESTADO GLOBAL IA
# ============================================================
_CFG = None
_IA_ENABLED = False
_GEMINI_MODELS: List[str] = []

# Evitar spam
_MODELOS_PROBADOS = set()
_MODELOS_OK_LOG = set()

# Caches
_SIM_CACHE: Dict[Tuple[str, str], float] = {}
_CLASSIFY_CACHE: Dict[str, Dict[str, Any]] = {}
_DECIDE_CACHE: Dict[str, Dict[str, Any]] = {}


# ============================================================
#  HELPERS
# ============================================================
def _sanitize_json_str(s: str) -> str:
    """Limpia strings de JSON IA: elimina texto suelto, saltos raros, ```json, etc."""
    if not s:
        return ""
    s = s.strip()
    s = s.replace("```json", "").replace("```", "")
    s = s.replace("\n", " ").replace("\r", " ")
    return s


def _extract_number(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"\b([01](?:\.\d+)?)\b", text)
    if m:
        try:
            return float(m.group(1))
        except:
            return None
    return None


def _local_sim(a: str, b: str) -> float:
    """Local fallback súper seguro."""
    a = normalize_text(a)
    b = normalize_text(b)
    return SequenceMatcher(None, a, b).ratio()


def normalize_text(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    replacements = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ü": "u", "ñ": "n", "´": "", "`": "", "’": "'", "“": '"', "”": '"',
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return re.sub(r"\s+", " ", text)


# ============================================================
#  GEMINI SETUP
# ============================================================
def _init_gemini() -> None:
    global _CFG, _IA_ENABLED, _GEMINI_MODELS

    if _CFG is not None:
        return

    try:
        _CFG = get_config()
    except EnvConfigError:
        warn("AIHelpers: No config IA → AI OFF")
        _IA_ENABLED = False
        return

    # IA desactivada expresamente
    if not getattr(_CFG, "activar_ia", False):
        warn("AI OFF por configuración (ACTIVAR_IA=false)")
        _IA_ENABLED = False
        return

    api_key = getattr(_CFG, "api_gemini_key", None)
    if not api_key:
        warn("AIHelpers: Sin API Key → AI OFF")
        _IA_ENABLED = False
        return

    try:
        genai.configure(api_key=api_key)
        _IA_ENABLED = True
    except Exception as e:
        error(f"AI OFF — Error configurando Gemini: {e}")
        _IA_ENABLED = False
        return

    model_env = os.getenv("PULSEFORGE_GEMINI_MODEL", "").strip()
    primary = model_env or "models/gemini-2.5-pro"

    fallbacks = [
        "models/gemini-2.5-flash",
        "models/gemini-1.5-flash",
        "models/gemini-flash-latest",
        "models/gemini-pro"
    ]

    _GEMINI_MODELS = [primary] + [m for m in fallbacks if m != primary]

    info("Gemini IA inicializada.")
    for m in _GEMINI_MODELS:
        ok(f" Modelo disponible → {m}")


# ============================================================
#  LLAMADA IA ROBUSTA
# ============================================================
def _call_gemini(prompt: str, timeout: int = 4) -> Optional[str]:
    """
    LLAMADA IA — A PRUEBA DE BOMBA
    - Timeout duro
    - Limitación de latencia
    - Model fallback
    - Sanitización de respuesta
    """

    _init_gemini()
    if not _IA_ENABLED:
        return None

    for model_name in _GEMINI_MODELS:

        # Log solo primera vez
        if model_name not in _MODELOS_PROBADOS:
            info(f"IA → probando modelo {model_name}")
            _MODELOS_PROBADOS.add(model_name)

        try:
            start = time.time()
            model = genai.GenerativeModel(model_name)

            # Límite de tiempo manual
            resp = model.generate_content(prompt, safety_settings={"HARASSMENT": "BLOCK_NONE"})
            elapsed = time.time() - start

            if elapsed > timeout:
                warn(f"Modelo {model_name} excedió timeout → {elapsed:.2f}s")
                continue

            text = getattr(resp, "text", None)

            if not text and getattr(resp, "candidates", None):
                parts = resp.candidates[0].content.parts
                text = "".join((getattr(p, "text", "") or "") for p in parts)

            if text:
                clean = _sanitize_json_str(text)

                if model_name not in _MODELOS_OK_LOG:
                    ok(f"IA OK → {model_name}")
                    _MODELOS_OK_LOG.add(model_name)

                return clean

        except Exception as e:
            warn(f"IA modelo falló ({model_name}) → {e}")

    error("IA → Todos los modelos fallaron. Usando fallback local.")
    return None


# ============================================================
#  IA — SIMILITUD
# ============================================================
def ai_similarity(a: str, b: str) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    key = (a_norm, b_norm)

    # Cache
    if key in _SIM_CACHE:
        return _SIM_CACHE[key]

    # Llamada IA
    prompt = f"""
Devuelve SOLO un número entre 0 y 1. Nada más.

Texto 1: "{a_norm}"
Texto 2: "{b_norm}"
"""

    text = _call_gemini(prompt)

    score = _extract_number(text) if text else None

    # Si IA falla → usar fallback seguro
    if score is None:
        score = _local_sim(a_norm, b_norm)

    score = max(0.0, min(1.0, float(score)))
    _SIM_CACHE[key] = score
    return score


# ============================================================
#  IA — CLASIFICACIÓN
# ============================================================
def ai_classify(description: str) -> Dict[str, Any]:
    desc_norm = normalize_text(description or "")

    if desc_norm in _CLASSIFY_CACHE:
        return _CLASSIFY_CACHE[desc_norm]

    prompt = f"""
Devuelve SOLO un JSON válido:

{{
  "tipo": "pago_factura" | "detraccion" | "transferencia" | "otro",
  "probabilidad": 0.0-1.0,
  "justificacion": "texto"
}}

Descripción: "{desc_norm}"
"""

    text = _call_gemini(prompt)
    clean = _sanitize_json_str(text or "")

    result = {
        "tipo": "otro",
        "probabilidad": 0.3,
        "justificacion": "Fallback IA",
    }

    try:
        m = re.search(r"\{.*\}", clean, re.S)
        if m:
            data = json.loads(m.group(0))
            result["tipo"] = data.get("tipo", "otro")
            result["probabilidad"] = float(data.get("probabilidad", 0.3))
            result["justificacion"] = (data.get("justificacion") or "").strip() or "Sin detalle IA"
    except:
        pass

    _CLASSIFY_CACHE[desc_norm] = result
    return result


# ============================================================
#  IA — DECISIÓN MATCH
# ============================================================
def ai_decide_match(payload: Dict[str, Any]) -> Dict[str, Any]:
    key_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    if key_json in _DECIDE_CACHE:
        return _DECIDE_CACHE[key_json]

    prompt = f"""
Devuelve SOLO un JSON válido:

{{
  "decision": "MATCH" | "MATCH_DUDOSO" | "NO_MATCH",
  "justificacion": "texto"
}}

Datos:
{json.dumps(payload, indent=2, ensure_ascii=False)}
"""

    text = _call_gemini(prompt)
    clean = _sanitize_json_str(text or "")

    result = {
        "decision": "MATCH_DUDOSO",
        "justificacion": "Fallback IA",
    }

    try:
        m = re.search(r"\{.*\}", clean, re.S)
        if m:
            data = json.loads(m.group(0))
            result["decision"] = data.get("decision", "MATCH_DUDOSO")
            result["justificacion"] = (data.get("justificacion") or "").strip() or "Sin detalle IA"
    except:
        pass

    _DECIDE_CACHE[key_json] = result
    return result


# ============================================================
#  MATCH CLIENTE
# ============================================================
def match_cliente(nombre_a: str, nombre_b: str) -> float:
    score = ai_similarity(nombre_a, nombre_b)
    if score >= 0.9 or score <= 0.3:
        ok(f"Match cliente → {score:.3f}")
    return score
