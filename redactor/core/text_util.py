from langdetect import detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException


def is_english_text(text: str, threshold: float = 0.90, margin: float = 0.20) -> bool:
    """
    Detect whether text is English.

    Returns True only if both conditions hold:
    - English probability >= threshold; and
    - English probability exceeds the next-highest language by at least margin.

    Notes:
    - Whitespace is normalised; empty/whitespace-only returns False.
    - The detector is seeded for deterministic results.
    - On detection errors (e.g., too short/ambiguous text), returns False.
    """
    # Ensure deterministic behavior across runs
    DetectorFactory.seed = 0

    # Normalize whitespace
    normalised = " ".join(text.split())
    if not normalised:
        # No text to analyze; treat as non-English
        return False

    try:
        langs = detect_langs(normalised)
        en_prob = next((lp.prob for lp in langs if lp.lang == "en"), 0.0)
        other_probs = [lp.prob for lp in langs if lp.lang != "en"]
        max_other = max(other_probs) if other_probs else 0.0
        return (en_prob >= threshold) and ((en_prob - max_other) >= margin)
    except LangDetectException:
        # Detection failed (e.g., too short/ambiguous); treat as non-English
        return False