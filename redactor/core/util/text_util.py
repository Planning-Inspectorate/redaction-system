from typing import List
from string import punctuation
from langdetect import detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from unidecode import unidecode
from unicodedata import category

import threading
_LANGDETECT_LOCK = threading.Lock()

DetectorFactory.seed = 0

def is_english_text(text: str, threshold: float = 0.90, margin: float = 0.20) -> bool:
    normalised = " ".join(text.split())
    if not normalised:
        return False

    if len(normalised) < 50: 
        return True  

    try:
        with _LANGDETECT_LOCK:
            langs = detect_langs(normalised)

        en_prob = next((lp.prob for lp in langs if lp.lang == "en"), 0.0)
        max_other = max((lp.prob for lp in langs if lp.lang != "en"), default=0.0)
        return (en_prob >= threshold) and ((en_prob - max_other) >= margin)
    except LangDetectException:
        return False


def normalise_punctuation_unidecode(text: str) -> str:
    """
    Replace punctuation characters with their closest ASCII equivalent.

    :param str text: Input text
    :return str: Text with punctuation replaced by closest ASCII equivalent
    """
    return "".join(
        c if not category(c).startswith("P") else unidecode(c) or c for c in text
    )


def get_normalised_words(text: str) -> List[str]:
    """
    Normalise the given text into a list of words for redaction matching

    :param str text: The text to normalise
    :return List[str]: The list of normalised words
    """
    text_normalised = (
        normalise_punctuation_unidecode(text)  # Normalise punctuation to ASCII
        .lower()
        .split(" ")
    )
    words_normalised = [
        word.strip().strip(  # Remove leading/trailing whitespace
            punctuation  # Remove punctuation around the word
        )
        for word in (text_normalised)
    ]
    return [word for word in words_normalised if word]  # Remove empty words
