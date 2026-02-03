from core.util.text_util import (
    is_english_text,
    normalise_punctuation_unidecode,
    get_normalised_words,
)


def test__is_english_text__detects_english():
    # Clear English sentence should be detected as English
    text = "This is a plain English sentence with multiple words used for detection."
    assert is_english_text(text) is True


def test__is_english_text__detects_non_english():
    # Clear non-English (French) sentence should not be detected as English
    text = (
        "C'est une phrase en français avec plusieurs mots et des accents comme élève."
    )
    assert is_english_text(text) is False


def test__is_english_text__whitespace_and_empty_returns_false():
    assert is_english_text("") is False
    assert is_english_text("    \n\t   ") is False


def test__is_english_text__short_or_ambiguous_returns_false():
    # Very short strings or those likely to cause detection issues should return False
    assert is_english_text("a") is False
    assert is_english_text("😀") is False
    assert is_english_text("12345") is False


def test__normalise_punctuation_unidecode__replaces_punctuation():
    input_text = "“quotes” and ‘apostrophes’."
    expected_output = "\"quotes\" and 'apostrophes'."
    assert normalise_punctuation_unidecode(input_text) == expected_output


def test__get_normalised_words__normalises_text():
    input_text = "Hello, World! This is a test."
    expected_output = ["hello", "world", "this", "is", "a", "test"]
    assert get_normalised_words(input_text) == expected_output


def test__get_normalised_words__handles_multiple_spaces_and_punctuation():
    input_text = "Multiple   spaces... and punctuation!!!"
    expected_output = ["multiple", "spaces", "and", "punctuation"]
    assert get_normalised_words(input_text) == expected_output
