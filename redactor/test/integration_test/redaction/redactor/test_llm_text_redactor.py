from redactor.core.redaction.redactor import LLMTextRedactor
from redactor.core.redaction.config import LLMTextRedactionConfig


def test__llm_text_redactor__redact():
    """
    Test that the LLMTextRedactor can generate redaction strings from input redaction config

    - Given we have some config for identifying redaction strings in text, and some text to analyse
    - When we pass the config to the LLMTextRedactor
    - Then the LLMTextRedactor should respond with a LLMTextRedactionResult that contains some expected result strings
    """
    # To boost the overall quality of the result, and to make the test stricter, extra context has been added to the system prompt here
    config = {
        "name": "config name",
        "redactor_type": "LLMTextRedaction",
        "model": "gpt-4.1-nano",
        "system_prompt": (
            "You will be sent text to analyse. The text is a quote from Star Wars. "
            "Please find all strings in the text that adhere to the following rules: "
        ),
        "redaction_terms": [
            "The names of characters",
            "Religions",
            "Genders, such as she, her, he, him, they, their",
        ],
    }
    source_text = (
        "Did you ever hear the tragedy of Darth Plagueis the wise? I thought not, it's not a story the Jedi would tell you. It's a Sith legend. "
        "Darth Plagueis was a dark lord of the Sith, so powerful and so wise, that he could use the Force to influence the midichlorians to "
        "create ... life. He had such a knowledge of the dark side, he could even keep the ones he cared about from dying."
        "The dark side of the force is a pathway to many abilities some consider to be unnatural. He became so powerful, the only thing he cared "
        "about was losing his power, which, eventually of course he did. Unfortunately, he taught his apprentice everything he knew, then his "
        "apprentice killed him in his sleep. It's ironic, he could save others from death, but not himself."
    )
    redactor_inst = LLMTextRedactor(LLMTextRedactionConfig(text=source_text, **config))
    redaction_result = redactor_inst.redact()
    redaction_strings_cleaned = [x.lower() for x in redaction_result.redaction_strings]
    expected_results = {
        "darth plagueis",
        "plagueis",
        "sith",
        "jedi",
        "he",
        "his",
        "himself",
    }
    matches = {
        expected_result: any(
            expected_result in redaction_string
            for redaction_string in redaction_strings_cleaned
        )
        for expected_result in expected_results
    }
    acceptance_threshold = 0.1
    match_percent = float(len(tuple(x for x in matches.values() if x))) / float(
        len(expected_results)
    )
    error_message = (
        f"Expected a match threshold of at least {acceptance_threshold}, but was {match_percent}."
        f"\nExpected results {expected_results}\nActual results: {redaction_strings_cleaned}"
    )
    assert match_percent >= acceptance_threshold, error_message
