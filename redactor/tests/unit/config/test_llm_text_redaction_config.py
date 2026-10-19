from src.redaction.config import LLMTextRedactionConfig


def test_llm_text_redactor__create_system_prompt():
    """
    - Given I have some llm redaction config with every section populated
    - When I call LLMTextRedactionConfig.create_system_prompt
    - Then the returned system prompt should be correctly formatted, with every
      section present
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1",
        text="some text",
        role="Some role",
        task="Some task",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
        must_redact=[
            "must redact X",
            "must redact Y",
        ],
        do_not_redact=[
            "do not redact P",
            "do not redact Q",
        ],
        examples=[
            "example 1",
            "example 2",
        ],
    )

    expected_system_prompt = (
        "<Role>\nSome role\n</Role>\n\n"
        "<Task>\nSome task\n</Task>\n\n"
        "<Terms>\n- rule A\n- rule B\n- rule C\n</Terms>\n\n"
        "<MustRedact>\n- must redact X\n- must redact Y\n</MustRedact>\n\n"
        "<DoNotRedact>\n- do not redact P\n- do not redact Q\n</DoNotRedact>\n\n"
        "<Examples>\n- example 1\n- example 2\n</Examples>"
    )
    actual_system_prompt = config.create_system_prompt()

    assert expected_system_prompt == actual_system_prompt


def test_llm_text_redactor__create_system_prompt_only_required_fields():
    """
    - Given I have some llm redaction config with only the required fields set
    - When I call LLMTextRedactionConfig.create_system_prompt
    - Then the returned system prompt should omit the optional sections entirely
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1",
        text="some text",
        role="Some role",
        task="Some task",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )

    expected_system_prompt = (
        "<Role>\nSome role\n</Role>\n\n"
        "<Task>\nSome task\n</Task>\n\n"
        "<Terms>\n- rule A\n- rule B\n- rule C\n</Terms>"
    )
    actual_system_prompt = config.create_system_prompt()

    assert expected_system_prompt == actual_system_prompt
