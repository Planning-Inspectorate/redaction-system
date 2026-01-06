from redactor.core.redaction.config import LLMTextRedactionConfig

def test_llm_text_redactor__create_system_prompt():
    """
    - Given I have some llm redaction config
    - When I call LLMTextRedactor._create_system_prompt
    - Then the returned system prompt should be correctly formatted
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1-nano",
        text="some text",
        system_prompt="Some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
        constraints=[
            "constraint X",
            "constraint Y",
        ],
    )

    expected_system_prompt = (
        "<SystemRole>\nSome system prompt\n</SystemRole>\n\n"
        "<Terms>\n- rule A\n- rule B\n- rule C\n</Terms>\n\n"
        f"{config.output_format_string}\n\n"
        "<Constraints>\n- constraint X\n- constraint Y\n</Constraints>"
    )
    actual_system_prompt = config.create_system_prompt()

    assert expected_system_prompt == actual_system_prompt

def test_llm_text_redactor__create_system_prompt_no_constraints():
    """
    - Given I have some llm redaction config
    - When I call LLMTextRedactor._create_system_prompt with no constraints
    - Then the returned system prompt should be correctly formatted
    """
    config = LLMTextRedactionConfig(
        name="config name",
        redactor_type="LLMTextRedaction",
        model="gpt-4.1-nano",
        text="some text",
        system_prompt="Some system prompt",
        redaction_terms=[
            "rule A",
            "rule B",
            "rule C",
        ],
    )

    expected_system_prompt = (
        "<SystemRole>\nSome system prompt\n</SystemRole>\n\n"
        "<Terms>\n- rule A\n- rule B\n- rule C\n</Terms>\n\n"
        f"{config.output_format_string}"
    )
    actual_system_prompt = config.create_system_prompt()

    assert expected_system_prompt == actual_system_prompt
