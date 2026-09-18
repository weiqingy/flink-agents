################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
from typing import Any, Dict, List, Mapping
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from pyflink.common.typeinfo import Types

from flink_agents.api.agents.types import OutputSchema
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.tools.tool import Tool
from flink_agents.integrations.chat_models.ollama_chat_model import (
    OllamaChatModelConnection,
)
from flink_agents.plan.function import PythonFunction
from flink_agents.plan.tools.function_tool import FunctionTool


class Person(BaseModel):
    """A representative flat BaseModel output schema."""

    name: str
    age: int


class Company(BaseModel):
    """A nested output schema, whose JSON schema carries a ``$defs`` entry."""

    name: str
    owner: Person


def _connection() -> OllamaChatModelConnection:
    """A connection whose Ollama client is a mock, so no server is contacted."""
    conn = OllamaChatModelConnection()
    response = MagicMock()
    response.message.role = "assistant"
    response.message.content = "ok"
    response.message.tool_calls = None
    response.prompt_eval_count = 1
    response.eval_count = 2
    mock_client = MagicMock()
    mock_client.chat.return_value = response
    conn._OllamaChatModelConnection__client = mock_client
    return conn


def _chat_call_kwargs(conn: OllamaChatModelConnection) -> Dict[str, Any]:
    return conn.client.chat.call_args.kwargs


def _messages() -> list[ChatMessage]:
    return [ChatMessage(role=MessageRole.USER, content="hi")]


def test_native_applied_for_base_model() -> None:
    """A BaseModel schema reaches the request as the native ``format`` argument."""
    conn = _connection()
    conn.chat(
        _messages(), model="qwen3", output_schema=OutputSchema(output_schema=Person)
    )
    native_format = _chat_call_kwargs(conn)["format"]
    assert native_format["properties"].keys() == {"name", "age"}


def test_format_absent_without_schema() -> None:
    """A call without a schema carries no ``format``, leaving generation unconstrained."""
    conn = _connection()
    conn.chat(_messages(), model="qwen3")
    assert "format" not in _chat_call_kwargs(conn)


def test_native_not_applied_for_row_type_info() -> None:
    """A RowTypeInfo schema has no native translation, so no ``format`` is derived."""
    conn = _connection()
    row_type = Types.ROW_NAMED(["name"], [Types.STRING()])
    conn.chat(
        _messages(), model="qwen3", output_schema=OutputSchema(output_schema=row_type)
    )
    assert "format" not in _chat_call_kwargs(conn)


def test_schema_is_model_json_schema() -> None:
    """The payload is pydantic's schema verbatim, ``$defs`` and all.

    A nested schema is used because it is the shape a hand-rolled translation
    diverges on: inlining or renaming a ``$defs`` entry breaks the ``$ref`` targets
    the server resolves when it builds the grammar.
    """
    conn = _connection()
    conn.chat(
        _messages(), model="qwen3", output_schema=OutputSchema(output_schema=Company)
    )
    assert _chat_call_kwargs(conn)["format"] == Company.model_json_schema()


def test_schema_not_passed_as_sampling_option() -> None:
    """The schema never reaches ``options``, which the server reads as sampling options.

    A schema written into the forwarded kwargs would arrive there instead of in
    ``format``, so the server would apply no grammar and report no error.
    """
    conn = _connection()
    conn.chat(
        _messages(), model="qwen3", output_schema=OutputSchema(output_schema=Person)
    )
    options = _chat_call_kwargs(conn)["options"]
    assert "format" not in options
    assert Person.model_json_schema() not in options.values()


@pytest.mark.parametrize(
    "model",
    ["qwen3", "llama3.2", "gemma3:270m", "mistral", "an-unknown-model", "", None],
)
def test_supports_native_structured_output(model: str | None) -> None:
    """Capability is reported for every model, including an absent model name.

    The capability is the server's, not the model's, so there is no model name it
    can be keyed on and none it should report not-capable for.
    """
    conn = OllamaChatModelConnection()
    assert conn.supports_native_structured_output(model) is True


def test_schema_accepted_not_rejected() -> None:
    """A schema with no native translation is answered, not refused.

    Rejecting is what a connection without native structured output does. This one
    has it, so a schema form it cannot translate natively leaves the request
    unconstrained rather than raising here. What governs the response then is the
    caller's configured strategy: the prompt engineering it already applied under
    ``AUTO`` or ``PROMPT``, and under a forced ``NATIVE`` a raise at the gate before
    this call is built.
    """
    conn = _connection()
    row_type = Types.ROW_NAMED(["name"], [Types.STRING()])
    response = conn.chat(
        _messages(), model="qwen3", output_schema=OutputSchema(output_schema=row_type)
    )
    assert response.content == "ok"


@pytest.mark.parametrize(
    "model_kwargs",
    [{"model": "qwen3"}, {"model": ""}],
    ids=["named", "blank"],
)
def test_effective_model_for_names_the_model_the_request_issues(
    model_kwargs: Dict[str, Any],
) -> None:
    """The hook names exactly the model the request is issued against.

    The hook duplicates the builder's own resolution rather than centralizing it, so
    the two can drift. The branch no longer consults the capability predicate, so the
    binding is taken against the model the request itself carries: were they to
    diverge, the gate would judge one model while the call went to another.

    Only parameter maps that name a model are exercised: this builder pops ``model``
    with no fallback, so an absent one is a request that cannot be built rather than a
    disagreement about which model to judge.
    """
    conn = _connection()

    named = conn.effective_model_for(model_kwargs)
    conn.chat(
        _messages(),
        output_schema=OutputSchema(output_schema=Person),
        **model_kwargs,
    )

    assert _chat_call_kwargs(conn)["model"] == named


def test_effective_model_for_resolves_nothing_without_a_model_param() -> None:
    """A parameter map naming no model resolves to nothing rather than raising.

    The request builder refuses that map, but the hook answers for whatever it is
    given, and the capability predicate accepts ``None`` without raising.
    """
    assert _connection().effective_model_for({}) is None


def _add(a: int, b: int) -> int:
    """Add two integers.

    Parameters
    ----------
    a : int
        first
    b : int
        second

    Returns:
    -------
    int
        sum
    """
    return a + b


def _query_recording_connection() -> tuple[OllamaChatModelConnection, List[bool]]:
    """A connection recording what the feasibility query answered on each request.

    Subclassing keeps the query itself under test rather than standing a stub in for
    it: the override notes the answer it gave and delegates to the real one.
    """
    answers: List[bool] = []

    class _RecordingConnection(OllamaChatModelConnection):
        def can_apply_native_structured_output(
            self,
            output_schema: OutputSchema | None,
            tools: List[Tool] | None,
            model_kwargs: Mapping[str, Any] | None,
        ) -> bool:
            answer = super().can_apply_native_structured_output(
                output_schema, tools, model_kwargs
            )
            answers.append(answer)
            return answer

    conn = _RecordingConnection()
    response = MagicMock()
    response.message.role = "assistant"
    response.message.content = "ok"
    response.message.tool_calls = None
    response.prompt_eval_count = 1
    response.eval_count = 2
    mock_client = MagicMock()
    mock_client.chat.return_value = response
    conn._OllamaChatModelConnection__client = mock_client
    return conn, answers


def test_feasibility_query_agrees_with_the_native_branch() -> None:
    """The answer matches whether the request ends up carrying a native ``format``.

    Comparing the answer against what the request carries, rather than against a
    literal, is what keeps the query and the branch from drifting in step. This
    connection reports every model capable, so the schema form is the only thing that
    moves. Clearing the record per case makes the single-element comparison an
    assertion that the query was reached exactly once on that request, too.
    """
    conn, answers = _query_recording_connection()
    tool = FunctionTool(func=PythonFunction.from_callable(_add))
    row_type = Types.ROW_NAMED(["name"], [Types.STRING()])

    for schema in (
        OutputSchema(output_schema=Person),
        OutputSchema(output_schema=row_type),
        None,
    ):
        for tools in (None, [], [tool]):
            answers.clear()

            conn.chat(_messages(), tools=tools, model="qwen3", output_schema=schema)

            carried = "format" in _chat_call_kwargs(conn)
            assert answers == [carried], f"schema {schema}, tools {tools}"


def _incapable_connection() -> OllamaChatModelConnection:
    """A connection reporting every model incapable, with its client mocked.

    This connection reports every model capable, so overriding the predicate is the
    only way to reach the incapable case at all.
    """

    class _IncapableConnection(OllamaChatModelConnection):
        def supports_native_structured_output(
            self, effective_model: str | None
        ) -> bool:
            return False

    conn = _IncapableConnection()
    response = MagicMock()
    response.message.role = "assistant"
    response.message.content = "ok"
    response.message.tool_calls = None
    response.prompt_eval_count = 1
    response.eval_count = 2
    mock_client = MagicMock()
    mock_client.chat.return_value = response
    conn._OllamaChatModelConnection__client = mock_client
    return conn


def test_feasibility_query_excludes_model_capability() -> None:
    """Feasibility is answered without consulting the capability predicate.

    This connection reports every model capable, so no model name can separate the two
    answers. A subclass that reports nothing capable can: the query must still answer
    ``True``, which fails the moment a capability conjunct is folded into the override.
    That folding is invisible to the binding test above, which moves both sides at once.
    """
    assert (
        _incapable_connection().can_apply_native_structured_output(
            OutputSchema(output_schema=Person), [], {"model": "qwen3"}
        )
        is True
    )


def test_schema_is_sent_when_the_model_is_reported_incapable() -> None:
    """A schema travels natively even where the connection reports the model incapable.

    The branch no longer consults the capability predicate, so the schema reaches the
    server and the server is what answers for it. Without this, nothing here would
    notice a capability conjunct being reintroduced.
    """
    conn = _incapable_connection()

    conn.chat(
        _messages(), model="qwen3", output_schema=OutputSchema(output_schema=Person)
    )

    assert _chat_call_kwargs(conn)["format"]["properties"].keys() == {"name", "age"}


def test_feasibility_query_is_asked_with_the_unstripped_kwargs() -> None:
    """The query sees the parameters as they arrived, not a copy ``chat`` has stripped.

    ``chat`` removes ``model`` from its own mapping before the native branch runs.
    Asked with that copy, an override reading it would answer about a request other
    than the one being built. No term of today's answer reads it, so this pins the
    shape rather than a live defect.
    """
    asked: List[Mapping[str, Any] | None] = []

    class _CapturingConnection(OllamaChatModelConnection):
        def can_apply_native_structured_output(
            self,
            output_schema: OutputSchema | None,
            tools: List[Tool] | None,
            model_kwargs: Mapping[str, Any] | None,
        ) -> bool:
            asked.append(model_kwargs)
            return super().can_apply_native_structured_output(
                output_schema, tools, model_kwargs
            )

    conn = _CapturingConnection()
    response = MagicMock()
    response.message.role = "assistant"
    response.message.content = "ok"
    response.message.tool_calls = None
    response.prompt_eval_count = 1
    response.eval_count = 2
    mock_client = MagicMock()
    mock_client.chat.return_value = response
    conn._OllamaChatModelConnection__client = mock_client

    conn.chat(
        _messages(),
        model="qwen3",
        temperature=0.5,
        output_schema=OutputSchema(output_schema=Person),
    )

    assert len(asked) == 1
    assert asked[0] is not None
    assert asked[0]["model"] == "qwen3"
    assert asked[0]["temperature"] == 0.5
