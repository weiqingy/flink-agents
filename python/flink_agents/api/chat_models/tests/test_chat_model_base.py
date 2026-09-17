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
from typing import Any, Dict, List, Mapping, Sequence

import pytest
from pydantic import BaseModel, Field, ValidationError
from pyflink.common.typeinfo import BasicTypeInfo, RowTypeInfo

from flink_agents.api.agents.types import OutputSchema
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.chat_models.chat_model import (
    BaseChatModelConnection,
    BaseChatModelSetup,
    StructuredOutputStrategy,
)
from flink_agents.api.prompts.prompt import Prompt
from flink_agents.api.tools.tool import Tool, ToolType


class _MinimalChatModelSetup(BaseChatModelSetup):
    """Minimal subclass that omits the `model` field declaration.

    Used to assert the `model` field is inherited from `BaseChatModelSetup`.
    """

    @property
    def model_kwargs(self) -> Dict[str, Any]:
        """Return chat model settings derived from the inherited `model` field."""
        return {"model": self.model}


class _Answer(BaseModel):
    """A representative BaseModel output schema."""

    text: str


class _StubTool(Tool):
    """Minimal tool stub; only its presence in the tools list matters."""

    @classmethod
    def tool_type(cls) -> ToolType:
        return ToolType.FUNCTION

    def call(self, *args: Any, **kwargs: Any) -> None:
        return None


class _RecordingConnection(BaseChatModelConnection):
    """Connection that captures the messages and kwargs it receives for inspection."""

    captured_messages: List[ChatMessage] = Field(default_factory=list)
    captured_kwargs: Dict[str, Any] = Field(default_factory=dict)
    captured_output_schema: OutputSchema | None = None

    def chat(
        self,
        messages: Sequence[ChatMessage],
        tools: List[Tool] | None = None,
        output_schema: OutputSchema | None = None,
        **kwargs: Any,
    ) -> ChatMessage:
        self.captured_messages = list(messages)
        self.captured_kwargs = dict(kwargs)
        self.captured_output_schema = output_schema
        return ChatMessage(role=MessageRole.ASSISTANT, content="ok")


class _RecordingChatModelSetup(BaseChatModelSetup):
    """Subclass that lets tests inject a connection without calling open()."""

    @property
    def model_kwargs(self) -> Dict[str, Any]:
        return {}


def _build_setup(
    prompt: Prompt,
) -> tuple[_RecordingChatModelSetup, _RecordingConnection]:
    setup = _RecordingChatModelSetup(connection="c", model="m", prompt=prompt)
    connection = _RecordingConnection()
    setup._resolved_connection = connection
    return setup, connection


def test_inherits_model_field_from_base() -> None:
    """A subclass that omits `model` still exposes it via inheritance."""
    setup = _MinimalChatModelSetup(connection="c", model="m1")
    assert setup.model == "m1"


def test_missing_model_raises_validation_error() -> None:
    """Constructing without `model` must raise a Pydantic ValidationError."""
    with pytest.raises(ValidationError):
        _MinimalChatModelSetup(connection="c")


def test_chat_fills_template_from_prompt_args_parameter() -> None:
    """chat() fills the prompt template from the `prompt_args` parameter."""
    prompt = Prompt.from_text(text="Task: {key}")
    setup, connection = _build_setup(prompt)

    setup.chat([], prompt_args={"key": "value"})

    assert len(connection.captured_messages) == 1
    assert connection.captured_messages[0].content == "Task: value"


def test_chat_does_not_read_template_vars_from_extra_args() -> None:
    """chat() must not read template variables from ChatMessage.extra_args."""
    prompt = Prompt.from_text(text="Task: {key}")
    setup, connection = _build_setup(prompt)

    user_message = ChatMessage(
        role=MessageRole.USER, content="hello", extra_args={"key": "value"}
    )
    setup.chat([user_message], prompt_args={})

    assert len(connection.captured_messages) == 2
    assert connection.captured_messages[0].content == "Task: {key}"
    assert connection.captured_messages[1].content == "hello"


def test_chat_refills_template_on_subsequent_invocations() -> None:
    """Each chat() invocation must re-fill the prompt template from the args."""
    prompt = Prompt.from_text(text="Task: {key}")
    setup, connection = _build_setup(prompt)

    setup.chat([], prompt_args={"key": "v1"})
    assert len(connection.captured_messages) == 1
    assert connection.captured_messages[0].content == "Task: v1"

    tool_response = ChatMessage(role=MessageRole.TOOL, content="tool result")
    setup.chat([tool_response], prompt_args={"key": "v1"})
    assert len(connection.captured_messages) == 2
    assert connection.captured_messages[0].content == "Task: v1"
    assert connection.captured_messages[1].content == "tool result"


def test_default_capability_predicate_is_false() -> None:
    """A connection reports no native structured output for any model by default."""
    connection = _RecordingConnection()

    assert connection.supports_native_structured_output("gpt-4o") is False
    assert connection.supports_native_structured_output("gpt-3.5-turbo") is False
    assert connection.supports_native_structured_output(None) is False


def test_output_schema_guard_rejects_a_schema() -> None:
    """The guard refuses a schema a connection cannot translate natively.

    Dropping it instead would return an unconstrained response that the caller has no
    way to tell apart from a schema-conforming one.
    """
    connection = _RecordingConnection()
    schema = OutputSchema(output_schema=_Answer)

    with pytest.raises(NotImplementedError, match="_RecordingConnection"):
        connection._reject_unsupported_output_schema(schema)


def test_output_schema_guard_passes_through_none() -> None:
    """A caller on the prompt-engineering fallback passes None and is let through."""
    connection = _RecordingConnection()

    assert connection._reject_unsupported_output_schema(None) is None


def test_setup_routes_output_schema_through_to_connection() -> None:
    """chat() forwards a caller's ``output_schema`` on to the connection intact.

    The setup filters what reaches the connection, so a schema it dropped or consumed
    would leave the connection unable to apply one at all. That the schema cannot land
    in ``**kwargs`` is a separate, tree-wide invariant covered by the connection
    signature guard.
    """
    setup = _RecordingChatModelSetup(connection="c", model="m")
    connection = _RecordingConnection()
    setup._resolved_connection = connection

    schema = OutputSchema(output_schema=_Answer)
    setup.chat([], output_schema=schema)

    assert connection.captured_output_schema is schema


def test_structured_output_strategy_defaults_to_auto() -> None:
    """The setup policy defaults to AUTO when unset."""
    setup = _RecordingChatModelSetup(connection="c", model="m")

    assert setup.structured_output_strategy is StructuredOutputStrategy.AUTO


@pytest.mark.parametrize("raw", ["NATIVE", "native", "Native"])
def test_structured_output_strategy_coerces_name_and_value_case_insensitively(
    raw: str,
) -> None:
    """The policy coerces from either its name or its value, in any case.

    Java serializes this enum as its name ("NATIVE") and its own resolver accepts any
    case, so a Python side that only accepted the lowercase value would reject what
    Java sends.
    """
    setup = _RecordingChatModelSetup(
        connection="c", model="m", structured_output_strategy=raw
    )

    assert setup.structured_output_strategy is StructuredOutputStrategy.NATIVE


def test_structured_output_strategy_normalizes_explicit_none_to_auto() -> None:
    """An explicitly null policy resolves to AUTO instead of being rejected.

    Java cannot distinguish a configuration that carries the key as null from one
    that omits it, and resolves both to AUTO, so a null arriving on Python must not
    fail validation or leave the attribute None.
    """
    setup = _RecordingChatModelSetup(
        connection="c", model="m", structured_output_strategy=None
    )

    assert setup.structured_output_strategy is StructuredOutputStrategy.AUTO


@pytest.mark.parametrize("raw", ["bogus", ""])
def test_structured_output_strategy_rejects_unrecognized_value(raw: str) -> None:
    """Only null normalizes to AUTO; every other unrecognized value still raises.

    An empty string reaches this field in practice from an empty YAML scalar or an
    unset environment substitution, and it must not be mistaken for an omitted value:
    normalizing on falsiness rather than on null would accept it as AUTO. A non-empty
    unrecognized name survives that same falsiness check, so it takes `"bogus"` to
    catch a resolver that coerces any unknown string to AUTO.
    """
    with pytest.raises(ValidationError):
        _RecordingChatModelSetup(
            connection="c", model="m", structured_output_strategy=raw
        )


def test_auto_strategy_resolves_to_native_only_when_capable() -> None:
    """AUTO defers to the model's capability."""
    assert StructuredOutputStrategy.AUTO.resolves_to_native(True) is True
    assert StructuredOutputStrategy.AUTO.resolves_to_native(False) is False


def test_native_strategy_forces_native_regardless_of_capability() -> None:
    """NATIVE resolves to native even when the model is not capable."""
    assert StructuredOutputStrategy.NATIVE.resolves_to_native(False) is True


def test_prompt_strategy_never_resolves_to_native() -> None:
    """PROMPT never resolves to native even when the model is capable."""
    assert StructuredOutputStrategy.PROMPT.resolves_to_native(True) is False


def test_connection_rejects_unrecognized_constructor_argument() -> None:
    """An unknown/misspelled constructor argument must raise, not be dropped.

    Regression test: BaseChatModelConnection previously inherited pydantic's
    default extra="ignore" behavior, so a caller who mistyped a config key (or
    passed a key that only exists on a different language's implementation)
    saw no error and no effect -- the value was silently discarded.
    """
    with pytest.raises(ValidationError, match="not_a_real_field"):
        _RecordingConnection(not_a_real_field="oops")


def test_setup_rejects_unrecognized_constructor_argument() -> None:
    """Same guarantee as the connection, for BaseChatModelSetup."""
    with pytest.raises(ValidationError, match="not_a_real_field"):
        _RecordingChatModelSetup(connection="c", model="m", not_a_real_field="oops")


def test_effective_model_for_reads_the_model_param() -> None:
    """The default effective model is the ``model`` parameter a request carries."""
    connection = _RecordingConnection()

    assert connection.effective_model_for({"model": "gpt-4o"}) == "gpt-4o"


def test_effective_model_for_returns_none_when_no_model_param() -> None:
    """A connection carrying no default of its own has no model to resolve.

    The capability predicate reports a ``None`` model not capable rather than raising,
    so answering ``None`` degrades to the prompt-engineering fallback.
    """
    connection = _RecordingConnection()

    assert connection.effective_model_for({}) is None
    assert connection.effective_model_for({"temperature": 0.5}) is None


def test_effective_model_for_returns_none_for_none_params() -> None:
    """No parameters at all resolve to no model, rather than raising."""
    connection = _RecordingConnection()

    assert connection.effective_model_for(None) is None


def test_effective_model_for_does_not_normalize_a_blank_model() -> None:
    """The hook answers with what it was given rather than validating it.

    Substituting a default for a blank model is what the connections carrying a
    default model do, and it belongs to their override because it is what their
    request builder does. Doing it here would invent a fallback for connections that
    have none.
    """
    connection = _RecordingConnection()

    assert connection.effective_model_for({"model": "   "}) == "   "


def test_effective_model_for_does_not_consume_the_params() -> None:
    """Reading the model leaves the mapping able to build the request it described.

    An override copying a request builder's ``pop`` idiom would hand that builder a
    map with the model already removed, so the contract is pinned on the default too.
    """
    connection = _RecordingConnection()
    model_kwargs = {"model": "gpt-4o", "temperature": 0.5}

    connection.effective_model_for(model_kwargs)

    assert model_kwargs == {"model": "gpt-4o", "temperature": 0.5}


def test_default_feasibility_predicate_is_false() -> None:
    """A connection reports no schema applicable to any request by default."""
    connection = _RecordingConnection()
    model_kwargs = {"model": "gpt-4o"}

    # Both forms an OutputSchema wraps: a BaseModel subclass, which a connection with
    # a native branch could translate, and a RowTypeInfo, which none translates.
    assert (
        connection.can_apply_native_structured_output(
            OutputSchema(output_schema=_Answer), [], model_kwargs
        )
        is False
    )
    assert (
        connection.can_apply_native_structured_output(
            OutputSchema(
                output_schema=RowTypeInfo(
                    field_types=[BasicTypeInfo.STRING_TYPE_INFO()],
                    field_names=["name"],
                )
            ),
            [],
            model_kwargs,
        )
        is False
    )


def test_feasibility_predicate_accepts_a_missing_schema_tools_and_kwargs() -> None:
    """A missing schema, missing tools or missing parameters must not raise.

    Each is an ordinary request to answer about rather than a misuse: an unconstrained
    request carries no schema, a request binding no tools may reach a builder as None
    rather than as an empty list, and a builder handed no parameters asks with the same
    None it was handed.
    """
    connection = _RecordingConnection()

    assert connection.can_apply_native_structured_output(None, None, None) is False
    assert (
        connection.can_apply_native_structured_output(
            OutputSchema(output_schema=_Answer), None, None
        )
        is False
    )


def test_feasibility_predicate_does_not_consume_the_model_kwargs() -> None:
    """Answering leaves the mapping able to build the request it answered about.

    An override copying a request builder's ``pop`` idiom would hand that builder a
    mapping with the key already removed, so the contract is pinned on the default too.
    """
    connection = _RecordingConnection()
    model_kwargs = {"model": "gpt-4o", "temperature": 0.5}

    connection.can_apply_native_structured_output(
        OutputSchema(output_schema=_Answer), [], model_kwargs
    )

    assert model_kwargs == {"model": "gpt-4o", "temperature": 0.5}


def test_feasibility_predicate_does_not_consume_the_tools() -> None:
    """The same tools go on to bind the request the answer was about.

    A connection whose native branch turns on whether any tool is bound would answer
    about one request and build another if answering emptied the list.
    """
    connection = _RecordingConnection()
    tool = _StubTool()
    tools = [tool]

    connection.can_apply_native_structured_output(
        OutputSchema(output_schema=_Answer), tools, {"model": "gpt-4o"}
    )

    assert tools == [tool]


class _GateConnection(_RecordingConnection):
    """Connection whose feasibility and capability answers are scripted.

    Records what it was asked and what a schema-carrying call carried. It extends
    ``_RecordingConnection`` rather than editing it, so the tests above keep
    asserting the inherited base answers.
    """

    feasible: bool = False
    capable: bool = False
    feasibility_asked: bool = False
    capability_asked: bool = False
    asked_schema: OutputSchema | None = None
    asked_tools: List[Tool] | None = None
    asked_model_kwargs: Dict[str, Any] | None = None
    asked_model: str | None = None
    captured_tools: List[Tool] | None = None

    def can_apply_native_structured_output(
        self,
        output_schema: OutputSchema | None,
        tools: List[Tool] | None,
        model_kwargs: Mapping[str, Any] | None,
    ) -> bool:
        """Answer the scripted feasibility, recording what it was asked about."""
        self.feasibility_asked = True
        self.asked_schema = output_schema
        self.asked_tools = None if tools is None else list(tools)
        self.asked_model_kwargs = None if model_kwargs is None else dict(model_kwargs)
        return self.feasible

    def effective_model_for(self, model_kwargs: Mapping[str, Any] | None) -> str | None:
        """Answer with a sentinel no parameter value can supply.

        Deliberately not the ``model`` parameter: a gate that read the mapping itself
        instead of asking this hook would still look correct against a connection
        inheriting the base body, which is exactly ``model_kwargs.get("model")``.
        """
        return "backing-model"

    def supports_native_structured_output(self, effective_model: str | None) -> bool:
        """Answer the scripted capability, recording the model it was asked about."""
        self.capability_asked = True
        self.asked_model = effective_model
        return self.capable

    def chat(
        self,
        messages: Sequence[ChatMessage],
        tools: List[Tool] | None = None,
        output_schema: OutputSchema | None = None,
        **kwargs: Any,
    ) -> ChatMessage:
        """Capture the tools alongside everything the base connection captures."""
        self.captured_tools = None if tools is None else list(tools)
        super().chat(messages, tools=tools, output_schema=output_schema, **kwargs)
        return ChatMessage(role=MessageRole.ASSISTANT, content="structured")


class _GateChatModelSetup(_RecordingChatModelSetup):
    """Setup whose ``model_kwargs`` a test can populate.

    ``_RecordingChatModelSetup`` returns an empty mapping from a property body, which
    cannot express the parameters a gate builds a call from. Subclassing leaves that
    fixture and its existing uses untouched.
    """

    parameters: Dict[str, Any] = Field(default_factory=dict)

    @property
    def model_kwargs(self) -> Dict[str, Any]:
        """Return a fresh copy, since callers merge per-call parameters into it."""
        return dict(self.parameters)


def _build_gate(
    *,
    feasible: bool = True,
    capable: bool = True,
    strategy: StructuredOutputStrategy = StructuredOutputStrategy.AUTO,
    prompt: Prompt | None = None,
) -> tuple[_GateChatModelSetup, _GateConnection]:
    setup = _GateChatModelSetup(
        connection="c",
        model="m",
        prompt=prompt,
        structured_output_strategy=strategy,
    )
    connection = _GateConnection(feasible=feasible, capable=capable)
    setup._resolved_connection = connection
    return setup, connection


def _pojo_schema() -> OutputSchema:
    """A schema form a connection with a native branch can translate."""
    return OutputSchema(output_schema=_Answer)


def _row_schema() -> OutputSchema:
    """A schema form no connection translates natively."""
    return OutputSchema(
        output_schema=RowTypeInfo(
            field_types=[BasicTypeInfo.STRING_TYPE_INFO()], field_names=["name"]
        )
    )


@pytest.mark.parametrize(
    ("row", "strategy", "schema_factory", "feasible", "capable", "expected"),
    [
        (1, StructuredOutputStrategy.AUTO, _pojo_schema, True, True, True),
        (2, StructuredOutputStrategy.AUTO, _pojo_schema, True, False, False),
        (3, StructuredOutputStrategy.AUTO, _pojo_schema, False, True, False),
        (4, StructuredOutputStrategy.AUTO, _row_schema, False, True, False),
        (5, StructuredOutputStrategy.AUTO, _row_schema, False, False, False),
        (6, StructuredOutputStrategy.PROMPT, _pojo_schema, True, True, False),
        (7, StructuredOutputStrategy.NATIVE, _pojo_schema, True, True, True),
        (8, StructuredOutputStrategy.NATIVE, _pojo_schema, True, False, True),
    ],
)
def test_will_apply_native_combines_policy_capability_and_feasibility(
    row: int,
    strategy: StructuredOutputStrategy,
    schema_factory: Any,
    feasible: bool,
    capable: bool,
    expected: bool,
) -> None:
    """Policy, capability and feasibility compose, one case per behavior-table row.

    A ``BaseModel`` subclass stands for a form a connection can translate and the
    ``RowTypeInfo`` wrapper for one none of them can.
    """
    setup, connection = _build_gate(
        feasible=feasible, capable=capable, strategy=strategy
    )
    setup.parameters["model"] = "gpt-4o"
    # Bound so the empty-tools assertion below compares against something. A ReAct
    # agent always binds tools, so a gate that asked feasibility with them would make
    # a connection that skips a native schema on a tool-carrying request report every
    # such request infeasible, and native structured output would never fire there.
    setup.tools.append(_StubTool())
    schema = schema_factory()

    assert setup.will_apply_native_structured_output(schema) is expected, (
        f"behavior table row {row}"
    )

    # Asked about the schema as handed over, and about a call binding no tools with
    # the parameters the setup would build the call from: the shape chat_structured
    # sends.
    assert connection.asked_schema is schema
    assert connection.asked_tools == []
    assert connection.asked_model_kwargs == {"model": "gpt-4o"}


def test_will_apply_native_asks_capability_about_the_effective_model() -> None:
    """Capability is asked about the model the connection's own hook names."""
    setup, connection = _build_gate()
    setup.parameters["model"] = "gpt-4o"
    setup.model = "a-deployment-name"

    assert setup.will_apply_native_structured_output(_pojo_schema()) is True

    # Three identities are kept distinct on purpose: the configured
    # "a-deployment-name", the "gpt-4o" in the parameters, and what the connection's
    # own hook returns. On a deployment-based provider capability belongs to the model
    # behind the deployment, so a gate reading either of the first two misclassifies
    # it in both directions.
    assert connection.asked_model == "backing-model"


def test_will_apply_native_does_not_consult_capability_for_an_infeasible_schema() -> (
    None
):
    """A schema no request can express is never put to the capability predicate.

    Feasibility is asked first, so a predicate answering without consulting the
    schema is never asked about a form its own connection has no translation for.
    """
    setup, connection = _build_gate(feasible=False, capable=True)

    assert setup.will_apply_native_structured_output(_row_schema()) is False

    assert connection.feasibility_asked is True
    assert connection.capability_asked is False


def test_will_apply_native_is_false_for_a_missing_schema() -> None:
    """A call carrying no schema is never a native one, whatever the policy.

    There is nothing to apply, so neither question arises and a forced NATIVE has
    nothing to fail fast about.
    """
    setup, connection = _build_gate(strategy=StructuredOutputStrategy.NATIVE)

    assert setup.will_apply_native_structured_output(None) is False

    assert connection.feasibility_asked is False
    assert connection.capability_asked is False


def test_will_apply_native_raises_for_native_policy_on_an_infeasible_schema() -> None:
    """A forced NATIVE fails fast on a schema the connection cannot apply at all.

    No provider error could report it, because no request expresses the schema in the
    first place, so the message carries both sides of the mismatch itself.
    """
    setup, _ = _build_gate(
        feasible=False, capable=True, strategy=StructuredOutputStrategy.NATIVE
    )
    wrapped = _row_schema()

    with pytest.raises(ValueError) as wrapped_error:
        setup.will_apply_native_structured_output(wrapped)
    with pytest.raises(ValueError) as pojo_error:
        setup.will_apply_native_structured_output(_pojo_schema())

    message = str(wrapped_error.value)
    assert "_GateConnection" in message
    # The wrapped shape rather than the wrapper: a user who configured a Row schema
    # learns nothing from a message that renders the same wrapper for every one.
    assert str(wrapped.output_schema) in message
    assert "OutputSchema" not in message
    assert f"{_Answer.__module__}.{_Answer.__qualname__}" in str(pojo_error.value)


def test_native_policy_on_an_incapable_model_sends_the_schema() -> None:
    """A forced NATIVE on an incapable model sends the schema rather than withholding.

    The schema travels with no second capability test at this level, so an explicit
    intent reaches the provider and a provider error is what answers it.
    """
    setup, connection = _build_gate(
        feasible=True, capable=False, strategy=StructuredOutputStrategy.NATIVE
    )
    schema = _pojo_schema()

    assert setup.will_apply_native_structured_output(schema) is True

    setup.chat_structured([ChatMessage(role=MessageRole.USER, content="hi")], schema)

    assert connection.captured_output_schema is schema


def test_chat_structured_sends_no_tools_and_does_not_prepend_the_bound_prompt() -> None:
    """The schema is the request's only description of the shape of the answer.

    Bound tools make some providers drop a native schema outright, and ``chat``
    prepends the bound prompt on the prompt alone, so a second pass over messages
    that already came through it would repeat that prompt.
    """
    setup, connection = _build_gate(prompt=Prompt.from_text(text="Task: {key}"))
    setup.tools.append(_StubTool())
    schema = _pojo_schema()

    response = setup.chat_structured(
        [ChatMessage(role=MessageRole.USER, content="hi")], schema
    )

    assert response.content == "structured"
    assert connection.captured_tools == []
    assert len(connection.captured_messages) == 1
    assert connection.captured_messages[0].content == "hi"
    assert connection.captured_output_schema is schema


def test_chat_structured_merges_per_call_parameters_over_the_setup_parameters() -> None:
    """Per-call parameters merge over the setup's own, exactly as ``chat`` merges."""
    setup, connection = _build_gate()
    setup.parameters["model"] = "gpt-4o"
    setup.parameters["temperature"] = 0.1
    messages = [ChatMessage(role=MessageRole.USER, content="hi")]
    schema = _pojo_schema()

    setup.chat_structured(messages, schema, temperature=0.9)
    assert connection.captured_kwargs == {"model": "gpt-4o", "temperature": 0.9}

    # A caller with nothing to override passes no per-call parameters at all.
    setup.chat_structured(messages, schema)
    assert connection.captured_kwargs == {"model": "gpt-4o", "temperature": 0.1}


def test_chat_structured_refuses_a_missing_schema() -> None:
    """Refuses rather than issuing an ordinary call.

    A connection reads a missing schema as an unconstrained request, so without the
    check the caller would receive an ordinary response from the one method whose
    purpose is to carry a schema.
    """
    setup, connection = _build_gate()

    with pytest.raises(TypeError, match="chat_structured"):
        setup.chat_structured([ChatMessage(role=MessageRole.USER, content="hi")], None)

    assert connection.captured_output_schema is None
    assert connection.captured_messages == []


def test_will_apply_native_requires_a_resolved_connection() -> None:
    """The gate has nothing to ask until open() has resolved the connection."""
    setup = _GateChatModelSetup(connection="c", model="m")

    with pytest.raises(TypeError, match="has not been resolved"):
        setup.will_apply_native_structured_output(_pojo_schema())


def test_chat_structured_requires_a_resolved_connection() -> None:
    """The same precondition as every other call this setup issues."""
    setup = _GateChatModelSetup(connection="c", model="m")

    with pytest.raises(TypeError, match="has not been resolved"):
        setup.chat_structured(
            [ChatMessage(role=MessageRole.USER, content="hi")], _pojo_schema()
        )
