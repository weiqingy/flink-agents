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
from typing import Any

import pytest
from pydantic import BaseModel

from flink_agents.api.agents.types import OutputSchema
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.runtime.java.java_chat_model import (
    JavaChatModelSetupImpl,
    _to_java_chat_message,
)


class _JavaResourceAdapter:
    def __init__(self) -> None:
        self.arguments: tuple[Any, ...] | None = None
        self.result = object()

    def fromPythonChatMessage(self, *arguments: Any) -> Any:
        self.arguments = arguments
        return self.result


def test_to_java_chat_message_extracts_java_safe_fields() -> None:
    adapter = _JavaResourceAdapter()
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="hello",
        tool_calls=[
            {
                "id": 7,
                "type": "function",
                "function": {"name": "lookup", "arguments": "{}"},
            }
        ],
        extra_args={"reasoning": "brief"},
    )

    result = _to_java_chat_message(adapter, message)

    assert result is adapter.result
    assert adapter.arguments == (
        "assistant",
        "hello",
        [
            {
                "id": "7",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{}"},
            }
        ],
        {"reasoning": "brief"},
    )


class _Answer(BaseModel):
    """A representative output schema to offer the bridge."""

    text: str


class _JavaResource:
    """Stand-in for the wrapped Java setup; neither new member reaches it."""

    def open(self) -> None:
        return None


def _build_java_setup(strategy: str = "auto") -> JavaChatModelSetupImpl:
    return JavaChatModelSetupImpl(
        j_resource=_JavaResource(),
        j_resource_adapter=_JavaResourceAdapter(),
        connection="c",
        model="m",
        structured_output_strategy=strategy,
    )


@pytest.mark.parametrize("strategy", ["auto", "NATIVE"])
def test_java_setup_never_applies_native_structured_output(strategy: str) -> None:
    """The bridge answers false, whatever strategy the descriptor recorded.

    No call this setup issues can carry a schema, so for none of them should one
    travel natively. False rather than a refusal, because the answer decides whether
    the caller keeps describing the schema in the prompt, and keeping it is the
    outcome that works here. A recorded request for native structured output names
    nothing this side could apply it to, so the forced case answers the same.
    """
    setup = _build_java_setup(strategy)

    assert (
        setup.will_apply_native_structured_output(OutputSchema(output_schema=_Answer))
        is False
    )


def test_java_setup_refuses_a_schema_carrying_chat() -> None:
    """``chat_structured`` raises rather than dropping the schema and calling anyway.

    Refusing is what keeps an unconstrained response from being mistaken for a
    schema-conforming one. It differs from the gate on purpose: the gate's answer
    decides whether the caller keeps the prompt fallback, so raising there would fail
    a call the other side may well apply natively itself.
    """
    setup = _build_java_setup()

    with pytest.raises(NotImplementedError, match="output schema"):
        setup.chat_structured(
            [ChatMessage(role=MessageRole.USER, content="hi")],
            OutputSchema(output_schema=_Answer),
        )
