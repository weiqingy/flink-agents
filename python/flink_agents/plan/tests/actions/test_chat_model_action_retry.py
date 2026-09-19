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
"""Tests for retry behavior in chat_model_action."""

import asyncio
import time
from typing import Any, Dict, List, Sequence
from unittest.mock import MagicMock, call
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from flink_agents.api.agents.agent import STRUCTURED_OUTPUT
from flink_agents.api.agents.react_agent import OutputSchema
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.chat_models.chat_model import (
    BaseChatModelConnection,
    BaseChatModelSetup,
)
from flink_agents.api.core_options import (
    AgentExecutionOptions,
    ErrorHandlingStrategy,
)
from flink_agents.api.events.chat_event import ChatResponseEvent
from flink_agents.api.events.tool_event import ToolRequestEvent, ToolResponseEvent
from flink_agents.api.metric_group import Counter, MetricGroup
from flink_agents.api.prompts.prompt import Prompt
from flink_agents.api.tools.tool import Tool, ToolType
from flink_agents.api.trace import (
    ExecutionEntityTypes,
    ExecutionProblemCategories,
    ExecutionReporter,
    LLMExecutionMetadataKeys,
)
from flink_agents.plan.actions.chat_model_action import (
    chat,
    process_chat_request_or_tool_response,
)

_LLM_METADATA = {LLMExecutionMetadataKeys.MODEL: "configured-model"}

# ============================================================================
# Mock infrastructure
# ============================================================================


class _MockCounter(Counter):
    """Mock counter that tracks inc calls."""

    def __init__(self) -> None:
        self._count = 0

    def inc(self, n: int = 1) -> None:
        self._count += n

    def dec(self, n: int = 1) -> None:
        self._count -= n

    def get_count(self) -> int:
        return self._count


class _MockMetricGroup(MetricGroup):
    """Mock metric group that tracks sub-groups and counters."""

    def __init__(self) -> None:
        self._sub_groups: dict[str, _MockMetricGroup] = {}
        self._counters: dict[str, _MockCounter] = {}

    def get_sub_group(self, name: str, value: str | None = None) -> "_MockMetricGroup":
        key = f"{name}={value}" if value is not None else name
        if key not in self._sub_groups:
            self._sub_groups[key] = _MockMetricGroup()
        return self._sub_groups[key]

    def get_counter(self, name: str) -> _MockCounter:
        if name not in self._counters:
            self._counters[name] = _MockCounter()
        return self._counters[name]

    def get_meter(self, name: str) -> Any:
        return MagicMock()

    def get_gauge(self, name: str) -> Any:
        return MagicMock()

    def get_histogram(self, name: str, window_size: int = 100) -> Any:
        return MagicMock()


class _MockMemoryObject:
    """Simple dict-backed memory object for testing."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    def get(self, path: str) -> Any:
        return self._store.get(path)

    def set(self, path: str, value: Any) -> None:
        self._store[path] = value


class _StructuredResult(BaseModel):
    result: int


def _create_mock_runner_context(
    chat_model: Any,
    max_retries: int = 3,
    retry_wait_interval_sec: int = 1,
    error_handling_strategy: ErrorHandlingStrategy = ErrorHandlingStrategy.RETRY,
    *,
    chat_async: bool = False,
) -> tuple[MagicMock, list, _MockMetricGroup, _MockMemoryObject]:
    """Create a mock RunnerContext with configurable retry settings.

    ``chat_async`` selects which durable seam the action uses. It defaults to False
    here, unlike the production default, so that a test opts in to the async seam
    deliberately; both seams are wired either way.

    Returns (ctx, sent_events, action_metric_group, sensory_memory).
    """
    sent_events = []
    metric_group = _MockMetricGroup()
    sensory_memory = _MockMemoryObject()
    chat_model.model = "configured-model"
    # Stated rather than left to the mock default. An unstubbed MagicMock attribute
    # answers this gate with a truthy mock, which would send every schema-carrying
    # test down the native finalization path, resolve chat_structured to another
    # auto-created mock, and fail far from the cause. Tests that want the native
    # path override this after this helper returns.
    if isinstance(chat_model, MagicMock):
        chat_model.will_apply_native_structured_output = MagicMock(return_value=False)

    config = MagicMock()
    option_values = {
        id(AgentExecutionOptions.ERROR_HANDLING_STRATEGY): error_handling_strategy,
        id(AgentExecutionOptions.MAX_RETRIES): max_retries,
        id(AgentExecutionOptions.RETRY_WAIT_INTERVAL): retry_wait_interval_sec,
        id(AgentExecutionOptions.CHAT_ASYNC): chat_async,
    }
    config.get = MagicMock(
        side_effect=lambda option: option_values.get(
            id(option), option.get_default_value()
        )
    )

    ctx = MagicMock(spec=ExecutionReporter)
    ctx.config = config
    ctx.sensory_memory = sensory_memory
    ctx.action_metric_group = metric_group
    ctx.send_event = MagicMock(side_effect=lambda e: sent_events.append(e))
    ctx.get_resource = MagicMock(return_value=chat_model)
    ctx.durable_execute = MagicMock(
        side_effect=lambda fn, *args, **kwargs: fn(*args, **kwargs)
    )

    async def _dispatch_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    ctx.durable_execute_async = MagicMock(side_effect=_dispatch_async)

    return ctx, sent_events, metric_group, sensory_memory


# ============================================================================
# Tests
# ============================================================================


class TestChatModelActionRetry:
    """Tests for retry behavior in chat()."""

    def test_chat_succeeds_without_retry(self) -> None:
        """No retry needed: retry_count=0, total_retry_wait_sec=0, no metrics."""
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(role=MessageRole.ASSISTANT, content="hello")
        )

        ctx, sent_events, metric_group, _ = _create_mock_runner_context(chat_model)
        request_id = uuid4()

        asyncio.run(
            chat(
                request_id,
                chat_model.connection,
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                None,
                ctx,
            )
        )

        assert len(sent_events) == 1
        event = sent_events[0]
        assert isinstance(event, ChatResponseEvent)
        assert event.retry_count == 0
        assert event.total_retry_wait_sec == 0

        # No retry metrics should be recorded
        assert len(metric_group._sub_groups) == 0
        ctx.report_execution_started.assert_called_once_with(
            ExecutionEntityTypes.LLM,
            chat_model.connection,
            _LLM_METADATA,
        )
        ctx.report_execution_succeeded.assert_called_once_with(
            ExecutionEntityTypes.LLM,
            chat_model.connection,
            _LLM_METADATA,
        )
        ctx.report_execution_failed.assert_not_called()

    def test_chat_retries_with_exponential_backoff(self) -> None:
        """Fail once then succeed: 1s interval, 1 retry -> wait 1s (1 * 2^0)."""
        call_count = 0

        def mock_chat(messages: Sequence[ChatMessage], **kwargs: Any) -> ChatMessage:
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                err_msg = "transient error"
                raise RuntimeError(err_msg)
            return ChatMessage(role=MessageRole.ASSISTANT, content="success")

        chat_model = MagicMock()
        chat_model.chat = mock_chat

        ctx, sent_events, metric_group, _ = _create_mock_runner_context(
            chat_model, max_retries=3, retry_wait_interval_sec=1
        )
        request_id = uuid4()

        start = time.monotonic()
        asyncio.run(
            chat(
                request_id,
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                None,
                ctx,
            )
        )
        elapsed = time.monotonic() - start

        assert len(sent_events) == 1
        event = sent_events[0]
        assert isinstance(event, ChatResponseEvent)
        assert event.retry_count == 1
        # 1s config. Exponential: 1s (2^0) = 1s total
        assert event.total_retry_wait_sec == 1
        assert elapsed >= 1.0

        # Verify metrics recorded under connection name
        model_group = metric_group.get_sub_group("model", chat_model.connection)
        assert model_group.get_counter("retryCount").get_count() == 1
        assert model_group.get_counter("retryWaitSec").get_count() == 1
        assert ctx.report_execution_started.call_count == 2
        ctx.report_execution_failed.assert_called_once()
        failed_args = ctx.report_execution_failed.call_args.args
        assert failed_args[0] == ExecutionEntityTypes.LLM
        assert failed_args[2] == _LLM_METADATA
        assert failed_args[-1] == ExecutionProblemCategories.MODEL_CALL_FAILED
        ctx.report_execution_succeeded.assert_called_once_with(
            ExecutionEntityTypes.LLM,
            "test-model",
            _LLM_METADATA,
        )

    def test_chat_exhausts_retries_and_raises(self) -> None:
        """All retries exhausted: exception raised, no event sent."""
        chat_model = MagicMock()
        chat_model.chat = MagicMock(side_effect=RuntimeError("persistent error"))

        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=2, retry_wait_interval_sec=0
        )
        request_id = uuid4()

        with pytest.raises(RuntimeError, match="persistent error"):
            asyncio.run(
                chat(
                    request_id,
                    "test-model",
                    [ChatMessage(role=MessageRole.USER, content="hi")],
                    {},
                    None,
                    ctx,
                )
            )

        assert len(sent_events) == 0
        assert ctx.report_execution_started.call_count == 3
        assert ctx.report_execution_failed.call_count == 3
        for failed_call in ctx.report_execution_failed.call_args_list:
            assert failed_call.args[0] == ExecutionEntityTypes.LLM
            assert failed_call.args[2] == _LLM_METADATA
            assert failed_call.args[-1] == ExecutionProblemCategories.MODEL_CALL_FAILED
        ctx.report_execution_succeeded.assert_not_called()

    def test_structured_output_parse_error_retries_without_failing_llm(
        self,
    ) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            side_effect=[
                ChatMessage(role=MessageRole.ASSISTANT, content="not-json"),
                ChatMessage(role=MessageRole.ASSISTANT, content='{"result": 42}'),
            ]
        )

        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=1, retry_wait_interval_sec=0
        )

        asyncio.run(
            chat(
                uuid4(),
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                OutputSchema(output_schema=_StructuredResult),
                ctx,
            )
        )

        assert chat_model.chat.call_count == 2
        assert len(sent_events) == 1
        response = sent_events[0].response
        assert response.extra_args[STRUCTURED_OUTPUT].result == 42

        ctx.report_execution_failed.assert_called_once()
        failed_args = ctx.report_execution_failed.call_args.args
        assert failed_args[0] == ExecutionEntityTypes.PARSER
        assert failed_args[1] == STRUCTURED_OUTPUT
        assert failed_args[-1] == ExecutionProblemCategories.MODEL_OUTPUT_PARSE_ERROR

        assert ctx.report_execution_started.call_count == 4
        assert ctx.report_execution_succeeded.call_count == 3
        assert (
            ctx.report_execution_succeeded.call_args_list.count(
                call(
                    ExecutionEntityTypes.LLM,
                    "test-model",
                    _LLM_METADATA,
                )
            )
            == 2
        )


class TestChatModelActionFinishReason:
    """Tests for the finish-reason gate on the common chat-response path."""

    def _run(self, ctx, output_schema=None) -> None:
        asyncio.run(
            chat(
                uuid4(),
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                output_schema,
                ctx,
            )
        )

    def test_truncated_text_response_rejected(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="partial answ",
                extra_args={"finish_reason": "length"},
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        with pytest.raises(ValueError, match="(?i)truncat") as exc_info:
            self._run(ctx)

        assert "token" in str(exc_info.value).lower()
        assert len(sent_events) == 0

    def test_content_filtered_text_response_rejected(self) -> None:
        # Matches a word unique to the filtering message. Both messages
        # interpolate the finish reason, so "content_filter" appears in either
        # one and cannot tell them apart.
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="",
                extra_args={"finish_reason": "content_filter"},
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        with pytest.raises(ValueError, match="(?i)withheld"):
            self._run(ctx)

        assert len(sent_events) == 0

    def test_truncated_tool_call_response_rejected_before_tool_dispatch(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="",
                tool_calls=[
                    {
                        "id": "call-1",
                        "function": {"name": "f", "arguments": ""},
                    }
                ],
                extra_args={"finish_reason": "length"},
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        with pytest.raises(ValueError, match="(?i)truncat"):
            self._run(ctx)

        # A truncated tool call carries arguments the model never finished
        # writing, so no ToolRequestEvent may leave the action.
        assert len(sent_events) == 0

    @pytest.mark.parametrize(
        "extra_args",
        [
            {"finish_reason": "stop"},
            {"finish_reason": "tool_calls"},
            {"finish_reason": "some_vendor_reason"},
            {},
        ],
        ids=["stop", "tool_calls", "unrecognized", "absent"],
    )
    def test_accepted_finish_reason_reaches_the_response_event(
        self, extra_args: dict
    ) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="hello",
                extra_args=extra_args,
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        self._run(ctx)

        assert len(sent_events) == 1
        assert isinstance(sent_events[0], ChatResponseEvent)
        assert sent_events[0].response.content == "hello"

    def test_accepted_finish_reason_dispatches_tool_request_event(self) -> None:
        # A response carrying tool calls passes the same finish-reason gate as a
        # text response, so an accepted reason must reach tool dispatch.
        tool_calls = [{"id": "call-1", "function": {"name": "f", "arguments": {}}}]
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="",
                tool_calls=tool_calls,
                extra_args={"finish_reason": "tool_calls"},
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        self._run(ctx)

        assert len(sent_events) == 1
        assert isinstance(sent_events[0], ToolRequestEvent)
        assert sent_events[0].tool_calls == tool_calls

    def test_ignore_strategy_drops_rejected_response_without_event(self) -> None:
        # Under IGNORE the record is dropped: the rejection does not propagate
        # and no event carries the truncated content downstream.
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="partial answ",
                extra_args={"finish_reason": "length"},
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model,
            max_retries=0,
            retry_wait_interval_sec=0,
            error_handling_strategy=ErrorHandlingStrategy.IGNORE,
        )

        self._run(ctx)

        assert len(sent_events) == 0

    @pytest.mark.parametrize("finish_reason", ["length", "content_filter"])
    def test_rejected_finish_reason_skips_structured_output(
        self, finish_reason: str
    ) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content='{"result": 42}',
                extra_args={
                    "finish_reason": finish_reason,
                    "model_name": "provider-model",
                    "promptTokens": 100,
                    "completionTokens": 50,
                },
            )
        )
        ctx, sent_events, metric_group, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        with pytest.raises(ValueError):
            self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        # The model call itself succeeded and spent its full token budget, so
        # both must be recorded before the response is rejected.
        ctx.report_execution_succeeded.assert_called_once_with(
            ExecutionEntityTypes.LLM, "test-model", _LLM_METADATA
        )
        chat_model._record_token_metrics.assert_called_once_with(
            "provider-model", 100, 50, metric_group
        )
        # The parse is never attempted, so nothing about it is reported and no
        # response leaves the action.
        ctx.report_execution_started.assert_called_once_with(
            ExecutionEntityTypes.LLM, "test-model", _LLM_METADATA
        )
        ctx.report_execution_failed.assert_not_called()
        assert len(sent_events) == 0


class TestChatResponseEventRetryFields:
    """Tests for ChatResponseEvent retry fields."""

    def test_default_retry_fields(self) -> None:
        """Default construction has retry_count=0, total_retry_wait_sec=0."""
        event = ChatResponseEvent(
            request_id=uuid4(),
            response=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
        )
        assert event.retry_count == 0
        assert event.total_retry_wait_sec == 0

    def test_with_retry_fields(self) -> None:
        """Full construction carries retry info."""
        event = ChatResponseEvent(
            request_id=uuid4(),
            response=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
            retry_count=5,
            total_retry_wait_sec=31,
        )
        assert event.retry_count == 5
        assert event.total_retry_wait_sec == 31


class TestRetryWaitIntervalConfig:
    """Tests for RETRY_WAIT_INTERVAL configuration."""

    def test_default_value(self) -> None:
        """Default value is 1 second."""
        assert AgentExecutionOptions.RETRY_WAIT_INTERVAL.get_default_value() == 1


class TestProcessToolResponsePromptArgsForwarding:
    """Locks the contract that `_process_tool_response` forwards the saved
    `prompt_args` from the tool-request-event context into the round-2 call
    to `chat_model.chat(...)`.
    """

    def test_forwards_saved_prompt_args_to_chat(self) -> None:
        initial_request_id = uuid4()
        tool_request_event_id = uuid4()
        tool_call_id = "call-1"
        saved_prompt_args = {"k": "v"}

        captured_prompt_args: list[dict] = []

        def mock_chat(messages: Sequence[ChatMessage], **kwargs: Any) -> ChatMessage:
            captured_prompt_args.append(kwargs.get("prompt_args"))
            return ChatMessage(role=MessageRole.ASSISTANT, content="done")

        chat_model = MagicMock()
        chat_model.chat = mock_chat

        ctx, sent_events, _, sensory_memory = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        # Pre-seed the tool-request-event context with saved prompt args so
        # _process_tool_response can look them up.
        sensory_memory.set(
            "_TOOL_REQUEST_EVENT_CONTEXT",
            {
                str(tool_request_event_id): {
                    "initial_request_id": str(initial_request_id),
                    "model": "test-model",
                    "prompt_args": saved_prompt_args,
                    "output_schema": None,
                }
            },
        )

        # Pre-seed the tool-call context with prior messages so
        # _update_tool_call_context can extend them with the tool response.
        sensory_memory.set(
            "_TOOL_CALL_CONTEXT",
            {
                str(initial_request_id): [
                    ChatMessage(role=MessageRole.USER, content="hi").model_dump(
                        mode="json"
                    )
                ]
            },
        )

        tool_response_event = ToolResponseEvent(
            request_id=tool_request_event_id,
            responses={tool_call_id: "42"},
            external_ids={},
        )

        asyncio.run(process_chat_request_or_tool_response(tool_response_event, ctx))

        assert len(captured_prompt_args) == 1
        assert captured_prompt_args[0] == saved_prompt_args
        assert len(sent_events) == 1
        assert isinstance(sent_events[0], ChatResponseEvent)

    def test_failed_tool_response_uses_generic_response_message(self) -> None:
        initial_request_id = uuid4()
        tool_request_event_id = uuid4()
        tool_call_id = "call-1"

        captured_messages: list[Sequence[ChatMessage]] = []

        def mock_chat(messages: Sequence[ChatMessage], **kwargs: Any) -> ChatMessage:
            captured_messages.append(messages)
            return ChatMessage(role=MessageRole.ASSISTANT, content="done")

        chat_model = MagicMock()
        chat_model.chat = mock_chat

        ctx, _, _, sensory_memory = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )
        sensory_memory.set(
            "_TOOL_REQUEST_EVENT_CONTEXT",
            {
                str(tool_request_event_id): {
                    "initial_request_id": str(initial_request_id),
                    "model": "test-model",
                    "prompt_args": {},
                    "output_schema": None,
                }
            },
        )
        sensory_memory.set(
            "_TOOL_CALL_CONTEXT",
            {
                str(initial_request_id): [
                    ChatMessage(role=MessageRole.USER, content="hi").model_dump(
                        mode="json"
                    )
                ]
            },
        )

        tool_response_event = ToolResponseEvent(
            request_id=tool_request_event_id,
            responses={tool_call_id: "Tool `query_order` execute failed."},
            external_ids={},
            success={tool_call_id: False},
            error={
                tool_call_id: "Missing config for injected tool parameter: tenant_id"
            },
        )

        asyncio.run(process_chat_request_or_tool_response(tool_response_event, ctx))

        assert captured_messages
        tool_message = captured_messages[0][-1]
        assert tool_message.role == MessageRole.TOOL
        assert tool_message.content == "Tool `query_order` execute failed."


# The conversion instruction the schema-carrying call appends. Spelled out here
# rather than imported from the action so the assertion pins the exact words a
# provider receives: a test that imported the constant would agree with any edit to
# it, including one that emptied it.
_FINALIZE_DIRECTIVE_TEXT = (
    "Convert the previous assistant response into the required structured output"
    " format. Preserve its meaning and do not add or infer any new information."
)


class TestNativeStructuredOutputFinalization:
    """Tests for the schema-carrying call issued once the loop settles on an answer."""

    def _run(self, ctx: Any, output_schema: OutputSchema) -> None:
        asyncio.run(
            chat(
                uuid4(),
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                output_schema,
                ctx,
            )
        )

    def _native_chat_model(self) -> MagicMock:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT, content="the answer is 42"
            )
        )
        chat_model.chat_structured = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT, content='{"result": 42}'
            )
        )
        return chat_model

    def _native_context(self, chat_model: MagicMock) -> tuple:
        ctx, sent_events, metric_group, memory = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )
        chat_model.will_apply_native_structured_output = MagicMock(return_value=True)
        return ctx, sent_events, metric_group, memory

    def test_native_schema_parses_the_finalization_response(self) -> None:
        chat_model = self._native_chat_model()
        ctx, sent_events, _, _ = self._native_context(chat_model)

        self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        # Both call counts, not only the final value: an unstubbed chat_structured
        # resolves to an auto-created mock that fails somewhere else entirely.
        assert chat_model.chat.call_count == 1
        assert chat_model.chat_structured.call_count == 1
        # The loop's answer is prose no parser could read, so only the finalization
        # response can satisfy this.
        assert len(sent_events) == 1
        assert sent_events[0].response.extra_args[STRUCTURED_OUTPUT].result == 42

    def test_schema_kept_in_the_prompt_issues_no_finalization_call(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT, content='{"result": 42}'
            )
        )
        chat_model.chat_structured = MagicMock()
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )
        # Stated rather than left to the helper's default: a schema that will not
        # travel natively is the case that must cost exactly what it always has.
        chat_model.will_apply_native_structured_output = MagicMock(return_value=False)
        schema = OutputSchema(output_schema=_StructuredResult)

        self._run(ctx, schema)

        # The gate was consulted and answered no. Without this the test cannot tell
        # a suppressed finalization from an action that never asks the question.
        chat_model.will_apply_native_structured_output.assert_called_once_with(schema)
        chat_model.chat_structured.assert_not_called()
        assert chat_model.chat.call_count == 1
        assert len(sent_events) == 1
        assert sent_events[0].response.extra_args[STRUCTURED_OUTPUT].result == 42

    def test_finalization_call_appends_the_final_answer_and_the_directive(self) -> None:
        chat_model = self._native_chat_model()
        loop_answer = chat_model.chat.return_value
        ctx, _, _, _ = self._native_context(chat_model)
        schema = OutputSchema(output_schema=_StructuredResult)

        self._run(ctx, schema)

        sent_messages, sent_schema = chat_model.chat_structured.call_args.args
        assert [m.content for m in sent_messages] == [
            "hi",
            "the answer is 42",
            _FINALIZE_DIRECTIVE_TEXT,
        ]
        assert sent_messages[1] is loop_answer
        assert sent_messages[2].role == MessageRole.USER
        assert sent_schema is schema
        # The schema travels through the tool-free channel, so the tool-binding
        # chat() path runs once for the loop and never for the conversion.
        assert chat_model.chat.call_count == 1

    def test_rejected_finish_reason_issues_no_finalization_call(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT,
                content="partial answ",
                extra_args={"finish_reason": "length"},
            )
        )
        chat_model.chat_structured = MagicMock()
        ctx, sent_events, _, _ = self._native_context(chat_model)
        schema = OutputSchema(output_schema=_StructuredResult)

        with pytest.raises(ValueError, match="(?i)truncat"):
            self._run(ctx, schema)

        # An answer the model never finished is abandoned before anything is paid to
        # convert it: the gate is asked, and the rejection still stops the call.
        chat_model.will_apply_native_structured_output.assert_called_once_with(schema)
        chat_model.chat_structured.assert_not_called()
        assert len(sent_events) == 0

    def test_tool_call_response_issues_no_finalization_call(self) -> None:
        tool_calls = [{"id": "call-1", "function": {"name": "f", "arguments": {}}}]
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT, content="", tool_calls=tool_calls
            )
        )
        chat_model.chat_structured = MagicMock()
        ctx, sent_events, _, _ = self._native_context(chat_model)
        schema = OutputSchema(output_schema=_StructuredResult)

        self._run(ctx, schema)

        # The loop is still asking for tools, so the answer to convert does not exist
        # yet: the gate is asked, and the outstanding tool calls still stop the call.
        chat_model.will_apply_native_structured_output.assert_called_once_with(schema)
        chat_model.chat_structured.assert_not_called()
        assert len(sent_events) == 1
        assert isinstance(sent_events[0], ToolRequestEvent)

    def test_finalization_call_emits_its_own_llm_span(self) -> None:
        chat_model = self._native_chat_model()
        ctx, _, _, _ = self._native_context(chat_model)

        self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        # A span covers one chat call, not one invocation: two calls reached the
        # provider, so two pairs are reported.
        llm_span = call(ExecutionEntityTypes.LLM, "test-model", _LLM_METADATA)
        assert ctx.report_execution_started.call_args_list.count(llm_span) == 2
        assert ctx.report_execution_succeeded.call_args_list.count(llm_span) == 2
        ctx.report_execution_failed.assert_not_called()

    def test_finalization_call_failure_is_reported_as_a_model_call_failure(
        self,
    ) -> None:
        chat_model = self._native_chat_model()
        chat_model.chat_structured = MagicMock(
            side_effect=RuntimeError("conversion call exploded")
        )
        ctx, sent_events, _, _ = self._native_context(chat_model)

        with pytest.raises(RuntimeError, match="conversion call exploded"):
            self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        # Without a span of its own the failure would land nowhere: the loop call's
        # success would be the last word and the error attributed to nothing.
        ctx.report_execution_failed.assert_called_once()
        failed_args = ctx.report_execution_failed.call_args.args
        assert failed_args[0] == ExecutionEntityTypes.LLM
        assert failed_args[1] == "test-model"
        assert failed_args[2] == _LLM_METADATA
        assert failed_args[-1] == ExecutionProblemCategories.MODEL_CALL_FAILED
        # Both calls started; only the loop's succeeded.
        llm_span = call(ExecutionEntityTypes.LLM, "test-model", _LLM_METADATA)
        assert ctx.report_execution_started.call_args_list.count(llm_span) == 2
        assert ctx.report_execution_succeeded.call_args_list.count(llm_span) == 1
        assert len(sent_events) == 0


class _StubTool(Tool):
    """Minimal tool; only its presence in the bound tool list matters."""

    @classmethod
    def tool_type(cls) -> ToolType:
        return ToolType.FUNCTION

    def call(self, *args: Any, **kwargs: Any) -> None:
        return None


class _RecordingConnection(BaseChatModelConnection):
    """Connection recording the messages of every request it receives.

    Separates the two channels by whether a schema came with the request, so a test
    can assert what each one carries.
    """

    unconstrained_requests: List[List[ChatMessage]] = Field(default_factory=list)
    unconstrained_tools: List[Tool] | None = None
    schema_carrying_requests: List[List[ChatMessage]] = Field(default_factory=list)
    schema_carrying_tools: List[Tool] | None = None

    def chat(
        self,
        messages: Sequence[ChatMessage],
        tools: List[Tool] | None = None,
        output_schema: OutputSchema | None = None,
        **kwargs: Any,
    ) -> ChatMessage:
        # Recorded exactly as received rather than normalized through ``or []``, so a
        # test can tell an empty list from an absent argument.
        if output_schema is None:
            self.unconstrained_requests.append(list(messages))
            self.unconstrained_tools = tools
            return ChatMessage(role=MessageRole.ASSISTANT, content="the answer is 42")
        self.schema_carrying_requests.append(list(messages))
        self.schema_carrying_tools = tools
        return ChatMessage(role=MessageRole.ASSISTANT, content='{"result": 42}')


class _PromptBoundChatModelSetup(BaseChatModelSetup):
    """Setup binding a prompt and holding a live connection, overriding neither
    ``chat`` nor ``chat_structured``, so both requests are built by the real methods.
    That is what lets a test see one render the bound prompt and the other leave it
    out.
    """

    @property
    def model_kwargs(self) -> Dict[str, Any]:
        return {}

    def will_apply_native_structured_output(
        self, output_schema: OutputSchema | None
    ) -> bool:
        return output_schema is not None


class TestNativeFinalizationRequestShape:
    """Tests asserting what the schema-carrying request actually carries."""

    def test_finalization_request_does_not_carry_the_bound_prompt(self) -> None:
        connection = _RecordingConnection()
        chat_model = _PromptBoundChatModelSetup(
            connection="c",
            model="m",
            prompt=Prompt.from_text(text="FRAMING-SENTINEL: answer only in haiku."),
        )
        chat_model._resolved_connection = connection
        # Bound so the two requests differ in tools rather than both carrying none:
        # against a setup that binds nothing, an empty list on the finalization proves
        # nothing, and a finalization wrongly passing the setup's tools would still
        # produce one.
        tool = _StubTool()
        chat_model.tools.append(tool)
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0
        )

        asyncio.run(
            chat(
                uuid4(),
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="what is the answer")],
                {},
                OutputSchema(output_schema=_StructuredResult),
                ctx,
            )
        )

        # The loop's own request renders the bound prompt, which is what makes the
        # sentinel a real signal here: it is visible at this seam whenever a prompt
        # is prepended.
        assert len(connection.unconstrained_requests) == 1
        assert any(
            "FRAMING-SENTINEL" in m.content
            for m in connection.unconstrained_requests[0]
        )

        # The schema-carrying request is the conversation, the answer the loop
        # settled on, and the conversion instruction. Asserted on content so a change
        # that prepends the prompt fails here instead of slipping past a length check.
        assert len(connection.schema_carrying_requests) == 1
        finalize_request = connection.schema_carrying_requests[0]
        assert [m.content for m in finalize_request] == [
            "what is the answer",
            "the answer is 42",
            _FINALIZE_DIRECTIVE_TEXT,
        ]
        assert all("FRAMING-SENTINEL" not in m.content for m in finalize_request)
        # The loop's request advertises the bound tool, so the finalization's empty
        # list is a real difference between the two requests.
        assert connection.unconstrained_tools is not None
        assert len(connection.unconstrained_tools) == 1
        assert connection.unconstrained_tools[0] is tool
        # A provider may drop a native schema from a request that also advertises
        # tools, so the schema-carrying call binds none. Distinguished from an absent
        # argument by the connection recording what it received verbatim.
        assert connection.schema_carrying_tools == []
        assert len(sent_events) == 1


class TestNativeFinalizationAsyncPath:
    """The finalization on the async durable seam, which is the production default."""

    def test_finalization_runs_on_the_async_durable_seam(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT, content="the answer is 42"
            )
        )
        chat_model.chat_structured = MagicMock(
            return_value=ChatMessage(
                role=MessageRole.ASSISTANT, content='{"result": 42}'
            )
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=0, retry_wait_interval_sec=0, chat_async=True
        )
        chat_model.will_apply_native_structured_output = MagicMock(return_value=True)

        asyncio.run(
            chat(
                uuid4(),
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                OutputSchema(output_schema=_StructuredResult),
                ctx,
            )
        )

        # AgentExecutionOptions.CHAT_ASYNC defaults to True, so this is the seam most
        # requests actually take. Both the loop call and the finalization go through
        # it, and neither through the synchronous one.
        assert ctx.durable_execute_async.call_count == 2
        ctx.durable_execute.assert_not_called()
        assert chat_model.chat.call_count == 1
        assert chat_model.chat_structured.call_count == 1
        assert len(sent_events) == 1
        assert sent_events[0].response.extra_args[STRUCTURED_OUTPUT].result == 42


class TestNativeGateFailureStrategy:
    """The gate is documented to raise; the configured strategy decides its fate."""

    def _run(self, ctx: Any, output_schema: OutputSchema) -> None:
        asyncio.run(
            chat(
                uuid4(),
                "test-model",
                [ChatMessage(role=MessageRole.USER, content="hi")],
                {},
                output_schema,
                ctx,
            )
        )

    def test_gate_failure_is_dropped_under_the_ignore_strategy(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock()
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model,
            max_retries=0,
            retry_wait_interval_sec=0,
            error_handling_strategy=ErrorHandlingStrategy.IGNORE,
        )
        chat_model.will_apply_native_structured_output = MagicMock(
            side_effect=ValueError("reports the output schema infeasible")
        )

        # Must not raise. A configuration error the gate reports is this request
        # failing, and IGNORE drops a failed request instead of propagating it. The
        # gate sitting outside the loop's try would let it escape past the strategy.
        self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        assert len(sent_events) == 0
        chat_model.chat.assert_not_called()

    def test_gate_failure_propagates_under_the_fail_strategy(self) -> None:
        chat_model = MagicMock()
        chat_model.chat = MagicMock()
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model,
            max_retries=0,
            retry_wait_interval_sec=0,
            error_handling_strategy=ErrorHandlingStrategy.FAIL,
        )
        chat_model.will_apply_native_structured_output = MagicMock(
            side_effect=ValueError("reports the output schema infeasible")
        )

        with pytest.raises(ValueError, match="infeasible"):
            self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        # No model call is paid for a policy no request can satisfy.
        chat_model.chat.assert_not_called()
        assert len(sent_events) == 0

    def test_gate_is_evaluated_once_across_retries(self) -> None:
        chat_model = MagicMock()
        # Sized to the real call count: one failure, then the answer.
        chat_model.chat = MagicMock(
            side_effect=[
                RuntimeError("transient error"),
                ChatMessage(role=MessageRole.ASSISTANT, content='{"result": 42}'),
            ]
        )
        ctx, sent_events, _, _ = _create_mock_runner_context(
            chat_model, max_retries=1, retry_wait_interval_sec=0
        )
        gate = MagicMock(return_value=False)
        chat_model.will_apply_native_structured_output = gate

        self._run(ctx, OutputSchema(output_schema=_StructuredResult))

        assert chat_model.chat.call_count == 2
        # Loop-invariant: the answer cannot change between attempts, so a retry must
        # not re-ask it. Evaluating inside the loop leaves every other test here green.
        assert gate.call_count == 1
        assert len(sent_events) == 1
