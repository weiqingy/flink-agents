/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.plan.actions;

import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.agents.AgentExecutionOptions;
import org.apache.flink.agents.api.agents.OutputSchema;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.chat.model.BaseChatModelConnection;
import org.apache.flink.agents.api.chat.model.BaseChatModelSetup;
import org.apache.flink.agents.api.context.DurableCallable;
import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.event.ChatResponseEvent;
import org.apache.flink.agents.api.event.ToolRequestEvent;
import org.apache.flink.agents.api.event.ToolResponseEvent;
import org.apache.flink.agents.api.metrics.FlinkAgentsMetricGroup;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.agents.api.tools.ToolResponse;
import org.apache.flink.agents.api.trace.ExecutionReporter;
import org.apache.flink.agents.api.trace.LLMExecutionMetadataKeys;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.metrics.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ChatModelAction#chat} driven end to end: retry behavior, execution reporting,
 * the finish-reason gate, and tool-response handling.
 */
class ChatModelActionRetryTest {

    private static final Map<String, Object> LLM_METADATA =
            Map.of(LLMExecutionMetadataKeys.MODEL, "configured-model");

    @Mock private RunnerContext mockCtx;

    @Mock private BaseChatModelSetup mockChatModel;

    @Mock private FlinkAgentsMetricGroup mockActionMetricGroup;

    @Mock private FlinkAgentsMetricGroup mockModelMetricGroup;

    @Mock private Counter mockRetryCountCounter;

    @Mock private Counter mockRetryWaitSecCounter;

    private MemoryObject sensoryMemory;
    private List<Event> sentEvents;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);
        sentEvents = new ArrayList<>();
        sensoryMemory = createStatefulMemoryObject();

        // Wire up ChatModel
        when(mockChatModel.getConnectionName()).thenReturn("test-connection");

        // Wire up RunnerContext
        when(mockCtx.getResource(anyString(), eq(ResourceType.CHAT_MODEL)))
                .thenReturn(mockChatModel);
        when(mockCtx.getSensoryMemory()).thenReturn(sensoryMemory);
        when(mockCtx.getActionMetricGroup()).thenReturn(mockActionMetricGroup);
        doAnswer(inv -> sentEvents.add(inv.getArgument(0))).when(mockCtx).sendEvent(any());
        when(mockCtx.<ChatMessage>durableExecute(any()))
                .thenAnswer(inv -> inv.<DurableCallable<ChatMessage>>getArgument(0).call());

        // Wire up metric group chain
        when(mockActionMetricGroup.getSubGroup(anyString(), anyString()))
                .thenReturn(mockModelMetricGroup);
        when(mockModelMetricGroup.getCounter("retryCount")).thenReturn(mockRetryCountCounter);
        when(mockModelMetricGroup.getCounter("retryWaitSec")).thenReturn(mockRetryWaitSecCounter);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    void chatSucceedsWithoutRetry_retryCountIsZero() throws Exception {
        configureRetryStrategy(3, 1);
        when(mockChatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "hello"));

        UUID requestId = UUID.randomUUID();
        ChatModelAction.chat(
                requestId,
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                mockCtx);

        assertThat(sentEvents).hasSize(1);
        ChatResponseEvent responseEvent = ChatResponseEvent.fromEvent(sentEvents.get(0));
        assertThat(responseEvent.getRetryCount()).isEqualTo(0);
        assertThat(responseEvent.getTotalRetryWaitSec()).isEqualTo(0);

        // No retry metrics should be recorded
        verify(mockActionMetricGroup, never()).getSubGroup(anyString(), anyString());
    }

    @Test
    void chatRecordsTokenMetricsWithRequestScopedMetricGroup() throws Exception {
        configureRetryStrategy(0, 0);
        FlinkAgentsMetricGroup actionA = mock(FlinkAgentsMetricGroup.class);
        FlinkAgentsMetricGroup actionB = mock(FlinkAgentsMetricGroup.class);
        when(mockCtx.getActionMetricGroup()).thenReturn(actionA, actionB);

        ChatMessage response =
                new ChatMessage(
                        MessageRole.ASSISTANT,
                        "hello",
                        Map.of(
                                "model_name", "provider-model",
                                "promptTokens", 100L,
                                "completionTokens", 50L));
        when(mockChatModel.chat(any(), any(), any())).thenReturn(response);

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                mockCtx);

        verify(mockChatModel).recordTokenMetrics(actionA, "provider-model", 100L, 50L);
        verify(mockChatModel, never()).recordTokenMetrics(actionB, "provider-model", 100L, 50L);
    }

    @Test
    void chatReportsLlmExecution() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "hello"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                reportingCtx);

        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        verify(reporter)
                .reportExecutionStarted(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(reporter)
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
    }

    @Test
    void chatReportsStructuredOutputParserExecution() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                Map.class,
                reportingCtx);

        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        verify(reporter)
                .reportExecutionStarted(
                        ExecutionReporter.EntityTypes.PARSER, Agent.STRUCTURED_OUTPUT, Map.of());
        verify(reporter)
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.PARSER, Agent.STRUCTURED_OUTPUT, Map.of());
    }

    @Test
    void chatRetriesStructuredOutputParseErrorWithoutFailingLlm() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(reportingCtx.getConfig())
                .thenReturn(readableConfig(Agent.ErrorHandlingStrategy.RETRY, 1, 0));
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(MessageRole.ASSISTANT, "not-json"),
                        new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                Map.class,
                reportingCtx);

        verify(chatModel, times(2)).chat(any(), any(), any());

        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        verify(reporter, times(2))
                .reportExecutionStarted(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(reporter, times(2))
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(reporter)
                .reportExecutionFailed(
                        eq(ExecutionReporter.EntityTypes.PARSER),
                        eq(Agent.STRUCTURED_OUTPUT),
                        eq(Map.of()),
                        any(Exception.class),
                        eq(ExecutionReporter.ProblemCategories.MODEL_OUTPUT_PARSE_ERROR));
        verify(reporter, never())
                .reportExecutionFailed(
                        eq(ExecutionReporter.EntityTypes.LLM),
                        eq("test-model"),
                        eq(LLM_METADATA),
                        any(Throwable.class),
                        any());
    }

    @Test
    void chatReportsEachRetriedModelInvocation() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(reportingCtx.getConfig())
                .thenReturn(readableConfig(Agent.ErrorHandlingStrategy.RETRY, 1, 0));
        when(chatModel.chat(any(), any(), any()))
                .thenThrow(new RuntimeException("transient error"))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "success"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                reportingCtx);

        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        verify(reporter, times(2))
                .reportExecutionStarted(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(reporter)
                .reportExecutionFailed(
                        eq(ExecutionReporter.EntityTypes.LLM),
                        eq("test-model"),
                        eq(LLM_METADATA),
                        any(RuntimeException.class),
                        eq(ExecutionReporter.ProblemCategories.MODEL_CALL_FAILED));
        verify(reporter)
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
    }

    @Test
    void chatRetriesWithExponentialBackoff() throws Exception {
        // 1 second base interval; fail once then succeed -> wait 1s (1 * 2^0)
        configureRetryStrategy(3, 1);

        AtomicInteger callCount = new AtomicInteger(0);
        when(mockChatModel.chat(any(), any(), any()))
                .thenAnswer(
                        inv -> {
                            int count = callCount.incrementAndGet();
                            if (count <= 1) {
                                throw new RuntimeException("transient error");
                            }
                            return new ChatMessage(MessageRole.ASSISTANT, "success");
                        });

        UUID requestId = UUID.randomUUID();

        long startTime = System.currentTimeMillis();
        ChatModelAction.chat(
                requestId,
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                mockCtx);
        long elapsed = System.currentTimeMillis() - startTime;

        assertThat(sentEvents).hasSize(1);
        ChatResponseEvent responseEvent = ChatResponseEvent.fromEvent(sentEvents.get(0));
        assertThat(responseEvent.getRetryCount()).isEqualTo(1);
        // Exponential backoff: 1000ms (1s * 2^0) total
        // 1 retry with 1s interval = 1s total
        assertThat(responseEvent.getTotalRetryWaitSec()).isEqualTo(1);
        assertThat(elapsed).isGreaterThanOrEqualTo(1000L);

        // Verify metrics recorded under connection name
        verify(mockActionMetricGroup).getSubGroup("model", mockChatModel.getConnectionName());
        verify(mockRetryCountCounter).inc(1);
        verify(mockRetryWaitSecCounter).inc(1);
    }

    @Test
    void chatExhaustsRetriesAndThrows() {
        configureRetryStrategy(2, 0);

        when(mockChatModel.chat(any(), any(), any()))
                .thenThrow(new RuntimeException("persistent error"));

        UUID requestId = UUID.randomUUID();

        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        requestId,
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        null,
                                        mockCtx))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("persistent error");

        assertThat(sentEvents).isEmpty();
    }

    @Test
    void chatResponseEventDefaultConstructorHasZeroRetryInfo() {
        UUID requestId = UUID.randomUUID();
        ChatMessage msg = new ChatMessage(MessageRole.ASSISTANT, "test");
        ChatResponseEvent event = new ChatResponseEvent(requestId, msg);

        assertThat(event.getRetryCount()).isEqualTo(0);
        assertThat(event.getTotalRetryWaitSec()).isEqualTo(0);
        assertThat(event.getRequestId()).isEqualTo(requestId);
    }

    @Test
    void chatResponseEventFullConstructorCarriesRetryInfo() {
        UUID requestId = UUID.randomUUID();
        ChatMessage msg = new ChatMessage(MessageRole.ASSISTANT, "test");
        ChatResponseEvent event = new ChatResponseEvent(requestId, msg, 5, 31);

        assertThat(event.getRetryCount()).isEqualTo(5);
        assertThat(event.getTotalRetryWaitSec()).isEqualTo(31);
    }

    @Test
    void retryWaitIntervalDefaultValue() {
        assertThat(AgentExecutionOptions.RETRY_WAIT_INTERVAL.getDefaultValue()).isEqualTo(1);
    }

    @Test
    void processToolResponseForwardsSavedArgumentsToChat() throws Exception {
        configureRetryStrategy(0, 0);

        UUID initialRequestId = UUID.randomUUID();
        UUID toolRequestEventId = UUID.randomUUID();
        String toolCallId = "call-1";
        Map<String, Object> savedPromptArgs = Map.of("k", "v");

        // Pre-seed sensory memory with the tool-request-event context that
        // processToolResponse will look up. This simulates a prior chat round
        // that produced a tool call.
        Map<UUID, Object> toolRequestEventContext = new HashMap<>();
        Map<String, Object> contextEntry = new HashMap<>();
        contextEntry.put("initialRequestId", initialRequestId);
        contextEntry.put("model", "test-model");
        contextEntry.put("prompt_args", savedPromptArgs);
        toolRequestEventContext.put(toolRequestEventId, contextEntry);
        sensoryMemory.set("_TOOL_REQUEST_EVENT_CONTEXT", toolRequestEventContext);

        // Pre-seed the tool-call context with the initial messages so
        // updateToolCallContext can extend them with the tool response.
        Map<UUID, Object> toolCallContext = new HashMap<>();
        toolCallContext.put(
                initialRequestId,
                new ArrayList<>(List.of(new ChatMessage(MessageRole.USER, "hi"))));
        sensoryMemory.set("_TOOL_CALL_CONTEXT", toolCallContext);

        when(mockChatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "done"));

        ToolResponseEvent toolResponseEvent =
                new ToolResponseEvent(
                        toolRequestEventId,
                        Map.of(toolCallId, ToolResponse.success("42")),
                        Map.of(toolCallId, true),
                        Map.of());

        ChatModelAction.processChatRequestOrToolResponse(toolResponseEvent, mockCtx);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> promptArgsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockChatModel).chat(any(), promptArgsCaptor.capture(), any());
        assertThat(promptArgsCaptor.getValue()).isEqualTo(savedPromptArgs);
    }

    @Test
    void chatRejectsTruncatedTextResponse() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "partial answ",
                                Map.of("finish_reason", "length")));

        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        UUID.randomUUID(),
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        null,
                                        reportingCtx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("truncated")
                .hasMessageContaining("token");

        assertThat(sentEvents).isEmpty();
    }

    @Test
    void chatRejectsContentFilteredTextResponse() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "",
                                Map.of("finish_reason", "content_filter")));

        // Both rejection messages interpolate the finish reason, so the literal
        // content_filter appears in either one and cannot tell them apart. These
        // match prose unique to the filtering message.
        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        UUID.randomUUID(),
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        null,
                                        reportingCtx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("withheld")
                .hasMessageContaining("content filter");

        assertThat(sentEvents).isEmpty();
    }

    @Test
    void chatRejectsTruncatedToolCallResponseBeforeDispatchingTools() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "",
                                List.of(
                                        Map.of(
                                                "id",
                                                "call-1",
                                                "function",
                                                Map.of("name", "f", "arguments", ""))),
                                Map.of("finish_reason", "length")));

        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        UUID.randomUUID(),
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        null,
                                        reportingCtx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("truncated");

        // A truncated tool call carries arguments the model never finished writing,
        // so no ToolRequestEvent may leave the action.
        assertThat(sentEvents).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"length", "content_filter"})
    void chatRejectedFinishReasonSkipsStructuredOutput(String finishReason) throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "{\"answer\":\"42\"}",
                                Map.of(
                                        "finish_reason",
                                        finishReason,
                                        "model_name",
                                        "provider-model",
                                        "promptTokens",
                                        100L,
                                        "completionTokens",
                                        50L)));

        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        UUID.randomUUID(),
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        Map.class,
                                        reportingCtx))
                .isInstanceOf(IllegalStateException.class);

        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        // The model call itself succeeded and spent its full token budget, so both
        // must be recorded before the response is rejected.
        verify(reporter)
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(chatModel).recordTokenMetrics(mockActionMetricGroup, "provider-model", 100L, 50L);
        // The parse is never attempted, so nothing about it is reported. The failed
        // check is unscoped: the rejection must not be reported as a failure of any
        // entity, the model call included.
        verify(reporter, never())
                .reportExecutionStarted(
                        eq(ExecutionReporter.EntityTypes.PARSER), anyString(), any());
        verify(reporter, never())
                .reportExecutionSucceeded(
                        eq(ExecutionReporter.EntityTypes.PARSER), anyString(), any());
        verify(reporter, never())
                .reportExecutionFailed(anyString(), anyString(), any(), any(), any());
        assertThat(sentEvents).isEmpty();
    }

    @Test
    void chatIgnoreStrategyDropsRejectedResponseWithoutEvent() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(reportingCtx.getConfig())
                .thenReturn(readableConfig(Agent.ErrorHandlingStrategy.IGNORE));
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "partial answ",
                                Map.of("finish_reason", "length")));

        // Under IGNORE the record is dropped: the rejection does not propagate and no
        // event carries the truncated content downstream.
        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                reportingCtx);

        assertThat(sentEvents).isEmpty();
    }

    private static Stream<Map<String, Object>> acceptedFinishReasons() {
        return Stream.of(
                Map.of("finish_reason", "stop"),
                Map.of("finish_reason", "tool_calls"),
                Map.of("finish_reason", "some_vendor_reason"),
                Map.of());
    }

    @ParameterizedTest
    @MethodSource("acceptedFinishReasons")
    void chatAcceptedFinishReasonReachesTheResponseEvent(Map<String, Object> extraArgs)
            throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "hello", extraArgs));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                null,
                reportingCtx);

        assertThat(sentEvents).hasSize(1);
        assertThat(ChatResponseEvent.fromEvent(sentEvents.get(0)).getResponse().getContent())
                .isEqualTo("hello");
    }

    /**
     * The conversion instruction the schema-carrying call appends. Spelled out here rather than
     * read from the invoker so the assertion pins the exact words a provider receives: a test that
     * imported the constant would agree with any edit to it, including one that emptied it.
     */
    private static final String FINALIZE_DIRECTIVE =
            "Convert the previous assistant response into the required structured"
                    + " output format. Preserve its meaning and do not add or infer"
                    + " any new information.";

    private static OutputSchema rowTypeSchema() {
        return new OutputSchema(
                new RowTypeInfo(
                        new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO},
                        new String[] {"answer"}));
    }

    @Test
    void nativeSchemaParsesTheFinalizationResponseNotTheLoopResponse() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        // Prose the parser could never read: only the finalization response can satisfy it.
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "the answer is 42"));
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(true);
        when(chatModel.chatStructured(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                Map.class,
                reportingCtx);

        verify(chatModel).chatStructured(any(), any(), any());
        assertThat(sentEvents).hasSize(1);
        assertThat(ChatResponseEvent.fromEvent(sentEvents.get(0)).getResponse().getContent())
                .isEqualTo("{\"answer\":\"42\"}");
    }

    @Test
    void schemaTheStrategyKeepsInThePromptIssuesNoFinalizationCall() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}"));
        // Stated rather than left to the mock default: a schema that will not travel natively
        // is the case that must cost exactly what it always has.
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(false);

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                rowTypeSchema(),
                reportingCtx);

        // The gate was consulted and answered no. Without this the test cannot tell a suppressed
        // finalization from an invoker that never asks the question at all.
        verify(chatModel).willApplyNativeStructuredOutput(any());
        verify(chatModel, never()).chatStructured(any(), any(), any());
        verify(chatModel, times(1)).chat(any(), any(), any());
        assertThat(sentEvents).hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void finalizationCallAppendsTheFinalAnswerAndTheConversionDirective() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        ChatMessage loopAnswer = new ChatMessage(MessageRole.ASSISTANT, "the answer is 42");
        when(chatModel.chat(any(), any(), any())).thenReturn(loopAnswer);
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(true);
        when(chatModel.chatStructured(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                Map.class,
                reportingCtx);

        ArgumentCaptor<List<ChatMessage>> messagesCaptor = ArgumentCaptor.forClass(List.class);
        verify(chatModel).chatStructured(messagesCaptor.capture(), any(), any());
        List<ChatMessage> sent = messagesCaptor.getValue();
        assertThat(sent).hasSize(3);
        assertThat(sent.get(0).getContent()).isEqualTo("hi");
        assertThat(sent.get(1)).isSameAs(loopAnswer);
        assertThat(sent.get(2).getRole()).isEqualTo(MessageRole.USER);
        assertThat(sent.get(2).getContent()).isEqualTo(FINALIZE_DIRECTIVE);
        // The schema travels through the tool-free channel, so the tool-binding chat() path
        // runs once for the loop and never for the conversion.
        verify(chatModel, times(1)).chat(any(), any(), any());
    }

    @Test
    void rejectedFinishReasonIssuesNoFinalizationCall() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "partial answ",
                                Map.of("finish_reason", "length")));
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(true);

        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        UUID.randomUUID(),
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        Map.class,
                                        reportingCtx))
                .isInstanceOf(IllegalStateException.class);

        // An answer the model never finished is abandoned before anything is paid to convert it:
        // the gate is asked, and the rejection still stops the conversion from being issued.
        verify(chatModel).willApplyNativeStructuredOutput(any());
        verify(chatModel, never()).chatStructured(any(), any(), eq(Map.class));
    }

    @Test
    void toolCallResponseIssuesNoFinalizationCall() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(
                        new ChatMessage(
                                MessageRole.ASSISTANT,
                                "",
                                List.of(
                                        Map.of(
                                                "id",
                                                "call-1",
                                                "type",
                                                "function",
                                                "function",
                                                Map.of("name", "lookup", "arguments", Map.of())))));
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(true);

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                Map.class,
                reportingCtx);

        // The loop is still asking for tools, so the answer to convert does not exist yet: the
        // gate is asked, and the outstanding tool calls still stop the conversion.
        verify(chatModel).willApplyNativeStructuredOutput(any());
        verify(chatModel, never()).chatStructured(any(), any(), eq(Map.class));
        assertThat(sentEvents).hasSize(1);
        assertThat(sentEvents.get(0).getType()).isEqualTo(ToolRequestEvent.EVENT_TYPE);
    }

    @Test
    void finalizationCallEmitsItsOwnLlmSpan() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "the answer is 42"));
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(true);
        when(chatModel.chatStructured(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}"));

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "hi")),
                Map.of(),
                Map.class,
                reportingCtx);

        // A span covers one chat call, not one invocation: two calls reached the provider, so
        // two pairs are reported.
        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        verify(reporter, times(2))
                .reportExecutionStarted(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(reporter, times(2))
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
    }

    @Test
    void finalizationCallFailureIsReportedAsAModelCallFailure() throws Exception {
        RunnerContext reportingCtx = reportingRunnerContext();
        BaseChatModelSetup chatModel = configureReportingChatContext(reportingCtx);
        when(chatModel.chat(any(), any(), any()))
                .thenReturn(new ChatMessage(MessageRole.ASSISTANT, "the answer is 42"));
        when(chatModel.willApplyNativeStructuredOutput(any())).thenReturn(true);
        when(chatModel.chatStructured(any(), any(), any()))
                .thenThrow(new RuntimeException("conversion call exploded"));

        assertThatThrownBy(
                        () ->
                                ChatModelAction.chat(
                                        UUID.randomUUID(),
                                        "test-model",
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        Map.class,
                                        reportingCtx))
                .hasMessageContaining("conversion call exploded");

        ExecutionReporter reporter = (ExecutionReporter) reportingCtx;
        // Without a span of its own the failure would land nowhere: the loop call's success
        // would be the last word and the error would be attributed to nothing.
        verify(reporter)
                .reportExecutionFailed(
                        eq(ExecutionReporter.EntityTypes.LLM),
                        eq("test-model"),
                        eq(LLM_METADATA),
                        any(Throwable.class),
                        eq(ExecutionReporter.ProblemCategories.MODEL_CALL_FAILED));
        // Both calls started; only the loop's succeeded.
        verify(reporter, times(2))
                .reportExecutionStarted(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
        verify(reporter, times(1))
                .reportExecutionSucceeded(
                        ExecutionReporter.EntityTypes.LLM, "test-model", LLM_METADATA);
    }

    @Test
    void finalizationRequestDoesNotCarryTheBoundPrompt() throws Exception {
        RecordingConnection connection = new RecordingConnection();
        PromptBoundChatModel chatModel =
                new PromptBoundChatModel("FRAMING-SENTINEL: answer only in haiku.", connection);
        RunnerContext ctx = reportingContextFor(chatModel);

        ChatModelAction.chat(
                UUID.randomUUID(),
                "test-model",
                List.of(new ChatMessage(MessageRole.USER, "what is the answer")),
                Map.of(),
                Map.class,
                ctx);

        // The loop's own request renders the bound prompt, which is what makes the sentinel a
        // real signal here: it is visible at this seam whenever a prompt is prepended.
        assertThat(connection.unconstrainedRequests).hasSize(1);
        assertThat(connection.unconstrainedRequests.get(0))
                .anySatisfy(m -> assertThat(m.getContent()).contains("FRAMING-SENTINEL"));

        // The schema-carrying request is the conversation, the answer the loop settled on, and
        // the conversion instruction. Asserted on content so a change that prepends the prompt
        // fails here instead of slipping past a length check.
        assertThat(connection.schemaCarryingRequests).hasSize(1);
        List<ChatMessage> finalizeRequest = connection.schemaCarryingRequests.get(0);
        assertThat(finalizeRequest)
                .extracting(ChatMessage::getContent)
                .containsExactly("what is the answer", "the answer is 42", FINALIZE_DIRECTIVE);
        assertThat(finalizeRequest)
                .noneSatisfy(m -> assertThat(m.getContent()).contains("FRAMING-SENTINEL"));
    }

    // --- Helper methods ---

    /**
     * A connection recording the messages of every request it receives, so a test can assert on
     * what a call actually carries. Overrides the schema-carrying overload because the base rejects
     * a non-null schema, which is what a connection without a native translation does.
     */
    private static class RecordingConnection extends BaseChatModelConnection {
        final List<List<ChatMessage>> unconstrainedRequests = new ArrayList<>();
        final List<List<ChatMessage>> schemaCarryingRequests = new ArrayList<>();

        RecordingConnection() {
            super(new ResourceDescriptor("recording-connection", Map.of()), null);
        }

        @Override
        public ResourceType getResourceType() {
            return ResourceType.CHAT_MODEL_CONNECTION;
        }

        @Override
        public ChatMessage chat(
                List<ChatMessage> messages, List<Tool> tools, Map<String, Object> modelParams) {
            unconstrainedRequests.add(List.copyOf(messages));
            return new ChatMessage(MessageRole.ASSISTANT, "the answer is 42");
        }

        @Override
        public ChatMessage chat(
                List<ChatMessage> messages,
                List<Tool> tools,
                Map<String, Object> modelParams,
                Object outputSchema) {
            schemaCarryingRequests.add(List.copyOf(messages));
            return new ChatMessage(MessageRole.ASSISTANT, "{\"answer\":\"42\"}");
        }
    }

    /**
     * A setup binding a prompt and holding a live connection, overriding neither {@code chat} nor
     * {@code chatStructured}. Both requests are therefore built by the real methods, which is what
     * lets a test see that one renders the bound prompt and the other leaves it out.
     */
    private static class PromptBoundChatModel extends BaseChatModelSetup {
        PromptBoundChatModel(String template, BaseChatModelConnection connection) {
            super(new ResourceDescriptor("prompt-bound", Map.of()), null);
            this.prompt = new org.apache.flink.agents.api.prompt.Prompt.LocalPrompt(template);
            this.connection = connection;
        }

        @Override
        public boolean willApplyNativeStructuredOutput(Object outputSchema) {
            return outputSchema != null;
        }

        @Override
        public Map<String, Object> getParameters() {
            // Mutable: both chat() and chatStructured() merge per-call parameters into whatever
            // this returns, so an immutable map would fail before either request is built.
            return new HashMap<>();
        }
    }

    private RunnerContext reportingContextFor(BaseChatModelSetup chatModel) throws Exception {
        RunnerContext ctx = reportingRunnerContext();
        when(ctx.getResource(anyString(), eq(ResourceType.CHAT_MODEL))).thenReturn(chatModel);
        MemoryObject memory = createStatefulMemoryObject();
        when(ctx.getSensoryMemory()).thenReturn(memory);
        when(ctx.getActionMetricGroup()).thenReturn(mockActionMetricGroup);
        when(ctx.<ChatMessage>durableExecute(any()))
                .thenAnswer(inv -> inv.<DurableCallable<ChatMessage>>getArgument(0).call());
        doAnswer(inv -> sentEvents.add(inv.getArgument(0))).when(ctx).sendEvent(any());
        when(ctx.getConfig()).thenReturn(readableConfig(Agent.ErrorHandlingStrategy.FAIL));
        return ctx;
    }

    private void configureRetryStrategy(int maxRetries, int waitIntervalSec) {
        when(mockCtx.getConfig())
                .thenAnswer(
                        inv -> {
                            // Return a mock ReadableConfiguration
                            return new org.apache.flink.agents.api.configuration
                                    .ReadableConfiguration() {
                                @Override
                                @SuppressWarnings("unchecked")
                                public <T> T get(
                                        org.apache.flink.agents.api.configuration.ConfigOption<T>
                                                option) {
                                    if (option == AgentExecutionOptions.ERROR_HANDLING_STRATEGY) {
                                        return (T) Agent.ErrorHandlingStrategy.RETRY;
                                    }
                                    if (option == AgentExecutionOptions.MAX_RETRIES) {
                                        return (T) Integer.valueOf(maxRetries);
                                    }
                                    if (option == AgentExecutionOptions.RETRY_WAIT_INTERVAL) {
                                        return (T) Integer.valueOf(waitIntervalSec);
                                    }
                                    if (option == AgentExecutionOptions.CHAT_ASYNC) {
                                        return (T) Boolean.FALSE;
                                    }
                                    return option.getDefaultValue();
                                }

                                @Override
                                public Integer getInt(String key, Integer defaultValue) {
                                    return defaultValue;
                                }

                                @Override
                                public Long getLong(String key, Long defaultValue) {
                                    return defaultValue;
                                }

                                @Override
                                public Float getFloat(String key, Float defaultValue) {
                                    return defaultValue;
                                }

                                @Override
                                public Double getDouble(String key, Double defaultValue) {
                                    return defaultValue;
                                }

                                @Override
                                public Boolean getBool(String key, Boolean defaultValue) {
                                    return defaultValue;
                                }

                                @Override
                                public String getStr(String key, String defaultValue) {
                                    return defaultValue;
                                }
                            };
                        });
    }

    private RunnerContext reportingRunnerContext() {
        return mock(RunnerContext.class, withSettings().extraInterfaces(ExecutionReporter.class));
    }

    private BaseChatModelSetup configureReportingChatContext(RunnerContext reportingCtx)
            throws Exception {
        BaseChatModelSetup chatModel = mock(BaseChatModelSetup.class);
        MemoryObject memory = createStatefulMemoryObject();

        when(chatModel.getConnectionName()).thenReturn("test-connection");
        when(chatModel.getModel()).thenReturn("configured-model");
        when(reportingCtx.getResource(anyString(), eq(ResourceType.CHAT_MODEL)))
                .thenReturn(chatModel);
        when(reportingCtx.getSensoryMemory()).thenReturn(memory);
        when(reportingCtx.getActionMetricGroup()).thenReturn(mockActionMetricGroup);
        when(reportingCtx.<ChatMessage>durableExecute(any()))
                .thenAnswer(inv -> inv.<DurableCallable<ChatMessage>>getArgument(0).call());
        doAnswer(inv -> sentEvents.add(inv.getArgument(0))).when(reportingCtx).sendEvent(any());
        when(reportingCtx.getConfig()).thenReturn(readableConfig(Agent.ErrorHandlingStrategy.FAIL));
        return chatModel;
    }

    private org.apache.flink.agents.api.configuration.ReadableConfiguration readableConfig(
            Agent.ErrorHandlingStrategy errorHandlingStrategy) {
        return readableConfig(errorHandlingStrategy, 0, 0);
    }

    private org.apache.flink.agents.api.configuration.ReadableConfiguration readableConfig(
            Agent.ErrorHandlingStrategy errorHandlingStrategy,
            int maxRetries,
            int retryWaitIntervalSec) {
        return new org.apache.flink.agents.api.configuration.ReadableConfiguration() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T get(org.apache.flink.agents.api.configuration.ConfigOption<T> option) {
                if (option == AgentExecutionOptions.ERROR_HANDLING_STRATEGY) {
                    return (T) errorHandlingStrategy;
                }
                if (option == AgentExecutionOptions.MAX_RETRIES) {
                    return (T) Integer.valueOf(maxRetries);
                }
                if (option == AgentExecutionOptions.RETRY_WAIT_INTERVAL) {
                    return (T) Integer.valueOf(retryWaitIntervalSec);
                }
                if (option == AgentExecutionOptions.CHAT_ASYNC) {
                    return (T) Boolean.FALSE;
                }
                return option.getDefaultValue();
            }

            @Override
            public Integer getInt(String key, Integer defaultValue) {
                return defaultValue;
            }

            @Override
            public Long getLong(String key, Long defaultValue) {
                return defaultValue;
            }

            @Override
            public Float getFloat(String key, Float defaultValue) {
                return defaultValue;
            }

            @Override
            public Double getDouble(String key, Double defaultValue) {
                return defaultValue;
            }

            @Override
            public Boolean getBool(String key, Boolean defaultValue) {
                return defaultValue;
            }

            @Override
            public String getStr(String key, String defaultValue) {
                return defaultValue;
            }
        };
    }

    /**
     * Creates a stateful MemoryObject backed by a HashMap, supporting isExist/get/set operations
     * needed by the retry stats accumulation logic.
     */
    private static MemoryObject createStatefulMemoryObject() {
        Map<String, Object> store = new HashMap<>();

        MemoryObject memoryObject = mock(MemoryObject.class);

        when(memoryObject.isExist(anyString()))
                .thenAnswer(inv -> store.containsKey(inv.<String>getArgument(0)));

        try {
            when(memoryObject.get(anyString()))
                    .thenAnswer(
                            inv -> {
                                String path = inv.getArgument(0);
                                Object value = store.get(path);
                                if (value == null) {
                                    throw new Exception("Path not found: " + path);
                                }
                                MemoryObject valueObj = mock(MemoryObject.class);
                                when(valueObj.getValue()).thenReturn(value);
                                return valueObj;
                            });

            when(memoryObject.set(anyString(), any()))
                    .thenAnswer(
                            inv -> {
                                store.put(inv.getArgument(0), inv.getArgument(1));
                                return null;
                            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return memoryObject;
    }
}
