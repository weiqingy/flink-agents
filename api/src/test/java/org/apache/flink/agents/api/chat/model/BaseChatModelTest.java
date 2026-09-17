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

package org.apache.flink.agents.api.chat.model;

import org.apache.flink.agents.api.agents.OutputSchema;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.prompt.Prompt;
import org.apache.flink.agents.api.resource.ResourceContext;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.agents.api.tools.ToolMetadata;
import org.apache.flink.agents.api.tools.ToolParameters;
import org.apache.flink.agents.api.tools.ToolResponse;
import org.apache.flink.agents.api.tools.ToolType;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for BaseChatModel class, Tests chat model functionality, prompt processing, and
 * response generation.
 */
class BaseChatModelTest {

    private TestChatModel chatModel;
    private Prompt simplePrompt;
    private Prompt conversationPrompt;

    /** Test implementation of BaseChatModel for testing purposes. */
    private static class TestChatModel extends BaseChatModelSetup {
        private String responsePrefix = "Test Response: ";

        public TestChatModel(ResourceDescriptor descriptor, ResourceContext resourceContext) {
            super(descriptor, resourceContext);
        }

        @Override
        public Map<String, Object> getParameters() {
            return Map.of();
        }

        @Override
        public ChatMessage chat(
                List<ChatMessage> messages,
                Map<String, Object> promptArgs,
                Map<String, Object> modelParams) {
            // Simple test implementation that echoes the last user message

            String lastUserContent = "";
            for (ChatMessage message : messages) {
                if (message.getRole() == MessageRole.USER) {
                    lastUserContent = message.getContent();
                }
            }

            if (lastUserContent.isEmpty()) {
                lastUserContent = "No user message found";
            }

            return new ChatMessage(MessageRole.ASSISTANT, responsePrefix + lastUserContent);
        }

        public void setResponsePrefix(String prefix) {
            this.responsePrefix = prefix;
        }
    }

    @BeforeEach
    void setUp() {
        chatModel =
                new TestChatModel(
                        new ResourceDescriptor(
                                TestChatModel.class.getName(), Collections.emptyMap()),
                        null);

        // Create simple prompt
        simplePrompt = Prompt.fromText("You are a helpful assistant. User says: {user_input}");

        // Create conversation prompt
        List<ChatMessage> conversationTemplate =
                Arrays.asList(
                        new ChatMessage(MessageRole.SYSTEM, "You are a helpful AI assistant."),
                        new ChatMessage(MessageRole.USER, "{user_message}"));
        conversationPrompt = Prompt.fromMessages(conversationTemplate);
    }

    @Test
    @DisplayName("Test ChatModel resource type")
    void testChatModelResourceType() {
        assertEquals(ResourceType.CHAT_MODEL, chatModel.getResourceType());
    }

    @Test
    @DisplayName("Test basic chat functionality")
    void testBasicChat() {
        Map<String, String> variables = new HashMap<>();
        variables.put("user_input", "Hello, how are you?");

        // Format the prompt with variables
        Prompt formattedPrompt =
                Prompt.fromMessages(simplePrompt.formatMessages(MessageRole.SYSTEM, variables));

        ChatMessage response =
                chatModel.chat(formattedPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertNotNull(response);
        assertEquals(MessageRole.ASSISTANT, response.getRole());
        assertTrue(response.getContent().contains("Test Response:"));
    }

    @Test
    @DisplayName("Test chat with conversation prompt")
    void testChatWithConversationPrompt() {
        Map<String, String> variables = new HashMap<>();
        variables.put("user_message", "What's the weather like?");

        Prompt formattedPrompt =
                Prompt.fromMessages(
                        conversationPrompt.formatMessages(MessageRole.SYSTEM, variables));

        ChatMessage response =
                chatModel.chat(formattedPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertNotNull(response);
        assertEquals(MessageRole.ASSISTANT, response.getRole());
        assertTrue(response.getContent().contains("What's the weather like?"));
    }

    @Test
    @DisplayName("Test chat with empty prompt")
    void testChatWithEmptyPrompt() {
        Prompt emptyPrompt = Prompt.fromText("");

        ChatMessage response =
                chatModel.chat(emptyPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertNotNull(response);
        assertEquals(MessageRole.ASSISTANT, response.getRole());
        assertTrue(response.getContent().contains("No user message found"));
    }

    @Test
    @DisplayName("Test chat with multiple user messages")
    void testChatWithMultipleUserMessages() {
        List<ChatMessage> multipleMessages =
                Arrays.asList(
                        new ChatMessage(MessageRole.SYSTEM, "You are a helpful assistant."),
                        new ChatMessage(MessageRole.USER, "First message"),
                        new ChatMessage(MessageRole.ASSISTANT, "I understand"),
                        new ChatMessage(
                                MessageRole.USER, "Second message - this should be the response"));

        Prompt multiPrompt = Prompt.fromMessages(multipleMessages);

        ChatMessage response =
                chatModel.chat(multiPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertNotNull(response);
        assertTrue(response.getContent().contains("Second message - this should be the response"));
    }

    @Test
    @DisplayName("Test chat model configuration")
    void testChatModelConfiguration() {
        chatModel.setResponsePrefix("Custom Response: ");

        Map<String, String> variables = new HashMap<>();
        variables.put("user_input", "Test message");

        Prompt formattedPrompt =
                Prompt.fromMessages(simplePrompt.formatMessages(MessageRole.SYSTEM, variables));

        ChatMessage response =
                chatModel.chat(formattedPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertTrue(response.getContent().startsWith("Custom Response:"));
    }

    @Test
    @DisplayName("Test chat with system-only prompt")
    void testChatWithSystemOnlyPrompt() {
        Prompt systemOnlyPrompt =
                Prompt.fromMessages(
                        Arrays.asList(
                                new ChatMessage(MessageRole.SYSTEM, "System instruction only")));

        ChatMessage response =
                chatModel.chat(systemOnlyPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertNotNull(response);
        assertEquals(MessageRole.ASSISTANT, response.getRole());
        assertTrue(response.getContent().contains("No user message found"));
    }

    @Test
    @DisplayName("Test chat response format")
    void testChatResponseFormat() {
        Map<String, String> variables = new HashMap<>();
        variables.put("user_input", "Format test");

        Prompt formattedPrompt =
                Prompt.fromMessages(simplePrompt.formatMessages(MessageRole.SYSTEM, variables));

        ChatMessage response =
                chatModel.chat(formattedPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        // Verify response structure
        assertNotNull(response.getRole());
        assertNotNull(response.getContent());
        assertNotNull(response.getToolCalls());
        assertNotNull(response.getExtraArgs());
        assertTrue(response.getContent().length() > 0);
    }

    /** Connection that captures the messages passed to it for assertions. */
    private static class RecordingConnection extends BaseChatModelConnection {
        List<ChatMessage> capturedMessages;
        Map<String, Object> capturedModelParams;

        RecordingConnection() {
            super(
                    new ResourceDescriptor(
                            RecordingConnection.class.getName(), Collections.emptyMap()),
                    null);
        }

        @Override
        public ChatMessage chat(
                List<ChatMessage> messages, List<Tool> tools, Map<String, Object> modelParams) {
            this.capturedMessages = new ArrayList<>(messages);
            this.capturedModelParams = new HashMap<>(modelParams);
            return new ChatMessage(MessageRole.ASSISTANT, "ok");
        }
    }

    /**
     * Connection whose feasibility and capability answers are scripted, recording what it was asked
     * and what a schema-carrying call carried. It leaves the three-argument chat() and the two
     * queries of {@link RecordingConnection} in place for every other test.
     */
    private static class GateConnection extends RecordingConnection {
        private final boolean feasible;
        private final boolean capable;

        boolean feasibilityAsked;
        boolean capabilityAsked;
        Object askedSchema;
        List<Tool> askedTools;
        Map<String, Object> askedModelParams;
        String askedModel;

        List<ChatMessage> structuredMessages;
        List<Tool> structuredTools;
        Map<String, Object> structuredModelParams;
        Object structuredSchema;

        GateConnection(boolean feasible, boolean capable) {
            this.feasible = feasible;
            this.capable = capable;
        }

        @Override
        protected boolean canApplyNativeStructuredOutput(
                @Nullable Object outputSchema,
                @Nullable List<Tool> tools,
                @Nullable Map<String, Object> modelParams) {
            this.feasibilityAsked = true;
            this.askedSchema = outputSchema;
            this.askedTools = tools == null ? null : new ArrayList<>(tools);
            this.askedModelParams = modelParams == null ? null : new HashMap<>(modelParams);
            return feasible;
        }

        /**
         * Deliberately not the {@code model} parameter. A gate that read the map itself instead of
         * asking this hook would still look correct against a connection that inherits the base
         * body, which is exactly {@code modelParams.get("model")}.
         */
        @Override
        protected String effectiveModelFor(@Nullable Map<String, Object> modelParams) {
            return "backing-model";
        }

        @Override
        protected boolean supportsNativeStructuredOutput(String effectiveModel) {
            this.capabilityAsked = true;
            this.askedModel = effectiveModel;
            return capable;
        }

        @Override
        public ChatMessage chat(
                List<ChatMessage> messages,
                List<Tool> tools,
                Map<String, Object> modelParams,
                @Nullable Object outputSchema) {
            this.structuredMessages = new ArrayList<>(messages);
            this.structuredTools = new ArrayList<>(tools);
            this.structuredModelParams = new HashMap<>(modelParams);
            this.structuredSchema = outputSchema;
            return new ChatMessage(MessageRole.ASSISTANT, "structured");
        }
    }

    /** Tool with no behavior, for asserting what a request binds rather than what a tool does. */
    private static class StubTool extends Tool {
        StubTool(String name) {
            super(new ToolMetadata(name, "stub", "{}"));
        }

        @Override
        public ToolType getToolType() {
            return ToolType.FUNCTION;
        }

        @Override
        public ToolResponse call(ToolParameters parameters) {
            return ToolResponse.success("");
        }
    }

    /** Subclass that exposes setters so we can inject the connection and prompt directly. */
    private static class RecordingChatModelSetup extends BaseChatModelSetup {
        final Map<String, Object> parameters = new HashMap<>();

        RecordingChatModelSetup(BaseChatModelConnection connection, Prompt prompt) {
            super(
                    new ResourceDescriptor(
                            RecordingChatModelSetup.class.getName(), Collections.emptyMap()),
                    null);
            this.connection = connection;
            this.prompt = prompt;
        }

        RecordingChatModelSetup(
                BaseChatModelConnection connection,
                Prompt prompt,
                StructuredOutputStrategy strategy) {
            this(connection, prompt);
            this.structuredOutputStrategy = strategy;
        }

        @Override
        public Map<String, Object> getParameters() {
            return new HashMap<>(parameters);
        }
    }

    @Test
    @DisplayName("chat() fills prompt template from promptArgs parameter")
    void testChatFillsTemplateFromPromptArgsParameter() {
        RecordingConnection connection = new RecordingConnection();
        Prompt prompt = Prompt.fromText("Task: {key}");
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, prompt);

        setup.chat(Collections.emptyList(), Map.of("key", "value"), Map.of());

        assertNotNull(connection.capturedMessages);
        assertEquals(1, connection.capturedMessages.size());
        assertEquals("Task: value", connection.capturedMessages.get(0).getContent());
    }

    @Test
    @DisplayName("chat() does not read template vars from ChatMessage.extraArgs")
    void testChatDoesNotReadTemplateVarsFromExtraArgs() {
        RecordingConnection connection = new RecordingConnection();
        Prompt prompt = Prompt.fromText("Task: {key}");
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, prompt);

        ChatMessage userMessage =
                new ChatMessage(MessageRole.USER, "hello", Map.of("key", "value"));
        setup.chat(List.of(userMessage), Map.of(), Map.of());

        assertNotNull(connection.capturedMessages);
        assertEquals(2, connection.capturedMessages.size());
        assertEquals("Task: {key}", connection.capturedMessages.get(0).getContent());
        assertEquals("hello", connection.capturedMessages.get(1).getContent());
    }

    @Test
    @DisplayName("chat() re-fills prompt template on subsequent invocations when args supplied")
    void testChatRefillsTemplateOnSubsequentInvocations() {
        RecordingConnection connection = new RecordingConnection();
        Prompt prompt = Prompt.fromText("Task: {key}");
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, prompt);

        setup.chat(Collections.emptyList(), Map.of("key", "v1"), Map.of());
        assertNotNull(connection.capturedMessages);
        assertEquals(1, connection.capturedMessages.size());
        assertEquals("Task: v1", connection.capturedMessages.get(0).getContent());

        ChatMessage toolResponse = new ChatMessage(MessageRole.TOOL, "tool result");
        setup.chat(List.of(toolResponse), Map.of("key", "v1"), Map.of());
        assertEquals(2, connection.capturedMessages.size());
        assertEquals("Task: v1", connection.capturedMessages.get(0).getContent());
        assertEquals("tool result", connection.capturedMessages.get(1).getContent());
    }

    @Test
    @DisplayName("Default chat() overload rejects an outputSchema it cannot translate")
    void testDefaultChatOverloadRejectsOutputSchema() {
        RecordingConnection connection = new RecordingConnection();

        // Dropping the schema instead would return an unconstrained response that the
        // caller has no way to tell apart from a schema-conforming one.
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        connection.chat(
                                List.of(new ChatMessage(MessageRole.USER, "hi")),
                                List.of(),
                                new HashMap<>(),
                                new Object()));

        // The rejection has to precede the delegation: a delegate-then-throw ordering
        // would still issue a real provider request before failing.
        assertNull(connection.capturedMessages);
    }

    @Test
    @DisplayName("Default chat() overload delegates to the 3-arg chat() for a null outputSchema")
    void testDefaultChatOverloadDelegatesForNullOutputSchema() {
        RecordingConnection connection = new RecordingConnection();
        Map<String, Object> modelParams = new HashMap<>();
        modelParams.put("temperature", 0.5);

        ChatMessage response =
                connection.chat(
                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                        List.of(),
                        modelParams,
                        null);

        // The 3-arg chat() ran (it is what produces "ok") and the overload added nothing
        // to modelParams that could travel on to a provider SDK request.
        assertEquals("ok", response.getContent());
        assertEquals(Map.of("temperature", 0.5), connection.capturedModelParams);
    }

    @Test
    @DisplayName("Default capability predicate reports no native structured output for any model")
    void testDefaultCapabilityPredicateIsFalse() {
        RecordingConnection connection = new RecordingConnection();

        assertFalse(connection.supportsNativeStructuredOutput("gpt-4o"));
        assertFalse(connection.supportsNativeStructuredOutput("gpt-3.5-turbo"));
        assertFalse(connection.supportsNativeStructuredOutput(null));
    }

    @Test
    @DisplayName("Default feasibility query reports no schema applicable on any request")
    void testDefaultFeasibilityPredicateIsFalse() {
        RecordingConnection connection = new RecordingConnection();
        Map<String, Object> modelParams = new HashMap<>();
        modelParams.put("model", "gpt-4o");

        // Both forms a schema arrives in: a POJO class, and a wrapper a connection would have
        // to unwrap before it could translate anything.
        assertFalse(
                connection.canApplyNativeStructuredOutput(String.class, List.of(), modelParams));
        assertFalse(
                connection.canApplyNativeStructuredOutput(
                        new OutputSchema(
                                new RowTypeInfo(
                                        new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO},
                                        new String[] {"name"})),
                        List.of(),
                        modelParams));
    }

    @Test
    @DisplayName("Feasibility query accepts a null schema and empty tools without raising")
    void testDefaultFeasibilityPredicateAcceptsNullSchemaAndEmptyTools() {
        RecordingConnection connection = new RecordingConnection();

        // An unconstrained request is an ordinary input to ask about, not a misuse. The
        // immutable map catches only a write that raises, which is why silent consumption
        // has a test of its own.
        assertFalse(connection.canApplyNativeStructuredOutput(null, List.of(), Map.of()));
    }

    @Test
    @DisplayName("Feasibility query leaves the parameters a request would be built from intact")
    void testDefaultFeasibilityPredicateDoesNotConsumeModelParams() {
        RecordingConnection connection = new RecordingConnection();
        Map<String, Object> modelParams = new HashMap<>();
        modelParams.put("model", "gpt-4o");
        modelParams.put("temperature", 0.5);

        connection.canApplyNativeStructuredOutput(String.class, List.of(), modelParams);

        // The same map goes on to build the request the answer was about, so a query that
        // took a key out of it would answer about one request and build another.
        assertEquals(Map.of("model", "gpt-4o", "temperature", 0.5), modelParams);
    }

    @Test
    @DisplayName("Feasibility query accepts a null tool list and null parameters without raising")
    void testDefaultFeasibilityPredicateAcceptsNullToolsAndNullModelParams() {
        RecordingConnection connection = new RecordingConnection();

        // Both are reachable from a request builder: a request binding no tools may carry a null
        // list rather than an empty one, and a builder handed null parameters asks with the same
        // null it was handed.
        assertFalse(connection.canApplyNativeStructuredOutput(String.class, null, null));
    }

    @Test
    @DisplayName("chatStructured() refuses a null schema rather than issuing an ordinary call")
    void testChatStructuredRejectsNullSchema() {
        GateConnection connection = new GateConnection(true, true);
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, null);

        NullPointerException thrown =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                setup.chatStructured(
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        null));
        assertTrue(thrown.getMessage().contains("chatStructured"));

        // Without the check the schema-carrying overload would have delegated to the
        // unconstrained one, so the caller would receive an ordinary response from the one
        // method whose purpose is to carry a schema. Neither overload was reached.
        assertNull(connection.structuredSchema);
        assertNull(connection.capturedMessages);
    }

    private static OutputSchema rowTypeInfoSchema() {
        return new OutputSchema(
                new RowTypeInfo(
                        new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO},
                        new String[] {"name"}));
    }

    /**
     * Policy x capability x feasibility, one case per row, with the schema form carried as the
     * value the gate is asked about. A POJO class stands for a form a connection can translate and
     * the RowTypeInfo wrapper for one none of them can.
     */
    private static Stream<Arguments> structuredOutputBehaviorRows() {
        return Stream.of(
                Arguments.of(1, StructuredOutputStrategy.AUTO, String.class, true, true, true),
                Arguments.of(2, StructuredOutputStrategy.AUTO, String.class, true, false, false),
                Arguments.of(3, StructuredOutputStrategy.AUTO, String.class, false, true, false),
                Arguments.of(
                        4, StructuredOutputStrategy.AUTO, rowTypeInfoSchema(), false, true, false),
                Arguments.of(
                        5, StructuredOutputStrategy.AUTO, rowTypeInfoSchema(), false, false, false),
                Arguments.of(6, StructuredOutputStrategy.PROMPT, String.class, true, true, false),
                Arguments.of(7, StructuredOutputStrategy.NATIVE, String.class, true, true, true),
                Arguments.of(8, StructuredOutputStrategy.NATIVE, String.class, true, false, true));
    }

    @ParameterizedTest(name = "row {0}")
    @MethodSource("structuredOutputBehaviorRows")
    @DisplayName("The gate composes the configured policy, the model's capability and feasibility")
    void testWillApplyNativeCombinesPolicyCapabilityAndFeasibility(
            int row,
            StructuredOutputStrategy strategy,
            Object outputSchema,
            boolean feasible,
            boolean capable,
            boolean expected) {
        GateConnection connection = new GateConnection(feasible, capable);
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, null, strategy);
        setup.parameters.put("model", "gpt-4o");
        // Bound so the empty-tools assertion below compares against something. A ReAct agent
        // always binds tools, so a gate that asked feasibility with them would make a provider
        // that skips a native schema on a tool-carrying request report every such request
        // infeasible, and native structured output would silently never fire there.
        setup.tools.add(new StubTool("lookup"));

        assertEquals(expected, setup.willApplyNativeStructuredOutput(outputSchema));

        // Asked about the schema as handed over, and about a call binding no tools with the
        // parameters the setup would build the call from - the shape chatStructured sends.
        assertSame(outputSchema, connection.askedSchema);
        assertEquals(List.of(), connection.askedTools);
        assertEquals(Map.of("model", "gpt-4o"), connection.askedModelParams);
    }

    @Test
    @DisplayName("The gate asks about the model effectiveModelFor names, not the configured one")
    void testWillApplyNativeAsksCapabilityAboutTheEffectiveModel() {
        GateConnection connection = new GateConnection(true, true);
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, null);
        setup.parameters.put("model", "gpt-4o");
        setup.model = "a-deployment-name";

        assertTrue(setup.willApplyNativeStructuredOutput(String.class));

        // Three identities are kept distinct on purpose: the configured "a-deployment-name", the
        // "gpt-4o" in the parameters, and what the connection's own hook returns. On a
        // deployment-based provider capability belongs to the model behind the deployment, so a
        // gate reading either of the first two misclassifies it in both directions.
        assertEquals("backing-model", connection.askedModel);
    }

    @Test
    @DisplayName("A schema no request can express is not put to the capability predicate")
    void testWillApplyNativeDoesNotConsultCapabilityForAnInfeasibleSchema() {
        GateConnection connection = new GateConnection(false, true);
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, null);

        assertFalse(setup.willApplyNativeStructuredOutput(rowTypeInfoSchema()));

        // Feasibility is asked first so that a predicate answering without consulting the
        // schema cannot decide a form its own connection has no translation for.
        assertTrue(connection.feasibilityAsked);
        assertFalse(connection.capabilityAsked);
    }

    @Test
    @DisplayName("A call carrying no schema is never a native one, whatever the policy")
    void testWillApplyNativeIsFalseForNullSchema() {
        GateConnection connection = new GateConnection(true, true);
        RecordingChatModelSetup setup =
                new RecordingChatModelSetup(connection, null, StructuredOutputStrategy.NATIVE);

        assertFalse(setup.willApplyNativeStructuredOutput(null));

        // There is nothing to apply, so neither question arises and NATIVE has nothing to fail
        // fast about.
        assertFalse(connection.feasibilityAsked);
        assertFalse(connection.capabilityAsked);
    }

    @Test
    @DisplayName("A forced NATIVE fails fast on a schema the connection cannot apply at all")
    void testWillApplyNativeThrowsForNativePolicyOnInfeasibleSchema() {
        GateConnection connection = new GateConnection(false, true);
        RecordingChatModelSetup setup =
                new RecordingChatModelSetup(connection, null, StructuredOutputStrategy.NATIVE);

        IllegalArgumentException wrapped =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> setup.willApplyNativeStructuredOutput(rowTypeInfoSchema()));
        IllegalArgumentException pojo =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> setup.willApplyNativeStructuredOutput(String.class));

        // No provider error could report this, because no request expresses the schema in the
        // first place, so the message carries both sides of the mismatch itself.
        assertTrue(wrapped.getMessage().contains(GateConnection.class.getName()));
        // The shape rather than the wrapper: every RowTypeInfo prints as the same wrapper class
        // name, which would tell a user nothing about which schema was rejected.
        assertTrue(wrapped.getMessage().contains(rowTypeInfoSchema().getSchema().toString()));
        assertFalse(wrapped.getMessage().contains(OutputSchema.class.getName()));
        assertTrue(pojo.getMessage().contains("java.lang.String"));
    }

    @Test
    @DisplayName("A forced NATIVE on an incapable model sends the schema rather than withholding")
    void testNativePolicyOnAnIncapableModelSendsTheSchema() {
        GateConnection connection = new GateConnection(true, false);
        RecordingChatModelSetup setup =
                new RecordingChatModelSetup(connection, null, StructuredOutputStrategy.NATIVE);

        assertTrue(setup.willApplyNativeStructuredOutput(String.class));

        setup.chatStructured(
                List.of(new ChatMessage(MessageRole.USER, "hi")), Map.of(), String.class);

        // The schema travels with no second capability test at this level, so an explicit
        // intent reaches the provider and a provider error is what answers it.
        assertSame(String.class, connection.structuredSchema);
    }

    @Test
    @DisplayName("chatStructured() binds no tools and does not prepend the bound prompt")
    void testChatStructuredSendsNoToolsAndDoesNotPrependTheBoundPrompt() {
        GateConnection connection = new GateConnection(true, true);
        RecordingChatModelSetup setup =
                new RecordingChatModelSetup(connection, Prompt.fromText("Task: {key}"));
        setup.tools.add(new StubTool("lookup"));

        ChatMessage response =
                setup.chatStructured(
                        List.of(new ChatMessage(MessageRole.USER, "hi")), Map.of(), String.class);

        assertEquals("structured", response.getContent());
        // Bound tools make some providers drop a native schema outright, and chat() prepends
        // the bound prompt on the prompt alone, so a second pass would repeat it.
        assertEquals(List.of(), connection.structuredTools);
        assertEquals(1, connection.structuredMessages.size());
        assertEquals("hi", connection.structuredMessages.get(0).getContent());
        assertSame(String.class, connection.structuredSchema);
    }

    @Test
    @DisplayName("chatStructured() merges per-call parameters over the setup's own")
    void testChatStructuredMergesModelParamsOverTheSetupParameters() {
        GateConnection connection = new GateConnection(true, true);
        RecordingChatModelSetup setup = new RecordingChatModelSetup(connection, null);
        setup.parameters.put("model", "gpt-4o");
        setup.parameters.put("temperature", 0.1);
        List<ChatMessage> messages = List.of(new ChatMessage(MessageRole.USER, "hi"));

        setup.chatStructured(messages, Map.of("temperature", 0.9), String.class);
        assertEquals(
                Map.of("model", "gpt-4o", "temperature", 0.9), connection.structuredModelParams);

        // A caller with nothing to override may pass null rather than an empty map.
        setup.chatStructured(messages, null, String.class);
        assertEquals(
                Map.of("model", "gpt-4o", "temperature", 0.1), connection.structuredModelParams);
    }

    @Test
    @DisplayName("The gate requires open() to have bound the connection")
    void testWillApplyNativeRequiresOpen() {
        RecordingChatModelSetup setup = new RecordingChatModelSetup(null, null);

        NullPointerException thrown =
                assertThrows(
                        NullPointerException.class,
                        () -> setup.willApplyNativeStructuredOutput(String.class));
        assertTrue(thrown.getMessage().contains("Connection is not initialized"));
    }

    @Test
    @DisplayName("chatStructured() requires open() to have bound the connection")
    void testChatStructuredRequiresOpen() {
        RecordingChatModelSetup setup = new RecordingChatModelSetup(null, null);

        NullPointerException thrown =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                setup.chatStructured(
                                        List.of(new ChatMessage(MessageRole.USER, "hi")),
                                        Map.of(),
                                        String.class));
        assertTrue(thrown.getMessage().contains("Connection is not initialized"));
    }

    @Test
    @DisplayName("Structured-output strategy defaults to AUTO when the descriptor omits it")
    void testStructuredOutputStrategyDefaultsToAuto() {
        RecordingChatModelSetup setup =
                new RecordingChatModelSetup(new RecordingConnection(), null);

        assertEquals(StructuredOutputStrategy.AUTO, setup.getStructuredOutputStrategy());
    }

    @Test
    @DisplayName("Structured-output strategy defaults to AUTO when the descriptor argument is null")
    void testStructuredOutputStrategyDefaultsToAutoForNullArgument() {
        // A descriptor argument present with a null value is indistinguishable from an
        // absent one here, so it resolves to the same default rather than failing.
        TestChatModel model =
                new TestChatModel(
                        new ResourceDescriptor(
                                TestChatModel.class.getName(),
                                Collections.singletonMap("structured_output_strategy", null)),
                        null);

        assertEquals(StructuredOutputStrategy.AUTO, model.getStructuredOutputStrategy());
    }

    @Test
    @DisplayName("Structured-output strategy is read from the descriptor argument")
    void testStructuredOutputStrategyReadFromDescriptor() {
        TestChatModel model =
                new TestChatModel(
                        new ResourceDescriptor(
                                TestChatModel.class.getName(),
                                Map.of("structured_output_strategy", "native")),
                        null);

        assertEquals(StructuredOutputStrategy.NATIVE, model.getStructuredOutputStrategy());
    }

    @Test
    @DisplayName("An unrecognized structured-output strategy is rejected instead of defaulting")
    void testUnknownStructuredOutputStrategyRejected() {
        ResourceDescriptor descriptor =
                new ResourceDescriptor(
                        TestChatModel.class.getName(),
                        Map.of("structured_output_strategy", "bogus"));

        assertThrows(IllegalArgumentException.class, () -> new TestChatModel(descriptor, null));
    }

    @Test
    @DisplayName("AUTO resolves to native only when the effective model is capable")
    void testAutoStrategyResolvesToNativeOnlyWhenCapable() {
        assertTrue(StructuredOutputStrategy.AUTO.resolvesToNative(true));
        assertFalse(StructuredOutputStrategy.AUTO.resolvesToNative(false));
    }

    @Test
    @DisplayName("NATIVE forces native even when the model is not capable")
    void testNativeStrategyForcesNativeRegardlessOfCapability() {
        assertTrue(StructuredOutputStrategy.NATIVE.resolvesToNative(false));
    }

    @Test
    @DisplayName("PROMPT never resolves to native even when the model is capable")
    void testPromptStrategyNeverResolvesToNative() {
        assertFalse(StructuredOutputStrategy.PROMPT.resolvesToNative(true));
    }

    @Test
    @DisplayName("Test chat with long input")
    void testChatWithLongInput() {
        StringBuilder longInput = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longInput.append("This is a long message part ").append(i).append(". ");
        }

        Map<String, String> variables = new HashMap<>();
        variables.put("user_input", longInput.toString());

        Prompt formattedPrompt =
                Prompt.fromMessages(simplePrompt.formatMessages(MessageRole.SYSTEM, variables));

        ChatMessage response =
                chatModel.chat(formattedPrompt.formatMessages(MessageRole.USER, new HashMap<>()));

        assertNotNull(response);
        assertTrue(response.getContent().length() > 0);
    }

    @Test
    @DisplayName("Effective model defaults to the model parameter a request would be built from")
    void testEffectiveModelForReadsTheModelParameter() {
        RecordingConnection connection = new RecordingConnection();
        Map<String, Object> modelParams = new HashMap<>();
        modelParams.put("model", "gpt-4o");

        assertEquals("gpt-4o", connection.effectiveModelFor(modelParams));
    }

    @Test
    @DisplayName("Effective model is null when the parameters name no model")
    void testEffectiveModelForReturnsNullWhenModelParameterAbsent() {
        RecordingConnection connection = new RecordingConnection();

        // A connection carrying no default of its own has no model to resolve, and the capability
        // predicate reports a null model not capable rather than throwing.
        assertNull(connection.effectiveModelFor(Map.of("temperature", 0.5)));
    }

    @Test
    @DisplayName("Effective model is null for null parameters")
    void testEffectiveModelForReturnsNullForNullParameters() {
        RecordingConnection connection = new RecordingConnection();

        assertNull(connection.effectiveModelFor(null));
    }
}
