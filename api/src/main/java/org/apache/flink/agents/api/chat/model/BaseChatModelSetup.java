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
import org.apache.flink.agents.api.metrics.FlinkAgentsMetricGroup;
import org.apache.flink.agents.api.prompt.Prompt;
import org.apache.flink.agents.api.resource.Resource;
import org.apache.flink.agents.api.resource.ResourceContext;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.skills.Skills;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseChatModelSetup extends Resource {
    protected final String connectionName;
    protected String model;
    protected Object prompt;
    protected List<String> toolNames;
    @Nullable protected List<String> skills;
    @Nullable protected String skillDiscoveryPrompt;
    protected List<String> allowedCommands;
    protected List<String> allowedScriptDirs;
    protected StructuredOutputStrategy structuredOutputStrategy;

    @Nullable protected BaseChatModelConnection connection;
    protected final List<Tool> tools = new ArrayList<>();

    public BaseChatModelSetup(ResourceDescriptor descriptor, ResourceContext resourceContext) {
        super(descriptor, resourceContext);
        this.connectionName = descriptor.getArgument("connection");
        this.model = descriptor.getArgument("model");
        this.prompt = descriptor.getArgument("prompt");
        this.toolNames = descriptor.getArgument("tools");
        this.skills = descriptor.getArgument("skills");
        List<String> declaredCommands = descriptor.getArgument("allowed_commands");
        this.allowedCommands =
                declaredCommands == null ? new ArrayList<>() : new ArrayList<>(declaredCommands);
        List<String> declaredScriptDirs = descriptor.getArgument("allowed_script_dirs");
        this.allowedScriptDirs =
                declaredScriptDirs == null
                        ? new ArrayList<>()
                        : new ArrayList<>(declaredScriptDirs);
        this.structuredOutputStrategy =
                StructuredOutputStrategy.fromArgument(
                        descriptor.getArgument("structured_output_strategy"),
                        StructuredOutputStrategy.AUTO);
    }

    /**
     * Trigger construction for resource objects.
     *
     * <p>Currently, in cross-language invocation scenarios, constructing resource object within an
     * async thread may encounter issues. We resolved this issue by moving the construction of the
     * resources object out of the method to be async executed and invoking it in the main thread.
     */
    @Override
    public void open() throws Exception {
        this.connection =
                (BaseChatModelConnection)
                        this.resourceContext.getResource(
                                this.connectionName, ResourceType.CHAT_MODEL_CONNECTION);
        if (this.prompt != null && this.prompt instanceof String) {
            this.prompt =
                    this.resourceContext.getResource((String) this.prompt, ResourceType.PROMPT);
        }
        if (this.skills != null) {
            this.skillDiscoveryPrompt =
                    this.resourceContext.generateAvailableSkillsPrompt(this.skills);
            List<String> mutable =
                    this.toolNames == null ? new ArrayList<>() : new ArrayList<>(this.toolNames);
            if (!mutable.contains(Skills.LOAD_SKILL_TOOL)) {
                mutable.add(Skills.LOAD_SKILL_TOOL);
            }
            if (!mutable.contains(Skills.BASH_TOOL)) {
                mutable.add(Skills.BASH_TOOL);
            }
            this.toolNames = mutable;
        }
        if (this.toolNames != null) {
            for (String name : this.toolNames) {
                this.tools.add((Tool) this.resourceContext.getResource(name, ResourceType.TOOL));
            }
        }
    }

    public abstract Map<String, Object> getParameters();

    /**
     * Record token usage metrics for the given model on the provided metric group.
     *
     * @param metricGroup the non-null metric group captured when the request was initiated
     * @param modelName the name of the model used
     * @param promptTokens the number of prompt tokens
     * @param completionTokens the number of completion tokens
     */
    public void recordTokenMetrics(
            FlinkAgentsMetricGroup metricGroup,
            String modelName,
            long promptTokens,
            long completionTokens) {
        FlinkAgentsMetricGroup modelGroup =
                Preconditions.checkNotNull(metricGroup, "Metric group must not be null.")
                        .getSubGroup("model", modelName);
        modelGroup.getCounter("promptTokens").inc(promptTokens);
        modelGroup.getCounter("completionTokens").inc(completionTokens);
    }

    /**
     * The setup's request-shaping step, shared by {@link #chat} and by the model-routing judge
     * (which must route on exactly what the selected model will receive): renders the bound {@link
     * Prompt} (if any) with the prompt args and prepends it to the non-empty conversation messages,
     * then injects the skill-discovery prompt (if any). Returns the input unchanged when neither is
     * configured.
     */
    public List<ChatMessage> prepareRequestMessages(
            List<ChatMessage> messages, Map<String, Object> promptArgs) {
        // Format input messages if set prompt. Read via the accessor so subclasses that override
        // getPrompt() are honored — the same contract the routing layer inspects.
        Object boundPrompt = getPrompt();
        if (boundPrompt != null) {
            Preconditions.checkState(
                    boundPrompt instanceof Prompt,
                    "Prompt is not initialized. Ensure open() is called before chat().");
            Prompt prompt = (Prompt) boundPrompt;
            Map<String, String> stringified = new HashMap<>();
            if (promptArgs != null) {
                for (Map.Entry<String, Object> entry : promptArgs.entrySet()) {
                    stringified.put(
                            entry.getKey(),
                            entry.getValue() != null ? entry.getValue().toString() : "");
                }
            }

            // append meaningful messages
            List<ChatMessage> promptMessages = prompt.formatMessages(MessageRole.USER, stringified);
            for (ChatMessage message : messages) {
                if ((message.getContent() != null && !message.getContent().isEmpty())
                        || message.getRole() == MessageRole.ASSISTANT) {
                    promptMessages.add(message);
                }
            }
            messages = promptMessages;
        }

        if (this.skillDiscoveryPrompt != null && !this.skillDiscoveryPrompt.isEmpty()) {
            int idx = ChatMessage.findFirstSystemMessage(messages);
            List<ChatMessage> mutated = new ArrayList<>(messages);
            mutated.add(idx + 1, new ChatMessage(MessageRole.SYSTEM, this.skillDiscoveryPrompt));
            messages = mutated;
        }
        return messages;
    }

    public ChatMessage chat(List<ChatMessage> messages) {
        return this.chat(messages, Collections.emptyMap(), Collections.emptyMap());
    }

    public ChatMessage chat(
            List<ChatMessage> messages,
            Map<String, Object> promptArgs,
            Map<String, Object> modelParams) {
        Preconditions.checkNotNull(
                connection,
                "Connection is not initialized. Ensure open() is called before chat().");

        messages = prepareRequestMessages(messages, promptArgs);

        Map<String, Object> params = this.getParameters();
        if (modelParams != null) {
            params.putAll(modelParams);
        }
        return connection.chat(messages, tools, params);
    }

    /**
     * Whether {@code outputSchema} should travel through the provider's native structured output on
     * a call issued through {@link #chatStructured(List, Map, Object)}, rather than be described to
     * the model in the prompt.
     *
     * <p>Framework-facing rather than a user entry point. It is public because the caller that has
     * to choose between those two channels lives outside this package; a user configures the
     * outcome through the structured-output strategy the descriptor carries instead of calling
     * this.
     *
     * <p>The answer composes what the connection reports about a request of that shape, what it
     * reports about the model its own {@link BaseChatModelConnection#effectiveModelFor(Map)} names
     * for such a call, and what the configured strategy makes of the two. Feasibility is asked
     * first, about a request binding no tools and the parameters {@link #getParameters()} returns.
     * Conjoining it is what keeps a capability predicate that answers without consulting the schema
     * from resolving a form its own connection has no translation for; asking it first is what
     * keeps that predicate and {@link BaseChatModelConnection#effectiveModelFor(Map)} from being
     * consulted about such a form at all, which matters because neither contract forbids an
     * override from raising.
     *
     * <p>A {@code true} is not a promise that {@link #chatStructured(List, Map, Object)} returns a
     * response: a connection may still raise once its native branch has decided to apply the
     * schema, as happens where the caller supplied a response format of its own that conflicts with
     * it. Such a failure is that connection's documented answer and reaches the caller as it was
     * raised.
     *
     * <p>Answers about a call rather than issuing one: this method sends no request. What the
     * connection's hooks do when consulted is their own contracts' business.
     *
     * @param outputSchema the schema a call would carry, or null for an unconstrained call, which
     *     is never a native one
     * @return true if the connection reports such a call feasible and the configured strategy,
     *     resolved against the connection's capability for the model its own {@link
     *     BaseChatModelConnection#effectiveModelFor(Map)} names for such a call, calls for a native
     *     schema
     * @throws IllegalArgumentException if the configured strategy is {@link
     *     StructuredOutputStrategy#NATIVE} and the connection could not apply this schema to such a
     *     call at all, which no provider error could report because no request expresses it
     * @throws NullPointerException if {@link #open()} has not bound the connection yet
     */
    public boolean willApplyNativeStructuredOutput(@Nullable Object outputSchema) {
        Preconditions.checkNotNull(
                connection,
                "Connection is not initialized. Ensure open() is called before"
                        + " willApplyNativeStructuredOutput().");
        if (outputSchema == null) {
            return false;
        }

        Map<String, Object> params = getParameters();
        if (!connection.canApplyNativeStructuredOutput(outputSchema, List.of(), params)) {
            if (structuredOutputStrategy == StructuredOutputStrategy.NATIVE) {
                String schemaDescription;
                if (outputSchema instanceof Class) {
                    schemaDescription = ((Class<?>) outputSchema).getName();
                } else if (outputSchema instanceof OutputSchema) {
                    // The wrapper prints identically for every schema it carries, so rendering it
                    // would leave a user unable to tell which one was rejected.
                    schemaDescription = String.valueOf(((OutputSchema) outputSchema).getSchema());
                } else {
                    schemaDescription = outputSchema.getClass().getName();
                }
                throw new IllegalArgumentException(
                        String.format(
                                "Structured output strategy NATIVE was requested, but %s reports"
                                        + " the output schema %s infeasible: no request it builds"
                                        + " can carry that schema. Use AUTO or PROMPT to describe"
                                        + " the schema in the prompt instead, or supply a schema"
                                        + " this connection can translate.",
                                connection.getClass().getName(), schemaDescription));
            }
            return false;
        }
        return structuredOutputStrategy.resolvesToNative(
                connection.supportsNativeStructuredOutput(connection.effectiveModelFor(params)));
    }

    /**
     * Issues one schema-carrying call to the connection, binding no tools and leaving the bound
     * prompt out of the messages, so that the schema is the request's only description of the shape
     * the answer should take.
     *
     * <p>Framework-facing rather than a user entry point. It serves the caller that has already
     * decided, through {@link #willApplyNativeStructuredOutput(Object)}, that the schema travels
     * natively; a user reaches a model through {@link #chat(List, Map, Map)}, which is where a
     * prompt, prompt arguments and tools belong.
     *
     * <p>The messages are sent as given. {@link #chat(List, Map, Map)} prepends the bound prompt
     * whenever one is bound, whatever prompt arguments it is handed, so a second pass over messages
     * that already came through it would repeat that prompt and the skill-discovery message with
     * it. Binding no tools is not a caller's choice either: a provider may drop a native schema
     * from a request that also advertises tools.
     *
     * <p>A schema is required. A null one would reach {@link BaseChatModelConnection#chat(List,
     * List, Map, Object)}, which delegates to the unconstrained overload for a null schema, so the
     * one method whose purpose is to carry a schema would quietly answer without one; {@link
     * #chat(List, Map, Map)} is how a caller asks for that deliberately.
     *
     * <p>A connection may raise from here once its native branch has decided to apply the schema,
     * as happens where the caller supplied a response format of its own that conflicts with it.
     * Such a failure reaches the caller as it was raised rather than becoming an unconstrained
     * response, so what this may throw is not limited to the exceptions listed below.
     *
     * @param messages the conversation to send, used as given
     * @param modelParams parameters for this call alone, merged over {@link #getParameters()} the
     *     same way {@link #chat(List, Map, Map)} merges them, may be null
     * @param outputSchema the schema the call carries, which must not be null
     * @return the connection's response
     * @throws UnsupportedOperationException if the connection has no native translation for {@code
     *     outputSchema}
     * @throws NullPointerException if {@code outputSchema} is null, or if {@link #open()} has not
     *     bound the connection yet
     */
    public ChatMessage chatStructured(
            List<ChatMessage> messages,
            @Nullable Map<String, Object> modelParams,
            Object outputSchema) {
        Preconditions.checkNotNull(
                connection,
                "Connection is not initialized. Ensure open() is called before chatStructured().");
        Preconditions.checkNotNull(
                outputSchema,
                "chatStructured() requires an output schema, which is the one thing it exists to"
                        + " carry. Call chat(List, Map, Map) for an unconstrained request.");

        Map<String, Object> params = getParameters();
        if (modelParams != null) {
            params.putAll(modelParams);
        }
        return connection.chat(messages, List.of(), params, outputSchema);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.CHAT_MODEL;
    }

    @VisibleForTesting
    public String getConnectionName() {
        return this.connectionName;
    }

    /** Returns the configured model or deployment identifier used by this setup. */
    public String getModel() {
        return model;
    }

    @VisibleForTesting
    public Object getPrompt() {
        return prompt;
    }

    @VisibleForTesting
    public List<String> getToolNames() {
        return toolNames;
    }

    @Nullable
    public List<String> getSkills() {
        return skills;
    }

    @Nullable
    public String getSkillDiscoveryPrompt() {
        return skillDiscoveryPrompt;
    }

    public List<String> getAllowedCommands() {
        return allowedCommands;
    }

    public List<String> getAllowedScriptDirs() {
        return allowedScriptDirs;
    }

    /**
     * The configured intent about how an output schema should be applied, defaulting to {@link
     * StructuredOutputStrategy#AUTO}. {@link StructuredOutputStrategy#resolvesToNative(boolean)}
     * combines this policy with the connection's model-dependent capability.
     *
     * @return the structured output strategy
     */
    public StructuredOutputStrategy getStructuredOutputStrategy() {
        return structuredOutputStrategy;
    }
}
