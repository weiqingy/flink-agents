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
package org.apache.flink.agents.api.chat.model.python;

import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.model.BaseChatModelSetup;
import org.apache.flink.agents.api.metrics.FlinkAgentsMetricGroup;
import org.apache.flink.agents.api.resource.ResourceContext;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.python.PythonObjectScope;
import org.apache.flink.agents.api.resource.python.PythonResourceAdapter;
import org.apache.flink.agents.api.resource.python.PythonResourceWrapper;
import pemja.core.object.PyObject;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Python-based implementation of ChatModelSetup that bridges Java and Python chat model
 * functionality. This class wraps a Python chat model setup object and provides Java interface
 * compatibility while delegating actual chat operations to the underlying Python implementation.
 */
public class PythonChatModelSetup extends BaseChatModelSetup implements PythonResourceWrapper {
    static final String FROM_JAVA_CHAT_MESSAGE = "python_java_utils.from_java_chat_message";

    static final String TO_JAVA_CHAT_MESSAGE = "python_java_utils.to_java_chat_message";

    private final PyObject chatModelSetup;
    private final PythonResourceAdapter adapter;
    private boolean closed;

    public PythonChatModelSetup(
            PythonResourceAdapter adapter,
            PyObject chatModelSetup,
            ResourceDescriptor descriptor,
            ResourceContext resourceContext) {
        super(descriptor, resourceContext);
        this.chatModelSetup = chatModelSetup;
        this.adapter = adapter;
    }

    @Override
    public void open() {
        this.adapter.callMethod(chatModelSetup, "open", Collections.emptyMap());
    }

    @Override
    public ChatMessage chat(
            List<ChatMessage> messages,
            Map<String, Object> promptArgs,
            Map<String, Object> modelParams) {
        checkState(
                chatModelSetup != null,
                "ChatModelSetup is not initialized. Cannot perform chat operation.");

        Map<String, Object> kwargs = new HashMap<>(modelParams);

        try (PythonObjectScope scope = new PythonObjectScope()) {
            List<Object> pythonMessages = new ArrayList<>();
            for (ChatMessage message : messages) {
                pythonMessages.add(scope.own(adapter.toPythonChatMessage(message)));
            }

            kwargs.put("messages", pythonMessages);
            kwargs.put("prompt_args", promptArgs != null ? promptArgs : Collections.emptyMap());

            Object pythonMessageResponse =
                    scope.own(adapter.callMethod(chatModelSetup, "chat", kwargs));
            return adapter.fromPythonChatMessage(pythonMessageResponse);
        }
    }

    /**
     * Always false: no call this class issues can carry an output schema, so for none of them
     * should one travel natively.
     *
     * <p>Two things put that out of reach rather than one. {@link #open()} here calls the Python
     * setup's own {@code open} and binds no connection, so the inherited body would have nothing to
     * ask; and {@link #chat(List, Map, Map)} above carries only messages and prompt arguments
     * across the bridge, so a schema has no way to travel with the call it would constrain.
     *
     * <p>False rather than a refusal, because the answer decides whether a caller keeps describing
     * the schema in the prompt, and keeping it is the outcome that works here. The configured
     * strategy is not consulted for the same reason: a request for native structured output
     * recorded on this side names nothing this class could apply it to.
     *
     * @param outputSchema the schema the call would carry, which this setup cannot carry
     * @return false
     */
    @Override
    public boolean willApplyNativeStructuredOutput(@Nullable Object outputSchema) {
        return false;
    }

    /**
     * Always refuses, because the bridge has no way to carry {@code outputSchema} to the Python
     * setup: {@link #chat(List, Map, Map)} puts messages and prompt arguments into the call's
     * keyword arguments and nothing else.
     *
     * <p>Refusing rather than dropping the schema and calling anyway, so that an unconstrained
     * response can never be mistaken for a schema-conforming one.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public ChatMessage chatStructured(
            List<ChatMessage> messages,
            @Nullable Map<String, Object> modelParams,
            Object outputSchema) {
        throw new UnsupportedOperationException(
                "A Python chat model setup cannot be given an output schema from Java: the bridge"
                        + " carries only messages and prompt arguments to the Python setup's chat."
                        + " Apply the schema on the Python side instead.");
    }

    @Override
    public Object getPythonResource() {
        return chatModelSetup;
    }

    @Override
    public PythonResourceAdapter getPythonResourceAdapter() {
        return adapter;
    }

    @Override
    public void setMetricGroup(FlinkAgentsMetricGroup metricGroup) {
        super.setMetricGroup(metricGroup);
        setPythonResourceMetricGroup(metricGroup);
    }

    @Override
    public Map<String, Object> getParameters() {
        return Map.of();
    }

    @Override
    public void close() throws Exception {
        if (closed || chatModelSetup == null) {
            return;
        }
        closed = true;
        try (chatModelSetup) {
            adapter.callMethod(chatModelSetup, "close", Map.of());
        }
    }
}
