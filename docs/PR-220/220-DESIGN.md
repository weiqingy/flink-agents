# Design — #220: Separate messages and arguments for prompt of `chat()`

## Problem Statement

`BaseChatModelSetup.chat()` (Java + Python) currently fills prompt-template variables by
flattening every input message's `extra_args` field into a single `Map<String, String>` /
dict and passing it to `Prompt.formatMessages`. This conflates two distinct concepts in one
field — chat-message metadata and prompt-template arguments — making the API surprising and
forcing callers to stuff template variables into a generic metadata bag. The fix is to take
prompt template arguments as an explicit, dedicated parameter on `chat()` and through the
event/action plumbing that feeds it.

## Approach Candidates

### Approach A — Explicit `arguments` parameter on `chat()` + new field on `ChatRequestEvent`

Add `arguments: Map<String, Object>` (Java) / `Mapping[str, Any]` (Python) as a dedicated
parameter on `BaseChatModelSetup.chat(...)`. Propagate the same field through
`ChatRequestEvent` so the runtime can plumb arguments from event emission, through
`ChatModelAction`, to the setup. Stop reading `message.extra_args` for template filling
entirely.

**Pros**
- Matches the universal external pattern (LangChain, LangChain4j, LlamaIndex, Semantic
  Kernel) — see `research-external.md`. Familiar to anyone coming from another framework.
- Self-documenting at the call site: `chat(messages, arguments, parameters)` separates
  message history, template variables, and model parameters into three explicit channels.
- Honors the `Prompt` resource as a first-class abstraction owned by `BaseChatModelSetup` —
  the runtime continues to fill prompts; callers don't pre-format.
- Localized change. `BaseChatModelConnection` and chat-model integrations are untouched —
  the connection sees fully-formatted messages, same as today.
- `extra_args` remains for its legitimate uses (`externalId`, `STRUCTURED_OUTPUT`,
  `refusal`, `reasoning`) — no risk to provider integrations.

**Cons**
- Breaking change for callers that currently set `extra_args` for template filling — 6 files
  in this repo (3 Java examples, 3 Python examples) plus 3 tests. Migration footprint is
  small and explicitly enumerated in `investigate.md`.
- Adds a parameter to `ChatRequestEvent`. Event-shape changes are coarser-grained than
  method signatures.

### Approach B — Caller pre-formats prompts before emitting `ChatRequestEvent`

Make the user responsible for calling `prompt.formatMessages(arguments)` themselves and
passing the resulting `List<ChatMessage>` into `ChatRequestEvent`. Drop the `Prompt`
resource from `BaseChatModelSetup` entirely (or keep it as a no-op).

**Pros**
- Simpler signature on `chat()` — no new parameter.
- No `ChatRequestEvent` shape change.

**Cons**
- Defeats the abstraction. The `Prompt` resource exists precisely so the runtime owns the
  formatting; pushing it to user code undoes the design.
- Worse ergonomics: every caller becomes responsible for boilerplate that used to be one
  field on a constructor.
- Doesn't align with external prior art — every surveyed framework keeps template
  substitution close to the LLM call, not in user code.

### Approach C — Introduce a dedicated message kind for prompt-template values

Add a new message role (e.g. `TEMPLATE_VARS`) or a sibling class to `ChatMessage` that
carries template arguments inline. `BaseChatModelSetup.chat()` would pull them out before
constructing the formatted prompt.

**Pros**
- Keeps the `chat()` signature unchanged (still just messages).

**Cons**
- Overloads `ChatMessage`'s role concept with something that is not actually a message.
- Awkward iteration semantics: every consumer of `messages` has to know whether to skip
  template-var entries.
- Diverges from external consensus.
- Bigger blast radius — every chat-model connection would need to know about the new role
  to filter it out.

## Decision

**Approach A**, with the following concrete shape:

```java
// Java — api/src/main/java/.../BaseChatModelSetup.java
public ChatMessage chat(List<ChatMessage> messages);  // convenience: empty args + params
public ChatMessage chat(
        List<ChatMessage> messages,
        Map<String, Object> arguments,
        Map<String, Object> parameters);

// The existing two-arg chat(messages, parameters) overload is REMOVED.
// Its single production caller (ChatModelAction:342) migrates to the three-arg form.
```

```python
# Python — python/flink_agents/api/chat_models/chat_model.py
def chat(
    self,
    messages: Sequence[ChatMessage],
    arguments: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> ChatMessage: ...
```

```java
// Java — api/src/main/java/.../event/ChatRequestEvent.java
public ChatRequestEvent(String model, List<ChatMessage> messages);
public ChatRequestEvent(String model, List<ChatMessage> messages,
                        @Nullable Object outputSchema);
public ChatRequestEvent(String model, List<ChatMessage> messages,
                        @Nullable Map<String, Object> arguments,
                        @Nullable Object outputSchema);   // new
public Map<String, Object> getArguments();   // new
```

```python
# Python — python/flink_agents/api/events/chat_event.py
def __init__(
    self,
    model: str,
    messages: List[ChatMessage],
    arguments: Dict[str, Any] | None = None,
    output_schema: OutputSchema | None = None,
) -> None: ...

@property
def arguments(self) -> Dict[str, Any]: ...
```

Rationale (tradeoffs):
- **Hard cutover** rather than reading `extra_args` as a deprecated fallback. Migration is
  6 files, all owned by this repo. Project doctrine ("no shims, no feature flags") and
  recent precedent (#685 was a similar hard cutover on `model` field).
- **`arguments` as the name** — matches the issue title verbatim and reads naturally next
  to `Prompt.formatMessages(MessageRole, Map<String, String> kwargs)`.
- **Convenience one-arg overload retained in Java** to avoid forcing every existing test
  caller to type `Map.of(), Map.of()`. The retention surfaced as zero overload-erasure
  conflict in `investigate-types.md`.
- **`arguments` slots between `messages` and `outputSchema`** in the Java
  `ChatRequestEvent` constructor for a unique 4-arg erasure; same ordering in Python.

## Detailed Design

### Setup-level behavior

The current Java body harvests every input message's `extraArgs`:

```java
// today, BaseChatModelSetup.java:127-135
Map<String, String> arguments = new HashMap<>();
for (ChatMessage message : messages) {
    for (Map.Entry<String, Object> entry : message.getExtraArgs().entrySet()) {
        arguments.put(entry.getKey(), entry.getValue().toString());
    }
}
List<ChatMessage> promptMessages = prompt.formatMessages(MessageRole.USER, arguments);
```

After:

```java
// new BaseChatModelSetup.java
if (this.prompt != null) {
    Prompt prompt = (Prompt) this.prompt;
    Map<String, String> stringified = new HashMap<>();
    if (arguments != null) {
        for (Map.Entry<String, Object> e : arguments.entrySet()) {
            stringified.put(e.getKey(),
                    e.getValue() != null ? e.getValue().toString() : "");
        }
    }
    List<ChatMessage> promptMessages =
            prompt.formatMessages(MessageRole.USER, stringified);
    for (ChatMessage message : messages) {
        if ((message.getContent() != null && !message.getContent().isEmpty())
                || message.getRole() == MessageRole.ASSISTANT) {
            promptMessages.add(message);
        }
    }
    messages = promptMessages;
}
```

Python mirror — same logic, swap `toString` for `str()`, stringify `arguments`, pass via
`**str_arguments` to `format_messages`. The "append meaningful messages" loop is unchanged.

### Event-action plumbing

```
user code emits:
   ChatRequestEvent(model, messages, arguments={"input": content})
                                              │
                                              ▼
ChatModelAction.chat(... ChatRequestEvent.arguments ...)
                                              │
                                              ▼
                       chatModel.chat(messages, arguments, parameters)
                                              │
                       ┌─ Prompt.formatMessages(stringified arguments)
                                              │
                       (extra_args UNTOUCHED for template filling)
                                              ▼
                       BaseChatModelConnection.chat(formatted_msgs, tools, parameters)
                                              ▼
                                          LLM provider
```

### Pemja bridge

`PythonChatModelSetup.chat()` (Java side) gains a parameter; body adds
`kwargs.put("arguments", arguments)` before `adapter.callMethod`. Python receives the
kwargs dict via `python_java_utils.call_method` and unpacks; `BaseChatModelSetup.chat()`
Python signature has `arguments` as a named parameter so the kwarg lands cleanly.

`JavaChatModelSetupImpl.chat()` (Python side) signature gains `arguments=None`; body passes
the new field through to the Java method.

### Examples + tests

Six caller files migrate from the anti-pattern. For each, the migration is mechanical:
move the dict from `ChatMessage(..., extra_args={...})` / `Map.of(...)` onto the
`ChatRequestEvent`'s new `arguments` field. Three test files (Java unit, Python unit, Python
runtime test of built-in actions) gain a positive assertion that arguments **do** fill the
template; the test of `extra_args`-as-template-source is removed because the path is gone.

## Prior Art Comparison

| Aspect | This design | LangChain | LangChain4j | Semantic Kernel | OpenAI SDK |
|---|---|---|---|---|---|
| Template vars: separate from messages? | **Yes** | Yes (dict / kwargs on invoke) | Yes (Map on `PromptTemplate.apply`) | Yes (`KernelArguments`) | N/A (no template layer) |
| Template syntax | `{name}` (existing) | `{name}` | `{name}` | `{{$name}}` | N/A |
| Where template fill happens | Inside `BaseChatModelSetup.chat` (existing position) | Inside `LLMChain` / LCEL chain | Caller calls `apply` then `generate` | Inside kernel | N/A |
| Field reused vs. dedicated | Dedicated (`arguments`) | Dedicated | Dedicated | Dedicated | N/A |

## Dependencies

None new. All work uses Jackson `Map<String, Object>` (already in Maven), Pydantic
`Dict[str, Any]` (already in Python pyproject), and Pemja kwargs marshalling (already in
the bridge). Verified against `api/pom.xml`, `plan/pom.xml`, `runtime/pom.xml`,
`dist/flink-2.2/pom.xml` — see `investigate-deps.md`.

## Test Strategy

**Java unit (`BaseChatModelTest`)**: positive test that `chat(messages, arguments, params)`
formats a `Prompt` template using the values from `arguments` — and a negative test that
`message.extraArgs` values are NOT picked up as template variables (proves the cutover).

**Python unit (`test_chat_model_base`)**: same two cases on the Python side.

**Bridge (`PythonChatModelSetupTest`)**: invoke the three-arg form, verify the Pemja kwargs
marshalled to Python include the `arguments` key.

**Runtime built-in (`test_built_in_actions`)**: migrate the `extra_args` anti-pattern in
`MockChatModel` and the caller's `ChatRequestEvent` to pass `arguments` instead; existing
output assertion stays the same to prove behavior parity.

**Edge cases tested**:
- `arguments=None` / `null` → treated as empty map (no template substitution).
- `arguments` has non-string values → stringified via `toString()`/`str()`.
- `arguments` has a key not in the template → no error, key ignored.
- Template has `{placeholder}` not in `arguments` → placeholder literal stays (current
  behavior preserved).
- No `Prompt` configured → `arguments` is silently ignored (matches today).

**NOT writing**:
- Per-integration test for each chat-model provider (OpenAI, Anthropic, etc.) — they don't
  override `chat()` (verified in `investigate-types.md`) and the formatting happens before
  the connection, so they're unaffected.
- Cross-language e2e tests for argument propagation — the existing Pemja bridge test asserts
  the kwargs marshalling; per-language tests assert the behavior at each level.

## Out of Scope

- Renaming `ChatMessage.extra_args` — still legitimately used for provider metadata.
- Removing the `extra_args` field — same reason.
- Changing `BaseChatModelConnection.chat()` signature — connection sees formatted messages
  only.
- Adding type-strict `arguments` validation (e.g. require `Map<String, String>` directly)
  — preserve today's lenient stringification for migration friendliness.

## Alternatives Considered

- Reading `extra_args` as a deprecated fallback for a release. Rejected — project doctrine
  ("no shims") and trivial migration footprint.
- Using `**kwargs` (Python) to absorb both model parameters and template arguments under
  the same key namespace. Rejected — collision risk between template variable names and
  model parameter names like `temperature`.
- Java builder pattern for `chat()`. Rejected — over-engineering for two map parameters.
