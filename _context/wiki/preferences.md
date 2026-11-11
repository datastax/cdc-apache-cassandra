# Working Preferences

## Coding standards

- **Java 8** target — no lambdas or APIs that require Java 9+
- Follow the existing Lombok-heavy style (`@Slf4j`, `@Getter`, `@AllArgsConstructor`, etc.)
- Mirror the existing pattern when adding a parallel implementation (e.g., `KafkaMutationSender` should mirror `AbstractPulsarMutationSender` / `PulsarMutationSender` structure)
- License header required on all new Java files — copy from [`LICENSE-HEADER.txt`](../../LICENSE-HEADER.txt)
- No breaking changes to existing Pulsar classes; new Kafka code lives in new classes / new packages
- Gradle multi-project conventions: new modules added to `settings.gradle`, dependencies declared in module `build.gradle`

## Java formatting (Google Java Style — new code only)

Apply these rules to **all new code**. Do not reformat pre-existing code that is unrelated to the change.

- **Always use braces** for `if`, `else`, `for`, `while`, `do` — even single-statement bodies.
  ```java
  // bad
  if (x != null) return x;

  // good
  if (x != null) {
      return x;
  }
  ```
- **No inline assignments** inside `if` conditions.
- **One statement per line** — do not chain assignments or returns on the same line.
- **4-space indentation**, no tabs.
- **Opening brace on the same line** as the declaration/control statement (K&R style).
- **Blank line** between methods; no blank line immediately after an opening brace or before a closing brace.
- **Imports**: no wildcard imports; grouped as (1) `java.*`, (2) `javax.*`, (3) third-party, (4) same-project; separated by a blank line.

## Planning before implementation

- Write a planning doc first (in `_context/wiki/`) before touching production code
- Planning docs should cover: motivation, proposed architecture, module changes, config changes, migration/compatibility notes, open questions
- Once the plan is reviewed and agreed, move to implementation

## AI collaboration preferences

- Read relevant source files before making claims or suggestions
- Produce minimal diffs — do not refactor unrelated code
- When adding a new capability that mirrors an existing one (e.g., Kafka alongside Pulsar), highlight the structural parallels explicitly in the plan
- Flag backward-compatibility risks prominently
- Use todo lists to track multi-step tasks
- Offer to update the wiki after completing a task that yields durable knowledge

## Pre-push checklist

Run these checks locally before pushing to avoid CI failures.

### Compile all modules (no DSE4 — requires private repo credentials)

```bash
./gradlew build -x test -x backfill-cli:compileJava
```

### Compile including DSE4 (requires `DSE_REPO_USERNAME` / `DSE_REPO_PASSWORD`)

```bash
./gradlew -Pdse4 \
  -PdseRepoUsername=$DSE_REPO_USERNAME \
  -PdseRepoPassword=$DSE_REPO_PASSWORD \
  build -x test -x backfill-cli:compileJava
```

> `agent-dse4` depends on `com.datastax.dse:dse-db` from a private DataStax repo.
> Skip the `-Pdse4` flag when those credentials are not available locally — CI will verify it.

### Run unit tests (no containers required)

```bash
./gradlew agent:test commons:test
```

### Run integration tests for a specific module (requires Docker)

```bash
./gradlew agent-c4:test \
  -PtestPulsarImage=datastax/lunastreaming \
  -PtestPulsarImageTag=2.10_3.4
```

### Known local limitations

| Constraint | Reason |
|------------|--------|
| `agent-dse4` can only be compiled in CI or with DSE repo credentials | `dse-db` artifact lives in a private DataStax Artifactory repo (HTTP 401 without credentials) |
| Integration tests require Docker | Testcontainers spins up real Cassandra + Pulsar/Kafka containers |
| `backfill-cli:compileJava` is excluded from CI build | Has a known separate dependency issue; excluded via `-x backfill-cli:compileJava` |
| `./gradlew build` fails locally if Docker daemon is not running | `agent-c3:docker` and `agent-c4:docker` tasks require Docker; compilation itself is unaffected |

## Dependency conflict rules

When a module depends on both `cassandra-all` **and** `kafka-clients` (directly or transitively via `:agent`), a Gradle capability conflict arises between `org.lz4:lz4-java` (pulled by Cassandra) and `at.yawk.lz4:lz4-java` (pulled by Kafka). Both jars declare the same capability `org.lz4:lz4-java`.

**Fix**: add a `dependencySubstitution` block to the affected module's `build.gradle`:

```groovy
configurations.all {
    resolutionStrategy.dependencySubstitution {
        substitute module('org.lz4:lz4-java') using module("at.yawk.lz4:lz4-java:${lz4javaVersion}")
    }
}
```

This pattern is already applied in `agent-c3`, `agent-c4`, `connector`, and `backfill-cli`. Any new module that combines Cassandra and Kafka dependencies must include it too.
