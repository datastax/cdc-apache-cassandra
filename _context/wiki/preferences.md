# Working Preferences

## Coding standards

- **Java 8** target — no lambdas or APIs that require Java 9+
- Follow the existing Lombok-heavy style (`@Slf4j`, `@Getter`, `@AllArgsConstructor`, etc.)
- Mirror the existing pattern when adding a parallel implementation (e.g., `KafkaMutationSender` should mirror `AbstractPulsarMutationSender` / `PulsarMutationSender` structure)
- License header required on all new Java files — copy from [`LICENSE-HEADER.txt`](../../LICENSE-HEADER.txt)
- No breaking changes to existing Pulsar classes; new Kafka code lives in new classes / new packages
- Gradle multi-project conventions: new modules added to `settings.gradle`, dependencies declared in module `build.gradle`

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
