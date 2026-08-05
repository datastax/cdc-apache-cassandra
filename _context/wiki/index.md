# datastax-cdc Wiki

Project context for the `cassandra-source-connector` CDC pipeline.

## Files

| File | Contents |
|------|----------|
| [project.md](project.md) | Project overview, goals, architecture, and key modules |
| [preferences.md](preferences.md) | Working standards, style preferences, and AI collaboration guidelines |
| [kafka-support-plan.md](kafka-support-plan.md) | Planning document for adding Kafka support alongside existing Pulsar support |

## Quick orientation

- **Language / build**: Java 8, Gradle multi-project
- **Repo root modules**: `commons`, `agent`, `agent-c3`, `agent-c4`, `agent-dse4`, `connector`, `connector-distribution`, `testcontainers`, `backfill-cli`
- **Current active branch**: `feat/kafka-support`
- **Primary constraint**: Kafka support must not break existing Pulsar backward compatibility
