# Changelog

All notable changes to Shoebox are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and releases follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `shoebox.ErrUnsettledAcks`, surfaced by `Drain` and `Shutdown` when buffered
  acknowledgements cannot be durably confirmed.
- `QueueStats.PendingAcks` reports buffered acknowledgements not yet committed.

### Fixed

- Failed acknowledgement batches are retained and retried per queue instead of
  dropping acknowledgements added during an in-flight write; settled IDs are
  resent idempotently without touching other messages.
- `Drain` and `Shutdown` no longer report success while a queue still has
  unsettled acknowledgements; other queues' settled results return independently.

- Fixed PostgreSQL upgrades from the v0.1.6 schema and repaired databases
  whose v0.1.7 migration marker skipped the schedule and durable-dedupe
  objects.

### Added

- Release automation for server binaries and the container image.

## [0.1.6] - 2026-08-18

### Added

- Configurable PostgreSQL schema selection.

## [0.1.5] - 2026-08-18

### Added

- Configurable in-memory deduplication policies, including bounded LRU mode.

## [0.1.4] - 2026-08-14

### Fixed

- Serialized PostgreSQL schema initialization across concurrent openers.

## [0.1.3] - 2026-08-12

### Fixed

- Queue draining of scheduled retries during shutdown.

## [0.1.2] - 2026-08-12

### Fixed

- Queue lifecycle handling and persistent dead-letter queue behavior.

## [0.1.1] - 2026-08-11

### Added

- PostgreSQL benchmark coverage.

## [0.1.0] - 2026-08-11

### Fixed

- Context handling while reading messages.

[Unreleased]: https://github.com/adexaja/shoebox/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/adexaja/shoebox/releases/tag/v0.1.6
[0.1.5]: https://github.com/adexaja/shoebox/releases/tag/v0.1.5
[0.1.4]: https://github.com/adexaja/shoebox/releases/tag/v0.1.4
[0.1.3]: https://github.com/adexaja/shoebox/releases/tag/v0.1.3
[0.1.2]: https://github.com/adexaja/shoebox/releases/tag/v0.1.2
[0.1.1]: https://github.com/adexaja/shoebox/releases/tag/v0.1.1
[0.1.0]: https://github.com/adexaja/shoebox/releases/tag/v0.1.0
