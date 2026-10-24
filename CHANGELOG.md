# Changelog

## 0.1.0

- Event bus with dot-separated topics and `*` / `#` patterns.
- SQLite storage shared between processes; durable subscription cursors; retention.
- MCP tools: `publish`, `subscribe`, `poll` (with `wait_seconds`), `unsubscribe`,
  `list_subscriptions`, `history`, `stats`.
- `eventure` command: `serve` (stdio, sse, streamable-http), `publish`, `poll`, `history`,
  `tail`, `stats`, `subscriptions`.
- `examples/demo.py`: two agents coordinating over real MCP stdio connections.
