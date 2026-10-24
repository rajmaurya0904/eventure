# eventure

**A local publish/subscribe event bus for AI agents, served as an MCP server.**

Two agents running side by side usually coordinate through a shared file, a hand-rolled queue, or you copy-pasting between terminals. Eventure gives each of them `publish`, `subscribe` and `poll` as ordinary MCP tools, backed by one SQLite file, so a planner can announce `task.square` and a worker, started separately, picks it up. No broker, no daemon, no ports.

```text
$ python examples/demo.py
worker   subscribed to task.*  (handle 4c19167e5a2b)
planner  published task.square n=3  -> 1 subscriber(s)
planner  published task.square n=4  -> 1 subscriber(s)
planner  published task.square n=5  -> 1 subscriber(s)
worker   got task.square n=3, squaring it
worker   got task.square n=4, squaring it
worker   got task.square n=5, squaring it
planner  result: 3^2 = 9
planner  result: 4^2 = 16
planner  result: 5^2 = 25
bus      6 events retained, subscribers: planner, worker
```

That is two separate `eventure serve` processes, each spoken to over MCP stdio. The same bus is reachable from a shell, which is handy for watching or poking at it:

```text
$ eventure poll reviewer "build.*.done"
no new events
$ eventure publish build.api.done '{"ok": true, "tests": 212}' --publisher ci
#1 10:01:16 build.api.done [ci] {"ok": true, "tests": 212}
delivered to 1 subscription
$ eventure publish build.web.failed '{"ok": false}' --publisher ci
#2 10:01:17 build.web.failed [ci] {"ok": false}
delivered to 0 subscriptions
$ eventure publish build.web.done '{"ok": true}' --publisher ci
#3 10:01:18 build.web.done [ci] {"ok": true}
delivered to 1 subscription
$ eventure poll reviewer "build.*.done"
#1 10:01:16 build.api.done [ci] {"ok": true, "tests": 212}
#3 10:01:18 build.web.done [ci] {"ok": true}
$ eventure poll reviewer "build.*.done"
no new events
```

## Install

Not on PyPI yet, so install from a clone (Python 3.11 or newer):

```bash
pip install -e .
```

## Usage

Register it with an MCP client. For Claude Desktop or any client that takes a command:

```json
{
  "mcpServers": {
    "eventure": { "command": "eventure", "args": ["serve"] }
  }
}
```

Every agent that uses the same database file shares the bus. Set it with `--db PATH` or the `EVENTURE_DB` environment variable; the default is `~/.eventure/bus.db`.

```bash
eventure serve                      # MCP over stdio (also what plain `eventure` does)
eventure serve --transport streamable-http --port 8000   # or sse, for HTTP clients
eventure publish jobs.new '{"id": 4}'   # publish from a shell
eventure poll alice "jobs.*" --wait 10  # read what alice has not seen, waiting up to 10s
eventure tail "#"                       # watch everything as it happens
```

`history`, `stats` and `subscriptions` show what the bus currently holds.

The MCP tools an agent sees are `publish`, `subscribe`, `poll`, `unsubscribe`, `list_subscriptions`, `history` and `stats`. Their descriptions tell the model how to use them, so no prompt engineering is needed beyond "use eventure to coordinate".

## How it works

- **Topics** are dot-separated words (`build.api.done`). Patterns add `*` for exactly one word and `#` for zero or more, as in AMQP: `build.*.done`, `agents.#`.
- **Storage** is one SQLite file in WAL mode. Separate server processes open the same file, which is what makes them one bus. Event ids increase monotonically.
- **Subscriptions are durable cursors.** `subscribe` returns a handle; `poll` returns matching events after the cursor and advances it in the same transaction, so each event is delivered once. Subscribing again under the same name and pattern resumes where you left off; a new subscription starts at "now" unless you ask for `replay`.
- **Shared handles split work.** Two consumers polling the same handle never receive the same event.
- **Waiting** is a short sleep-and-retry loop inside `poll` (`wait_seconds`, at most 30), not a push notification.
- **Retention** keeps the newest 10,000 events. A poll reports `pruned` if the subscriber fell behind far enough to lose events.
- **Validation:** topics up to 200 characters and 16 words, payloads any JSON up to 64 KiB. Bad input comes back to the agent as a tool error with a readable message.

## Limitations

- No push. Agents must call `poll`; MCP servers cannot wake a model on their own, so `wait_seconds` is the closest thing to blocking.
- Local only. The bus is a file on one machine; there is no networking between machines, no authentication, and anything that can read the file can read every event.
- At-most-once delivery. The cursor moves when events are returned, so an agent that crashes after polling but before acting loses them (use `peek` and `history` if you need to re-read).
- Bounded history: 10,000 events by default, and `pruned` is an upper bound, not an exact count.
- The HTTP transports are the SDK's own and bind to localhost by default; they are not hardened for exposure.
- Tested against `mcp` 1.x (`>=1.9,<2`). The 2.x SDK renamed `FastMCP` and is not supported.

## Development

```bash
pip install -e ".[dev]"
pytest -q
ruff check .
```

The suite includes tests that start real server subprocesses and talk MCP to them, so it takes about half a minute.

## Licence

MIT, see [LICENSE](LICENSE).
