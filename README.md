# Eventure

**An MCP server that exposes a simple API to publish/subscribe to local events, enabling AI agents to coordinate via event bus.**

AI agents running locally need a simple way to coordinate without external dependencies. Setting up message brokers or custom plumbing for each project wastes time and adds complexity. Eventure provides a zero-configuration event bus that lets agents communicate via local events, so you can focus on the agent logic.

## Example

<!-- A real command and its REAL pasted output. Run it first. -->

## Install

```bash
pip install -e ".[dev]"
```

## Usage

<!-- The 2-4 commands people actually need, one line each. -->

## How it works

<!-- 3-6 bullets: what it checks, computes or produces. -->

## Limitations

Eventure runs in a single process and stores events only in memory. It does not persist data to disk, so a restart loses all events. The server is not designed for clustering or high‑availability; it cannot be accessed from other machines or across network boundaries. There is no authentication or authorization, and message ordering is not guaranteed beyond the order of arrival in the single process. These constraints make it suitable for local experimentation and prototyping, but not for production deployments that require durability, scalability, or security.

## Development

```bash
pip install -e ".[dev]"
pytest -q
ruff check .
```

## License

MIT -- see [LICENSE](LICENSE).
