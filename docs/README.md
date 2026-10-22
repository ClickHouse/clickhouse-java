# Documentation layers

Use the narrowest authoritative layer for a change:

1. `docs/clickhouse-docs/*.mdx` is the source for the published ClickHouse website pages.
2. `docs/integration-*.md` provides repository-local integration guidance and links to the published reference for exhaustive details.
3. `docs/features.md` is the compatibility contract for stable `client-v2` and `jdbc-v2` behavior.
4. Module and root `README.md` files are landing pages and quick starts, not exhaustive references.
5. Public Javadocs own symbol-level API behavior.

Release-specific compatibility and migration notes belong in `docs/releases/`. The
[`migration-helpers`](../migration-helpers/) module contains utilities for translating legacy client and JDBC
configuration.

When documentation changes, run:

```shell
python3 scripts/check-relative-links.py
```

The checker validates relative links in the maintained root, contribution, pull-request-template, and `docs/`
Markdown sources. External URLs and website-root routes are intentionally excluded.
