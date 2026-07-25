# Context Manager Migration

Seahorse is now the default context manager. The legacy context manager and its
silent fallback behavior have been removed.

## Existing Configurations

Configurations that omit `agents.defaults.context_manager` use `seahorse`.
Configurations that already select `seahorse` require no change.

Replace an explicit legacy selection:

```json
{
  "agents": {
    "defaults": {
      "context_manager": "legacy"
    }
  }
}
```

with either the default:

```json
{
  "agents": {
    "defaults": {
      "context_manager": "seahorse"
    }
  }
}
```

or explicit stateless operation:

```json
{
  "agents": {
    "defaults": {
      "context_manager": "none"
    }
  }
}
```

`none` persists canonical session records but does not assemble stored history
or summaries into model prompts. It also does not register Seahorse retrieval
tools or run context compaction.

Unknown manager names, legacy selection, invalid Seahorse configuration, and
Seahorse initialization failures now stop startup with an actionable error.
They no longer fall back to a different context strategy.
