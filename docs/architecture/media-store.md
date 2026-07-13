# Media Store Durability

`MediaStore` owns the lifecycle of `media://` references. Session JSONL stores
only those opaque references; it never stores local file paths.

## Durable Index

Each gateway keeps a bounded index at:

```text
<workspace>/state/media/index.json
```

The index contains the ref, local path, media metadata, scope, cleanup policy,
and creation time for every active media entry. It is written as a complete,
atomic snapshot before `Store` returns a new ref and before lifecycle cleanup
removes refs. This makes a gateway restart recover post-change references
without teaching session/history code about filesystem paths.

At startup, the store restores only entries whose local files still exist. It
rewrites the index without missing files. `Resolve` also checks that the file
still exists and durably invalidates a stale ref before returning an
unavailable error. A deleted temporary file is therefore unavailable rather
than accidentally resolving to a fabricated mapping.

The index is workspace-local, so profiles sharing one PicoClaw binary do not
share media references. It is an index only: media file retention remains
controlled by the existing cleanup policy and the underlying storage.

## Migration Limitation

References created before durable indexing cannot be recovered after a restart:
their UUID-to-path mapping was never written to disk. Those historical refs
remain unavailable. New refs created after this feature is deployed recover
across restarts while their files remain on disk.
