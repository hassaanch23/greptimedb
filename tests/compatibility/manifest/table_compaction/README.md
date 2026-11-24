# File-only compaction manifest compatibility

The hand-authored frozen fixtures use the JSON schema of `RegionEdit` and
`RegionManifest` at `4092fe6549`. They are schema fixtures, not captures from a
released binary. They preserve L0/L1 metadata, sequences and flush boundaries.

Legacy readers can still deserialize file edits. A new publication optionally includes
a job ID and a digest in the same edit as its files (`publication-edit.json`).
Checkpoints retain the last 4096 distinct publications per region. Replaying an
identical edit leaves its position unchanged. If the receipt has expired, a retry
must revalidate the inputs and cannot claim this job succeeded from missing inputs
alone. Older binaries can open the files but do not maintain the new receipt field.
Callers retain output protection until recovery or GC resolves liveness.

Publication tests (including partial success, lost ACKs and reopened regions):

```sh
cargo nextest run -p mito2 --features enterprise -E 'test(engine::table_compaction)'
```

```sh
cargo nextest run -p mito2 -E 'test(manifest::tests::compatibility)'
cargo nextest run -p mito2 --features enterprise -E 'test(manifest::tests::compatibility)'
```

The tests exercise legacy delta/checkpoint reads and verify that serialization
still contains no task-state fields. Optional future core fields remain readable.
Unknown SST levels are rejected; the shared sentinel level (the u8 maximum)
requires Enterprise capability.
This does not establish downgrade safety after shared files are introduced.

The earlier, unpublished prototype persisted `table_compaction` / `extensions`
task records. Canonical delta/checkpoint decoders reject those records in both
builds rather than silently dropping recovery evidence. They require explicit
migration; this version does not resume that prototype's Prepare/Commit protocol.
