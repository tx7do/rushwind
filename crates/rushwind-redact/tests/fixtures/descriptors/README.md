# fixtures/descriptors

Checked-in `FileDescriptorSet` blobs (protoc wire format, imports
included) backing `tests/fixture.rs`. They are produced by the
REFERENCE `protoc`, not protox: protox's serializer drops custom-option
values from the output set, and the `(redact.v1)` option bytes are the
entire subject under test — the same reason the production build.rs
uses `buf build` for its annotated closure.

Regenerate (protoc 25.x, options byte-faithful across minor versions):

```sh
cd crates/rushwind-redact/tests
SRC=fixtures/proto OUT=fixtures/descriptors
protoc -I $SRC --include_imports --descriptor_set_out=$OUT/fixture.binpb fixture/v1/fixture.proto
# the refusal cases live inline below (package bad.v1 / auto.v1 / internal.v1 / bare.v1)
```

Layout:

| file | contents |
|---|---|
| `fixture.binpb` | `fixture/v1/fixture.proto` (+ vendored `redact/v1/redact.proto`, WKTs) — the supported-rule corpus |
| `cases/*.binpb` | one unsupported `(redact.value)` shape each — every plan build must refuse |
| `autodetect.binpb` | file-level `(redact.auto_detect)` — refused |
| `internal.binpb` | `(redact.internal_service)` — refused |
| `bare.binpb` | no redact import at all — empty plan |
