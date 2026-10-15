# docs/dev

Working notes that are **not** part of the stable specification: verification
logs, benchmark procedures and capacity experiments. Anything that becomes a
permanent guarantee belongs in [`docs/architecture.md`](../architecture.md) or
[`docs/design.md`](../design.md).

- [`verification-log.md`](verification-log.md): closed-loop evidence recorded
  while validating a change.
- [`verify-with-xlite.sh`](verify-with-xlite.sh): local smoke with a stream mock
  and xlite in front of Flux (`thin-l7`). Not part of the public product surface.
