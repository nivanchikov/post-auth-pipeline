# Post-Auth Operation Pipeline Implementation Notes

> **Status:** Updated after simplification. The design source of truth is
> `docs/superpowers/specs/2026-06-17-post-auth-operation-pipeline-design.md`.

## Current Shape

- `OperationPipeline` remains a generic DAG runner. It is not auth-specific.
- Operation identity is the operation type (`SomeOperation.self`), stored internally as
  `ObjectIdentifier`.
- One operation type may appear once per pipeline.
- `dependsOn` uses operation types, so authoring stays non-stringly typed.
- The engine still supports diamond dependencies, default-skip cascade, guard overrides,
  fail-open/fail-close policies, and typed context reads.
- `RunState` is internal per-run state: one outcome map plus dependency waiters. It no longer
  has live/working maps, rerun staging, `beginRun`, `publish`, or `liveSnapshot`.
- Pipeline aborts surface a concrete `PipelineFailure` with the failing operation name and
  diagnostic message.
- Pipeline reruns are not a first-class v1 feature. Add explicit rerun semantics only if a
  concrete workflow needs them.
- Per-operation timeouts are structured/cooperative task-group races. Operations that may hang
  must use cancellation-aware APIs.

## Auth Integration

- `AuthModule` still depends only on `GatingRunner`.
- `EngineGatingRunner` runs the gating pipeline in its own private engine run.
- `PostLoginCoordinator` observes `AuthState`, runs the post-login pipeline in a separate
  private engine run after authentication, and cancels it on logout.
- Gating and post-login do not share pipeline context. Shared domain data should flow through
  injected collaborators/stores when needed.

## Verification

Run:

```bash
swift test
```
