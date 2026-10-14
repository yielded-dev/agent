# Code and test practices

Apply these to upstream library contributions, including Effect-TS/effect and
effect-agent, while implementing or revising the change and during final review.
Adapt them to the target library's architecture and conventions. The repository's
testing policy decides whether to add a test; these rules shape the tests you do write.

## Keep code precise and focused

- Model distinct supported inputs and outcomes explicitly. Keep required data
  required; supporting another valid shape must not accept malformed input or
  invent missing data. Normalize differences at the boundary where practical.
- Preserve meaningful distinctions between absent, null, incomplete, failed, and
  completed states. Add defaults only when the supported contract warrants them.
- Reuse existing types, schemas, and source literals where available instead of
  copying definitions. Keep success and failure variants narrow and retain useful
  partial data when the contract supports it.
- Check related paths when they implement the same contract, such as streaming
  and complete responses. Extend a fix when evidence establishes the same bug.
- Prefer a local change over a new abstraction or public API when it expresses the
  contract clearly. Remove redundant scaffolding and implementation narration.
  Judge clarity, precision, and coverage rather than line count; terse code should
  still make the behavior easy to understand.

## Make each test earn its place

- Choose representative cases for distinct outcomes. Use a cross-product matrix
  when interactions between dimensions require it.
- Keep fixtures and assertions focused on the behavior under test. Include the
  real event sequence when lifecycle behavior matters; remove incidental repeated
  assertions without removing meaningful coverage.
- Exercise the boundary where the bug occurs, including relevant malformed inputs.
  Test externally observable behavior without prescribing an implementation when
  several correct implementations exist.
- Preserve type tests for changed inference, narrowing, and nullable or required
  fields. Assert the supported contract independently of the implementation.
