# Designing the library boundary

Start with the current caller and the intended reuse. Look for an existing library to extend before creating another. One consumer is enough to justify the boundary; implement the behavior it needs without inventing features, extension points, or platform support for hypothetical consumers.

Move the current reusable workflow behind the API, including invariants and recovery rules it already needs. Extracting types and helpers while each application repeats the workflow misses the benefit. Extraction does not justify adding hypothetical cases. Applications own product policy, records, credentials, provisioned resources, and presentation. Supply those dependencies through the smallest contracts the workflow needs; library code must not import its consuming app. Require storage atomicity or recovery guarantees only where the supported behavior depends on them.

Keep the public surface small and make inputs, results, failures, dependencies, and resource lifetimes explicit. In Effect code, expose Effect operations with concrete error and service requirements, define services at their domain owner, and compose concrete Layers at the owning application or subsystem boundary. Keep runtime execution and Promise conversion at explicit integration edges. Follow the installed guidance, and keep platform adapters separate from reusable logic where that distinction is needed. Consumers use public exports, including inside the same repository.

When shaping domain state and public types, use [domain modeling](domain-modeling.md). For operations with retries, partial completion, or concurrent writers, use [reliable operations](reliable-operations.md).
