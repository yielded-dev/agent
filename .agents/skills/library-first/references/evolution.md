# Evolving and extracting a library

Evolve contracts deliberately. Check affected consumers and persisted or wire formats; follow the project's compatibility policy and coordinate necessary migrations. Keep one implementation, adopt it in the current app, and remove the replaced duplicate. Fix shared defects in their owning library.

Finish with the boundary actually used, relevant checks passing, and a short explanation of the public contract, what the app supplies, and any remaining limitation. Document usage and non-obvious guarantees beside the API. If moving repositories later, account for runtime assets and dependencies as well as code.

For a coordinated refactor or migration, use [evolving designs](evolving-designs.md). Follow the target repository's publication policy and current user authorization; preparing a reviewable change does not itself authorize publishing it.
