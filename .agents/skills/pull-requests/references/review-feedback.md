# Assess review feedback

Use this for findings from human reviewers, model reviews, and automated tools. Identify the claimed failure, unmet requirement, or concrete maintenance cost before choosing a change. A reviewer's confidence or severity label does not establish the claim. Explicit user instructions and mandatory repository checks remain requirements.

Start with the current diff, callers, documented invariants, and existing evidence. A finding may describe an older revision, miss an existing guarantee, or disagree with an intentional contract. Run a focused check when a material factual question remains; reading the relevant code can be sufficient. Do not demand a failing test for a clear simplification or requirement correction.

Address supported findings with the smallest useful change. The reviewer may identify a real problem while proposing unnecessary machinery. Preserve meaningful error distinctions, resource ownership, and critical-path performance when choosing the fix. Explain unsupported or superseded findings briefly with the relevant evidence. Uncertainty remains uncertainty; ask when resolving it requires the user's intent or a material tradeoff.

Reuse valid verification and run the checks affected by an actual change. Follow the task's existing authorization for posting replies or resolving review threads.
