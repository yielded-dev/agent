---
title: Architecture
description: Understand Thread ownership, execution boundaries, recovery, and limits.
---

A Thread separates conversation history, active work, and model context. Your application
chooses the host and controls access; Yielded Agent advances the work under that host's ownership.

| Reader question                                         | Page                                      |
| ------------------------------------------------------- | ----------------------------------------- |
| How do Thread, Run, Attempt, and Turn fit together?     | [Runtime model](/concepts/runtime-model/) |
| What survives interruption, and what remains uncertain? | [Durability](/concepts/durability/)       |
| What bounds execution and spending?                     | [Budgets and limits](/concepts/budgets/)  |

Continue with [platforms](/platforms/) to choose where ownership lives, or
[context management](/guide/context-management/) to see how the model's working view stays small.
