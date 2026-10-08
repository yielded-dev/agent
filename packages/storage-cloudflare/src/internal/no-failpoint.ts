import { Effect } from "effect";

import type { DoStorageFailpointHandler } from "../DoStorageFailpoint.ts";

/** Identity of the production hook; arbitrary test hooks keep their Effect semantics. */
export const noFailpoint: DoStorageFailpointHandler = () => Effect.void;
