import * as Drizzle from "@yielded/auth-persistence-drizzle/SqliteDo";
import { EmailSignInTargets, EmailUnavailable } from "@yielded/auth/Email";
import {
  OAuthSignInPersistence,
  OAuthRegistrationIntents,
  OAuthUnavailable,
} from "@yielded/auth/OAuth";
import { ProofPersistence } from "@yielded/auth/Proofs";
import { AuthenticationAuthority } from "@yielded/auth/Sessions";
import { eq } from "drizzle-orm";
import { Effect, Layer } from "effect";

import { emailSignInMapping, emailRegistrationMapping } from "./email-schema";
import { oauthSignInMapping, oauthIntentMapping, oauthRegistrationMapping } from "./oauth-schema";
import { proofs } from "./proof-schema";
import { subject, subjectMapping, subjectId, credentialMapping } from "./schema";
import type { AppAuth } from "./server";
import { sessionsMapping } from "./session-schema";

export const persistenceLayer = (AppAuth: AppAuth) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const database = yield* Drizzle.Database;
      const proof = yield* Drizzle.makeProofPersistenceServices(proofs);
      const email = yield* Drizzle.makeEmailSignInServices(emailSignInMapping);

      const emailRegistration = yield* Drizzle.makeEmailRegistrationServices(
        emailRegistrationMapping,
        proofs,
      );

      const oauth = yield* Drizzle.makeOAuthSignInServices(oauthSignInMapping);

      const intents = yield* Drizzle.makeOAuthRegistrationIntentServices(oauthIntentMapping);

      const registration = yield* Drizzle.makeOAuthRegistrationServices(oauthRegistrationMapping);

      const sessions = yield* Drizzle.makeStatefulSessionServices(sessionsMapping);

      const authority = yield* Drizzle.makeAuthenticationAuthorityServices({
        subject: subjectMapping,
        credential: credentialMapping,
        subjectId,
        isConstraintConflict: () => false,
      });

      const claims = Effect.fn("Auth.claims")(function* (id: string) {
        const rows = yield* database
          .select({ displayName: subject.displayName })
          .from(subject)
          .where(eq(subject.id, id));

        if (rows.length !== 1) return yield* Effect.fail("Missing account" as const);

        return rows[0]!;
      });

      return Layer.mergeAll(
        Layer.succeed(ProofPersistence, proof.proofPersistence),
        Layer.succeed(EmailSignInTargets, email.emailSignInTargets),
        Layer.succeed(
          AppAuth.strategies.emailRegistration.RegistrationAuthority,
          emailRegistration.registrationAuthority,
        ),
        Layer.succeed(AppAuth.strategies.email.SessionClaims, {
          resolve: ({ subjectId }) =>
            claims(subjectId).pipe(Effect.mapError(() => EmailUnavailable.make({}))),
        }),
        Layer.succeed(AppAuth.strategies.oauth.SessionClaims, {
          resolve: ({ subjectId, identity }) =>
            claims(subjectId).pipe(
              Effect.map((local) => ({
                ...local,
                displayName: identity.profile?.displayName ?? local.displayName,
              })),
              Effect.mapError(() => OAuthUnavailable.make({})),
            ),
        }),
        Layer.succeed(OAuthSignInPersistence, oauth.oauthSignInPersistence),
        Layer.succeed(OAuthRegistrationIntents, intents.oauthRegistrationIntents),
        Layer.succeed(
          AppAuth.strategies.oauth.registration.RegistrationAuthority,
          registration.registrationAuthority,
        ),
        Layer.succeed(
          AppAuth.sessions.StatefulSessionPersistence,
          sessions.statefulSessionPersistence,
        ),
        Layer.succeed(AppAuth.sessions.SessionRepository, sessions.sessionRepository),
        Layer.succeed(AuthenticationAuthority, authority.authenticationAuthority),
      );
    }),
  );
