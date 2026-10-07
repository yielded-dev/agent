import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Navigate } from "@tanstack/react-router";
import { Atom } from "effect/reactivity";
import { ArrowLeft } from "lucide-react";
import { useEffect, useState } from "react";

// Copied unchanged from yielded-dev/brand assets/. Regenerate there, not here.
import lockupInk from "./brand/lockup-agent-ink.svg?url";
import lockupPaper from "./brand/lockup-agent-paper.svg?url";
import markInk from "./brand/mark-ink.svg?url";
import markPaper from "./brand/mark-paper.svg?url";
import {
  loginTarget,
  leaveLogin,
  consumeCallback,
  oauthLogin,
  loginView,
  requestEmailCode,
  verifyEmailCode,
  type LoginLoadingStep,
} from "./client";

/** One account signs in to every app on agent.yielded.dev; copy names the one the visitor returns to. */
const destinations = {
  "/travel/": {
    title: "Sign in to plan a trip",
    lede: "Elsewhere is a travel planner built with yielded agent. Bring your own OpenAI key to start a conversation.",
    back: null,
  },
  "/browser-use/": {
    title: "Sign in to the browser lab",
    lede: "Allowlisted accounts run Jev and model agents on the lab’s keys. Without one, you can still bring your own.",
    back: { href: "/browser-use/", label: "Back to the browser lab" },
  },
} as const;

export function Login({ callback = false }: { readonly callback?: boolean }) {
  const view = useAtomValue(loginView(callback));
  const returnTarget = useAtomValue(loginTarget(callback));
  const startOAuth = useAtomSet(oauthLogin);
  const consume = useAtomSet(consumeCallback);
  const leave = useAtomSet(leaveLogin);
  const requestCode = useAtomSet(requestEmailCode);
  const verifyCode = useAtomSet(verifyEmailCode);
  const [mode, setMode] = useState<"register" | "signin">("signin");
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");

  useEffect(() => {
    if (callback) consume();
  }, [callback, consume]);
  useEffect(() => {
    if (view._tag === "Authenticated") leave(view.returnTarget);
  }, [view, leave]);
  if (view._tag === "Authenticated")
    return view.returnTarget === "/travel/" ? (
      <Navigate to="/" replace />
    ) : (
      <LoginLoading step="session" />
    );
  if (view._tag === "Loading") return <LoginLoading step={view.step} />;

  const { pending, busy } = view;
  const destination = destinations[returnTarget];

  return (
    <LoginShell back={destination.back}>
      <section className="login-card" aria-labelledby="login-title">
        <p className="login-kicker">agent.yielded.dev{returnTarget}</p>
        <h1 id="login-title">{pending ? "Check your email" : destination.title}</h1>
        {!pending ? (
          <>
            <p className="login-lede">{destination.lede}</p>
            <button
              className="login-primary"
              disabled={busy}
              onClick={() => startOAuth({ provider: "yielded", returnTarget })}
            >
              Continue with Yielded →
            </button>
            <button
              className="login-quiet login-switch"
              disabled={busy}
              onClick={() => startOAuth({ provider: "yielded", returnTarget, selectAccount: true })}
            >
              Use another Yielded account
            </button>
            <details>
              <summary>Use an existing Agent account</summary>
              <button
                className="login-secondary"
                disabled={busy}
                onClick={() => startOAuth({ provider: "github", returnTarget })}
              >
                <GithubMark /> Continue with GitHub
              </button>
            </details>
            <div className="login-divider">
              <span>or use email</span>
            </div>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                requestCode({ mode, email, returnTarget });
              }}
            >
              <div className="login-modes" aria-label="Email account action">
                <button
                  type="button"
                  aria-pressed={mode === "signin"}
                  onClick={() => setMode("signin")}
                >
                  Sign in
                </button>
                <button
                  type="button"
                  aria-pressed={mode === "register"}
                  onClick={() => setMode("register")}
                >
                  Create account
                </button>
              </div>
              <label htmlFor="login-email">Email address</label>
              <input
                id="login-email"
                type="email"
                autoComplete="email"
                required
                maxLength={254}
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                disabled={busy}
              />
              <button className="login-secondary" disabled={busy}>
                {busy
                  ? "Sending…"
                  : mode === "register"
                    ? "Create account with email"
                    : "Send sign-in code"}
              </button>
            </form>
          </>
        ) : (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              verifyCode({ pending, code });
              setCode("");
            }}
          >
            <p className="login-lede" role="status">
              {view.registered
                ? "Your account is ready. We sent a new code to finish signing in."
                : pending.mode === "register"
                  ? "Enter the code to create your account. Then we’ll send a fresh sign-in code."
                  : "If this email has an account, a sign-in code is on its way."}
            </p>
            <p className="login-recipient">{pending.email}</p>
            <label htmlFor="login-code">Six-digit code</label>
            <input
              id="login-code"
              className="login-code"
              inputMode="numeric"
              autoComplete="one-time-code"
              pattern="[0-9]{6}"
              minLength={6}
              maxLength={6}
              required
              value={code}
              onChange={(event) => setCode(event.target.value)}
              disabled={busy}
            />
            <button className="login-primary" disabled={busy}>
              {busy
                ? "Checking…"
                : pending.mode === "register"
                  ? "Verify email and create account"
                  : "Sign in"}
            </button>
            <button
              className="login-quiet"
              type="button"
              disabled={busy}
              onClick={() => {
                requestCode(Atom.Reset);
                verifyCode(Atom.Reset);
                setCode("");
              }}
            >
              Start again or request another code
            </button>
            <p className="login-note">
              Codes expire after five minutes. Wait at least 30 seconds before requesting another.
            </p>
          </form>
        )}
        {view.error && (
          <p className="login-alert" role="alert">
            {view.error === "provider"
              ? "We couldn’t finish the provider sign-in. Please start a new attempt."
              : view.error === "email"
                ? "We couldn’t complete that step. Check your code or request a new one."
                : "We couldn’t check your sign-in. Please try again."}
          </p>
        )}
        {view.cancelled && (
          <p className="login-notice" role="status">
            Sign-in was cancelled. You can start again when you’re ready.
          </p>
        )}
        <p className="login-note">
          Existing email and GitHub accounts stay separate until explicitly connected. Matching
          email addresses never joins accounts.
        </p>
      </section>
    </LoginShell>
  );
}

export function LoginLoading({ step }: { readonly step: LoginLoadingStep }) {
  return (
    <LoginShell>
      <section className="login-card login-loading" aria-labelledby="login-loading-title">
        <div className="login-loading-content" role="status" aria-live="polite">
          <div className="login-loader" aria-hidden="true">
            <picture>
              <source media="(prefers-color-scheme: dark)" srcSet={markPaper} />
              <img src={markInk} alt="" width={63} height={90} />
            </picture>
          </div>
          <h1 id="login-loading-title">
            {step === "yielded"
              ? "Connecting to Yielded"
              : step === "github"
                ? "Connecting to GitHub"
                : step === "callback"
                  ? "Signing you in"
                  : "Getting things ready"}
          </h1>
          <p>
            {step === "yielded"
              ? "Using your Yielded account, or taking you to GitHub to sign in."
              : step === "github"
                ? "Taking you to GitHub to continue."
                : step === "callback"
                  ? "Finishing up. You’ll be on your way in a moment."
                  : "One moment while we check your sign-in."}
          </p>
        </div>
      </section>
    </LoginShell>
  );
}

function LoginShell({
  back = null,
  children,
}: {
  readonly back?: { readonly href: string; readonly label: string } | null;
  readonly children: React.ReactNode;
}) {
  return (
    <div className="login-page">
      <header className="login-top">
        <a className="login-brand" href="https://yielded.dev/agent/">
          <picture>
            <source media="(prefers-color-scheme: dark)" srcSet={lockupPaper} />
            <img src={lockupInk} alt="yielded Agent" width={773} height={113} />
          </picture>
        </a>
        {back && (
          <a className="login-back" href={back.href}>
            <ArrowLeft size={15} aria-hidden="true" /> {back.label}
          </a>
        )}
      </header>
      <main className="login-main">{children}</main>
      <footer className="login-foot">
        Your Agent session covers the travel planner and browser lab. Signing out of Agent leaves
        your Yielded Auth session active.
      </footer>
    </div>
  );
}

/** GitHub's mark (Octicons mark-github). */
function GithubMark() {
  return (
    <svg viewBox="0 0 16 16" width="17" height="17" aria-hidden="true" fill="currentColor">
      <path d="M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z" />
    </svg>
  );
}
