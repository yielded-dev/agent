export function FundingPanel() {
  return (
    <section className="openai-connection" aria-label="Hosted access">
      <h3>Hosted access</h3>
      <p className="settings-help">
        Account permissions and hosted model access are managed by Yielded.
      </p>
      <a href="https://auth.yielded.dev/oauth-settings">Manage Yielded access</a>
    </section>
  );
}
