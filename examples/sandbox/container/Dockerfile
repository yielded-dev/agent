FROM docker.io/library/node:24-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6
COPY --from=docker.io/cloudflare/sandbox:1.0.3@sha256:fc1740c3cba26461f0ab1771287c84018ab2cca43a70ebb823087c85d780aaec /usr/local/bin/sandbox-shim /usr/local/bin/sandbox-shim
COPY --from=docker.io/oven/bun:1.4.2@sha256:9114c058aeae42162ee16dd5084b95fe9473970bb6bcb5b232ab1630f0546895 /usr/local/bin/bun /usr/local/bin/bun
RUN apt-get update && apt-get install --yes --no-install-recommends bash ca-certificates curl file git ripgrep python3 python3-pip python3-venv procps zip unzip tini \
    && rm -rf /var/lib/apt/lists/*
# Keep the test runner, matching browser and its system libraries in the image.
# /node_modules is an ancestor of /workspace, so Node resolves these in new projects.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers
RUN npm install --prefix /opt/playwright --no-audit --no-fund --ignore-scripts --save-exact @playwright/test@1.64.1 \
    && ln -s /opt/playwright/node_modules /node_modules \
    && ln -s /node_modules/.bin/playwright /usr/local/bin/playwright \
    && playwright install --with-deps --only-shell chromium \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*
# The supervisor creates /workspace at boot so a restore can atomically replace it.
WORKDIR /
EXPOSE 4503
ENTRYPOINT []
CMD ["sleep", "infinity"]
