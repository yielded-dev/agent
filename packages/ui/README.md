# Yielded UI

A small workspace library for conversation interfaces, built with Base UI,
Streamdown, and the AI Elements composition patterns. Sandbox and Travel Planner
consume the same components. It has no dependency on an agent runtime or transport.

```tsx
import { Message, MessageResponse, Tool } from "@yielded/ui";

<Message from="assistant">
  <MessageResponse streaming={pending}>{text}</MessageResponse>
</Message>
<Tool title="Run tests" status="complete" input="python3 -m unittest" output="OK" />
```

The application owns state, requests, tool identity, and the text shown as reasoning.
`Conversation` follows new output until the reader scrolls up. `MessageResponse`
handles incomplete Markdown, disables raw HTML, and accepts a host URL policy and
image renderer; remote images are off by default.

Use Tailwind 4 in the application and include the package sources:

```css
@import "tailwindcss";
@source "../node_modules/@yielded/ui/src";
@source "../node_modules/streamdown/dist/*.js";

@theme inline {
  --color-background: var(--background);
  --color-foreground: var(--foreground);
  --color-card: var(--card);
  --color-card-foreground: var(--foreground);
  --color-primary: var(--primary);
  --color-primary-foreground: var(--primary-foreground);
  --color-muted: var(--muted);
  --color-muted-foreground: var(--muted-foreground);
  --color-border: var(--border);
  --color-ring: var(--ring);
  --color-destructive: var(--destructive);
}
```

Paths are relative to the importing stylesheet. Set these variables and `--radius`
in the host's theme selector; fonts inherit from the host. There is no theme
provider or global stylesheet. See the two examples for dark and light themes.
