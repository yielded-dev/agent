import type { ComponentProps } from "react";
import { Streamdown, type StreamdownProps } from "streamdown";

import { cn } from "./class-names.ts";

export function Message({
  from,
  className,
  ...props
}: ComponentProps<"article"> & { readonly from: "user" | "assistant" }) {
  return (
    <article
      data-slot="message"
      data-from={from}
      className={cn(
        "min-w-0 text-sm leading-relaxed text-foreground [overflow-wrap:anywhere]",
        from === "user" &&
          "ml-auto w-fit max-w-[90%] rounded-[var(--radius,0.5rem)] bg-muted px-4 py-3",
        className,
      )}
      {...props}
    />
  );
}

export interface MessageResponseProps {
  readonly children: string;
  readonly streaming?: boolean;
  readonly className?: string;
  readonly components?: StreamdownProps["components"];
  readonly urlTransform?: StreamdownProps["urlTransform"];
}

const safeUrl = (url: string) => (/^https?:\/\//i.test(url) ? url : undefined);

const components: StreamdownProps["components"] = {
  strong: "strong",
  em: "em",
  pre: "pre",
  code: "code",
  a: ({ href, children }) =>
    href ? (
      <a href={href} target="_blank" rel="noopener noreferrer">
        {children}
      </a>
    ) : (
      <span>{children}</span>
    ),
  // Hosts opt into remote images through their own renderer and URL policy.
  img: () => null,
};

/** Streaming Markdown without raw HTML. Hosts own link and image policy. */
export function MessageResponse({
  children,
  streaming = false,
  className,
  components: hostComponents,
  urlTransform = safeUrl,
}: MessageResponseProps) {
  return (
    <Streamdown
      className={cn(
        "space-y-3 [overflow-wrap:anywhere] [&_a]:underline [&_a]:underline-offset-4 [&_blockquote]:border-l-2 [&_blockquote]:border-border [&_blockquote]:pl-4 [&_blockquote]:text-muted-foreground [&_code]:font-mono [&_h1]:text-xl [&_h2]:text-lg [&_h3]:text-base [&_h1]:font-semibold [&_h2]:font-semibold [&_h3]:font-semibold [&_ol]:list-decimal [&_ol]:pl-5 [&_ul]:list-disc [&_ul]:pl-5 [&_pre]:overflow-x-auto [&_pre]:rounded-[var(--radius,0.5rem)] [&_pre]:bg-muted [&_pre]:p-4 [&_pre]:text-xs [&_table]:w-full [&_th]:text-left [&_th]:font-semibold [&_td]:border-b [&_td]:border-border [&_td]:p-2 [&_th]:p-2 [&_p]:whitespace-normal",
        className,
      )}
      mode={streaming ? "streaming" : "static"}
      isAnimating={streaming}
      parseIncompleteMarkdown={streaming}
      controls={false}
      skipHtml
      rehypePlugins={[]}
      allowedElements={[
        "p",
        "a",
        "img",
        "strong",
        "em",
        "del",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "ul",
        "ol",
        "li",
        "blockquote",
        "pre",
        "code",
        "hr",
        "br",
        "table",
        "thead",
        "tbody",
        "tr",
        "th",
        "td",
      ]}
      urlTransform={urlTransform}
      components={{ ...components, ...hostComponents }}
    >
      {children}
    </Streamdown>
  );
}
