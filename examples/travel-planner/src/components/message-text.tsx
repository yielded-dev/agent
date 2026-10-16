import { MessageResponse, type MessageResponseProps } from "@yielded/ui";

import { safeTravelUrl } from "../travel-content";
import { TravelImage } from "./travel/photo-gallery";

export const safeMessageUrl = (url: string | undefined) =>
  typeof url === "string" && /^\/trips\/[a-zA-Z0-9-]{1,80}\/[1-9]\d{0,8}$/.test(url)
    ? url
    : safeTravelUrl(url);

const components: MessageResponseProps["components"] = {
  a: ({ href, children }) => {
    const url = safeMessageUrl(href);

    return url ? (
      <a href={url} target="_blank" rel="noopener noreferrer">
        {children}
      </a>
    ) : (
      <span>{children}</span>
    );
  },
  img: ({ src, alt }) =>
    typeof src === "string" && safeTravelUrl(src) ? (
      <span className="message-photo">
        <TravelImage key={src} src={src} alt={alt || "Photo from the linked source"} />
      </span>
    ) : null,
};

/** No raw HTML, embedded frames, or executable links from model output. */
export function MessageText({
  text,
  streaming = false,
}: {
  readonly text: string;
  readonly streaming?: boolean;
}) {
  return (
    <MessageResponse
      className="message-markdown"
      streaming={streaming}
      urlTransform={safeMessageUrl}
      components={components}
    >
      {text}
    </MessageResponse>
  );
}
