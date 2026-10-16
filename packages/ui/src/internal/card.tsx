import type { ComponentProps } from "react";

import { cn } from "./class-names.ts";

export function Card({ className, ...props }: ComponentProps<"section">) {
  return (
    <section
      data-slot="card"
      className={cn(
        "overflow-hidden rounded-[calc(var(--radius,0.5rem)*2)] border border-solid border-border bg-card text-card-foreground",
        className,
      )}
      {...props}
    />
  );
}

export function CardContent({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("p-4", className)} {...props} />;
}
