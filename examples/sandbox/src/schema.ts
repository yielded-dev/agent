import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

// One database per authenticated account; conversation content stays in its Thread.
export const workspaces = sqliteTable("workspaces", {
  position: integer().primaryKey({ autoIncrement: true }),
  id: text().notNull().unique(),
  title: text(),
  addedAt: integer("added_at").notNull(),
});
