CREATE TABLE `workspaces` (
	`position` integer PRIMARY KEY AUTOINCREMENT,
	`id` text NOT NULL UNIQUE,
	`title` text,
	`added_at` integer NOT NULL
);
