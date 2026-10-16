import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const SearchActivities = Tool.make("search_activities", {
  description: "Find activities in a city.",
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Array(Schema.String),
});

export const TravelTools = Toolkit.make(SearchActivities);

// Sample data. A real handler can query your database or a travel API.
const activities = [
  { city: "Lisbon", name: "Riverside walk" },
  { city: "Lisbon", name: "Food market" },
  { city: "Lisbon", name: "City museum" },
];

export const TravelToolsLive = TravelTools.toLayer({
  search_activities: ({ city }) =>
    Effect.succeed(activities.filter((a) => a.city === city).map((a) => a.name)),
});
