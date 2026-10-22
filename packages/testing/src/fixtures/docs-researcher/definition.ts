/**
 * A deterministic document corpus, summarizer, and coordinator with bounded result projection.
 *
 * @since 0.1.0
 */
import * as Agent from "@yielded/agent/agent";
import { AgentPolicy } from "@yielded/agent/agent-policy";
import { type RuntimeBinding } from "@yielded/agent/agent-runtime";
import * as Subagent from "@yielded/agent/subagent";
import { SubagentPolicy } from "@yielded/agent/subagent";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

// ---------------------------------------------------------------------------
// Docs Researcher (P7 internal agent #3, plan §6): a coordinator that
// delegates per-document summarization to a doc-summarizer child through the
// S2 durable delegation surface, with the child's content tools served —
// and validated — through the MCP connector against a scripted MCP fixture.
// The corpus, tools, and both Agent Definitions are deterministic fixtures in
// the travel-planner style so DN tests (and any later DC assembly) reuse them.
// ---------------------------------------------------------------------------

/**
 * A branded document identifier containing at most sixty-four characters.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ResearchDocumentId = Schema.NonEmptyString.check(Schema.isMaxLength(64)).pipe(
  Schema.brand("@effect-agent/testing/docs-researcher/ResearchDocumentId"),
);

/**
 * The decoded value of the ResearchDocumentId schema.
 *
 * @category models
 * @since 0.1.0
 */
export type ResearchDocumentId = typeof ResearchDocumentId.Type;

const BoundedTitle = Schema.NonEmptyString.check(Schema.isMaxLength(120));
const BoundedBody = Schema.NonEmptyString.check(Schema.isMaxLength(16 * 1024));

/**
 * Bounded summary text: the ONLY child-derived text that may cross to the parent.
 *
 * @category schemas
 * @since 0.1.0
 */
export const BoundedSummary = Schema.NonEmptyString.check(Schema.isMaxLength(240));

/**
 * The document identifier requested from the content library.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DocumentQuery extends Schema.Class<DocumentQuery>("DocumentQuery")({
  documentId: ResearchDocumentId,
}) {}

/**
 * One bounded research document as the MCP content server exposes it.
 *
 * @category schemas
 * @since 0.1.0
 */
export class ResearchDocument extends Schema.Class<ResearchDocument>("ResearchDocument")({
  documentId: ResearchDocumentId,
  title: BoundedTitle,
  body: BoundedBody,
}) {}

/**
 * The content library could not supply the requested document.
 *
 * @category errors
 * @since 0.1.0
 */
export class DocumentUnavailable extends Schema.TaggedError<DocumentUnavailable>()(
  "DocumentUnavailable",
  {
    documentId: ResearchDocumentId,
    message: Schema.String,
  },
) {}

/**
 * The content store behind the scripted MCP server.
 *
 * @category services
 * @since 0.1.0
 */
export class DocumentLibrary extends Context.Service<
  DocumentLibrary,
  {
    readonly fetch: (query: DocumentQuery) => Effect.Effect<ResearchDocument, DocumentUnavailable>;
  }
>()("@effect-agent/testing/docs-researcher/DocumentLibrary") {}

/**
 * The one content tool the doc-summarizer child uses. Its authored JSON
 * schema is what MCP discovery must serve byte-for-byte: the scripted MCP
 * fixture derives its discovery entry from `Tool.getJsonSchema(FetchDocument)`
 * and the assembly checks the discovered schemas before worker registration.
 *
 * @category constructors
 * @since 0.1.0
 */
export const FetchDocument = Tool.make("fetch_document", {
  description: "Fetch one bounded research document by its identifier.",
  parameters: DocumentQuery,
  success: ResearchDocument,
  failure: DocumentUnavailable,
  failureMode: "error",
  dependencies: [DocumentLibrary],
});

/**
 * The document fetch tool exposed by the MCP content fixture.
 *
 * @category constructors
 * @since 0.1.0
 */
export const DocContentToolkit = Toolkit.make(FetchDocument);

/**
 * Connects the document fetch handler to DocumentLibrary.
 *
 * @category layers
 * @since 0.1.0
 */
export const docContentToolkitLayer = DocContentToolkit.toLayer({
  fetch_document: (query) => Effect.flatMap(DocumentLibrary, (library) => library.fetch(query)),
});

// ---------------------------------------------------------------------------
// Deterministic corpus. Every body deliberately embeds BOTH a secret marker
// and a distinctive body phrase: the tests assert that neither ever reaches
// the parent Thread, the parent prompts, or a redacted preview — only
// the bounded summary crosses the delegation boundary (SUB-015, SEC-008).
// ---------------------------------------------------------------------------

/**
 * Never allowed outside a child Thread or an unredacted fixture value.
 *
 * @category models
 * @since 0.1.0
 */
export const docsDocumentBodySecret = "docs-vault-secret-771";

const decodeDocumentId = Schema.decodeSync(ResearchDocumentId);

interface CorpusEntry {
  readonly document: ResearchDocument;
  readonly bodyPhrase: string;
  readonly summary: string;
}

const corpusEntries = new Map<string, CorpusEntry>(
  [
    {
      documentId: "durability-notes",
      title: "Durability protocol notes",
      bodyPhrase: "amber-ledger-passage",
      summary:
        "Settlement results are recorded exactly once while external side effects stay at-least-once.",
    },
    {
      documentId: "subagent-notes",
      title: "Subagent join notes",
      bodyPhrase: "cobalt-join-corridor",
      summary: "A parent joins only the verified settlement of its own established child.",
    },
  ].map((entry) => [
    entry.documentId,
    {
      document: ResearchDocument.make({
        documentId: decodeDocumentId(entry.documentId),
        title: entry.title,
        body: `${entry.bodyPhrase}: internal working notes. ${docsDocumentBodySecret}. ${entry.summary} Raw notes stay inside the child Thread.`,
      }),
      bodyPhrase: entry.bodyPhrase,
      summary: entry.summary,
    },
  ]),
);

/**
 * The corpus document ids in canonical fixture order.
 *
 * @category models
 * @since 0.1.0
 */
export const researchCorpusDocumentIds: ReadonlyArray<ResearchDocumentId> = [
  decodeDocumentId("durability-notes"),
  decodeDocumentId("subagent-notes"),
];

const requireCorpusEntry = (documentId: string): CorpusEntry => {
  const entry = corpusEntries.get(documentId);

  if (entry === undefined) {
    // oxlint-disable-next-line effect/no-untyped-throw, effect/no-native-error -- Synchronous deterministic fixture lookup rejects programmer-supplied corpus configuration.
    throw new Error(`No deterministic corpus entry exists for document ${documentId}`);
  }

  return entry;
};

/**
 * Deterministic library lookup shared by the scripted MCP content handlers.
 *
 * @category utils
 * @since 0.1.0
 */
export const researchDocumentLookup = (
  query: DocumentQuery,
): Effect.Effect<ResearchDocument, DocumentUnavailable> => {
  const entry = corpusEntries.get(query.documentId);

  return entry === undefined
    ? Effect.fail(
        DocumentUnavailable.make({
          documentId: query.documentId,
          message: "No deterministic corpus entry exists for this document.",
        }),
      )
    : Effect.succeed(entry.document);
};

/**
 * The full fixture document (body includes the secret marker — child-side only).
 *
 * @category utils
 * @since 0.1.0
 */
export const researchDocumentFor = (documentId: string): ResearchDocument =>
  requireCorpusEntry(documentId).document;

/**
 * The distinctive body phrase used by context-isolation assertions.
 *
 * @category utils
 * @since 0.1.0
 */
export const documentBodyPhrase = (documentId: string): string =>
  requireCorpusEntry(documentId).bodyPhrase;

// ---------------------------------------------------------------------------
// Doc Summarizer: the child Agent Definition. Its toolkit is the authored
// `DocContentToolkit`; the harness registers its worker Binding only after
// MCP discovery validates that exact toolkit (mcp.ts).
// ---------------------------------------------------------------------------

/**
 * The document and focus passed into the summarizer child.
 *
 * @category schemas
 * @since 0.1.0
 */
export class SummaryBrief extends Schema.Class<SummaryBrief>("SummaryBrief")({
  documentId: ResearchDocumentId,
  focus: Schema.NonEmptyString,
}) {}

/**
 * A document identifier and bounded summary returned by the child.
 *
 * @category schemas
 * @since 0.1.0
 */
export class DocumentSummary extends Schema.Class<DocumentSummary>("DocumentSummary")({
  documentId: ResearchDocumentId,
  summary: BoundedSummary,
}) {}

/**
 * The summary the scripted child writes after fetching the document.
 *
 * @category utils
 * @since 0.1.0
 */
export const documentSummaryFor = (documentId: string): DocumentSummary =>
  DocumentSummary.make({
    documentId: requireCorpusEntry(documentId).document.documentId,
    summary: requireCorpusEntry(documentId).summary,
  });

/**
 * Encode the expected document summary as JSON for a scripted model response.
 *
 * @category utils
 * @since 0.1.0
 */
export const encodedDocumentSummary = (documentId: string): string =>
  JSON.stringify(Schema.encodeSync(DocumentSummary)(documentSummaryFor(documentId)));

/**
 * A child agent that fetches one document and returns a bounded summary.
 *
 * @category constructors
 * @since 0.1.0
 */
export const DocSummarizer = Agent.make("doc-summarizer", {
  input: SummaryBrief,
  output: DocumentSummary,
  instructions:
    "Fetch the briefed document with fetch_document exactly once, then return only a JSON document summary. Never copy raw notes or secrets into the summary.",
  toolkit: DocContentToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  description: "Summarize one bounded research document fetched through MCP content tools.",
});

// ---------------------------------------------------------------------------
// Delegation Definition: the coordinator sees exactly one Tool with explicit
// projections and finite bounds. `projectResult` is the declassification
// boundary (SUB-015): only the bounded summary crosses; the fetched body —
// secret marker included — stays in the child Thread.
// ---------------------------------------------------------------------------

/**
 * The document requested through the coordinator summary tool.
 *
 * @category schemas
 * @since 0.1.0
 */
export class SummaryRequest extends Schema.Class<SummaryRequest>("SummaryRequest")({
  documentId: ResearchDocumentId,
}) {}

/**
 * The document summary permitted to cross the child-to-parent boundary.
 *
 * @category schemas
 * @since 0.1.0
 */
export class SummaryFinding extends Schema.Class<SummaryFinding>("SummaryFinding")({
  documentId: ResearchDocumentId,
  summary: BoundedSummary,
}) {}

/**
 * A child summary failure exposed through its error tag.
 *
 * @category errors
 * @since 0.1.0
 */
export class DocumentSummaryFailed extends Schema.TaggedError<DocumentSummaryFailed>()(
  "DocumentSummaryFailed",
  {
    childErrorTag: Schema.NonEmptyString,
  },
) {}

/**
 * Finite per-invocation bounds (SUB-009): one fetch per child, two children per Run.
 *
 * @category constructors
 * @since 0.1.0
 */
export const documentSummaryPolicy = SubagentPolicy.make({
  maxChildren: 2,
  maxConcurrency: 2,
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "10 seconds",
});

/**
 * Delegates document summarization and projects the bounded summary to the coordinator.
 *
 * @category constructors
 * @since 0.1.0
 */
export const delegateDocumentSummary = Subagent.make("delegate_document_summary", {
  description:
    "Summarize one research document through the doc-summarizer child and return a bounded finding.",
  target: DocSummarizer,
  parameters: SummaryRequest,
  success: SummaryFinding,
  failure: DocumentSummaryFailed,
  prepareInput: (request) =>
    Effect.succeed(
      SummaryBrief.make({
        documentId: request.documentId,
        focus: "summarize:durability-claims",
      }),
    ),
  projectResult: (summary) =>
    Effect.succeed(
      SummaryFinding.make({
        documentId: summary.documentId,
        summary: summary.summary,
      }),
    ),
  policy: documentSummaryPolicy,
});

/**
 * Total mapping from every expected child Run failure to the declared Tool failure (SUB-028).
 *
 * @category utils
 * @since 0.1.0
 */
export const mapSummaryChildFailure = (failure: { readonly _tag: string }): DocumentSummaryFailed =>
  DocumentSummaryFailed.make({ childErrorTag: failure._tag });

/**
 * Runtime wiring: the immutable delegation plus one explicit child Binding (S2 declaration).
 *
 * @category layers
 * @since 0.1.0
 */
export const docsSummaryHandlersLayer = <ModelProvides, ModelRequires>(
  childBinding: RuntimeBinding<
    typeof SummaryBrief,
    typeof DocumentSummary,
    string,
    Toolkit.Tools<typeof DocContentToolkit>,
    ModelProvides,
    ModelRequires
  >,
) =>
  Subagent.layer(delegateDocumentSummary, childBinding, {
    mapChildFailure: mapSummaryChildFailure,
  });

// ---------------------------------------------------------------------------
// Docs Researcher: the parent Agent Definition.
// ---------------------------------------------------------------------------

/**
 * A mission and document identifiers supplied to the research coordinator.
 *
 * @category schemas
 * @since 0.1.0
 */
export class ResearchRequest extends Schema.Class<ResearchRequest>("ResearchRequest")({
  question: Schema.NonEmptyString,
  documentIds: Schema.Array(ResearchDocumentId).check(Schema.isMinLength(1)),
}) {}

/**
 * The coordinator output containing bounded document findings.
 *
 * @category schemas
 * @since 0.1.0
 */
export class ResearchDigest extends Schema.Class<ResearchDigest>("ResearchDigest")({
  findings: Schema.Array(SummaryFinding),
  nextAction: Schema.Literal("review"),
}) {}

/**
 * Parent-only transcript markers proving child context isolation (SUB-006/SUB-015).
 *
 * @category models
 * @since 0.1.0
 */
export const docsCoordinatorConfidentialMarker = "docs-coordinator-vault-19x";
/**
 * Synthetic private mission marker used to detect leakage into the summarizer child.
 *
 * @category models
 * @since 0.1.0
 */
export const docsMissionConfidentialMarker = "docs-mission-dossier-42f";

/**
 * The bounded document summary delegation tool exposed to the coordinator.
 *
 * @category constructors
 * @since 0.1.0
 */
export const DocsResearcherToolkit = Toolkit.make(delegateDocumentSummary.tool);

/**
 * A coordinator that delegates document summaries and returns a research digest.
 *
 * @category constructors
 * @since 0.1.0
 */
export const DocsResearcher = Agent.make("docs-researcher", {
  input: ResearchRequest,
  output: ResearchDigest,
  instructions: [
    "You are the Effect Agent P7 docs-researcher coordinator.",
    `Coordinator-only context: ${docsCoordinatorConfidentialMarker}.`,
    "Call delegate_document_summary once per requested document in one Tool batch.",
    "Return only a JSON digest built from the delegated findings. This is read-only research.",
  ].join("\n"),
  toolkit: DocsResearcherToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
  description: "Coordinate per-document summarization through one declared delegation Tool.",
});

/**
 * The default two-document research mission.
 *
 * @category models
 * @since 0.1.0
 */
export const researchMissionRequest = ResearchRequest.make({
  question: `Summarize the durability and subagent notes; keep ${docsMissionConfidentialMarker} inside the coordinator thread.`,
  documentIds: researchCorpusDocumentIds,
});

/**
 * The coordinator's expected final digest for the given documents.
 *
 * @category utils
 * @since 0.1.0
 */
export const expectedResearchDigest = (
  documentIds: ReadonlyArray<string> = researchCorpusDocumentIds,
): ResearchDigest =>
  ResearchDigest.make({
    findings: documentIds.map((documentId) => {
      const summary = documentSummaryFor(documentId);

      return SummaryFinding.make({
        documentId: summary.documentId,
        summary: summary.summary,
      });
    }),
    nextAction: "review",
  });
