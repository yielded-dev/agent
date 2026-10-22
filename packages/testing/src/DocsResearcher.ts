/**
 * Document research fixtures with MCP discovery, bounded child summaries, and durable delegation controls.
 *
 * @since 0.1.0
 */
export {
  ResearchDocumentId,
  BoundedSummary,
  DocumentQuery,
  ResearchDocument,
  DocumentUnavailable,
  DocumentLibrary,
  FetchDocument,
  DocContentToolkit,
  docContentToolkitLayer,
  docsDocumentBodySecret,
  researchCorpusDocumentIds,
  researchDocumentLookup,
  researchDocumentFor,
  documentBodyPhrase,
  SummaryBrief,
  DocumentSummary,
  documentSummaryFor,
  encodedDocumentSummary,
  DocSummarizer,
  SummaryRequest,
  SummaryFinding,
  DocumentSummaryFailed,
  documentSummaryPolicy,
  delegateDocumentSummary,
  mapSummaryChildFailure,
  docsSummaryHandlersLayer,
  ResearchRequest,
  ResearchDigest,
  docsCoordinatorConfidentialMarker,
  docsMissionConfidentialMarker,
  DocsResearcherToolkit,
  DocsResearcher,
  researchMissionRequest,
  expectedResearchDigest,
} from "./fixtures/docs-researcher/definition.ts";

export {
  docsResearcherDeploymentId,
  docsResearcherProducerId,
  docsResearcherPrincipal,
  docsCoordinatorDigests,
  docsSummarizerDigests,
  docsResearcherSubmitOptions,
  docsResearcherSubmitAgent,
  summarizeCallId,
  fetchCallId,
  type DocsResearcherHarnessOptions,
  type DocsResearcherHarness,
  makeDocsResearcherHarness,
  redactedDocumentPreview,
  type DocsResearcherHarnessRequirements,
} from "./fixtures/docs-researcher/harness.ts";

export {
  docsMcpRequest,
  docsMcpIdentity,
  docsMcpConnectorLayer,
  docsMcpOversizedConnectorLayer,
  docsMcpMismatchedConnectorLayer,
  assertDiscoveryMatchesAuthoredToolkit,
  DocsMcpDiscoveryEvidence,
} from "./fixtures/docs-researcher/mcp.ts";
