// Version 1.2.0

const DATA_LOG_PREFIX = 'lib:Data';

/**
 * Documentation interne : liste des symboles exposés publiquement par la librairie.
 * Les tests unitaires s’appuient sur cette liste pour détecter les régressions
 * (ex. suppression involontaire d’une fonction globale).
 */
const LIB_PUBLIC_SYMBOLS = Object.freeze([
  'DATA_LOG_PREFIX',
  'handleException',
  'requeteAPIDonnees',
  'processData',
  'handleResponses',
  'markResponsesAsRead',
  'createIngestionServices',
  'buildExecutionTargets',
  'resolveBatchLimit',
  'resolveIsoTimestamp',
  'resolveUnreadDataset',
  'collectResponseArtifacts',
  'ingestBigQueryPayloads',
  'runExternalListsSync',
  'initBigQueryConfig',
  'ensureBigQueryCoreTables',
  'getBigQueryConfig',
  'bqComputeTableName',
  'bqExtractAliasPart',
  'bqRunDeduplicationForForm',
  'bqParentTableId',
  'bqBackfillForm',
  'DriveMediaService',
  'ExternalListsService',
  'SheetInterfaceHelpers',
  'SheetConfigHelpers',
  'SheetDriveExports',
  'FormResponseSnapshot',
  'gestionFeuilles',
  'isNumeric',
  'fetchUnreadResponses',
  'ingestResponsesBatch',
  'finalizeIngestionRun'
]);

function getLibPublicSymbols() {
  return LIB_PUBLIC_SYMBOLS;
}
