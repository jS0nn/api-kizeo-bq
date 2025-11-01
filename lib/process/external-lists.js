function runExternalListsSync(formulaire, snapshot, fetchFn, apiPath, logFn, options) {
  var log = typeof logFn === 'function' ? logFn : console.log.bind(console);
  if (!snapshot) {
    return { metadataUpdateStatus: 'SKIPPED' };
  }

  var requester = typeof fetchFn === 'function' ? fetchFn : requeteAPIDonnees;
  var metadataUpdateStatus = 'SKIPPED';
  var deps = options && typeof options === 'object' ? options : {};
  var externalServiceCandidate = deps.externalService;
  var report =
    typeof deps.handleException === 'function'
      ? deps.handleException
      : typeof handleException === 'function'
      ? handleException
      : null;

  try {
    var finalUnreadResponse = requester('GET', apiPath);
    var finalUnread = finalUnreadResponse ? finalUnreadResponse.data : null;
    if (!Array.isArray(finalUnread && finalUnread.data) || finalUnread.data.length !== 0) {
      return { metadataUpdateStatus: metadataUpdateStatus };
    }

    try {
      var externalService =
        externalServiceCandidate && typeof externalServiceCandidate.updateFromSnapshot === 'function'
          ? externalServiceCandidate
          : typeof ExternalListsService !== 'undefined' &&
            ExternalListsService &&
            typeof ExternalListsService.updateFromSnapshot === 'function'
          ? ExternalListsService
          : null;

      if (!externalService) {
        log('ExternalListsService indisponible, synchronisation ignorée.');
        metadataUpdateStatus = 'SKIPPED';
      } else {
        var listeAjour = externalService.updateFromSnapshot(formulaire, snapshot, {
          fetch: requester,
          handleException: report || undefined,
          log: log
        });

        if (listeAjour === null) {
          log(
            'ExternalListsService.updateFromSnapshot a échoué, on conserve néanmoins le résumé d’ingestion.'
          );
          metadataUpdateStatus = 'FAILED';
        } else {
          metadataUpdateStatus = listeAjour || 'OK';
        }
      }
    } catch (metadataError) {
      if (report) {
        report('handleResponses.majListeExterne', metadataError, {
          formId: (formulaire && formulaire.id) || 'unknown'
        });
      } else {
        log('handleResponses.majListeExterne: ' + metadataError);
      }
      metadataUpdateStatus = 'ERROR';
    }
  } catch (finalUnreadError) {
    if (report) {
      report('handleResponses.finalUnreadCheck', finalUnreadError, {
        formId: (formulaire && formulaire.id) || 'unknown'
      });
    }
    metadataUpdateStatus = 'ERROR';
  }

  return { metadataUpdateStatus: metadataUpdateStatus };
}
