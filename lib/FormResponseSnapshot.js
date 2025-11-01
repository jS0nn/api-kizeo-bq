// FormResponseSnapshot Version 0.3.0
/**
 * Construit une représentation normalisée d'une réponse Kizeo.
 * Cette représentation (headers + row + métadonnées médias/sous-formulaires)
 * est utilisée par :
 *   - collectResponseArtifacts → ingestion BigQuery (notamment médias)
 *   - ExternalListsService → mise à jour des listes externes Kizeo
 * Elle ne persiste rien dans les feuilles Google Sheets.
 */

function formSnapshotLegacyLog(message, context) {
  if (typeof console !== 'undefined' && typeof console.log === 'function') {
    var suffix = context ? ' ' + JSON.stringify(context) : '';
    console.log('snapshot:FormResponse: ' + message + suffix);
  }
}

function resolveDriveMediaService() {
  if (
    typeof DriveMediaService === 'undefined' ||
    typeof DriveMediaService.getDefault !== 'function'
  ) {
    throw new Error('DriveMediaService indisponible');
  }
  return DriveMediaService.getDefault();
}

function formSnapshotExtractFields(dataResponse) {
  try {
    var fieldsData = [[], [], []];
    var index = 0;
    for (var champ in dataResponse.fields) {
      if (!Object.prototype.hasOwnProperty.call(dataResponse.fields, champ)) {
        continue;
      }
      fieldsData[0][index] = champ;
      fieldsData[1][index] = dataResponse.fields[champ].type;
      fieldsData[2][index] = dataResponse.fields[champ].value;
      index++;
    }
    return fieldsData;
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('FormResponseSnapshot.getDataFromFields', error);
    }
    return null;
  }
}

function formSnapshotPrepareDataForSheet(dataResponse) {
  try {
    var baseResponseData = [
      dataResponse.form_id,
      dataResponse.form_unique_id,
      dataResponse.id,
      dataResponse.user_id,
      dataResponse.last_name,
      dataResponse.first_name,
      dataResponse.answer_time,
      dataResponse.update_time,
      dataResponse.origin_answer
    ];

    var kizeoData = formSnapshotExtractFields(dataResponse);
    if (kizeoData === null) {
      return null;
    }

    var headers = [
      'form_id',
      'form_unique_id',
      'id',
      'user_id',
      'last_name',
      'first_name',
      'answer_time',
      'update_time',
      'origin_answer'
    ].concat(kizeoData[0]);
    var values = baseResponseData.concat(
      kizeoData[2].map(function (value) {
        return isNumeric(value) ? parseFloat(value) : value;
      })
    );

    return [headers, values, baseResponseData, kizeoData];
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('FormResponseSnapshot.prepareDataForSheet', error);
    }
    return null;
  }
}

function formSnapshotBuildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias) {
  try {
    var prepared = formSnapshotPrepareDataForSheet(dataResponse);
    if (prepared === null) {
      return null;
    }
    var headers = prepared[0];
    var values = prepared[1];
    var baseData = prepared[2];
    var tabFields = prepared[3];
    var baseLength = baseData.length;

    for (var i = 0; i < tabFields[0].length; i++) {
      var fieldName = tabFields[0][i];
      var fieldTypeRaw = tabFields[1][i];
      var fieldValue = tabFields[2][i];
      var targetIndex = baseLength + i;

      if (isSubformField(fieldTypeRaw, fieldValue)) {
        var subformRows = normalizeSubformRows(fieldValue);
        values[targetIndex] = JSON.stringify(subformRows);
        continue;
      }

      var fieldType = (fieldTypeRaw || '').toString().toLowerCase();
      if (fieldType === 'photo' || fieldType === 'signature') {
        var driveService;
        try {
          driveService = resolveDriveMediaService();
        } catch (error) {
          handleException('FormResponseSnapshot.resolveDriveMediaService', error, {
            formId: formulaire.id
          });
        }

        var mediaInfo = null;
        if (driveService && typeof driveService.processField === 'function') {
          try {
            mediaInfo = driveService.processField(
              formulaire.id,
              dataResponse.id,
              fieldName,
              fieldValue,
              spreadsheetBdD,
              { fieldType: fieldType, formName: formulaire.nom }
            );
          } catch (error) {
            handleException('FormResponseSnapshot.processField', error, {
              formId: formulaire.id,
              fieldName: fieldName
            });
          }
        }

        var formula = mediaInfo && mediaInfo.formula ? mediaInfo.formula : '';
        values[targetIndex] = formula;
        if (medias && mediaInfo && Array.isArray(mediaInfo.files) && mediaInfo.files.length) {
          mediaInfo.files.forEach(function (fileMeta) {
            var publicUrl = '';
            if (driveService && typeof driveService.normalizeDrivePublicUrl === 'function') {
              try {
                publicUrl = driveService.normalizeDrivePublicUrl(
                  fileMeta.driveUrl || fileMeta.driveViewUrl || ''
                );
              } catch (error) {
                handleException('FormResponseSnapshot.normalizeDrivePublicUrl', error, {
                  formId: formulaire.id,
                  fieldName: fieldName
                });
              }
            }

            medias.push({
              dataId: dataResponse.id,
              formId: formulaire.id,
              formName: formulaire.nom,
              formUniqueId: dataResponse.form_unique_id || '',
              fieldName: fieldName,
              fieldType: fieldType,
              mediaId: fileMeta.mediaId || '',
              fileName: fileMeta.fileName,
              driveFileId: fileMeta.fileId,
              driveUrl: fileMeta.driveUrl,
              driveViewUrl: fileMeta.driveViewUrl || '',
              drivePublicUrl: fileMeta.drivePublicUrl || publicUrl,
              folderId: fileMeta.folderId || '',
              folderUrl: fileMeta.folderUrl || '',
              id: formula,
              formula: formula,
              name: [fieldName, fileMeta.mediaId, fileMeta.fileId]
                .filter(function (part) {
                  return part && part !== '';
                })
                .join('_'),
              parentAnswerTime: dataResponse.answer_time || dataResponse._answer_time || '',
              parentUpdateTime: dataResponse.update_time || dataResponse._update_time || ''
            });
          });
        }
        continue;
      }

      if (Array.isArray(fieldValue)) {
        values[targetIndex] = fieldValue
          .map(function (value) {
            return value === null || value === undefined ? '' : value;
          })
          .join(', ');
        continue;
      }

      if (fieldValue && typeof fieldValue === 'object') {
        values[targetIndex] = JSON.stringify(fieldValue);
        continue;
      }

      values[targetIndex] = fieldValue;
    }

    return { existingHeaders: headers, rowEnCours: values };
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('FormResponseSnapshot.buildRowSnapshot', error);
    }
    return null;
  }
}

var FormResponseSnapshot = {
  extractFields: formSnapshotExtractFields,
  prepareDataForSheet: formSnapshotPrepareDataForSheet,
  buildRowSnapshot: formSnapshotBuildRowSnapshot
};

var ExternalSnapshot = FormResponseSnapshot;
