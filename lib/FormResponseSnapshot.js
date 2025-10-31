(function (global) {
  /**
   * FormResponseSnapshot
   * --------------------
   * Construit une représentation normalisée d'une réponse Kizeo.
   * Cette représentation (headers + row + métadonnées médias/sous-formulaires)
   * est utilisée par :
   *   - collectResponseArtifacts → ingestion BigQuery (notamment médias)
   *   - ExternalListsService → mise à jour des listes externes Kizeo
   * Elle ne persiste rien dans les feuilles Google Sheets.
   */
  if (typeof FormResponseSnapshot !== 'undefined') {
    return;
  }

  function legacyLog(message, context) {
    if (typeof console !== 'undefined' && typeof console.log === 'function') {
      const suffix = context ? ` ${JSON.stringify(context)}` : '';
      console.log(`snapshot:FormResponse: ${message}${suffix}`);
    }
  }

  /**
   * Extrait les champs d'une réponse Kizeo et retourne un tableau [names, types, values].
   * @param {Object} dataResponse - réponse détaillée Kizeo (fields.*)
   * @return {Array|null}
   */
  function getDataFromFields(dataResponse) {
    try {
      const fieldsData = [[], [], []];
      let index = 0;
      for (const champ in dataResponse.fields) {
        fieldsData[0][index] = champ;
        fieldsData[1][index] = dataResponse.fields[champ].type;
        fieldsData[2][index] = dataResponse.fields[champ].value;
        index++;
      }
      return fieldsData;
    } catch (e) {
      handleException('FormResponseSnapshot.getDataFromFields', e);
      return null;
    }
  }

  /**
   * Construit la liste d'entêtes et de valeurs prêtes à être consommées par les services.
   * @param {Object} dataResponse
   * @return {Array|null} [headers, values, baseResponseData, tabFields]
   */
  function prepareDataForSheet(dataResponse) {
    try {
      const baseResponseData = [
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

      const kizeoData = getDataFromFields(dataResponse);
      if (kizeoData === null) {
        return null;
      }

      const headers = [
        'form_id',
        'form_unique_id',
        'id',
        'user_id',
        'last_name',
        'first_name',
        'answer_time',
        'update_time',
        'origin_answer',
        ...kizeoData[0]
      ];
      const values = [
        ...baseResponseData,
        ...kizeoData[2].map((value) => (isNumeric(value) ? parseFloat(value) : value))
      ];

      return [headers, values, baseResponseData, kizeoData];
    } catch (e) {
      handleException('FormResponseSnapshot.prepareDataForSheet', e);
      return null;
    }
  }

  /**
   * Produit le snapshot complet : headers, ligne, collecte médias/sous-formulaires.
   * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} spreadsheetBdD - contexte pour les médias (gestionChampImage)
   * @param {Object} formulaire - métadonnées du formulaire
   * @param {Object} dataResponse - réponse détaillée
   * @param {Array} medias - collection mutée de médias pour BigQuery
   * @return {{ existingHeaders: string[], rowEnCours: any[] }|null}
   */
  function buildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias) {
    try {
      const prepared = prepareDataForSheet(dataResponse);
      if (prepared === null) {
        return null;
      }
      const [headers, values, baseData, tabFields] = prepared;
      const baseLength = baseData.length;

      for (let i = 0; i < tabFields[0].length; i++) {
        const fieldName = tabFields[0][i];
        const fieldTypeRaw = tabFields[1][i];
        const fieldValue = tabFields[2][i];
        const targetIndex = baseLength + i;

        if (isSubformField(fieldTypeRaw, fieldValue)) {
          const subformRows = normalizeSubformRows(fieldValue);
          values[targetIndex] = JSON.stringify(subformRows);
          continue;
        }

        const fieldType = (fieldTypeRaw || '').toString().toLowerCase();
        if (fieldType === 'photo' || fieldType === 'signature') {
          const mediaInfo = gestionChampImage(
            formulaire.id,
            dataResponse.id,
            fieldName,
            fieldValue,
            spreadsheetBdD,
            { fieldType: fieldType, formName: formulaire.nom }
          );
          values[targetIndex] = mediaInfo && mediaInfo.formula ? mediaInfo.formula : '';
          if (medias && mediaInfo && Array.isArray(mediaInfo.files) && mediaInfo.files.length) {
            mediaInfo.files.forEach((fileMeta) => {
              medias.push({
                dataId: dataResponse.id,
                formId: formulaire.id,
                formName: formulaire.nom,
                formUniqueId: dataResponse.form_unique_id || '',
                fieldName,
                fieldType,
                mediaId: fileMeta.mediaId || '',
                fileName: fileMeta.fileName,
                driveFileId: fileMeta.fileId,
                driveUrl: fileMeta.driveUrl,
                driveViewUrl: fileMeta.driveViewUrl || '',
                drivePublicUrl:
                  fileMeta.drivePublicUrl ||
                  normalizeDrivePublicUrl(fileMeta.driveUrl || fileMeta.driveViewUrl || ''),
                folderId: fileMeta.folderId || '',
                folderUrl: fileMeta.folderUrl || '',
                id: mediaInfo.formula,
                formula: mediaInfo.formula,
                name: [fieldName, fileMeta.mediaId, fileMeta.fileId]
                  .filter((part) => part && part !== '')
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
            .map((value) => (value === null || value === undefined ? '' : value))
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
    } catch (e) {
      handleException('FormResponseSnapshot.buildRowSnapshot', e);
      return null;
    }
  }

  /**
   * Interface compatible avec l'ancien module SheetSnapshot (persistance Sheets désactivée).
   * Retourne simplement le snapshot afin de conserver l'appel contractuel.
   */
  function persistSnapshot(spreadsheetBdD, formulaire, snapshot) {
    legacyLog('persistSnapshot (FormResponseSnapshot): bypass legacy Sheets', {
      formId: formulaire && formulaire.id
    });
    return snapshot;
  }

  const service = {
    extractFields: getDataFromFields,
    prepareDataForSheet,
    buildRowSnapshot,
    persistSnapshot
  };

  global.FormResponseSnapshot = service;
  // Compatibilité temporaire (anciennes références).
  if (typeof global.ExternalSnapshot === 'undefined') {
    global.ExternalSnapshot = service;
  }
})(this);
