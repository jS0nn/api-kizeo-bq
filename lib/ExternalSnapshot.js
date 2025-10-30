(function (global) {
  if (typeof ExternalSnapshot !== 'undefined') {
    return;
  }

  function legacyLog(message, context) {
    if (typeof console !== 'undefined' && typeof console.log === 'function') {
      const suffix = context ? ` ${JSON.stringify(context)}` : '';
      console.log(`legacy:ExternalSnapshot: ${message}${suffix}`);
    }
  }

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
      handleException('ExternalSnapshot.getDataFromFields', e);
      return null;
    }
  }

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
      handleException('ExternalSnapshot.prepareDataForSheet', e);
      return null;
    }
  }

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
      handleException('ExternalSnapshot.buildRowSnapshot', e);
      return null;
    }
  }

  function persistSnapshot(spreadsheetBdD, formulaire, snapshot) {
    legacyLog('persistSnapshot (ExternalSnapshot): bypass legacy Sheets', {
      formId: formulaire && formulaire.id
    });
    return snapshot;
  }

  global.ExternalSnapshot = {
    extractFields: getDataFromFields,
    prepareDataForSheet,
    buildRowSnapshot,
    persistSnapshot
  };
})(this);
