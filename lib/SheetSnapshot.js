(function (global) {
  if (typeof SheetSnapshot !== 'undefined') {
    return;
  }

  function withLegacyRecord(marker, fn) {
    if (typeof recordLegacyUsage === 'function') {
      try {
        recordLegacyUsage(marker);
      } catch (e) {
        handleException('SheetSnapshot.recordLegacyUsage', e, { marker });
      }
    }
    return fn();
  }

  function getDataFromFields(dataResponse) {
    try {
      const fieldsData = [[], [], []];
      let i = 0;
      for (const champ in dataResponse.fields) {
        fieldsData[0][i] = champ;
        fieldsData[1][i] = dataResponse.fields[champ].type;
        fieldsData[2][i] = dataResponse.fields[champ].value;
        i++;
      }
      return fieldsData;
    } catch (e) {
      handleException('getDataFromFields', e);
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
      handleException('prepareDataForSheet', e);
      return null;
    }
  }

  function buildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias) {
    try {
      const prepared = prepareDataForSheet(dataResponse);
      if (prepared === null) return null;
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
              const displayName = [fieldName, fileMeta.mediaId, fileMeta.fileId]
                .filter((part) => part && part !== '')
                .join('_');
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
                name: displayName,
                parentAnswerTime: dataResponse.answer_time || dataResponse._answer_time || '',
                parentUpdateTime: dataResponse.update_time || dataResponse._update_time || ''
              });
            });
          }
          continue;
        }

        if (Array.isArray(fieldValue)) {
          values[targetIndex] = fieldValue.map((v) => (v === null || v === undefined ? '' : v)).join(', ');
        } else if (fieldValue && typeof fieldValue === 'object') {
          values[targetIndex] = JSON.stringify(fieldValue);
        } else {
          values[targetIndex] = fieldValue;
        }
      }

      return { existingHeaders: headers, rowEnCours: values };
    } catch (e) {
      handleException('buildRowSnapshot', e);
      return null;
    }
  }

  function prepareSheet(sheetFormulaire, headers) {
    try {
      if (sheetFormulaire.getLastRow() === 0) {
        sheetFormulaire.getRange(1, 1, 1, headers.length).setValues([headers]);
      }
      return 'sheet preparee';
    } catch (e) {
      handleException('prepareSheet', e);
      return null;
    }
  }

  function getColumnIndices(values, headers, existingHeaders, sheetEnCours) {
    try {
      return values.map((value, index) => {
        const headerIndex = existingHeaders.indexOf(headers[index]);
        if (headerIndex === -1) {
          sheetEnCours.getRange(1, sheetEnCours.getLastColumn() + 1).setValue(headers[index]);
          return sheetEnCours.getLastColumn();
        }
        return headerIndex + 1;
      });
    } catch (e) {
      handleException('getColumnIndices', e);
      return null;
    }
  }

  function prepareDataToRowFormat(spreadsheetBdD, values, columnIndices, baseData, tabFields, formulaire, dataResponse, medias) {
    try {
      const rowValues = [];
      const dataId = dataResponse.id;
      const baseLength = baseData.length;

      for (let i = 0; i < values.length; i++) {
        if (i < baseLength) {
          rowValues[columnIndices[i] - 1] = values[i];
          continue;
        }

        const offset = i - baseLength;
        const fieldName = tabFields[0][offset];
        const fieldTypeRaw = tabFields[1][offset];
        const fieldValue = tabFields[2][offset];
        const columnIndex = columnIndices[i] - 1;

        if (isSubformField(fieldTypeRaw, fieldValue)) {
          const subformRows = normalizeSubformRows(fieldValue);
          rowValues[columnIndex] = subformRows.length
            ? gestionTableaux(spreadsheetBdD, formulaire, dataId, fieldName, subformRows)
            : '';
          continue;
        }

        const fieldType = (fieldTypeRaw || '').toString().toLowerCase();

        if (fieldType === 'photo' || fieldType === 'signature') {
          const mediaInfo = gestionChampImage(
            formulaire.id,
            dataId,
            fieldName,
            fieldValue,
            spreadsheetBdD,
            { fieldType: fieldType, formName: formulaire.nom }
          );
          const formula = mediaInfo && mediaInfo.formula ? mediaInfo.formula : '';
          rowValues[columnIndex] = formula;

          if (mediaInfo && Array.isArray(mediaInfo.files) && mediaInfo.files.length) {
            mediaInfo.files.forEach((fileMeta) => {
              const displayName = [fieldName, fileMeta.mediaId, fileMeta.fileId]
                .filter((part) => part && part !== '')
                .join('_');
              medias.push({
                dataId,
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
                id: formula,
                formula: formula,
                name: displayName,
                parentAnswerTime: dataResponse.answer_time || dataResponse._answer_time || '',
                parentUpdateTime: dataResponse.update_time || dataResponse._update_time || ''
              });
            });
          }
          continue;
        }

        if (Array.isArray(fieldValue)) {
          rowValues[columnIndex] = fieldValue
            .map((v) => (v === null || v === undefined ? '' : v))
            .join(', ');
          continue;
        }

        if (fieldValue && typeof fieldValue === 'object') {
          rowValues[columnIndex] = JSON.stringify(fieldValue);
          continue;
        }

        rowValues[columnIndex] = values[i];
      }
      return rowValues;
    } catch (e) {
      handleException('prepareDataToRowFormat', e);
      return null;
    }
  }

  function persistSnapshot(spreadsheetBdD, formulaire, snapshot) {
    if (!snapshot || !spreadsheetBdD || !formulaire) {
      return snapshot;
    }
    const sheet = getFormSheetById(spreadsheetBdD, formulaire.id);
    if (!sheet) {
      if (typeof recordLegacyUsage === 'function') {
        recordLegacyUsage('sheet_missing');
      }
      console.log(`${DATA_LOG_PREFIX || 'lib:Data'}: feuille legacy introuvable pour form=${formulaire.id}`);
      return snapshot;
    }

    try {
      prepareSheet(sheet, snapshot.existingHeaders);
      const headerLength = sheet.getLastColumn();
      const existingHeaders =
        headerLength > 0 ? sheet.getRange(1, 1, 1, headerLength).getValues()[0] : [];
      const columnIndices = getColumnIndices(snapshot.rowEnCours, snapshot.existingHeaders, existingHeaders, sheet);
      if (columnIndices === null) {
        return snapshot;
      }

      const refreshedHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      const rowValues = new Array(refreshedHeaders.length).fill('');
      columnIndices.forEach((colIndex, idx) => {
        if (colIndex && colIndex > 0) {
          rowValues[colIndex - 1] = snapshot.rowEnCours[idx];
        }
      });
      sheet.appendRow(rowValues);
      if (typeof recordLegacyUsage === 'function') {
        recordLegacyUsage('sheet_append');
      }
      return {
        existingHeaders: refreshedHeaders,
        rowEnCours: rowValues
      };
    } catch (e) {
      handleException('persistLegacySnapshotToSheet', e, { formId: formulaire.id });
      return snapshot;
    }
  }

  function saveDataToSheet(spreadsheetBdD, dataResponse, formulaire, sheetFormulaire, medias) {
    try {
      const [headers, values, baseData, tabFields] = prepareDataForSheet(dataResponse);
      if (!headers || !values) return null;
      if (prepareSheet(sheetFormulaire, headers) === null) return null;
      const existingHeaders = sheetFormulaire.getRange(1, 1, 1, sheetFormulaire.getLastColumn()).getValues()[0];
      const columnIndices = getColumnIndices(values, headers, existingHeaders, sheetFormulaire);
      if (columnIndices === null) return null;

      const rowValues = prepareDataToRowFormat(
        spreadsheetBdD,
        values,
        columnIndices,
        baseData,
        tabFields,
        formulaire,
        dataResponse,
        medias
      );

      sheetFormulaire.appendRow(rowValues);

      return { rowEnCours: rowValues, existingHeaders };
    } catch (e) {
      handleException('saveDataToSheet', e);
      return null;
    }
  }

  const snapshot = {
    prepareDataForSheet,
    extractFields: getDataFromFields,
    prepareSheet,
    getColumnIndices,
    prepareDataToRowFormat,
    buildRowSnapshot,
    persistSnapshot,
    saveDataToSheet
  };

  global.SheetSnapshot = snapshot;
})(this);
