(function (global) {
  if (typeof SheetSnapshot !== 'undefined') {
    return;
  }

  function ensureCore() {
    if (typeof global.ExternalSnapshot === 'undefined') {
      throw new Error('ExternalSnapshot indisponible');
    }
    return global.ExternalSnapshot;
  }

  function legacyLog(message, context) {
    if (typeof console !== 'undefined' && typeof console.log === 'function') {
      const suffix = context ? ` ${JSON.stringify(context)}` : '';
      console.log(`legacy:SheetSnapshot: ${message}${suffix}`);
    }
  }

  function persistSubformRows(spreadsheetBdD, formulaire, dataId, fieldName, subformRows) {
    if (!Array.isArray(subformRows) || !subformRows.length) {
      return '';
    }

    if (typeof gestionTableaux !== 'function') {
      if (typeof recordLegacyUsage === 'function') {
        try {
          recordLegacyUsage('tableaux_fallback_json');
        } catch (e) {
          handleException('SheetSnapshot.recordLegacyUsage', e, { marker: 'tableaux_fallback_json' });
        }
      }
      legacyLog('gestionTableaux indisponible, stockage JSON du sous-formulaire.', {
        formId: formulaire && formulaire.id,
        fieldName: fieldName,
        dataId: dataId
      });
      try {
        return JSON.stringify(subformRows);
      } catch (e) {
        handleException('SheetSnapshot.persistSubformRows.stringify', e, {
          formId: formulaire && formulaire.id,
          fieldName: fieldName
        });
        return '';
      }
    }

    if (typeof recordLegacyUsage === 'function') {
      try {
        recordLegacyUsage('tableaux_call');
      } catch (e) {
        handleException('SheetSnapshot.recordLegacyUsage', e, { marker: 'tableaux_call' });
      }
    }

    return gestionTableaux(spreadsheetBdD, formulaire, dataId, fieldName, subformRows);
  }

  function prepareSheet(sheetFormulaire, headers) {
    try {
      if (
        !sheetFormulaire ||
        typeof sheetFormulaire.getLastRow !== 'function' ||
        typeof sheetFormulaire.getRange !== 'function'
      ) {
        throw new Error('prepareSheet: sheetFormulaire invalide ou incomplet');
      }
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
      if (
        !sheetEnCours ||
        typeof sheetEnCours.getLastColumn !== 'function' ||
        typeof sheetEnCours.getRange !== 'function'
      ) {
        throw new Error('getColumnIndices: sheetEnCours invalide ou incomplet');
      }
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
          rowValues[columnIndex] = persistSubformRows(spreadsheetBdD, formulaire, dataId, fieldName, subformRows);
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
          rowValues[columnIndex] = fieldValue.map((v) => (v === null || v === undefined ? '' : v)).join(', ');
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

    if (typeof recordLegacyUsage === 'function') {
      try {
        recordLegacyUsage('persist_snapshot');
      } catch (e) {
        handleException('SheetSnapshot.recordLegacyUsage', e, { marker: 'persist_snapshot' });
      }
    }

    const sheet = getFormSheetById(spreadsheetBdD, formulaire.id);
    if (!sheet) {
      legacyLog('feuille legacy introuvable', { formId: formulaire.id });
      return snapshot;
    }

    try {
      if (prepareSheet(sheet, snapshot.existingHeaders) === null) {
        return snapshot;
      }
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
    if (typeof recordLegacyUsage === 'function') {
      recordLegacyUsage('saveDataToSheet_call');
    }
    try {
      const core = ensureCore();
      const prepared = core.prepareDataForSheet(dataResponse);
      if (!prepared) {
        return null;
      }
      const [headers, values, baseData, tabFields] = prepared;
      if (prepareSheet(sheetFormulaire, headers) === null) {
        return null;
      }
      const existingHeaders = sheetFormulaire.getRange(1, 1, 1, sheetFormulaire.getLastColumn()).getValues()[0];
      const columnIndices = getColumnIndices(values, headers, existingHeaders, sheetFormulaire);
      if (columnIndices === null) {
        return null;
      }

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
    extractFields: (dataResponse) => ensureCore().extractFields(dataResponse),
    prepareDataForSheet: (dataResponse) => ensureCore().prepareDataForSheet(dataResponse),
    buildRowSnapshot: (spreadsheetBdD, formulaire, dataResponse, medias) =>
      ensureCore().buildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias),
    prepareSheet,
    getColumnIndices,
    prepareDataToRowFormat,
    persistSnapshot,
    saveDataToSheet
  };

  global.SheetSnapshot = snapshot;
})(this);
