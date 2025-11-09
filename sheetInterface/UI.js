if (typeof libKizeo === 'undefined' || libKizeo === null) {
  throw new Error('libKizeo indisponible (sheetInterface/UI)');
}

var reportException = libKizeo.handleException;
if (typeof reportException !== 'function') {
  throw new Error('handleException indisponible via libKizeo');
}

var fetchKizeo = libKizeo.requeteAPIDonnees;
if (typeof fetchKizeo !== 'function') {
  throw new Error('requeteAPIDonnees indisponible via libKizeo');
}

var computeTableName = libKizeo.bqComputeTableName;
if (typeof computeTableName !== 'function') {
  throw new Error('bqComputeTableName indisponible via libKizeo');
}

var sheetInterfaceHelpers = libKizeo.SheetInterfaceHelpers || null;
if (!sheetInterfaceHelpers) {
  throw new Error('SheetInterfaceHelpers indisponible via libKizeo');
}

var gestionFeuilles = libKizeo.gestionFeuilles;
if (typeof gestionFeuilles !== 'function') {
  throw new Error('gestionFeuilles indisponible via libKizeo');
}

var ensureBigQueryCoreTables = libKizeo.ensureBigQueryCoreTables;
if (typeof ensureBigQueryCoreTables !== 'function') {
  throw new Error('ensureBigQueryCoreTables indisponible via libKizeo');
}

var deleteFormTables = libKizeo.bqDeleteFormTables;
if (typeof deleteFormTables !== 'function') {
  throw new Error('bqDeleteFormTables indisponible via libKizeo');
}

/**
 * Crée un menu personnalisé dans la feuille de calcul active et ajoute des éléments avec des actions correspondantes.
 * 
 * Le menu affiche des options pour sélectionner un formulaire Kizeo, mettre à jour l'onglet actif et réinitialiser l'onglet actif.
 */
function afficheMenu() {
  try {
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('Configuration Kizeo')
      .addItem('Initialiser BigQuery', 'initBigQueryConfigFromSheet')
      .addItem('Selectionner le formulaire Kizeo', 'chargeSelectForm')
      .addItem('Actualiser BigQuery', 'majSheet')
      .addItem('Forcer la déduplication BigQuery', 'launchManualDeduplication')
      .addItem('Configurer la mise à jour automatique', 'openTriggerFrequencyDialog')
      .addSeparator()
      .addItem('Supprimer les déclencheurs automatiques', 'confirmDeleteTriggers')
      .addToUi();
  } catch (e) {
    reportException('afficheMenu', e);
  }
}



/**
 * Charge une interface utilisateur HTML pour sélectionner un formulaire.
 * 
 * L'interface est créée à partir du fichier HTML 'afficheSelectForm.html'.
 */
function chargeSelectForm() {
  try {
    const htmlServeur = HtmlService.createTemplateFromFile('afficheSelectForm.html');
    const htmlOutput = htmlServeur.evaluate().setWidth(800).setHeight(800);
    SpreadsheetApp.getUi().showModalDialog(htmlOutput, ' ');
  } catch (e) {
    reportException('chargeSelectForm', e);
  }
}

function confirmDeleteTriggers() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Confirmation',
    "Souhaitez-vous supprimer tous les déclencheurs automatiques ? Cette action arrête les mises à jour planifiées et la déduplication.",
    ui.ButtonSet.YES_NO
  );

  if (response !== ui.Button.YES) {
    return;
  }

  try {
    const mainHandler = typeof MAIN_TRIGGER_FUNCTION === 'undefined' ? 'main' : MAIN_TRIGGER_FUNCTION;
    const dedupHandler =
      typeof DEDUP_TRIGGER_FUNCTION === 'undefined' ? 'runBigQueryDeduplication' : DEDUP_TRIGGER_FUNCTION;
    deleteTriggersByFunction(mainHandler);
    deleteTriggersByFunction(dedupHandler);
    setStoredTriggerFrequency(TRIGGER_DISABLED_KEY);
    persistTriggerFrequencyToSheet(TRIGGER_DISABLED_KEY);
    ui.alert(
      'Information',
      'Les déclencheurs automatiques ont été supprimés. Les mises à jour planifiées et la déduplication sont désormais stoppées.',
      ui.ButtonSet.OK
    );
  } catch (e) {
    uiHandleException('confirmDeleteTriggers', e);
    ui.alert('Erreur', 'La suppression des déclencheurs a échoué. Consultez les journaux pour plus de détails.', ui.ButtonSet.OK);
  }
}

/**
 * Charge la liste des formulaires depuis une API.
 * 
 * Les formulaires sont triés par ordre alphabétique.
 * 
 * @returns {Array} - Le tableau des formulaires.
 */
function chargelisteFormulaires() {
  try {
    const listeFormulaires = fetchKizeo('GET', `/forms`).data;
    const tableauForms = listeFormulaires.forms.sort((a, b) => a.name.localeCompare(b.name));

    return tableauForms;
  } catch (e) {
    reportException('chargelisteFormulaires', e);
  }
}

function describeBigQueryTableStatus(options) {
  if (!sheetInterfaceHelpers || typeof sheetInterfaceHelpers.describeBigQueryTableStatus !== 'function') {
    throw new Error('SheetInterfaceHelpers.describeBigQueryTableStatus indisponible.');
  }
  const request = options || {};
  return sheetInterfaceHelpers.describeBigQueryTableStatus({
    tableName: request.tableName || '',
    formId: request.formId || '',
    formName: request.formName || ''
  });
}

function deleteExistingBigQueryTables(options) {
  if (typeof deleteFormTables !== 'function') {
    throw new Error('bqDeleteFormTables indisponible via libKizeo');
  }
  const request = options || {};
  const formId = request.id || request.formId;
  const formName = request.nom || request.formName || '';
  const tableName = request.tableName || '';

  if (!formId || !tableName) {
    throw new Error('Paramètres insuffisants pour supprimer la table BigQuery.');
  }

  const formulaire = {
    id: formId,
    nom: formName,
    tableName: tableName,
    alias: request.alias || ''
  };

  return deleteFormTables(formulaire, { parentTableId: tableName });
}

/**
 * Enregistre les données du formulaire dans la feuille de calcul.
 * 
 * Affiche un avertissement si l'ID du formulaire est vide.
 * Sinon, lance la mise à jour des données du formulaire.
 * 
 * @param {Object} formulaire - L'objet formulaire contenant le nom et l'ID du formulaire.
 */
function enregistrementUI(formulaire) {
  let user;
  try {
    user = Session.getActiveUser().getEmail();
    const ui = SpreadsheetApp.getUi();
    const rawTableName = formulaire.tableName ? String(formulaire.tableName).trim() : '';
    const formulaireData = { nom: formulaire.nom, id: formulaire.id };

    Logger.log(`enregistrementUI du formulaire: ${formulaireData.nom} / ${formulaireData.id} / ${user}`);

    if (!formulaireData.id) {
      ui.alert('Avertissement', 'Veuillez choisir un formulaire.', ui.ButtonSet.OK);
      Logger.log('enregistrementUI: Avertissement - Veuillez choisir un formulaire.');
      return;
    }

    let tableName = '';
    try {
      tableName = computeTableName(formulaireData.id, formulaireData.nom, rawTableName);
    } catch (computeError) {
      Logger.log(`enregistrementUI: échec calcul nom table -> ${computeError}`);
    }

    if (!tableName) {
      ui.alert('Avertissement', "Impossible de déterminer le nom de la table BigQuery.", ui.ButtonSet.OK);
      Logger.log('enregistrementUI: Avertissement - Nom de table BigQuery manquant.');
      return;
    }

    if (tableName.length > MAX_BQ_TABLE_NAME_LENGTH) {
      ui.alert(
        'Avertissement',
        `Le nom de table BigQuery doit contenir ${MAX_BQ_TABLE_NAME_LENGTH} caractères au maximum.`,
        ui.ButtonSet.OK
      );
      Logger.log(
        `enregistrementUI: Avertissement - Nom de table BigQuery trop long (${tableName.length} caractères).`
      );
      return;
    }

    const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
    if (getEtatExecution() === 'enCours') {
      console.log('enregistrementUI: exécution précédente détectée avant configuration.');
      if (sheetInterfaceHelpers) {
        sheetInterfaceHelpers.notifyExecutionAlreadyRunning({
          shouldThrow: true,
          errorMessage: 'EXECUTION_EN_COURS',
          showToast: false,
          showAlert: false
        });
      }
    }
    setScriptProperties('enCours');
    try {
      const targetSheet = gestionFeuilles(spreadsheetBdD, formulaireData);
      if (!targetSheet) {
        ui.alert('Erreur', 'Impossible de préparer la feuille de configuration.', ui.ButtonSet.OK);
        Logger.log('enregistrementUI: gestionFeuilles a renvoyé null.');
        return;
      }

      const existingConfig = readFormConfigFromSheet(targetSheet);
      const currentAction = existingConfig && existingConfig.action ? existingConfig.action.toString().trim() : '';
      const actionCode = currentAction || createActionCode();
      const existingBatchLimit = sanitizeBatchLimitValue(existingConfig[CONFIG_BATCH_LIMIT_KEY]);
      const resolvedBatchLimit =
        existingBatchLimit !== null ? existingBatchLimit : DEFAULT_KIZEO_BATCH_LIMIT;
      const resolvedIngestBigQuery = sanitizeBooleanConfigFlag(
        existingConfig[CONFIG_INGEST_BIGQUERY_KEY],
        true
      );
      const finalConfig = Object.assign({}, existingConfig, {
        form_id: formulaireData.id,
        form_name: formulaireData.nom,
        bq_table_name: tableName,
        action: actionCode,
        [CONFIG_BATCH_LIMIT_KEY]: resolvedBatchLimit,
        [CONFIG_INGEST_BIGQUERY_KEY]: resolvedIngestBigQuery
      });

      writeFormConfigToSheet(targetSheet, finalConfig);
      ensureDeduplicationTrigger();
      console.log(`Enregistrement UI -> action=${actionCode}, table=${tableName}`);
      try {
        ensureBigQueryCoreTables();
      } catch (ensureError) {
        reportException('enregistrementUI.ensureCore', ensureError, {
          formId: formulaireData.id,
          tableName
        });
      }
      try {
        const toastMessage =
          "Formulaire configuré. Utilisez 'Actualiser BigQuery' pour récupérer les nouvelles données.";
        const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
        if (spreadsheet) {
          spreadsheet.toast(toastMessage, 'Configuration terminée', 8);
        }
      } catch (toastError) {
        console.log(`enregistrementUI: toast KO -> ${toastError}`);
      }
    } finally {
      setScriptProperties('termine');
    }
  } catch (e) {
    reportException('enregistrementUI', e, { formulaire: formulaire, user: user });
    throw e;
  }
}

/**
 * Lancement manuel du programme de maj
 * 
 */
function majSheet() {
  if (getEtatExecution() === 'enCours') {
    console.log('majSheet: exécution précédente détectée.');
    if (sheetInterfaceHelpers) {
      sheetInterfaceHelpers.notifyExecutionAlreadyRunning({
        toastMessage:
          "Une mise à jour est déjà en cours. Relancez depuis le menu seulement lorsqu'elle sera terminée.",
        alertMessage:
          "Une mise à jour est déjà en cours. Patientez avant de relancer depuis le menu."
      });
    }
    return;
  }
  main({ origin: 'menu' });
}

/**
 * Ouvre la boîte de dialogue de configuration du déclencheur automatique.
 */
function openTriggerFrequencyDialog() {
  try {
    const htmlTemplate = HtmlService.createTemplateFromFile('timeIntervalSelector.html');
    htmlTemplate.selectedFrequency = getStoredTriggerFrequency();
    const htmlOutput = htmlTemplate.evaluate()
      .setWidth(400)
      .setHeight(430);
    SpreadsheetApp.getUi().showModalDialog(htmlOutput, 'Configuration de la mise à jour automatique');
  } catch (e) {
    uiHandleException('openTriggerFrequencyDialog', e);
  }
}

/**
 * Traite le choix d'intervalle sélectionné par l'utilisateur. Appel depuis le HTML
 * 
 * @param {string} choix - Le choix de l'utilisateur (format: [type][valeur] ou "none")
 */
function processIntervalChoice(choix) {
  const ui = SpreadsheetApp.getUi();
  const sanitizedChoice = sanitizeTriggerFrequency(choix || TRIGGER_DISABLED_KEY);

  if (choix && sanitizedChoice !== choix) {
    ui.alert(
      'Fréquence ajustée',
      `La valeur ${choix} n'est pas prise en charge. Application de ${describeTriggerOption(sanitizedChoice)}.`,
      ui.ButtonSet.OK
    );
  }

  const storedChoice = setStoredTriggerFrequency(sanitizedChoice);

  try {
    const option = configureTriggerFromKey(storedChoice);
    persistTriggerFrequencyToSheet(storedChoice);
    if (storedChoice === TRIGGER_DISABLED_KEY) {
      ui.alert('Information', 'Mise à jour automatique désactivée.', ui.ButtonSet.OK);
    } else if (option) {
      ui.alert('Information', `Mise à jour automatique programmée : ${option.label}.`, ui.ButtonSet.OK);
    }
  } catch (e) {
    uiHandleException('processIntervalChoice.configure', e, {
      triggerChoice: storedChoice
    });
    ui.alert('Erreur', "La configuration du déclencheur a échoué.", ui.ButtonSet.OK);
    return;
  }
}
