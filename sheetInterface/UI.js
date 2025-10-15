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
      .addToUi();
  } catch (e) {
    libKizeo.handleException('afficheMenu', e);
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
    libKizeo.handleException('chargeSelectForm', e);
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
    const listeFormulaires = libKizeo.requeteAPIDonnees('GET',`/forms`).data;
    const tableauForms = listeFormulaires.forms.sort((a, b) => a.name.localeCompare(b.name));

    return tableauForms;
  } catch (e) {
    libKizeo.handleException('chargelisteFormulaires', e);
  }
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
      if (typeof libKizeo.bqComputeTableName === 'function') {
        tableName = libKizeo.bqComputeTableName(formulaireData.id, formulaireData.nom, rawTableName);
      }
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
    setScriptProperties('enCours');
    try {
      const targetSheet = libKizeo.gestionFeuilles(spreadsheetBdD, formulaireData);
      if (!targetSheet) {
        ui.alert('Erreur', 'Impossible de préparer la feuille de configuration.', ui.ButtonSet.OK);
        Logger.log('enregistrementUI: gestionFeuilles a renvoyé null.');
        return;
      }

      const existingConfig = readFormConfigFromSheet(targetSheet);
      const currentAction = existingConfig && existingConfig.action ? existingConfig.action.toString().trim() : '';
      const actionCode = currentAction || createActionCode();
      const finalConfig = Object.assign({}, existingConfig, {
        form_id: formulaireData.id,
        form_name: formulaireData.nom,
        bq_table_name: tableName,
        action: actionCode
      });

      writeFormConfigToSheet(targetSheet, finalConfig);
      ensureDeduplicationTrigger();
      console.log(`Enregistrement UI -> action=${actionCode}, table=${tableName}`);
      try {
        libKizeo.ensureBigQueryCoreTables();
      } catch (ensureError) {
        libKizeo.handleException('enregistrementUI.ensureCore', ensureError, {
          formId: formulaireData.id,
          tableName
        });
      }
      main();
    } finally {
      setScriptProperties('termine');
    }
  } catch (e) {
    libKizeo.handleException('enregistrementUI', e, { formulaire: formulaire, user: user });
  }
}

/**
 * Lancement manuel du programme de maj
 * 
 */
function majSheet() {
  main();
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
      .setHeight(350);
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
