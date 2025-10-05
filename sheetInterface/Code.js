//Version 4.0

/**    DOC :
  https://www.kizeoforms.com/doc/swagger/v3/#/
    types GET: 
      /users : get all users
      /forms : list all forms
      /forms/{formId} : Get form definition
      /forms/{formId}/data : Get the list of all data of a form (not read)
      /forms/{formId}/data/all :Get the list of all data of a form
      /forms/{formId}/data/readnew : Get content of unread data
      /forms/{formId}/data/{dataId} : Get data of a form
      /forms/push/inbox : Receive new pushed data
      /forms/{formId}/data/{dataId}/pdf : Get PDF data of a form
      /forms/{formId}/exports : Get list of Word and Excel exports
      /forms/{formId}/data/{dataId}/exports/{exportId} : Export data
      /forms/{formId}/data/{dataId}/exports/{exportId}/pdf : Export data (PDF)
      /lists : Get External Lists
      /lists/{listId} : Get External List Definition
      /lists/{listId}/complete : Get External List Definition (Without taking in account filters)
      groups...
*/


  /**
  TODO : 
  - gestion des codes dans les formulaires kizeo
  - gestion des champs cases à choix multiples


  ⚠⚠⚠ Attention aux champs cases à choix multiples  (doit on concatainer les réponses ou créer un nouvel onglet ?) ⚠⚠⚠

  */

//Variable globale : nombre de formulaires chargés dans chaque requete
let nbFormulairesACharger=30;

function onOpen() {
  afficheMenu();
  console.log("Fin de onOpen");
  const props = PropertiesService.getDocumentProperties();
  const projectId = props.getProperty('BQ_PROJECT_ID');
  const dataset = props.getProperty('BQ_DATASET');
  const location = props.getProperty('BQ_LOCATION');
  console.log(`ScriptProperties BQ -> project=${projectId || 'NULL'}, dataset=${dataset || 'NULL'}, location=${location || 'NULL'}`);
}

/**
 * Définit les propriétés du script avec un état d'exécution donné.
 *
 * @param {string} etat - L'état d'exécution à définir.
 */
function setScriptProperties(etat){
  var scriptProperties = PropertiesService.getScriptProperties();
  scriptProperties.setProperty('etatExecution', etat);
}

function getEtatExecution() {
  return PropertiesService.getScriptProperties().getProperty('etatExecution');
}

function initBigQueryConfigFromSheet() {
  try {
    const ui = SpreadsheetApp.getUi();
    const defaults = libKizeo.initBigQueryConfig();
    const refreshedProps = PropertiesService.getDocumentProperties();
    refreshedProps.setProperties({
      BQ_PROJECT_ID: defaults.projectId,
      BQ_DATASET: defaults.datasetId,
      BQ_LOCATION: defaults.location || ''
    }, true);
    const finalProject = refreshedProps.getProperty('BQ_PROJECT_ID');
    const finalDataset = refreshedProps.getProperty('BQ_DATASET');
    const finalLocation = refreshedProps.getProperty('BQ_LOCATION');
    Logger.log(`initBigQueryConfigFromSheet -> project=${finalProject}, dataset=${finalDataset}, location=${finalLocation}`);
    ui.alert(`Configuration BigQuery initialisée :\nProjet=${finalProject}\nDataset=${finalDataset}\nLocation=${finalLocation || 'default'}`);
  } catch (e) {
    libKizeo.handleException('initBigQueryConfigFromSheet', e);
  }
}

// ----------------------
// Helpers Export (libérables dans libKizeo)
// ----------------------
/**
 * Retourne l'ID d'un sous‑répertoire existant ou fraîchement créé sous un dossier parent.
 * @param {string} parentFolderId – dossier parent
 * @param {string} subFolderName – nom du sous dossier à récupérer / créer
 * @return {string} id du sous‑dossier
 */
function getOrCreateSubFolder(parentFolderId, subFolderName) {
  const parent = DriveApp.getFolderById(parentFolderId);
  const it = parent.getFoldersByName(subFolderName);
  // Vérifie si un dossier avec ce nom existe déjà
  if (it.hasNext()) {
    // Si oui, retourne l'ID du dossier existant
    return it.next().getId();
  } else {
    // Sinon, crée un nouveau dossier et retourne son ID
    return parent.createFolder(subFolderName).getId();
  }
}

/**
 * Sauvegarde un blob PDF dans un dossier cible.
 */
function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  const fileName = `${formulaireNom}_${dataId}_${new Date()
    .toISOString()
    .replace(/[:.]/g, '-')}`;
  libKizeo.saveBlobToFolder(pdfBlob, targetFolderId, fileName);
}

/**
 * Copie des médias vers un sous‑dossier « media » sans écraser les fichiers déjà présents.
 * Un média est considéré comme déjà présent s'il existe un fichier du même nom dans le dossier cible.
 */
function exportMedias(mediaList, targetFolderId) {
  if (!mediaList?.length) return;

  const mediaFolderId = getOrCreateSubFolder(targetFolderId, 'media');
  const mediaFolder = DriveApp.getFolderById(mediaFolderId);

  mediaList.forEach((m) => {
    try {
      const displayName = m.name || m.fileName || `media_${m.dataId || 'unknown'}`;

      const candidateId = m.driveFileId || '';
      if (!candidateId && !m.id) {
        console.log(`ID manquant pour le média ${displayName}`);
        return;
      }

      // Extraire l'ID du fichier de la formule HYPERLINK si aucun ID dédié n'est présent
      let fileId = candidateId;
      if (!fileId && typeof m.id === 'string' && m.id.includes('id=')) {
        fileId = m.id.split('id=')[1].split('"')[0];
      }

      if (!fileId && typeof m.driveUrl === 'string' && m.driveUrl.includes('id=')) {
        fileId = m.driveUrl.split('id=')[1].split('&')[0];
      }

      if (!fileId) {
        console.log(`Impossible de déterminer l'ID Drive pour ${displayName}`);
        return;
      }

      const alreadyThere = mediaFolder.getFilesByName(displayName);
      if (alreadyThere.hasNext()) return;

      const file = DriveApp.getFileById(fileId);
      file.makeCopy(displayName, mediaFolder);
      
    } catch (e) {
      // Utiliser m.id au lieu de fileId qui pourrait ne pas être défini en cas d'erreur précoce
      console.log(`Erreur copie média ${m.name || m.fileName} : ${e.message}\nID original: ${m.driveFileId || m.id}`);
    }
  });
}

function readFormConfigFromSheet(sheet) {
  if (!sheet) return {};
  try {
    if (typeof libKizeo !== 'undefined' && typeof libKizeo.readConfigFromSheet === 'function') {
      const config = libKizeo.readConfigFromSheet(sheet);
      if (config) return config;
    }
  } catch (err) {
    console.log(`readFormConfigFromSheet fallback: ${err && err.message ? err.message : err}`);
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return {};
  }
  const values = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
  const config = {};
  values.forEach((row) => {
    const key = row[0];
    if (!key) return;
    config[String(key).trim()] = row[1];
  });
  return config;
}

function resolveFormulaireContext(spreadsheetBdD) {
  const sheets = spreadsheetBdD.getSheets();
  if (!sheets.length) return null;

  const evaluateSheet = function (sheet) {
    if (!sheet) return null;
    const sheetName = sheet.getName() || '';
    const [nameFromTitle, idFromTitle] = sheetName.split(' || ');
    const config = readFormConfigFromSheet(sheet) || {};
    const rawId = config.form_id || idFromTitle || '';
    const formId = rawId.toString().trim();
    if (!formId) return null;
    const rawName = config.form_name || nameFromTitle || '';
    const formName = rawName.toString().trim() || `Form ${formId}`;
    return {
      sheet,
      formulaire: {
        nom: formName,
        id: formId
      }
    };
  };

  const activeCandidate = evaluateSheet(spreadsheetBdD.getActiveSheet());
  if (activeCandidate) {
    return activeCandidate;
  }

  for (let i = 0; i < sheets.length; i++) {
    const candidate = evaluateSheet(sheets[i]);
    if (candidate) {
      return candidate;
    }
  }

  return null;
}

/**
 * Met à jour les données pour le formulaire configuré dans l'onglet unique du classeur.
 * Les réponses sont ingérées dans BigQuery et les exports Drive (PDF/médias) sont déclenchés si requis.
 */
function main() {
  const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  const context = resolveFormulaireContext(spreadsheetBdD);

  if (!context) {
    console.log('main: aucun formulaire configuré pour ce classeur.');
    return;
  }

  if (getEtatExecution() === 'enCours') {
    console.log('Exécution précédente toujours en cours.');
    console.log("En cas de blocage, réinitialisez l'état manuellement ou exécutez setScriptProperties('termine').");
    return;
  }

  setScriptProperties('enCours');

  try {
    const formulaire = context.formulaire;
    const action = libKizeo.ensureFormActionCode(spreadsheetBdD, formulaire.id);

    // ---------- Récupération des nouvelles données pour les exports ----------
    const unreadResp = libKizeo.requeteAPIDonnees(
      'GET',
      `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated`
    );

    if (!unreadResp || unreadResp.data.status !== 'ok') {
      console.log(`Erreur API unread : ${unreadResp?.data?.status}`);
      return;
    }

    const nouvellesDonnees = Array.isArray(unreadResp.data.data) ? unreadResp.data.data : [];

    // ---------- Préparation BigQuery et ingestion ----------
    const processResult = libKizeo.processData(
      spreadsheetBdD,
      formulaire,
      action,
      nbFormulairesACharger
    );
    const mediasIndexes = processResult?.medias || [];

    if (!nouvellesDonnees.length) {
      Logger.log('Pas de nouveaux enregistrements');
      return;
    }

    // ---------- Boucle par nouvel enregistrement ----------
    nouvellesDonnees.forEach((data) => {
      const dataFields = data || {};
      const dataId = data._id;

      /* localisation de l'export */
      const driveUrlEntry = Object.entries(dataFields).find(([k, v]) => k.includes('driveexport') && v);
      const driveUrl = driveUrlEntry ? driveUrlEntry[1] : null;

      if (!driveUrl) {
        // Pas de driveexport → on arrête ici cet iteration de forEach
        return;
      }
      console.log(`Un export est configuré pour ${dataId}, l'adresse est ${driveUrl}`);
      if (typeof driveUrl !== 'string' || !driveUrl.includes('drive.google.com')) return;
      const folderIdMatch =
        driveUrl.match(/drive\/u\/\d+\/folders\/([^?\s/]+)/) || driveUrl.match(/drive\/folders\/([^?\s/]+)/);
      if (!folderIdMatch) return;
      const folderId = folderIdMatch[1];

      /* sous‑répertoire optionnel */
      const subFolderName = Object.entries(dataFields).find(([k, v]) => k.includes('sousrepertoireexport') && v)?.[1] || null;
      const targetFolderId = subFolderName ? getOrCreateSubFolder(folderId, subFolderName) : folderId;

      /* type d'export */
      const typeExport =
        (Object.entries(dataFields).find(([k, v]) => k.includes('typeexport') && v)?.[1] || 'pdf')
          .toString()
          .toLowerCase(); // si aucun champ typeexport n'est trouvé on traite comme pdf
      console.log(`Type d'export ${typeExport} pour ${dataId}`);
      /* actions */
      if (['pdf', 'pdfmedia'].includes(typeExport)) {
        console.log('Export type PDF pour ' + dataId);
        try {
          const pdfResp = libKizeo.requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/${dataId}/pdf`);
          exportPdfBlob(formulaire.nom, dataId, pdfResp.data, targetFolderId);
        } catch (e) {
          Logger.log(`Erreur export PDF : ${e.message}`);
        }
      }
      if (['media', 'pdfmedia'].includes(typeExport)) {
        console.log('Export type media pour ' + dataId);
        const mediasPourRecord = mediasIndexes.filter((m) => m.dataId === dataId);
        exportMedias(mediasPourRecord, targetFolderId);
      }
    });
  } catch (e) {
    libKizeo.handleException('main', e);
  } finally {
    setScriptProperties('termine');
  }
}

/**
 * Réinitialise complètement la feuille si l'utilisateur est d'accord.
 * Toutes les feuilles sauf 'Reinit' sont supprimées et 'Reinit' est vidée.
 * Tous les déclencheurs sont supprimés et l'utilisateur est invité à définir une nouvelle durée de déclenchement.
 * Les données sont marquées comme non lues sur le serveur Kizeo
 * Ensuite, la fonction onOpen est exécutée et la fonction de sélection de formulaire est chargée.
 */
function reInit() {
  const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  const context = resolveFormulaireContext(spreadsheetBdD);
  const sheetEnCours = context ? context.sheet : spreadsheetBdD.getActiveSheet();
  const ui = SpreadsheetApp.getUi();

  const scriptProperties = PropertiesService.getScriptProperties();
  const etatExecution = scriptProperties.getProperty('etatExecution');

  const formulaireEnCours = context ? context.formulaire : null;
  if (!formulaireEnCours || !formulaireEnCours.id) {
    ui.alert(
      'Avertissement',
      'Impossible d’identifier le formulaire actif pour ce classeur.',
      ui.ButtonSet.OK
    );
    Logger.log('reInit: formulaire introuvable, opération annulée.');
    return;
  }

  const action = libKizeo.ensureFormActionCode(spreadsheetBdD, formulaireEnCours.id);

  const responseReInit = ui.alert('Avertissement', 'Souhaitez vous réinitialiser totalement le sheet?', ui.ButtonSet.OK_CANCEL);
  if (responseReInit === ui.Button.OK) {
    if (etatExecution === "enCours") {
      let forceReInit = ui.alert('Avertissement', 'Une exécution est en cours, souhaitez-vous forcer la réinitialisation?', ui.ButtonSet.OK_CANCEL);
      if (forceReInit === ui.Button.CANCEL) {
        throw new Error('Reinit : Exécution en cours');
      }
    }

    if (action && typeof libKizeo.marquerReponseNonLues === 'function') {
      try {
        libKizeo.marquerReponseNonLues(sheetEnCours, action);
      } catch (err) {
        console.log(
          `reInit: marquerReponseNonLues indisponible (${err && err.message ? err.message : err})`
        );
      }
    } else {
      console.log('reInit: marquerReponseNonLues non exécutée (données non stockées en Sheet).');
    }
    
    sheetEnCours.setName('Reinit');

    let range = sheetEnCours.getDataRange();
    range.clearFormat();

    deleteAllTriggers();
    if (etatExecution === "enCours") {
      setScriptProperties('termine');
    }
    
    let onglets = spreadsheetBdD.getSheets();
    for (let i = 0; i < onglets.length; i++) {
      if (onglets[i].getName() !== 'Reinit') {
        spreadsheetBdD.deleteSheet(onglets[i]);
      } else {
        let range = onglets[i].getDataRange();
        range.clearContent();
      }
    }
    
    setScriptProperties('termine');
    deleteAllTriggers();
    
    // Demander la configuration du déclencheur
    askForTimeInterval();
  }
}
