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
      if (!m.id) {
        console.log(`ID manquant pour le média ${m.name}`);
        return;
      }

      // Extraire l'ID du fichier de la formule HYPERLINK
      let fileId = m.id;
      if (fileId.includes('id=')) {
        fileId = fileId.split('id=')[1].split('"')[0];
      }

      const alreadyThere = mediaFolder.getFilesByName(m.name);
      if (alreadyThere.hasNext()) return;

      const file = DriveApp.getFileById(fileId);
      file.makeCopy(m.name, mediaFolder);
      
    } catch (e) {
      // Utiliser m.id au lieu de fileId qui pourrait ne pas être défini en cas d'erreur précoce
      console.log(`Erreur copie média ${m.name} : ${e.message}\nID original: ${m.id}`);
    }
  });
}

/**
 * Met à jour les données pour chaque onglet de feuille.
 * Si de nouvelles réponses sont trouvées pour le formulaire correspondant à un onglet, les données sont enregistrées.
 * Sinon, un message de log est affiché.
 */
function main() {
  const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  const onglets = spreadsheetBdD.getSheets();

  if (getEtatExecution() === 'enCours') {
    console.log('Exécution précédente toujours en cours.');
    console.log("En cas de blocage, réinitialisez l'état manuellement ou exécutez setScriptProperties('termine').");
    return;
  }

  setScriptProperties('enCours');

  try {
    for (const onglet of onglets) {
      const ongletName = onglet.getName();
      const [formNom, formId, extra] = ongletName.split(' || ');
      if (extra || !formId) continue; // hors nomenclature

      const action = libKizeo.ensureFormActionCode(spreadsheetBdD, formId);

      // ---------- Récupération des nouvelles données ----------
      const unreadResp = libKizeo.requeteAPIDonnees(
        'GET',
        `/forms/${formId}/data/unread/${action}/${nbFormulairesACharger}?includeupdated`
      );
      if (!unreadResp || unreadResp.data.status !== 'ok') {
        console.log(`Erreur API unread : ${unreadResp?.data?.status}`);
        continue;
      }
      const nouvellesDonnees = unreadResp.data.data;
      // Toujours préparer BigQuery / feuilles, même si aucune donnée à écrire.
      const processResult = libKizeo.processData(
        spreadsheetBdD,
        { nom: formNom, id: formId },
        action,
        nbFormulairesACharger
      );
      const mediasIndexes = processResult?.medias || []; // [{dataId,name,id}, …]

      if (!nouvellesDonnees.length) {
        Logger.log('Pas de nouveaux enregistrements');
        continue;
      }

      // ---------- Boucle par nouvel eneregistrement ----------
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
        console.log(`Un export est configuré pour ${dataId}, l'adresse est ${driveUrl}`)
        if (typeof driveUrl !== 'string' || !driveUrl.includes('drive.google.com')) return;
        const folderIdMatch =
          driveUrl.match(/drive\/u\/\d+\/folders\/([^?\s/]+)/) || driveUrl.match(/drive\/folders\/([^?\s/]+)/);
        if (!folderIdMatch) return;
        const folderId = folderIdMatch[1];

        /* sous‑répertoire optionnel */
        const subFolderName = Object.entries(dataFields).find(([k, v]) => k.includes('sousrepertoireexport') && v)?.[1] || null;
        const targetFolderId = subFolderName ? getOrCreateSubFolder(folderId, subFolderName) : folderId;

        /* type d'export */
        const typeExport = (Object.entries(dataFields).find(([k, v]) => k.includes('typeexport') && v)?.[1] || 'pdf').toString().toLowerCase();  //si aucun champ typeexport n'est trouvé on traite comme pdf
        console.log(`Type d'export ${typeExport} pour ${dataId}`)
        /* actions */
        if (['pdf', 'pdfmedia'].includes(typeExport)) {
          console.log("Export type PDF pour "+dataId)
          try {
            const pdfResp = libKizeo.requeteAPIDonnees('GET', `/forms/${formId}/data/${dataId}/pdf`);
            exportPdfBlob(formNom, dataId, pdfResp.data, targetFolderId);
          } catch (e) {
            Logger.log(`Erreur export PDF : ${e.message}`);
          }
        }
        if (['media', 'pdfmedia'].includes(typeExport)) {
          console.log("Export type media pour "+dataId)
          const mediasPourRecord = mediasIndexes.filter((m) => m.dataId === dataId);
          exportMedias(mediasPourRecord, targetFolderId);
        }
      });
    }
    setScriptProperties('termine');
  } catch (e) {
    setScriptProperties('termine');
    libKizeo.handleException('main', e);
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
  let spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  let sheetEnCours = spreadsheetBdD.getActiveSheet();
  
  var scriptProperties = PropertiesService.getScriptProperties();
  var etatExecution = scriptProperties.getProperty('etatExecution');
  //action : limite la portée de l'action markasread et unread à un spreadSheet : Attention si plusieurs fichiers sheet portent le meme nom !!!
  const ongletActif = sheetEnCours.getName();
  const [, formIdActif] = ongletActif.split(' || ');
  let action = null;
  if (formIdActif) {
    action = libKizeo.ensureFormActionCode(spreadsheetBdD, formIdActif);
  }

  let ui = SpreadsheetApp.getUi();
  let responseReInit = ui.alert('Avertissement', 'Souhaitez vous réinitialiser totalement le sheet?', ui.ButtonSet.OK_CANCEL);
  if (responseReInit === ui.Button.OK) {
    if (etatExecution === "enCours") {
      let forceReInit = ui.alert('Avertissement', 'Une exécution est en cours, souhaitez-vous forcer la réinitialisation?', ui.ButtonSet.OK_CANCEL);
      if (forceReInit === ui.Button.CANCEL) {
        throw new Error('Reinit : Exécution en cours');
      }
    }

    if (action) {
      libKizeo.marquerReponseNonLues(sheetEnCours, action);
    } else {
      console.log('reInit: action introuvable, marquage des réponses non lues ignoré.');
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
