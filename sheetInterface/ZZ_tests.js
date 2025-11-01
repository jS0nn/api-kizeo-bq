
function test() {
  const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  const onglets = spreadsheetBdD.getSheets();
  //action : limite la portée de l'action markasread et unread à un spreadSheet : Attention si plusieurs fichiers sheet portent le meme nom !!!
  let action=SpreadsheetApp.getActiveSpreadsheet().getName()
  const onglet=spreadsheetBdD.getActiveSheet()
  const ongletName = onglet.getName();
  const ongletTabName = ongletName.split(' || ');
  const formulaire = {
    nom: ongletTabName[0],
    id: ongletTabName[1]
  };
  if (ongletTabName.length < 3 && ongletTabName.length > 1) {
    const lastRow=onglet.getLastRow()
    let reponseAPIExports = libKizeo.requeteAPIDonnees('GET', `/forms/${formulaire.id}/exports`);
    let exportId=reponseAPIExports.data.exports[0].id;
    let reponseAPIlisteData = libKizeo.requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/all`);
    let dataId=reponseAPIlisteData.data.data[19].id;
    
    dataId=226762019		

    let reponseAPIExportsData = libKizeo.requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/${dataId}/pdf`);
    // Sauvegarde le blob d'image dans le dossier
    let folderId="1tEyg0CoAa_KcscictxmgLSgTDwMXY13e"
    let idFichier = libKizeo.DriveMediaService.getDefault().saveBlobToFolder(reponseAPIExportsData.data, folderId, "nomImage4");
    //let reponseAPIExportsDataPDF = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/${dataId}/exports/${exportId}/pdf`);
    let bp=0;
  }

  let bp=0
}



/**
 * Envoie une requête HTTP à l'API Kizeo et renvoie la réponse.
 *
 * @param {string} methode - La méthode HTTP à utiliser pour la requête.
 * @param {string} type - Le chemin d'accès à l'API.
 * @param {Object} donnees - Les données à inclure dans le corps de la requête (optionnel).
 * @return {Object} Un objet contenant les données de réponse et le code de statut HTTP.
 */
function requeteAPIDonneesExport(methode, type) {
  let ssToken=SpreadsheetApp.openById('1CtkKyPck3rZ97AbRevzNGiRojranofxMz28zmtSP4LI')
  let tokenKizeo=ssToken.getSheetByName('token').getRange(1, 1,).getValue();
  
  // Prépare les paramètres de la requête

  const settings = {
    'async': true,
    'crossDomain': true,
    'method': methode,
    'headers': {
      "accept": "application/pdf",
      'Content-Type': 'application/json',
      'Authorization': tokenKizeo,
      'cache-control': 'no-cache',
    },
    'muteHttpExceptions': true,
  };

  let reponse;
  let data;
  let contentType;
  let responseCode;

  // Exécute la requête et gère les erreurs
  try {
    let adresse='https://forms.kizeo.com/rest/v3'+type
    reponse = UrlFetchApp.fetch(adresse, settings);
    responseCode = reponse.getResponseCode();
    contentType = reponse.getHeaders()['Content-Type'];

    if (responseCode < 200 || responseCode >= 300) {
      throw new Error(`Erreur HTTP: ${responseCode}`);
    }
  } catch (e) {
    // Loggue l'erreur et envoie un e-mail
    let context = {
      'methode': methode,
      'type': type
      }; 
    libKizeo.handleException('requeteApiDonnees', e, context)
    return {'data': data, 'responseCode': responseCode};;
  }

  // Traite la réponse en fonction de son type de contenu
  if (contentType === 'image/jpeg' || contentType === 'image/png' || contentType === 'application/pdf' ) {
    data = reponse.getBlob();
  } else if (contentType === 'application/json') {
    try {
      const json = reponse.getContentText();
      data = JSON.parse(json);
    } catch (e) {
      libKizeo.handleException('requeteApiDonnees Analyse JSON', e)
    }
  }

  return {'data': data, 'responseCode': responseCode};
}
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

function zzDescribeScenarioSheetInterface() {
  if (typeof libKizeo === 'undefined') {
    throw new Error('zzDescribeScenarioSheetInterface requiert la librairie libKizeo.');
  }
  const library = libKizeo;
  if (typeof library.bqComputeTableName !== 'function') {
    throw new Error('libKizeo.bqComputeTableName est requis pour zzDescribeScenarioSheetInterface.');
  }

  const fakeSheet = {
    getName: () => 'Config',
    getLastRow: () => 2,
    getRange: () => ({
      setValues: () => {},
      clearContent: () => {},
      getValues: () => [['Paramètre', 'Valeur']]
    })
  };

  const config = {
    form_id: 'FORM_SCENARIO',
    form_name: 'Formulaire Scénario',
    action: 'ACTION_SCENARIO',
    bq_table_name: 'form_scenario',
    [CONFIG_BATCH_LIMIT_KEY]: '15',
    [CONFIG_INGEST_BIGQUERY_KEY]: 'true'
  };

  const validation = validateFormConfig(config, fakeSheet);

  if (
    typeof library.SheetInterfaceHelpers === 'undefined' ||
    typeof library.SheetInterfaceHelpers.ensureBigQueryConfigAvailability !== 'function'
  ) {
    throw new Error('libKizeo.SheetInterfaceHelpers.ensureBigQueryConfigAvailability est requis pour zzDescribeScenarioSheetInterface.');
  }

  const availability = library.SheetInterfaceHelpers.ensureBigQueryConfigAvailability(
    validation.config ? validation.config[CONFIG_INGEST_BIGQUERY_KEY] : 'false',
    fakeSheet.getName()
  );

  const summary = {
    isValid: validation.isValid,
    tableName: validation.config ? validation.config.bq_table_name : null,
    batchLimit: validation.config ? validation.config[CONFIG_BATCH_LIMIT_KEY] : null,
    availability
  };

  Logger.log(`zzDescribeScenarioSheetInterface -> ${JSON.stringify(summary)}`);

  return summary;
}
  
