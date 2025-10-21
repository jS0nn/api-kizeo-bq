/**
 * Main du fichier utilisant la bibliotheque id : 15F8a-5rU-4plaAPJuhyYC8-ndbEbWtqHw8RX94_C7cc
 * Met à jour les données pour chaque onglet de feuille.
 * Si de nouvelles réponses sont trouvées pour le formulaire correspondant à un onglet, les données sont enregistrées.
 * Sinon, un message de log est affiché.
 */

function reduireJSON2(jsonObj, nbMaxTab) {
  function reduire(obj) {
    if (Array.isArray(obj)) {
      return obj.slice(0, nbMaxTab).map(reduire);
    }
    if (typeof obj === 'object' && obj !== null) {
      const newObj = {};
      Object.keys(obj).forEach((key) => {
        newObj[key] = reduire(obj[key]);
      });
      return newObj;
    }
    return obj;
  }
  return reduire(jsonObj);
}

function reduireJSON(jsonObj, limites) {
  function reduire(obj, key) {
    if (Array.isArray(obj)) {
      return obj.slice(0, limites.nbMaxTab).map((item) => reduire(item, null));
    }
    if (typeof obj === 'object' && obj !== null) {
      const newObj = {};
      let keys = Object.keys(obj);
      if (key && limites.listeObjAReduire && Object.prototype.hasOwnProperty.call(limites.listeObjAReduire, key)) {
        const maxProps = limites.listeObjAReduire[key];
        keys = keys.slice(0, maxProps);
      }
      keys.forEach((k) => {
        newObj[k] = reduire(obj[k], k);
      });
      return newObj;
    }
    return obj;
  }
  return reduire(jsonObj, null);
}

function zzDescribeScenarioIngestion1018296() {
  const formulaire = {
    nom: 'Scenario Mock 1018296',
    id: '1018296',
    tableName: 'mock_table_1018296',
    alias: 'mock_alias'
  };
  const fakeUnreadData = {
    status: 'ok',
    data: [
      {
        _id: 'record-001',
        summary: 'Mock summary'
      }
    ]
  };
  const detailedRecord = {
    form_id: '1018296',
    form_unique_id: 'FORM-001',
    id: 'record-001',
    user_id: 'user-1',
    last_name: 'Durand',
    first_name: 'Alex',
    answer_time: '2024-05-10T08:00:00Z',
    update_time: '2024-05-10T09:00:00Z',
    origin_answer: 'mobile',
    fields: {
      temperature_air: { type: 'number', value: 18.5 },
      commentaires: { type: 'text', value: 'RAS' }
    }
  };
  const callLog = [];
  const bigQueryCapture = {
    raw: [],
    parent: [],
    parentColumns: [],
    subTables: null,
    media: []
  };
  let unreadCallCount = 0;

  const stubFetch = (method, path) => {
    if (method === 'GET' && path.indexOf('/data/unread/') !== -1) {
      unreadCallCount += 1;
      if (unreadCallCount === 1) {
        return { data: fakeUnreadData, responseCode: 200 };
      }
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/record-001') !== -1) {
      return { data: { data: detailedRecord }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/all') !== -1) {
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'POST' && path.indexOf('/markasreadbyaction/') !== -1) {
      return { data: { status: 'ok' }, responseCode: 200 };
    }
    return { data: { status: 'unknown' }, responseCode: 200 };
  };

  const services = createIngestionServices({
    fetch: stubFetch,
    logger: {
      log: (message) => callLog.push(message)
    },
    now: () => new Date('2024-05-10T10:00:00Z'),
    bigQuery: {
      ingestRawBatch: (formulaireLocal, rows) => {
        bigQueryCapture.raw = rows.slice();
      },
      ingestParentBatch: (formulaireLocal, rows, columns) => {
        bigQueryCapture.parent = rows.slice();
        bigQueryCapture.parentColumns = (columns || []).slice();
      },
      ingestSubTablesBatch: (formulaireLocal, tables) => {
        bigQueryCapture.subTables = tables;
      },
      ingestMediaBatch: (formulaireLocal, rows) => {
        bigQueryCapture.media = rows.slice();
      }
    }
  });

  const medias = [];
  const apiPath = `/forms/${formulaire.id}/data/unread/test-action/1?includeupdated`;

  const result = handleResponses(
    { getId: () => 'MOCK-SPREADSHEET' },
    formulaire,
    apiPath,
    'test-action',
    medias,
    false,
    {
      services,
      targets: {
        bigQuery: true,
        externalLists: false,
        sheet: false
      },
      unreadPayload: fakeUnreadData
    }
  );

  Logger.log(
    JSON.stringify(
      {
        scenario: 'zzDescribeScenarioIngestion1018296',
        result,
        mediasCount: medias.length,
        rawRowCount: bigQueryCapture.raw.length,
        parentRowCount: bigQueryCapture.parent.length,
        logSample: callLog.slice(0, 5)
      },
      null,
      2
    )
  );

  return result;
}

function zzDescribeScenarioIngestion1018296SansBigQuery() {
  const formulaire = {
    nom: 'Scenario Mock 1018296',
    id: '1018296',
    tableName: 'mock_table_1018296',
    alias: 'mock_alias'
  };
  const fakeUnreadData = {
    status: 'ok',
    data: [
      {
        _id: 'record-001',
        summary: 'Mock summary'
      }
    ]
  };
  const detailedRecord = {
    form_id: '1018296',
    form_unique_id: 'FORM-001',
    id: 'record-001',
    user_id: 'user-1',
    last_name: 'Durand',
    first_name: 'Alex',
    answer_time: '2024-05-10T08:00:00Z',
    update_time: '2024-05-10T09:00:00Z',
    origin_answer: 'mobile',
    fields: {
      temperature_air: { type: 'number', value: 18.5 },
      commentaires: { type: 'text', value: 'RAS' }
    }
  };
  const callLog = [];
  const bigQueryCalls = {
    raw: 0,
    parent: 0,
    subTables: 0,
    media: 0
  };
  let unreadCallCount = 0;

  const stubFetch = (method, path) => {
    if (method === 'GET' && path.indexOf('/data/unread/') !== -1) {
      unreadCallCount += 1;
      if (unreadCallCount === 1) {
        return { data: fakeUnreadData, responseCode: 200 };
      }
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/record-001') !== -1) {
      return { data: { data: detailedRecord }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/all') !== -1) {
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'POST' && path.indexOf('/markasreadbyaction/') !== -1) {
      return { data: { status: 'ok' }, responseCode: 200 };
    }
    return { data: { status: 'unknown' }, responseCode: 200 };
  };

  const services = createIngestionServices({
    fetch: stubFetch,
    logger: {
      log: (message) => callLog.push(message)
    },
    now: () => new Date('2024-05-10T10:00:00Z'),
    bigQuery: {
      ingestRawBatch: () => {
        bigQueryCalls.raw += 1;
      },
      ingestParentBatch: () => {
        bigQueryCalls.parent += 1;
      },
      ingestSubTablesBatch: () => {
        bigQueryCalls.subTables += 1;
      },
      ingestMediaBatch: () => {
        bigQueryCalls.media += 1;
      }
    }
  });

  const medias = [];
  const apiPath = `/forms/${formulaire.id}/data/unread/test-action/1?includeupdated`;

  const result = handleResponses(
    { getId: () => 'MOCK-SPREADSHEET' },
    formulaire,
    apiPath,
    'test-action',
    medias,
    false,
    {
      services,
      targets: {
        bigQuery: false,
        externalLists: false,
        sheet: false
      },
      unreadPayload: fakeUnreadData
    }
  );

  Logger.log(
    JSON.stringify(
      {
        scenario: 'zzDescribeScenarioIngestion1018296SansBigQuery',
        result,
        mediasCount: medias.length,
        bigQueryCalls,
        logSample: callLog.slice(0, 5)
      },
      null,
      2
    )
  );

  return result;
}

function main_Test() {
  const spreadsheetBdD = SpreadsheetApp.openById("15F8a-5rU-4plaAPJuhyYC8-ndbEbWtqHw8RX94_C7cc");
  const onglets = spreadsheetBdD.getSheets();


  var etatExecution = "RaS pour le test";
  const nbFormulairesACharger=10;

  //action : limite la portée de l'action markasread et unread à un spreadSheet : Attention si plusieurs fichiers sheet portent le meme nom !!!
  let action='testMain'

    // Vérifie si l'exécution précédente est terminée (ou si c'est la première fois).
  if (etatExecution !== 'enCours') {
    // Marque cette exécution comme étant "en cours".
    try{
      for (const onglet of onglets) {
        const ongletName = onglet.getName();
        const ongletTabName = ongletName.split(' || ');
        const formulaire = {
          nom: ongletTabName[0],
          id: ongletTabName[1]
        };
        if (ongletTabName.length < 3 && ongletTabName.length > 1) {
          const lastRow=onglet.getLastRow()
          let reponseAPI =  requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated`);
          if (!reponseAPI) {
            throw new Error('La réponse de requeteAPIDonnees est indéfinie');
          }
          let listeReponses = reponseAPI.data;
          if (!listeReponses || !listeReponses.status || listeReponses.status !== "ok") {
            console.log(`Erreur requeteAPIDonnees : statut ${listeReponses ? listeReponses.status : 'inconnu'}`);
          }
          if (listeReponses && listeReponses.data.length > 0) {
             processData(spreadsheetBdD, formulaire,action,nbFormulairesACharger);
          } else {
            Logger.log('Pas de nouveaux enregistrements');
          }
        }
      }

    } catch (error) {
       handleException('main', error);
    }
  }else {
    // Si l'exécution précédente est toujours en cours, vous pouvez choisir de ne rien faire
    // ou d'ajouter une logique spécifique pour gérer ce cas.
    console.log("L'exécution précédente est toujours en cours.");
    console.log("En cas de problème, veuillez réinitialiser l'onglet");
  }
}




function testRequetelistform (){
  let testBp
  let action="testDebug0"
  let nbFormulairesACharger=3
  let idFormulaire="710028"   //id du formulaire  
  //let idReponse="191428737"   //id de la réponse au formulaire idFormulaire    136860393
  //let dataReponse2=requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/${idReponse}`)  //data de la réponse idReponse 

  var limites = {
    nbMaxTab: 3,
    listeObjAReduire: {
      "fields": 2,
      "options":4
    }
  };
  //let dataReponse=requeteAPIDonnees('GET', `/forms/1036551/data/unread/test/10?includeupdated&format=basic`)  //data de la réponse idReponse 
  let dataReponse=requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/unread/${action}/${nbFormulairesACharger}`);
  let dataLigth=reduireJSON(dataReponse, limites)

  emailLogger(dataLigth)
  testBp
}


function testmainSheet(){
  let t0="0.1"
  let t1=isNumeric(t0)
  let t2=parseFloat(t0)

  const nbFormulairesACharger=10;
  const spreadsheetBdD = SpreadsheetApp.openById('1VeUH3ypEDqyTiSUwm_N3-Xqzo0LvHzijULYfklgKjTA')
  const action="test1"
  const ongletTabName = ['Test Jso/Aro Liste externe automatique (récupéré)','914817']; 
  const formulaire = {
    nom: ongletTabName[0],
    id: ongletTabName[1]
  };
  //processData(spreadsheetBdD, formulaire,action,nbFormulairesACharger)

  // Appliquer un format  à toute la feuille
  //let range = spreadsheetBdD.getDataRange();
  //range.setNumberFormat("#,###.##########");
  /*

  let startTime = new Date(); // Début de la mesure du temps
  let reponseAPI = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}`);
  let reponseAPI2 = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?format=basic`);
  let reponseAPI3 = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated&format=basic`);
  let reponseAPI4 = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated&format=simple`);
  
  let timeTaken = new Date() - startTime; // Temps écoulé en millisecondes
  console.log('Temps écoulé: ' + timeTaken + ' ms');

  if (ongletTabName.length < 3 && ongletTabName.length > 1) {
    let reponseAPI = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}`);
    if (!reponseAPI) {
      throw new Error('La réponse de requeteAPIDonnees est indéfinie');
    }
    let listeReponses = reponseAPI.data;
    if(!listeReponses.status){
      throw new Error(`Erreur requeteAPIDonnees`);
    }
    if(listeReponses.status!="ok"){
      throw new Error(`Erreur requeteAPIDonnees`);
    }
    if (listeReponses && listeReponses.data.length > 0) {
      processData(spreadsheetBdD, formulaire,action,nbFormulairesACharger);
    } else {
      Logger.log('Pas de nouveaux enregistrements');
    }
  }
  */
}


function testRequete2 (){
  let testBp

  let idFormulaire="710028"   //id du formulaire  
  let idReponse="191428737"   //id de la réponse au formulaire idFormulaire    136860393
  let dataReponse=requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/${idReponse}`)  //data de la réponse idReponse 
  emailLogger(dataReponse.data)
  let testnum_ph1=dataReponse.data.data.fields.ph1.value
  let testnum_niveau_dynamique_au_moment_d1=dataReponse.data.data.fields.niveau_dynamique_au_moment_d1.value
  //niveau_dynamique_au_moment_d1
  let isNum_ph1=isNumeric(testnum_ph1)
  let float_ph1=parseFloat(testnum_ph1)
  let float_niveau_dynamique_au_moment_d=parseFloat(testnum_niveau_dynamique_au_moment_d1)
  let float_temperature_c_1=parseFloat(dataReponse.data.data.fields.temperature_c_1.value)
  //isNumeric(value) ? parseFloat(value) : value)
  let test=isNumeric(testnum_ph1) ? parseFloat(testnum_ph1) : testnum_ph1
  testBp
}

function testMainUI(){
  console.log('test')
  let formulaire={id:710028,nom:"Prélèvements - Piézomètres"};

  let dataEnCours={}
  majListeExterne(formulaire, dataEnCours) 

  
  let bp=1;
}



function testReinitUI(){
  reInitOngletActif()
}

function testEnregistrementUI(){
  let formulaire={id:962645 ,nom:"Soitec Pilote rev3", action:"TestAction"}   
  //  let formulaire={id:900548,nom:"Labo - MES"} 
  // let formulaire={id:893304,nom:"Labo - Mesures in-situ (pH, cond., redox, oxygène)"}   
  // let formulaire={id:856959,nom:"Tuto - 1er formulaire"}
  // requeteAPIDonnees('ettt', 'type') 
  console.log(formulaire)
  enregistrementUI(formulaire) 
}



function testPhoto(){
  //c75248f893304pu592427_20230705143825_31651f20-bc3a-484b-b264-8272d7f9f5d2
  let formulaire={id:893304,nom:"Labo - Mesures in-situ (pH, cond., redox, oxygène)"}   //165657529

  let mediaName='c75248f893304pu592427_20230705143825_31651f20-bc3a-484b-b264-8272d7f9f5d2'
  let idReponse=165657529
  let champEnCours="image1"
  let image=requeteAPIDonnees('GET',`/forms/${formulaire.id}/data/${idReponse}/medias/${mediaName}`) 
  
  if(image.responseCode==404){
    console.error('Photo non trouvée')
  }else{
    let nomImage=idReponse + "||"+champEnCours+"||"+mediaName+".jpg"
    gestionChampPhoto(image.data,nomImage)
  }
  
  let bp=1
}




function testRequete (){
  let testBp
  const spreadsheetBdD = SpreadsheetApp.openById("1pNbuRk7gweWpmOXrNxhJ8KLiqyvnnyv2J2e-Z-QGmpU");
  var action=spreadsheetBdD.getName()
  let listeFormulaires=requeteAPIDonnees(`/forms`)   //liste les formulaires
  let idFormulaire=710028   //id du formulaire  
  let listeReponses=requeteAPIDonnees(`/forms/${idFormulaire}/data/all`)   //liste des réponses au formulaire idFormulaire
  let listeReponses2=requeteAPIDonnees(`/forms/${idFormulaire}/data/unread/${action}/10`) 
  let idReponse=136861958   //id de la réponse au formulaire idFormulaire    136860393
  let dataReponse=requeteAPIDonnees(`/forms/${idFormulaire}/data/${idReponse}`)  //data de la réponse idReponse 

  testBp
}

function testNonLues(type) {
  let formulaires=requeteAPIDonnees('GET',`/forms`).data
  let listeReponses=requeteAPIDonnees('GET',`/forms/856959/data/readnew`).data //134908884 134937998
  let testBp
}

function testMarquerLu(type) {
  //let formulaires=requeteAPIDonnees('GET',`/forms`).data
  let listeReponses=requeteAPIDonnees('GET',`/forms/816148/data/readnew`).data
  //https://www.kizeoforms.com/rest/v3/forms/{formId}/markasread.
  let lues={
          "data_ids": [134937998]
            }
  let elementLus=requeteAPIDonnees('POST',`/forms/816148/markasread`,lues).data
  let testBp
}

function testMarquerLuAction(type) {
  //ne lit que les non lus par l'action XX et marque en lu par l'action XX 
  let action="testJSO"
  let listeReponses=requeteAPIDonnees('GET','/forms/816148/data/unread').data
  let listeReponsesAction=requeteAPIDonnees('GET',`/forms/816148/data/unread/`+action+"/100").data
  //https://www.kizeoforms.com/rest/v3/forms/{formId}/markasread.
  let lues={
          "data_ids": [135013804]
            }
  let elementLus=requeteAPIDonnees('POST',`/forms/816148/markasreadbyaction/`+action,lues).data
  let listeReponses2=requeteAPIDonnees('GET',`/forms/816148/data/unread/`+action+"/100").data
  let testBp
}



function testStoreDataInGoogleSheet(){
  const dataObject={}
  dataObject.id_response=987
  dataObject.values={test1:14,"GE1 Dernier Delta Nombre de demarrages":3,"GE3 Dernier Nombre de demarrages":5002,test4:8}
  const sheetName = "Calculs"; // Change this to your desired sheet name
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  storeDataInGoogleSheet(dataObject,sheet)

}

function storeDataInGoogleSheet(dataObject,sheet) {
  try {
    const headers = ["id_response", ...Object.keys(dataObject.values)];
    const values = [dataObject.id_response, ...Object.values(dataObject.values)];

    // Find the last row with data in the sheet
    const lastRow = sheet.getLastRow();

    // If the sheet is empty, add headers as the first row
    if (lastRow === 0) {
      sheet.appendRow(headers);
    }

    // Get the headers from the first row in the sheet
    const existingHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

    // Find the column index for each value in the dataObject
    const columnIndices = values.map((value, index) => {
      const headerIndex = existingHeaders.indexOf(headers[index]);
      if (headerIndex === -1) {
        // If the header doesn't exist in the sheet, add it
        sheet.getRange(1, sheet.getLastColumn() + 1).setValue(headers[index]);
        return sheet.getLastColumn();
      }
      return headerIndex + 1;
    });

    // Append the values to the sheet
    const rowValues = [];
    for (let i = 0; i < values.length; i++) {
      rowValues[columnIndices[i] - 1] = values[i];
    }
    sheet.appendRow(rowValues);
    let testbp=10
  } catch (error) {
    Logger.log("Error: " + error);
  }
}



function testListeExterne(){
  let testBp
  configurerDeclencheurHoraire(5)
  let spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  let methode='GET';
  let type= '/lists' ;
  let listeExterneTotales=requeteAPIDonnees(methode,type).data;
  /*
  let onglets = spreadsheetBdD.getSheets();
  // Parcourir tous les onglets et mettre à jour leurs données
  for (let i = 0; i < onglets.length; i++) {
    let onglet = onglets[i].getName();
    let nomOngletTab=onglet.split(" || ");
    let formulaire = {
      nom: nomOngletTab[0],
      id: nomOngletTab[1]
    };
    if(nomOngletTab.length<3 && nomOngletTab.length>1 ){
      let sheetEnCours = onglets[i];
      for(let i = 0; i < listeExterneTotales.lists.length; i++){
        let listeEnCours=listeExterneTotales.lists[i].name.split(" || ");
        let liste={
          nom:listeEnCours[0],
          idFormulaire:listeEnCours[1],
          idListe:listeExterneTotales.lists[i].id
        };
        if(formulaire.id==liste.idFormulaire){
          console.log("liste à traiter!");
          let finfichier=sheetEnCours.getLastRow()
          type= `/lists/${liste.idListe}`
          let detailListeExterne=requeteAPIDonnees(methode,type).data
          let variables=[]
          const splitArray = detailListeExterne.list.items[0].split('|');
          for(let i = 1; i < splitArray.length; i++){     //premiere valeur correspond au label      --------> regle à fixer
            let variable=splitArray[i].split(':');
            variables[i]=variable[0]
            testBp
          }
          testBp
        }

      }
    }
  }
  */
  let formulaire={id:900548,nom:"Labo - MES"}  

  type= `/lists/${386615}`   //386615
  
  let detailListeExterne=requeteAPIDonnees(methode,type).data
  let testfonctionreplace=replaceKizeoData(detailListeExterne.list.items, "GE2 - Compteur Heure de fonctionnement (heure)", 100)


  console.log(detailListeExterne.list.items)
  let split=detailListeExterne.list.items[0].split('|')
  let split1=detailListeExterne.list.items[1].split('|')
  let split1Length=split1[1].length
  let split1Length2=split1[2].length
  let split2=detailListeExterne.list.items[2].split('|')
  let split20=split2[0].split('\\')
  let split201=split20[1].split(':')
  let nouvelleChaine='CHANGEMENT CHARBON - CUVE3'
  split201.splice(0, 2, nouvelleChaine);
  split2[3]='50000:50010'
  let separator=":"
  split20=joinArrayWithSeparator(split201,separator)
  separator="|"
  let testJoin=joinArrayWithSeparator(split2,separator)
  detailListeExterne.list.items[2]=testJoin
  methode='PUT'
  let items={items:detailListeExterne.list.items}

  let testPUT=requeteAPIDonnees(methode,type,items).data
  
  testBp
}



function testajouteTrigger(){
  configurerDeclencheurHoraire(5)
}

function joinArrayWithSeparator(array,separator) {
  // Vérifier si le tableau est vide ou nul
  if (!array || array.length === 0) {
    return "";
  }
  // Utiliser la méthode Array.join() pour concaténer les éléments du tableau avec le séparateur "|"
  const joinedString = array.join(separator);
  return joinedString;
}
