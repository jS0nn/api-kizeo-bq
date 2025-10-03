/*
  ToDo :
    Optimiser : envoyer le token depuis l'appel de fonction (et eviter de le lire à chaque appel api)
    pour  majListeExterne : ne pas traiter les données mises à jour mais seulement les nvlles données
*/

/**
 * Récupère les enregistrements non lus, les écrit dans le classeur et collecte la liste
 * des médias créés sur Drive pendant l'opération.
 * @return {Object} { medias: [{dataId,name,id}, …] }
 */
function processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger) {
  try {
    const medias = []; // collection partagée
    //const isFeuilleBdDNonExistante = gestionFeuilles(spreadsheetBdD, formulaire);
    const apiPath = `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated`;
    let dataEnCours = { rowEnCours: [], existingHeaders: [] };
    const handled = handleResponses(spreadsheetBdD, formulaire, apiPath, dataEnCours, action, medias);

    if (handled === null) return { medias: [] }; // aucune donnée
    console.log('Fin de processData');
    return { medias }; // renvoi attendu par main()
  } catch (e) {
    handleException('processData', e);
    return { medias: [] };
  }
}


/**
 * La fonction `handleResponses` traite les réponses du formulaire (récupérées via l API) 
 * et les stocke dans une feuille de calcul Google.
 *
 * @param {Object} spreadsheetBdD - La feuille de calcul Google où seront stockées les données.
 * @param {Object} formulaire - L'objet du formulaire contenant les informations du formulaire (id, nom, etc).
 * @param {string} apiPath - Le chemin vers l'API qui renvoie les données du formulaire.
 * @param {Object} dataEnCours - Un objet contenant les données actuellement traitées.
 * @param {Array} medias – tableau collecteur muté pour accumuler les médias
 * @returns {Object|null} - Renvoie un objet contenant les données traitées ou null en cas d'erreur.
 */
function handleResponses(spreadsheetBdD, formulaire, apiPath, dataEnCours, action, medias) {
  try {
    const sheetFormulaire = spreadsheetBdD.getSheetByName(`${formulaire.nom} || ${formulaire.id}`);
    let listeReponses = requeteAPIDonnees('GET', apiPath).data;
    if (!listeReponses.data.length) {
      console.log('Pas d\'enregistrement à traiter');
      return null;
    }
    else{
      console.log(`Nombre d'enregistrements à traiter : ${listeReponses.data.length}`)
    }

    const bigQueryBatch = [];

    listeReponses.data.forEach((rep) => {
      const repFull = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/${rep._id}`).data;
      if (repFull && repFull.data) {
        bigQueryBatch.push(repFull.data);
      }
      dataEnCours = saveDataToSheet(
        spreadsheetBdD,
        repFull.data,
        formulaire,
        sheetFormulaire,
        medias // on transmet le collecteur
      );

      if (dataEnCours === null) throw new Error('Erreur saveDataToSheet');
      // marquer comme lu
      // On marque cet enregistrement comme lues
      const datalue = { data_ids: [repFull.data.id] };
      const elementLus = requeteAPIDonnees('POST', `/forms/${formulaire.id}/markasreadbyaction/${action}`, datalue);
    });

    if (bigQueryBatch.length) {
      bqIngestRawKizeoBatch(formulaire, bigQueryBatch);
    }
   
    //on verifie s'il n'y a plus de données non lues 
    listeReponses = requeteAPIDonnees('GET', apiPath).data;
    if (listeReponses.data.length === 0) {// S'il n'y a pas de réponse à traiter, on met à jour la liste externe eventuelle
      //let jsonDataEnCours= JSON.stringify(dataEnCours);
      //console.log(jsonDataEnCours)

      // On met à jour la liste externe avec les nouvelles données
      const listeAjour = majListeExterne(formulaire, dataEnCours);
      
      //On vérifie les doublons éventuels sur toutes les feuilles du classeur
      const sheets = spreadsheetBdD.getSheets();
      for (let i = 0; i < sheets.length; i++) {
        gestionDesDonneesMaJ(sheets[i]);
      }
      
      //On verifie les doublons eventuels
      //gestionDesDonneesMaJ(spreadsheetBdD);

      if (listeAjour === null) {
        console.log('listeAjour === null')
        return null;
      }
    }

    return dataEnCours;
  } catch (e) {
    handleException('handleResponses', e);
    return null;
  }
}


/**
 * Fonction pour stocker les données provenant d'une réponse dans une feuille de calcul Google Spreadsheets.
 *
 * @param {Object} spreadsheetBdD - Objet représentant le tableau Google Spreadsheet.
 * @param {Object} dataResponse - Objet contenant les données de la réponse à stocker.
 * @param {Object} formulaire - Objet contenant les informations sur le formulaire.
 * @param {Object} sheetFormulaire - Objet représentant la feuille de calcul où stocker les données.
 * @return {Object} Un objet contenant les données d'exécution de la fonction, ou null en cas d'erreur.
 */
function saveDataToSheet(spreadsheetBdD, dataResponse, formulaire, sheetFormulaire, medias) {
  try {
    const start = new Date();
    const [headers, values, baseData, tabFields] = prepareDataForSheet(dataResponse);
    //console.log(formulaire)
    //console.log(headers)
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
      medias // <- collecteur
    );

    sheetFormulaire.appendRow(rowValues);

    const duree = new Date() - start;
    console.log(`Durée saveDataToSheet : ${duree}`);

    return { rowEnCours: rowValues, existingHeaders };
  } catch (e) {
    handleException('saveDataToSheet', e);
    return null;
  }
}

/**
 * Prépare les données pour être stockées dans la feuille de calcul.
 *
 * @param {Object} dataResponse - Objet contenant les données de la réponse.
 * @return {Array} Un tableau contenant les en-têtes et les valeurs des données, ou null en cas d'erreur.
 */
function prepareDataForSheet(dataResponse) {
  try {
    // Extraction des données de base de la réponse
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

    // Obtention des champs de l'objet dataResponse
    const kizeoData = getDataFromFields(dataResponse);   //[Nom du champ,type,valeur]
    if(kizeoData===null){
      return null;
    }

    // Préparation des en-têtes et des valeurs à insérer dans la feuille de calcul
    const headers = [
      "form_id",
      "form_unique_id",
      "id",
      "user_id",
      "last_name",
      "first_name",
      "answer_time",
      "update_time",
      "origin_answer",
      ...kizeoData[0]
    ];
    const values = [...baseResponseData, ...kizeoData[2].map(value => isNumeric(value) ? parseFloat(value) : value)];

    return [headers, values,baseResponseData,kizeoData];
  } catch (e) {
    // Gestion des erreurs avec la fonction handleException
    handleException('prepareDataForSheet', e);
    return null;
  }
}


/**
 * Prépare un tableau contenant les champs des données de réponse de l'API.
 *
 * @param {Object} dataResponse - Les données de réponse.
 * @return {Array|null} - Le tableau des champs ou null en cas d'erreur.
 */
function getDataFromFields(dataResponse) {
  try {
    let fieldsData = [[], [], []];  // Initialisation du tableau Fields
    let i = 0;
    for (let champ in dataResponse.fields) { // Parcours des champs de dataResponse
      fieldsData[0][i] = champ;               // Nom du champ
      fieldsData[1][i] = dataResponse.fields[champ].type;  // Type du champ
      fieldsData[2][i] = dataResponse.fields[champ].value; // Valeur du champ
      i++;
    }
    return fieldsData;
  } catch (e) {
    handleException('getDataFromFields', e);
    return null;
  }
}


/**
 * Prépare la feuille Google en ajoutant les en-têtes si elle est vide.
 *
 * @param {Object} sheetFormulaire - La feuille Google Sheets sur laquelle écrire.
 * @param {Array} headers - Les en-têtes à ajouter si la feuille est vide.
 * @return {null} - Retourne null en cas d'erreur.
 */
function prepareSheet(sheetFormulaire, headers) {
  try {
    // Si la feuille est vide, ajoutez les en-têtes
    if (sheetFormulaire.getLastRow() === 0) {
      sheetFormulaire.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
    return "sheet preparee"
  } catch (e) {
    handleException('prepareSheet', e);
    return null;
  }
}


/**
 * Retourne les indices des colonnes correspondantes aux valeurs dans le tableau de données.
 *
 * @param {Array} values - Le tableau des valeurs.
 * @param {Array} headers - Le tableau des entêtes.
 * @param {Array} existingHeaders - Les entêtes existantes dans la feuille de calcul.
 * @param {Object} sheetEnCours - La feuille de calcul en cours.
 * @return {Array|null} - Le tableau des indices de colonnes ou null en cas d'erreur.
 */
function getColumnIndices(values, headers, existingHeaders, sheetEnCours) {
  try{
    return values.map((value, index) => {
      const headerIndex = existingHeaders.indexOf(headers[index]);
      if (headerIndex === -1) {
        // Si l'entête n'est pas encore dans la feuille, on l'ajoute à la fin
        sheetEnCours.getRange(1, sheetEnCours.getLastColumn() + 1).setValue(headers[index]);
        return sheetEnCours.getLastColumn();
      }
      // On retourne l'indice de la colonne (+1 car l'indexation commence à 0)
      return headerIndex + 1;
    });
  }catch (e) {
    handleException('getColumnIndices', e);
    return null;
  }
}

/**
 * Retourne un tableau de valeurs de rangées adaptées à l'écriture dans la feuille de calcul.
 *
 * @param {Object} spreadsheetBdD - La feuille de calcul de la base de données.
 * @param {Array} values - Le tableau des valeurs.
 * @param {Array} columnIndices - Les indices de colonnes correspondants aux valeurs.
 * @param {Array} donneeBaseEnregistrement - Les données de base pour l'enregistrement.
 * @param {Array} tabFields - Les champs du tableau.
 * @param {Object} formulaire - Le formulaire.
 * @param {Object} dataResponse - Les données de réponse.
 * @return {Array|null} - Le tableau des valeurs de rangées ou null en cas d'erreur.
 */
function prepareDataToRowFormat(spreadsheetBdD, values, columnIndices, baseData, tabFields, formulaire, dataResponse, medias) {
  try {
    const rowValues = [];
    const dataId = dataResponse.id; // utile pour medias

    for (let i = 0; i < values.length; i++) {
      const fieldType = tabFields[1][i - baseData.length];
      const fieldName = tabFields[0][i - baseData.length];

      if (fieldType === 'subform') {
        rowValues[columnIndices[i] - 1] = tabFields[2][i - baseData.length].length
          ? gestionTableaux(
              spreadsheetBdD,
              formulaire,
              dataId,
              fieldName,
              tabFields[2][i - baseData.length]
            )
          : '';
      } else if (fieldType === 'photo' || fieldType === 'signature') {
        const idPhoto = tabFields[2][i - baseData.length];
        const fileId = gestionChampImage(formulaire.id, dataId, fieldName, idPhoto, spreadsheetBdD);
        rowValues[columnIndices[i] - 1] = fileId; // supposé retourner l'ID ou l'URL

        if (fileId) {
          medias.push({ dataId, name: `${fieldName}_${idPhoto}`, id: fileId });
        }
      } else {
        rowValues[columnIndices[i] - 1] = values[i];
      }
    }
    return rowValues;
  } catch (e) {
    handleException('prepareDataToRowFormat', e);
    return null;
  }
}

