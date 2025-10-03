
/**
 * Gère la création et la suppression des feuilles dans le fichier Google Sheets.
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} spreadsheetBdD - Le fichier Google Sheets à gérer.
 * @param {Object} formulaire - L'objet contenant les informations du formulaire.
 * @return {boolean} Retourne 'true' si la feuille n'existe pas dans le fichier (ou vide), 'false' sinon.
 */
function gestionFeuilles(spreadsheetBdD, formulaire) {
  try {
    let feuilleVide=true
    Logger.log('GestionFeuilles : ' + formulaire.nom);
    const nomSheets = spreadsheetBdD.getSheets().map(sheet => sheet.getName());
    const nomFeuille = formulaire.nom + ' || ' + formulaire.id;
  
    if (!nomSheets.includes(nomFeuille)) {
      if (nomSheets.includes('Reinit')) {
        spreadsheetBdD.getSheetByName('Reinit').setName(nomFeuille);
      } else {
        spreadsheetBdD.insertSheet(nomFeuille);
        Logger.log('Ajout de la Feuille : ' + nomFeuille);
      }
    }else{
      const sheetBdD = spreadsheetBdD.getSheetByName(nomFeuille);
      let bp=sheetBdD.getLastRow();
      if(sheetBdD.getLastRow() > 1){
        feuilleVide=false
      }
    }

    if (nomSheets.includes('Feuille 1')) {
      const sheetF1 = spreadsheetBdD.getSheetByName('Feuille 1');
      spreadsheetBdD.deleteSheet(sheetF1);
      Logger.log('Suppression de la Feuille : Feuille 1 ');
    }    
    
    return feuilleVide;
  } catch (e) {
    handleException('gestionFeuilles', e);
  }
}


/**
 * Formats an object into a JSON-like string and sends it via email.
 * 
 * @param {Object} javascriptObject - The object to be formatted.
 * @param {string} functionName - The name of the calling function.
 * @param {string} context - Additionnal info to send.
 */
function emailLogger(javascriptObject, functionName = '', context = {}, fileName = 'data.json') {
  // Convert the javascriptObject to a JSON string with indentation for readability
  const jsonString = JSON.stringify(javascriptObject, null, 2);
  // Create a Blob from the JSON string
  const jsonBlob = Utilities.newBlob(jsonString, 'application/json', fileName);
  // Truncate the JSON string for the email body
  const truncatedJsonString = jsonString.substring(0, 500) + "...(truncated)";
  
  // Get information about the current user
  //const userEmail = Session.getActiveUser().getEmail();
  // Get the ID of the current script
  const scriptId = ScriptApp.getScriptId();
  // Build the script URL
  const scriptUrl = `https://script.google.com/d/${scriptId}/edit`;

  // Build the Debug email
  let subject= `Debug Json ${functionName}`
  let bodyMessage = `Debug in function ${functionName}, Please find the attached JSON file.\n\n`;
  //bodyMessage += `User : ${userEmail}\n`;
  bodyMessage += `Script URL : ${scriptUrl}\n\n\n`;
  bodyMessage += `500 premiers caracteres du Json : \n\n ${truncatedJsonString}\n\n\n`; 

  // Add context information, if available
  for (const [key, value] of Object.entries(context)) {
    bodyMessage += `${key} : ${value}\n`;
  }

  // Send the formatted JSON string by email
  MailApp.sendEmail({
    to: "jsonnier@sarpindustries.fr",
    subject: subject,
    body: bodyMessage,
    attachments: [jsonBlob]
  });
}

/**
 * Appliquer un format nombre sur toute la feuille
 */
function formatNumberAllSheets(spreadsheetBdD) {

  // Récupérer toutes les feuilles du classeur
  let sheets = spreadsheetBdD.getSheets();
  
  // Appliquer le format numérique à toutes les cellules de toutes les feuilles
  for (let i = 0; i < sheets.length; i++) {
    let sheet = sheets[i];
    let range = sheet.getDataRange();
    range.setNumberFormat("#,###.##########"); // Permet de conserver jusqu'à 10 décimales significatives
  }

  Logger.log('Le format a été appliqué à toutes les feuilles.');
}

/**
 * Fonction pour réduire la taille d'un JSON en limitant le nombre d'éléments des tableaux
 * @param {Object} jsonObj - Le JSON à réduire
 * @param {number} nbMaxTab - Nombre maximum d'éléments par tableau
 * @return {Object} - Le JSON réduit
 */
function reduireJSON2(jsonObj, nbMaxTab) {
  // Fonction récursive pour parcourir et réduire le JSON
  function reduire(obj) {
    if (Array.isArray(obj)) {
      // Si c'est un tableau, on le tronque et on applique la réduction à ses éléments
      return obj.slice(0, nbMaxTab).map(reduire);
    } else if (typeof obj === 'object' && obj !== null) {
      // Si c'est un objet, on parcourt ses propriétés
      var newObj = {};
      for (var key in obj) {
        if (obj.hasOwnProperty(key)) {
          newObj[key] = reduire(obj[key]);
        }
      }
      return newObj;
    } else {
      // Pour les types primitifs, on retourne la valeur telle quelle
      return obj;
    }
  }
  // Appel initial de la fonction récursive
  return reduire(jsonObj);
}

/**
 * Fonction pour réduire la taille d'un JSON en limitant le nombre d'éléments des tableaux et des objets spécifiques
 * @param {Object} jsonObj - Le JSON à réduire
 * @param {Object} limites - Objet contenant nbMaxTab et listeObjAReduire
 * @return {Object} - Le JSON réduit
 */
function reduireJSON(jsonObj, limites) {
  // Fonction récursive pour parcourir et réduire le JSON
  function reduire(obj, key) {
    if (Array.isArray(obj)) {
      // Si c'est un tableau, on le tronque et on applique la réduction à ses éléments
      return obj.slice(0, limites.nbMaxTab).map(function(item) {
        return reduire(item, null); // Pas de clé pour les éléments du tableau
      });
    } else if (typeof obj === 'object' && obj !== null) {
      // Si c'est un objet
      var newObj = {};
      var keys = Object.keys(obj);
      
      if (key && limites.listeObjAReduire && limites.listeObjAReduire.hasOwnProperty(key)) {
        // Si l'objet est dans listeObjAReduire, on limite le nombre de propriétés
        var maxProps = limites.listeObjAReduire[key];
        keys = keys.slice(0, maxProps);
      }
      
      keys.forEach(function(k) {
        newObj[k] = reduire(obj[k], k);
      });
      return newObj;
    } else {
      // Pour les types primitifs, on retourne la valeur telle quelle
      return obj;
    }
  }
  // Appel initial de la fonction récursive
  return reduire(jsonObj, null);
}


/**
 * Détermine si la valeur est numérique.
 *
 * @param {any} value - La valeur à vérifier.
 * @return {boolean} Retourne true si la valeur est numérique, sinon false.
 */
function isNumeric(value) {
  return !isNaN(parseFloat(value)) && isFinite(value);
}
