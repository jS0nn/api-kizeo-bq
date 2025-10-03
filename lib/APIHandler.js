
/**
 * Envoie une requête HTTP à l'API Kizeo et renvoie la réponse.
 *
 * @param {string} methode - La méthode HTTP à utiliser pour la requête.
 * @param {string} type - Le chemin d'accès à l'API.
 * @param {Object} donnees - Les données à inclure dans le corps de la requête (optionnel).
 * @return {Object} Un objet contenant les données de réponse et le code de statut HTTP.
 */
function requeteAPIDonnees(methode, type, donnees) {
  let ssToken=SpreadsheetApp.openById('1CtkKyPck3rZ97AbRevzNGiRojranofxMz28zmtSP4LI')
  let tokenKizeo=ssToken.getSheetByName('token').getRange(1, 1,).getValue();
  
  // Prépare les paramètres de la requête
  if (donnees===undefined){
    donnees={}
  }
  else{
    donnees=JSON.stringify(donnees)
  }
  const settings = {
    'async': true,
    'crossDomain': true,
    'method': methode,
    'headers': {
      'content-type': 'application/json',
      'authorization': tokenKizeo,
      'cache-control': 'no-cache',
    },
    'payload': donnees,
    'muteHttpExceptions': true,
  };

  let reponse;
  let data;
  let contentType;
  let responseCode;

  // Exécute la requête et gère les erreurs
  try {
    reponse = UrlFetchApp.fetch('https://www.kizeoforms.com/rest/v3' + type, settings);
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
    handleException('requeteApiDonnees', e, context)
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
      handleException('requeteApiDonnees Analyse JSON', e)
    }
  }

  return {'data': data, 'responseCode': responseCode};
}