/**
 * Configure un déclencheur horaire pour une fonction spécifiée.
 *
 * @param {number} valeur - La valeur de l'intervalle
 * @param {string} type - Le type d'intervalle ('M' pour minutes, 'H' pour heures)
 */
function configurerDeclencheurHoraire(valeur, type) {
  try {
    // Supprimer les déclencheurs existants pour éviter les duplications
    deleteAllTriggers();

    const functionName = 'main';
    const trigger = ScriptApp.newTrigger(functionName).timeBased();

    if (type === 'M') {
      trigger.everyMinutes(valeur);
      Logger.log(`Déclencheur configuré: toutes les ${valeur} minute(s)`);
    } else if (type === 'H') {
      trigger.everyHours(valeur);
      Logger.log(`Déclencheur configuré: toutes les ${valeur} heure(s)`);
    } else if (type === 'D') {
      trigger.everyDays(valeur);
      Logger.log(`Déclencheur configuré: tous les ${valeur} jour(s)`);
    }

    trigger.create();
  } catch (e) {
    uiHandleException('configurerDeclencheurHoraire', e, { valeur, type });
  }
}

/**
 * Supprime tous les déclencheurs du projet.
 */
function deleteAllTriggers() {
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    for (const trigger of allTriggers) {
      ScriptApp.deleteTrigger(trigger);
    }
  } catch (e) {
    uiHandleException('deleteAllTriggers', e);
  }
}

/**
 * Définit les propriétés du script avec l'état 'termine'.
 */
function setScriptPropertiesTermine(){
  setScriptProperties('termine');
}

function uiHandleException(functionName, error, context) {
  const message = error && error.message ? error.message : String(error);
  console.log(`Erreur ${functionName}: ${message}`);
  if (context && typeof context === 'object') {
    try {
      console.log(`Context ${functionName}: ${JSON.stringify(context)}`);
    } catch (jsonError) {
      console.log(`Context ${functionName}: conversion JSON impossible (${jsonError})`);
    }
  }

  const lowerMessage = message ? message.toLowerCase() : '';
  const authRelated =
    lowerMessage.indexOf('autorisation') !== -1 ||
    lowerMessage.indexOf('authorization') !== -1 ||
    lowerMessage.indexOf('auth') !== -1;

  if (!authRelated && typeof libKizeo !== 'undefined' && typeof libKizeo.handleException === 'function') {
    try {
      libKizeo.handleException(functionName, error, context);
    } catch (mailError) {
      const fallbackMessage = mailError && mailError.message ? mailError.message : String(mailError);
      console.log(`uiHandleException fallback (${functionName}): ${fallbackMessage}`);
    }
  }
}
