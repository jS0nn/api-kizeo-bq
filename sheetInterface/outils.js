const MAIN_TRIGGER_FUNCTION = 'main';
const DEDUP_TRIGGER_FUNCTION = 'runBigQueryDeduplication';
const DEDUP_TRIGGER_INTERVAL_HOURS = 1;

function deleteTriggersByFunction(functionName) {
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    allTriggers.forEach((trigger) => {
      if (!functionName || trigger.getHandlerFunction() === functionName) {
        ScriptApp.deleteTrigger(trigger);
      }
    });
  } catch (e) {
    uiHandleException('deleteTriggersByFunction', e, { functionName });
  }
}

function ensureDeduplicationTrigger() {
  try {
    deleteTriggersByFunction(DEDUP_TRIGGER_FUNCTION);
    ScriptApp.newTrigger(DEDUP_TRIGGER_FUNCTION).timeBased().everyHours(DEDUP_TRIGGER_INTERVAL_HOURS).create();
    Logger.log('Déclencheur de déduplication configuré: toutes les heures.');
  } catch (e) {
    uiHandleException('ensureDeduplicationTrigger', e);
  }
}

/**
 * Configure un déclencheur horaire pour une fonction spécifiée.
 *
 * @param {number} valeur - La valeur de l'intervalle
 * @param {string} type - Le type d'intervalle ('M' pour minutes, 'H' pour heures)
 */
function configurerDeclencheurHoraire(valeur, type) {
  try {
    // Supprimer les déclencheurs existants pour éviter les duplications
    deleteTriggersByFunction(MAIN_TRIGGER_FUNCTION);

    const trigger = ScriptApp.newTrigger(MAIN_TRIGGER_FUNCTION).timeBased();

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
  deleteTriggersByFunction();
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
