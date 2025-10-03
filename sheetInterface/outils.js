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
    } else { // type === 'H'
      trigger.everyHours(valeur);
      Logger.log(`Déclencheur configuré: toutes les ${valeur} heure(s)`);
    }
    
    trigger.create();
  } catch (e) {
    handleException('configurerDeclencheurHoraire', e);
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
    handleException('deleteAllTriggers', e);
  }
}

/**
 * Définit les propriétés du script avec l'état 'termine'.
 */
function setScriptPropertiesTermine(){
  setScriptProperties('termine');
}

