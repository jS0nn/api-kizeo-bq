/**
 * Crée un menu personnalisé dans la feuille de calcul active et ajoute des éléments avec des actions correspondantes.
 * 
 * Le menu affiche des options pour sélectionner un formulaire Kizeo, mettre à jour l'onglet actif et réinitialiser l'onglet actif.
 */
function afficheMenu() {
  try {
    const ui = SpreadsheetApp.getUi();
    const menu = ui.createMenu('Configuration Kizeo')
      .addItem('Selectionner le formulaire Kizeo', 'chargeSelectForm')
      .addItem('Actualiser le sheet', 'majSheet')
      .addItem('Réinitialiser le sheet', 'reInitOngletActif')
      .addToUi();
  } catch (e) {
    libKizeo.handleException('afficheMenu', e);
  }
}



/**
 * Charge une interface utilisateur HTML pour sélectionner un formulaire.
 * 
 * L'interface est créée à partir du fichier HTML 'afficheSelectForm.html'.
 */
function chargeSelectForm() {
  try {
    const htmlServeur = HtmlService.createTemplateFromFile('afficheSelectForm.html');
    const htmlOutput = htmlServeur.evaluate().setWidth(800).setHeight(800);
    SpreadsheetApp.getUi().showModalDialog(htmlOutput, ' ');
  } catch (e) {
    libKizeo.handleException('chargeSelectForm', e);
  }
}

/**
 * Charge la liste des formulaires depuis une API.
 * 
 * Les formulaires sont triés par ordre alphabétique.
 * 
 * @returns {Array} - Le tableau des formulaires.
 */
function chargelisteFormulaires() {
  try {
    const listeFormulaires = libKizeo.requeteAPIDonnees('GET',`/forms`).data;
    const tableauForms = listeFormulaires.forms.sort((a, b) => a.name.localeCompare(b.name));

    return tableauForms;
  } catch (e) {
    libKizeo.handleException('chargelisteFormulaires', e);
  }
}

/**
 * Enregistre les données du formulaire dans la feuille de calcul.
 * 
 * Affiche un avertissement si l'ID du formulaire est vide.
 * Sinon, lance la mise à jour des données du formulaire.
 * 
 * @param {Object} formulaire - L'objet formulaire contenant le nom et l'ID du formulaire.
 */
function enregistrementUI(formulaire) {
  let user;
  try {
    user = Session.getActiveUser().getEmail();
    console.log("enregistrementUI")
    Logger.log(`enregistrementUI du formulaire: ${formulaire.nom} / ${formulaire.id} / ${user}`);
    
    if (!formulaire.id) {
      const ui = SpreadsheetApp.getUi();
      ui.alert('Avertissement', 'Veuillez choisir un formulaire.', ui.ButtonSet.OK);
      Logger.log('enregistrementUI: Avertissement - Veuillez choisir un formulaire.');
      return;
    }

    const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
    let action=SpreadsheetApp.getActiveSpreadsheet().getName()
    setScriptProperties('enCours')
    //console.log(formulaire);
    libKizeo.gestionFeuilles(spreadsheetBdD, formulaire)   
    setScriptProperties('termine')

    main() //une fois préparé on lance main
  } catch (e) {
    libKizeo.handleException('enregistrementUI', e, { formulaire: formulaire, user: user });
  }
}


/**
 * Réinitialise l'onglet actif et tous les tableaux liés à ce formulaire.
 * 
 * Affiche un avertissement si l'onglet actif n'est pas lié à un formulaire.
 * Sinon, efface le contenu de l'onglet actif et de tous les tableaux liés à ce formulaire,
 * puis lance la mise à jour des données du formulaire correspondant.
 */
function reInitOngletActif() {
  var scriptProperties = PropertiesService.getScriptProperties();
  
  var scriptProperties = PropertiesService.getScriptProperties();
  var etatExecution = scriptProperties.getProperty('etatExecution');
  if(etatExecution==="enCours"){
      ui.alert('Avertissement', 'Une exécution est en cours, veuillez patienter et relancer la réinitialisation.',ui.ButtonSet.OK);
      throw new Error('Reinit : Exécution en cours');
  }

  try {
    const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
    const activeSheet = spreadsheetBdD.getActiveSheet();
    const nomOngletActif = activeSheet.getName();
    const nomOngletActifTab = nomOngletActif.split(' || ');

    if (nomOngletActifTab.length > 2) {
      const ui = SpreadsheetApp.getUi();
      ui.alert('Avertissement', 'Seuls les onglets des Formulaires peuvent être réinitialisés.', ui.ButtonSet.OK);
      Logger.log('reInitOngletActif: Avertissement - Seuls les onglets des Formulaires peuvent être réinitialisés.');
    } else {
      const range = activeSheet.getDataRange();
      range.clearContent();
      const formulaire = {
        nom: nomOngletActifTab[0],
        id: nomOngletActifTab[1]
      };

      const onglets = spreadsheetBdD.getSheets();
      // Parcourir tous les onglets et mettre à jour leurs données
      for (let i = 0; i < onglets.length; i++) {
        const onglet = onglets[i].getName();
        const nomOngletTab = onglet.split(' || ');
        if (nomOngletTab.length > 2 && nomOngletActifTab[1] === nomOngletTab[1]) {
          // Il s'agit d'un tableau du formulaire actif
          Logger.log('Effacer le tableau');
          spreadsheetBdD.getSheetByName(onglet).getDataRange().clearContent();
        }
      }
      libKizeo.processData(spreadsheetBdD, formulaire);
      setScriptProperties('termine');
    }
  } catch (e) {
    libKizeo.handleException('reInitOngletActif', e);
  }
}


/**
 * Lancement manuel du programme de maj
 * 
 */
function majSheet() {
  main()
}

/**
 * Affiche l'interface de sélection d'intervalle de temps pour la mise à jour automatique.
 * Utilise un fichier HTML séparé pour une meilleure organisation.
 */
function askForTimeInterval() {
  try {
    const htmlTemplate = HtmlService.createTemplateFromFile('timeIntervalSelector.html');
    const htmlOutput = htmlTemplate.evaluate()
      .setWidth(400)
      .setHeight(350);
    SpreadsheetApp.getUi().showModalDialog(htmlOutput, 'Configuration de la mise à jour automatique');
  } catch (e) {
    libKizeo.handleException('askForTimeInterval', e);
  }
}

/**
 * Traite le choix d'intervalle sélectionné par l'utilisateur. Appel depuis le HTML
 * 
 * @param {string} choix - Le choix de l'utilisateur (format: [type][valeur] ou "none")
 */
function processIntervalChoice(choix) {
  const ui = SpreadsheetApp.getUi();
  
  if (choix === 'none') {
    // Supprimer tous les déclencheurs existants
    deleteAllTriggers()
    ui.alert('Information', 'Mise à jour automatique désactivée', ui.ButtonSet.OK);
    chargeSelectForm();
    return;
  }
  
  // Extraire le type (M ou H) et la valeur
  const type = choix.charAt(0);
  const valeur = parseInt(choix.substring(1), 10);
  
  // Configurer le déclencheur
  configurerDeclencheurHoraire(valeur, type);
  
  // Afficher un message de confirmation
  const uniteTxt = type === 'M' ? 'minute' : 'heure';
  const pluriel = valeur > 1 ? 's' : '';
  // Continuer le processus après la configuration du déclencheur
  chargeSelectForm();
}