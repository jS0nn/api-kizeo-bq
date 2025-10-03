
/**
 * Met à jour la liste externe en fonction des données du formulaire.
 * La liste externe doit etre de la forme :
 *    - nom liste || ID du formulaire auquel elle appartient
 *    - premiere ligne : nom des tags à remplacer : ex Intervention|filtre_1_compteur_volume_bi2|filtre_2_compteur_volume_bi|ge1_compteur_heure_de_fonct1|ge2_compteur_heure_de_fonct1
 *    - deuxieme ligne valeur des données à remplacer ex: CHANGEMENT CHARBON - CUVE1|1000000|||35000
 *    - separateur : |
 * formulaire = {
            nom: "nom du formulaire",
            id: "id du formulaire"
          };
 * @param {Object} formulaire - Le formulaire contenant les données pour mettre à jour la liste externe.
 * 
 * @param {Object} dataEnCours - Les données pour mettre à jour la liste externe.
 * dataEnCours=     {
            "existingHeaders": [tableau des noms de colonnes],
            "rowEnCours": [tableau des valeurs de colonnes de la derniere ligne]
          }
 */
function majListeExterne(formulaire, dataEnCours) {
  try{
    
    // Définit la méthode de requête et le type (endpoint) pour l'API
    let methode = 'GET';
    let type = '/lists';

    // Récupère toutes les listes externes via l'API
    let listeExterneTotales = requeteAPIDonnees(methode, type).data;

    // Parcours de toutes les listes externes
    for (let i = 0; i < listeExterneTotales.lists.length; i++) {
      let tableauNomListeEnCours = listeExterneTotales.lists[i].name.split(" || ");
      let liste = {   //le nom de la liste à traiter est de forme "nom quelconque||idFOrmulaire"
        nom: tableauNomListeEnCours[0],
        idFormulaire: tableauNomListeEnCours[1],
        idListe: listeExterneTotales.lists[i].id
      };

      // Vérification si le formulaire actuel correspond à la liste externe actuelle
      if (formulaire.id == liste.idFormulaire) {
        //console.log(dataEnCours)
        console.log("Liste à traiter!");
        type = `/lists/${liste.idListe}`;

        // Récupération des détails de la liste externe via l'API
        let detailListeExterne = requeteAPIDonnees(methode, type).data;
        let variables = [];
        // split de la ligne items[0] qui correspond au noms des tags dans kizeo
        const splitArray = detailListeExterne.list.items[0].split('|');

        for (let i = 1; i < splitArray.length; i++) {
          let variable = splitArray[i].split(':');   //variable de la liste qui correspond au tag kizeo recherché (et donc à un nom de colonne dans le sheet)
          variables[i] = variable[0];

          let indexVariableEnCours = dataEnCours.existingHeaders.indexOf(variable[0]);
          let valeurVariableEnCours = dataEnCours.rowEnCours[indexVariableEnCours];

          //remplacement de la donnée de tu Tag X avec la valeur de la donnée présente dans le formulaire
          let replacedData=replaceKizeoData(detailListeExterne.list.items, variable[0], valeurVariableEnCours);
          if(replacedData===null){return null}
          methode = 'PUT';
        }

        // Préparation des données pour la mise à jour de la liste
        let items = {items: detailListeExterne.list.items};

        // Mise à jour de la liste via l'API
        let miseAJourDeLaListe = requeteAPIDonnees(methode, type, items).data;
      }
    }
    return "Mise A Jour OK"
  }catch (e) {
    handleException('majListeExterne', e);
    return null;
  }

}

function test_majListeExterne() {
  let formulaire = {
            nom: "Test Jso/Aro Liste externe automatique",
            id: "914817"
          };
  const dataEnCours={}
  majListeExterne(formulaire, dataEnCours)
}



/**
 * Remplace une valeur dans un tableau en utilisant la syntaxe Kizeo.
 *
 * @param {Array} array - Le tableau dans lequel rechercher et remplacer une valeur.
 * @param {string} searchValue - La valeur à rechercher.
 * @param {string} replaceValue - La valeur par laquelle remplacer.
 * @return {Array} - Le tableau avec les valeurs remplacées.
 */
function replaceKizeoData(array, searchValue, replaceValue) {
  try{
    let separator = '|';

    const searchValueKizeo = `${searchValue}:${searchValue}`;
    const replaceValueKizeo = `${replaceValue}:${replaceValue}`;

    // Divise la première entrée du tableau (les headears) en utilisant le séparateur
    const splitArray = array[0].split(separator);

    // Trouve l'index de la valeur à rechercher dans le tableau divisé
    const index = splitArray.findIndex(value => value.trim() === searchValueKizeo);

    // Si la valeur à rechercher est trouvée, la remplace dans le tableau
    if (index !== -1) {
      const splitArray1 = array[1].split(separator);
      splitArray1[index] = replaceValueKizeo;
      array[1] = splitArray1.join(separator);
    } else {
      console.log("Valeur non trouvée !");
    }

    // Renvoie le tableau modifié
    return array;
  } catch (e) {
    handleException('replaceKizeoData', e);
    return null;
  }
}