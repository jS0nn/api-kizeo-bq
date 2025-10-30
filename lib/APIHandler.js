
function requeteAPIDonnees(methode, type, donnees) {
  if (typeof KizeoClient === 'undefined' || typeof KizeoClient.requeteAPIDonnees !== 'function') {
    throw new Error('KizeoClient.requeteAPIDonnees indisponible');
  }
  return KizeoClient.requeteAPIDonnees(methode, type, donnees);
}
