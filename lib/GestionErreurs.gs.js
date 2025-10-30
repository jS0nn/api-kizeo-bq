const GESTION_ERREURS_LOG_PREFIX = 'lib:GestionErreurs:handleException';
const ERROR_MAIL_RECIPIENT = 'jsonnier@sarpindustries.fr';
const ERROR_MAIL_THROTTLE_SECONDS = 300;

function resolveProjectName() {
  try {
    if (typeof ProjectApp !== 'undefined' && ProjectApp !== null && typeof ProjectApp.getProjectName === 'function') {
      const projectName = ProjectApp.getProjectName();
      if (projectName) {
        return projectName;
      }
    }
  } catch (e) {
    // ignore and try DriveApp fallback
  }

  try {
    const scriptId = ScriptApp.getScriptId();
    if (typeof DriveApp !== 'undefined' && DriveApp !== null && typeof DriveApp.getFileById === 'function') {
      const file = DriveApp.getFileById(scriptId);
      if (file && typeof file.getName === 'function') {
        const driveName = file.getName();
        if (driveName) {
          return driveName;
        }
      }
    }
    return scriptId || 'unknown-project';
  } catch (e) {
    return 'unknown-project';
  }
}

function safeGetActiveSpreadsheet() {
  try {
    return SpreadsheetApp.getActiveSpreadsheet();
  } catch (e) {
    console.log(`${GESTION_ERREURS_LOG_PREFIX}: aucun classeur actif (${e})`);
    return null;
  }
}

function safeGetActiveUserEmail() {
  try {
    return Session.getActiveUser().getEmail() || 'unknown';
  } catch (e) {
    console.log(`${GESTION_ERREURS_LOG_PREFIX}: email utilisateur indisponible (${e})`);
    return 'unknown';
  }
}

function shouldSendErrorMail(cacheKey) {
  try {
    const cache = CacheService.getScriptCache();
    if (!cache) return true;
    const existing = cache.get(cacheKey);
    if (existing) {
      console.log(`${GESTION_ERREURS_LOG_PREFIX}: mail throttled pour ${cacheKey}`);
      return false;
    }
    cache.put(cacheKey, '1', ERROR_MAIL_THROTTLE_SECONDS);
    return true;
  } catch (e) {
    console.log(`${GESTION_ERREURS_LOG_PREFIX}: CacheService indisponible (${e})`);
    return true;
  }
}

/**
 * Handles exceptions and sends an email with details about the error.
 *
 * @param {string} functionName - The name of the function where the exception occurred.
 * @param {Error} error - The Error object representing the exception.
 * @param {Object} [context={}] - An object containing additional context information.
 */
function handleException(functionName, error, context = {}) {
  const enrichedContext = Object.assign({}, context || {});
  const fileInfo = safeGetActiveSpreadsheet();
  if (fileInfo) {
    enrichedContext['File ID'] = fileInfo.getId();
    enrichedContext['File Name'] = fileInfo.getName();
    enrichedContext['File URL'] = 'https://docs.google.com/spreadsheets/d/' + fileInfo.getId() + '/edit?usp=drive_link';
  } else {
    enrichedContext['File ID'] = 'N/A';
    enrichedContext['File Name'] = 'N/A';
  }

  const userEmail = safeGetActiveUserEmail();
  const scriptId = ScriptApp.getScriptId();
  const scriptUrl = `https://script.google.com/d/${scriptId}/edit`;

  const errorMessageParts = [
    `Function: ${functionName}`,
    `Error: ${error}`,
    `User: ${userEmail}`,
    `Script URL: ${scriptUrl}`,
    `Stack trace: ${error && error.stack ? error.stack : 'N/A'}`
  ];

  Object.keys(enrichedContext).forEach((key) => {
    errorMessageParts.push(`${key}: ${enrichedContext[key]}`);
  });

  const errorMessage = errorMessageParts.join('\n');
  console.error(`${GESTION_ERREURS_LOG_PREFIX}: ${errorMessage}`);

  const throttleKey = `${functionName}|${error && error.message ? error.message : 'unknown'}`;
  if (!shouldSendErrorMail(throttleKey)) {
    return;
  }

  const projectName = resolveProjectName();
  try {
    const recipients = [];
    if (userEmail && userEmail !== 'unknown') {
      recipients.push(userEmail);
    }
    if (ERROR_MAIL_RECIPIENT) {
      recipients.push(ERROR_MAIL_RECIPIENT);
    }
    const uniqueRecipients = Array.from(new Set(recipients.filter((value) => value && value.trim())));
    const toField = uniqueRecipients.length ? uniqueRecipients.join(',') : ERROR_MAIL_RECIPIENT;
    MailApp.sendEmail({
      to: toField,
      subject: fileInfo
        ? `[${projectName}] Script Error: ${fileInfo.getName()} - ${functionName}`
        : `[${projectName}] Script Error - ${functionName}`,
      body: errorMessage
    });
  } catch (mailError) {
    console.error(`${GESTION_ERREURS_LOG_PREFIX}: envoi email échoué (${mailError})`);
  }
}
