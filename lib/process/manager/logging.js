// ProcessManager Logging Version 0.1.0

var PROCESS_MANAGER_LOG_PREFIX = 'lib:ProcessManager';

function processReportException(scope, error, context) {
  if (typeof handleException === 'function') {
    handleException(scope, error, context);
    return;
  }

  var message = error && error.message ? error.message : String(error);
  var serializedContext = '';
  if (context) {
    try {
      serializedContext = ' | context=' + JSON.stringify(context);
    } catch (stringifyError) {
      serializedContext = ' | context=<non-serializable>';
    }
  }
  console.error(PROCESS_MANAGER_LOG_PREFIX + ':' + scope + ' -> ' + message + serializedContext);
}
