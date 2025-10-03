/**
 * Handles exceptions and sends an email with details about the error.
 * 
 * @param {string} functionName - The name of the function where the exception occured.
 * @param {Error} error - The Error object representing the exception.
 * @param {Object} [context={}] - An object containing additional context information.
 */
function handleException(functionName, error, context = {}) {
  //If not passed get default context

  const fileInfo = SpreadsheetApp.getActiveSpreadsheet();
  context['File ID']=fileInfo.getId();
  context['File Name']=fileInfo.getName();
  context['File URL']='https://docs.google.com/spreadsheets/d/'+fileInfo.getId()+'/edit?usp=drive_link';
  
  // Get information about the current user
  const userEmail = Session.getActiveUser().getEmail();

  // Get the ID of the current script
  const scriptId = ScriptApp.getScriptId();

  // Build the script URL
  const scriptUrl = `https://script.google.com/d/${scriptId}/edit`;

  // Build the error message
  let errorMessage = `Error in function ${functionName}, the error message is : ${error}\n\n`;
  errorMessage += `User : ${userEmail}\n`;
  errorMessage += `Script URL : ${scriptUrl}\n\n\n`;
  errorMessage += `Stack trace: ${error.stack}\n`; // Add the stack trace to the error message


  // Add context information, if available
  for (const [key, value] of Object.entries(context)) {
    errorMessage += `${key} : ${value}\n`;
  }

  // Log the error message
  console.error(errorMessage);

  // Send the error message by email
  MailApp.sendEmail({
    to: "jsonnier@sarpindustries.fr", //userEmail+",jsonnier@sarpindustries.fr",  
    subject: `Script Error: ${fileInfo.getName()} - ${functionName}`,
    body: errorMessage
  });
}
