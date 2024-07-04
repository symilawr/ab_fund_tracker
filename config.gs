// config.gs
function getEnvironmentVariable(key) {
  const scriptProperties = PropertiesService.getScriptProperties();
  return scriptProperties.getProperty(key);
}
