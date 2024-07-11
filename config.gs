function getEnvironmentVariable(key) {
  const scriptProperties = PropertiesService.getScriptProperties();
  const value = scriptProperties.getProperty(key);
  Logger.log('Retrieved environment variable: ' + key + ' = ' + value);
  return value;
}