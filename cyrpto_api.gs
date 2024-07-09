function runAllDataFetchFunctions() {
  var createDate = new Date();
  var timeout = 300000; // 5 minutes

  // Fetch Zapper data
  if (new Date().getTime() - createDate.getTime() < timeout) {
    fetchZapperData(createDate);
  }

  // Fetch wallet data
  if (new Date().getTime() - createDate.getTime() < timeout) {
    fetchWalletData(createDate);
  }

  // // Fetch Mobula transaction data
  // if (new Date().getTime() - createDate.getTime() < timeout) {
  //   fetchMobulaTransactionData(createDate);
  // }
}

function fetchMobulaTransactionData(createDate) {
  const API_KEY = getEnvironmentVariable('MOBULA_API_KEY');
  const walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallets");
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Transactions") ||
                SpreadsheetApp.getActiveSpreadsheet().insertSheet("Transactions");

  const walletAddresses = walletSheet.getRange("A2:A")
                                     .getValues()
                                     .flat()
                                     .filter(String)
                                     .filter(isValidEthereumAddress); // Get all wallet addresses

  if (!createDate) {
    createDate = new Date();
  }

  // Clear existing data
  sheet.clear();
  // Set headers
  sheet.appendRow([
    'Wallet_Address', 'Timestamp', 'Asset_Name', 'Asset_Symbol', 'Asset_Contract', 'Asset_Logo', 
    'Type', 'Method_ID', 'Hash', 'Blockchain', 'Amount', 'Amount_USD', 
    'To', 'From', 'Block_Number', 'Tx_Cost', 'Create_Date'
  ]);

  const latestTimestamp = fetchLatestTimestamps('transactions');

  for (let i = 0; i < walletAddresses.length; i++) {
    const walletAddress = walletAddresses[i];
    let continueFetching = true;
    let lastTimestamp = latestTimestamps[walletAddress] ? latestTimestamps[walletAddress]['ETH'] : null;

    while (continueFetching) {
      let apiUrl = `https://api.mobula.io/api/1/wallet/transactions?wallet=${walletAddress}`;
      if (lastTimestamp) {
        apiUrl += `&from=${lastTimestamp}`;
      }

      const options = {
        'headers': {
          'Authorization': `Bearer ${API_KEY}`
        }
      };

      try {
        const response = UrlFetchApp.fetch(apiUrl, options);
        if (response.getResponseCode() !== 200) {
          throw new Error(`Error: ${response.getResponseCode()} - ${response.getContentText()}`);
        }
        const data = JSON.parse(response.getContentText());

        if (data && data.data && data.data.length > 0) {
          data.data.forEach(transaction => {
            sheet.appendRow([
              walletAddress,
              new Date(transaction.timestamp),
              transaction.asset.name,
              transaction.asset.symbol,
              transaction.asset.contract,
              transaction.asset.logo,
              transaction.type,
              transaction.method_id,
              transaction.hash,
              transaction.blockchain,
              transaction.amount,
              transaction.amount_usd,
              transaction.to,
              transaction.from,
              transaction.block_number,
              transaction.tx_cost || '',  // Handle potential missing tx_cost field
              createDate
            ]);
          });

          lastTimestamp = data.data[data.data.length - 1].timestamp;
          Logger.log(`Last Timestamp for wallet ${walletAddress}: ${lastTimestamp}`);
        } else {
          Logger.log(`No transactions found for wallet address: ${walletAddress}`);
          continueFetching = false;
        }

        // Check if there are more pages of transactions to fetch
        if (data && data.pagination) {
          continueFetching = data.pagination.total > data.pagination.limit;
        } else {
          continueFetching = false;
        }

      } catch (e) {
        Logger.log(`Exception for wallet address ${walletAddress}: ${e.message}`);
        continueFetching = false;
      }

      // Check if the script is close to timing out
      if (new Date().getTime() - createDate.getTime() > 300000) { // 5 minutes
        ScriptApp.newTrigger("fetchMobulaTransactionData")
          .timeBased()
          .after(1 * 60 * 1000) // 1 minute later
          .create();
        return;
      }
    }
  }
}


function fetchWalletData(createDate) {
  var walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallets");
  var walletDataSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallet_Data") ||
                        SpreadsheetApp.getActiveSpreadsheet().insertSheet("Wallet_Data");
  var walletAssetsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallet_Assets") ||
                          SpreadsheetApp.getActiveSpreadsheet().insertSheet("Wallet_Assets");
  var apiKey = getEnvironmentVariable('MOBULA_API_KEY');

  var walletAddresses = walletSheet.getRange("A2:A")
                                   .getValues()
                                   .flat()
                                   .filter(String)
                                   .filter(isValidEthereumAddress); // Get all wallet addresses

  if (!createDate) {
    createDate = new Date();
  }  

  // Clear existing data
  walletDataSheet.clear();
  walletAssetsSheet.clear();

  // Set headers
  walletDataSheet.appendRow([
    'Wallet', 'Total_Wallet_Balance', 'Total_Realized_PnL', 'Total_Unrealized_PnL', 'Asset_Count',
    '24h_Realized_PnL', '24h_Unrealized_PnL', '7d_Realized_PnL', '7d_Unrealized_PnL',
    '30d_Realized_PnL', '30d_Unrealized_PnL', '1y_Realized_PnL', '1y_Unrealized_PnL', 'Create_Date'
  ]);
  walletAssetsSheet.appendRow([
    'Wallet', 'Asset_Name', 'Asset_Symbol', 'Asset_ID', 'Realized_PnL', 'Unrealized_PnL',
    'Allocation', 'Price', 'Price_Bought', 'Price_Change_24h', 'Price_Change_1h', 'Total_Invested',
    'Min_Buy_Price', 'Max_Buy_Price', 'Estimated_Balance', 'Token_Balance', 'Create_Date'
  ]);

  for (var i = 0; i < walletAddresses.length; i++) {
    var walletAddress = walletAddresses[i];
    var apiUrl = `https://api.mobula.io/api/1/wallet/portfolio?wallet=${walletAddress}`;

    var options = {
      'headers': {
        'Authorization': `Bearer ${apiKey}`
      },
      'muteHttpExceptions': true // This will allow you to see the full error response
    };

    try {
      var response = UrlFetchApp.fetch(apiUrl, options);
      var data = JSON.parse(response.getContentText());

      if (data && data.data) {
        var walletData = data.data;
        var totalPnLHistory = walletData.total_pnl_history || {};
        walletDataSheet.appendRow([
          walletData.wallet,
          walletData.total_wallet_balance,
          walletData.total_realized_pnl,
          walletData.total_unrealized_pnl,
          walletData.assets.length,
          totalPnLHistory['24h'] ? totalPnLHistory['24h'].realized : '',
          totalPnLHistory['24h'] ? totalPnLHistory['24h'].unrealized : '',
          totalPnLHistory['7d'] ? totalPnLHistory['7d'].realized : '',
          totalPnLHistory['7d'] ? totalPnLHistory['7d'].unrealized : '',
          totalPnLHistory['30d'] ? totalPnLHistory['30d'].realized : '',
          totalPnLHistory['30d'] ? totalPnLHistory['30d'].unrealized : '',
          totalPnLHistory['1y'] ? totalPnLHistory['1y'].realized : '',
          totalPnLHistory['1y'] ? totalPnLHistory['1y'].unrealized : '',
          createDate
        ]);

        walletData.assets.forEach(function(asset) {
          walletAssetsSheet.appendRow([
            walletData.wallet,
            asset.asset.name,
            asset.asset.symbol,
            asset.asset.id,
            asset.realized_pnl,
            asset.unrealized_pnl,
            asset.allocation,
            asset.price,
            asset.price_bought,
            asset.price_change_24h,
            asset.price_change_1h,
            asset.total_invested,
            asset.min_buy_price,
            asset.max_buy_price,
            asset.estimated_balance,
            asset.token_balance,
            createDate
          ]);
        });
      } else {
        Logger.log('No data found for wallet address: ' + walletAddress);
      }

    } catch (e) {
      Logger.log('Exception for wallet address ' + walletAddress + ': ' + e.message);
    }
  }
}


function fetchZapperData(createDate) {
  var walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallets");
  var zapperWalletDataSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Zapper_Wallet_Data") ||
                              SpreadsheetApp.getActiveSpreadsheet().insertSheet("Zapper_Wallet_Data");
  var zapperWalletAssetsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Zapper_Wallet_Assets") ||
                                SpreadsheetApp.getActiveSpreadsheet().insertSheet("Zapper_Wallet_Assets");
  const API_KEY = getEnvironmentVariable('ZAPPER_API_KEY');

  var walletAddresses = walletSheet.getRange("A2:A")
                                   .getValues()
                                   .flat()
                                   .filter(String)
                                   .filter(isValidEthereumAddress); // Get all wallet addresses

  if (!createDate) {
    createDate = new Date();
  }

  // Clear existing data
  zapperWalletDataSheet.clear();
  zapperWalletAssetsSheet.clear();

  // Set headers
  zapperWalletDataSheet.appendRow(['Wallet_Address', 'Updated_At', 'Balance_USD', 'Asset_Count', 'Create_Date']);
  zapperWalletAssetsSheet.appendRow([
    'address', 'network', 'updatedAt', 'token_id', 'token_address', 'token_name', 'token_symbol', 
    'token_decimals', 'token_coingeckoId', 'token_updatedAt', 'token_createdAt', 'token_price', 
    'token_networkId', 'token_marketCap', 'token_priceUpdatedAt', 'token_balance', 'token_balanceUSD', 
    'token_balanceRaw', 'Create_Date'
  ]);

  walletAddresses.forEach(function(walletAddress) {
    var apiUrl = `https://api.zapper.xyz/v2/balances/tokens?addresses%5B%5D=${walletAddress.toLowerCase()}`;
    var options = {
      "method": "get",
      "headers": {
        "Authorization": `Basic ${API_KEY}`,
        "accept": "*/*"
      },
      "muteHttpExceptions": true
    };
    try {
      var response = UrlFetchApp.fetch(apiUrl, options);
      var responseCode = response.getResponseCode();
      if (responseCode == 200) {
        var data = JSON.parse(response.getContentText());
        Logger.log("API Response: " + JSON.stringify(data)); // Log the entire response for debugging
        var balanceData = data[walletAddress.toLowerCase()];

        if (balanceData && balanceData.length > 0) {
          // Calculate asset count
          var assetCount = balanceData.length;
          zapperWalletDataSheet.appendRow([walletAddress, balanceData[0].updatedAt, balanceData[0].token.balanceUSD, assetCount, createDate]);

          // Append wallet assets data
          balanceData.forEach(function(balanceItem) {
            var token = balanceItem.token;
            zapperWalletAssetsSheet.appendRow([
              walletAddress, balanceItem.network, balanceItem.updatedAt, token.id, token.address, token.name, token.symbol, 
              token.decimals, token.coingeckoId, token.updatedAt, token.createdAt, token.price, 
              token.networkId, token.marketCap, token.priceUpdatedAt, token.balance, token.balanceUSD, 
              token.balanceRaw, createDate
            ]);
          });

        } else {
          Logger.log("No balances found for the specified wallet address: " + walletAddress);
        }
      } else {
        Logger.log("Error: " + responseCode + " - " + response.getContentText());
      }
    } catch (e) {
      Logger.log("Exception: " + e.message);
    }
  });
}



function fetchETHPrices() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("ETH Prices") ||
                SpreadsheetApp.getActiveSpreadsheet().insertSheet("ETH Prices");

  const latestTimestamp = fetchLatestTimestamps('eth_prices');
  const fromTimestamp = latestTimestamp ? `&from=${latestTimestamp}` : '';
  const apiUrl = `https://api.mobula.io/api/1/market/history?asset=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&blockchain=ethereum${fromTimestamp}`;

  const options = {
    'method': 'get',
    'muteHttpExceptions': true
  };

  try {
    const response = UrlFetchApp.fetch(apiUrl, options);
    if (response.getResponseCode() !== 200) {
      throw new Error(`Error: ${response.getResponseCode()} - ${response.getContentText()}`);
    }
    const data = JSON.parse(response.getContentText());

    // Clear the sheet if it has existing data
    sheet.clear();
    sheet.appendRow(['Timestamp', 'Price']);

    // Populate the sheet with data
    if (data && data.data && data.data.price_history && data.data.price_history.length > 0) {
      data.data.price_history.forEach(priceData => {
        sheet.appendRow([
          new Date(priceData[0]),
          priceData[1]
        ]);
      });
    } else {
      Logger.log("No price data found.");
    }

  } catch (e) {
    Logger.log(`Exception: ${e.message}`);
  }
}

function fetchLatestTimestamps(tableId) {
  const PROJECT_ID = getEnvironmentVariable('PROJECT_ID');
  const DATASET_ID = getEnvironmentVariable('DATASET_ID');
  const QUERY = `
    SELECT
      MAX(Timestamp) as Latest_Timestamp
    FROM
      \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
  `;
  Logger.log("Executing query: " + QUERY);
  const request = {
    query: QUERY,
    useLegacySql: false,
    location: 'US' // Specify the dataset location
  };

  try {
    const queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
    Logger.log("Query results: " + JSON.stringify(queryResults));
    const jobId = queryResults.jobReference.jobId;
    let job = BigQuery.Jobs.get(PROJECT_ID, jobId);

    while (job.status.state !== 'DONE') {
      Utilities.sleep(1000);
      job = BigQuery.Jobs.get(PROJECT_ID, jobId);
    }

    Logger.log(`Job status: ${job.status.state}`);
    Logger.log(`Job statistics: ${JSON.stringify(job.statistics)}`);

    const rows = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId).rows;
    Logger.log(`Query results: ${JSON.stringify(rows)}`);

    if (rows && rows.length > 0) {
      // Convert the scientific notation to a number and then to a Date object
      const latestTimestamp = parseFloat(rows[0].f[0].v) * 1000; // Multiply by 1000 to convert to milliseconds
      Logger.log(`Latest timestamp: ${latestTimestamp}`);
      return latestTimestamp;
    } else {
      Logger.log('No data found.');
      return null;
    }
  } catch (e) {
    Logger.log('Error fetching latest timestamps: ' + e.message);
    return null;
  }
}

function isValidEthereumAddress(address) {
  return /^0x[a-fA-F0-9]{40}$/.test(address);
}

function removeDuplicateRows(sheetName) {
  const spreadsheetId = '1nquhw_n2hIp6uRYcIncygoTUp9fHD2UzYyoWNkaA4eE'; // Replace with your actual spreadsheet ID

  try {
    const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
    const sheet = spreadsheet.getSheetByName(sheetName);
    const data = sheet.getDataRange().getValues();

    Logger.log(`Checking for duplicates in sheet: ${sheetName}`);

    const uniqueRows = [];
    const rowsToDelete = [];

    data.forEach((row, index) => {
      const rowString = JSON.stringify(row);
      if (uniqueRows.includes(rowString)) {
        rowsToDelete.push(index + 1);
      } else {
        uniqueRows.push(rowString);
      }
    });

    // Delete rows from bottom to top to avoid row shifting issues
    rowsToDelete.reverse().forEach(rowIndex => {
      Logger.log(`Deleting row ${rowIndex} in sheet ${sheetName}`);
      sheet.deleteRow(rowIndex);
    });

    Logger.log(`Deleted ${rowsToDelete.length} duplicate rows from ${sheetName}`);

  } catch (error) {
    Logger.log('Error: ' + JSON.stringify(error));
    throw new Error('Failed to remove duplicate rows: ' + JSON.stringify(error));
  }
}

function fetchTableSchema(projectId, datasetId, tableId) {
  const table = BigQuery.Tables.get(projectId, datasetId, tableId);
  return table.schema.fields;
}

function convertToFloat(value) {
  return parseFloat(value.toString());
}

function formatDataAccordingToSchema(data, schema) {
  const formattedData = [];

  data.forEach(row => {
    const formattedRow = {};
    schema.forEach(field => {
      const fieldName = field.name;
      const fieldType = field.type;
      let value = row[fieldName];

      if (fieldType === 'FLOAT' && typeof value === 'string' && value.includes('E')) {
        value = convertToFloat(value);
      }

      formattedRow[fieldName] = value;
    });
    formattedData.push(formattedRow);
  });

  return formattedData;
}

function moveSheetToBigQuery(spreadsheet, sheetName, projectId, datasetId, tableId) {
  removeDuplicateRows(sheetName); // Remove duplicate rows first

  const sheet = spreadsheet.getSheetByName(sheetName);
  const data = sheet.getDataRange().getValues();

  Logger.log(`Processing data from sheet: ${sheetName}`);

  const rows = [];
  const headers = data[0].map(header => header.replace(/\s+/g, '_')); // Convert headers to use underscores

  const fieldMap = {
    'Buy/Sell': 'Buy_Sell'
  };

  for (let i = 1; i < data.length; i++) {
    const row = {};

    for (let j = 0; j < headers.length; j++) {
      let cellValue = data[i][j];
      let fieldName = headers[j];

      if (fieldMap[fieldName]) {
        fieldName = fieldMap[fieldName];
      }

      // If the cell is a date object, format it as a string
      if (Object.prototype.toString.call(cellValue) === '[object Date]') {
        cellValue = Utilities.formatDate(cellValue, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
      } else if (fieldName.toLowerCase().includes('date')) {
        // Attempt to format string dates
        const date = new Date(cellValue);
        if (!isNaN(date.getTime())) {
          cellValue = Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
        } else {
          Logger.log(`Invalid date detected in sheet ${sheetName}, row ${i + 1}, column ${headers[j]}, value: ${cellValue}`);
          throw new Error(`Invalid date detected in sheet ${sheetName}, row ${i + 1}, column ${headers[j]}`);
        }
      }

      // If the data is missing, insert null
      row[fieldName] = cellValue === '' || cellValue === null ? null : cellValue;
    }

    rows.push(row);
  }

  Logger.log(`Prepared ${rows.length} rows for BigQuery (${sheetName})`);

  // Fetch the table schema
  const schema = fetchTableSchema(projectId, datasetId, tableId);

  // Format data according to schema
  const formattedRows = formatDataAccordingToSchema(rows, schema);

  const job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        writeDisposition: 'WRITE_TRUNCATE',
        sourceFormat: 'NEWLINE_DELIMITED_JSON'
      }
    }
  };

  Logger.log(`Job configuration for ${sheetName}: ${JSON.stringify(job)}`);

  const chunkSize = 1000; // Define chunk size
  for (let start = 0; start < formattedRows.length; start += chunkSize) {
    const chunk = formattedRows.slice(start, start + chunkSize);
    const jsonLines = chunk.map(row => JSON.stringify(row)).join('\n');
    const blob = Utilities.newBlob(jsonLines, 'application/json');
    
    let insertJob;
    try {
      insertJob = BigQuery.Jobs.insert(job, projectId, blob);
    } catch (error) {
      Logger.log(`Error inserting job for ${sheetName} chunk starting at ${start}: ${error.message}`);
      throw error;
    }

    Logger.log(`Job status for ${sheetName} chunk starting at ${start}: ${insertJob.status.state}`);

    const jobId = insertJob.jobReference.jobId;
    let jobStatus = BigQuery.Jobs.get(projectId, jobId);
    while (jobStatus.status.state === 'RUNNING') {
      Logger.log(`Job status for ${sheetName} chunk starting at ${start}: ${jobStatus.status.state}`);
      Utilities.sleep(1000); // Wait for 1 second before checking again
      jobStatus = BigQuery.Jobs.get(projectId, jobId);
    }

    if (jobStatus.status.state === 'DONE') {
      if (jobStatus.status.errorResult) {
        Logger.log(`Error for ${sheetName} chunk starting at ${start}: ${jobStatus.status.errorResult.message}`);
        Logger.log(`Error details for ${sheetName} chunk starting at ${start}: ${JSON.stringify(jobStatus.status.errors)}`);
        throw new Error(`Job failed for ${sheetName} chunk starting at ${start}: ${jobStatus.status.errorResult.message}`);
      } else {
        const outputRows = jobStatus.statistics.load.outputRows;
        Logger.log(`Job completed successfully for ${sheetName} chunk starting at ${start}. Number of records inserted: ${outputRows}`);
      }
    }
  }
}

function pushToBigQuery() {
  const PROJECT_ID = getEnvironmentVariable('PROJECT_ID');
  const DATASET_ID = getEnvironmentVariable('DATASET_ID');
  const spreadsheetId = '1nquhw_n2hIp6uRYcIncygoTUp9fHD2UzYyoWNkaA4eE'; // Replace with your actual spreadsheet ID

  try {
    const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
    const sheetNames = spreadsheet.getSheets().map(sheet => sheet.getName());

    const excludeSheets = ['Wallets', 'Sheet4', 'Transactions', 'ETH Prices'];

    sheetNames.forEach(sheetName => {
      if (!excludeSheets.includes(sheetName)) {
        Logger.log(`Starting to process sheet: ${sheetName}`);
        moveSheetToBigQuery(spreadsheet, sheetName, PROJECT_ID, DATASET_ID, sheetName.toLowerCase().replace(/\s+/g, '_'));
        Logger.log(`Finished processing sheet: ${sheetName}`);
      }
    });

  } catch (error) {
    Logger.log('Error: ' + JSON.stringify(error));
    throw new Error('Failed to push data to BigQuery: ' + JSON.stringify(error));
  }
}


function authorize() {
  const ui = SpreadsheetApp.getUi();
  ui.alert('This is just to trigger the authorization flow.');
}

function onOpen() {
  SpreadsheetApp.getUi().createMenu('BigQuery')
    .addItem('Push to BigQuery', 'pushToBigQuery')
    .addItem('Authorize', 'authorize')
    .addToUi();
}

function isValidEthereumAddress(address) {
  return /^0x[a-fA-F0-9]{40}$/.test(address);
}
