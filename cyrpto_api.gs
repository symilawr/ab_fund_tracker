function runAllDataFetchFunctions() {
  var createDate = new Date();
  var timeout = 300000; // 5 minutes

  // // Fetch Zapper data
  // if (new Date().getTime() - createDate.getTime() < timeout) {
  //   fetchZapperData(createDate);
  // }

  // // Fetch wallet data
  // if (new Date().getTime() - createDate.getTime() < timeout) {
  //   fetchWalletData(createDate);
  // }
  
  // Fetch Mobula transaction data
  if (new Date().getTime() - createDate.getTime() < timeout) {
    fetchMobulaTransactionData(createDate);
  }
}

function fetchMobulaTransactionData(createDate) {
  const API_KEY = '397f3301-24c7-4316-8d7c-cd6f92639c0a';
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

  // Set headers if the sheet is empty
  if (sheet.getLastRow() === 0) {
    sheet.appendRow([
      'Wallet_Address', 'Timestamp', 'Asset_Name', 'Asset_Symbol', 'Asset_Contract', 'Asset_Logo', 
      'Type', 'Method_ID', 'Hash', 'Blockchain', 'Amount', 'Amount_USD', 
      'To', 'From', 'Block_Number', 'Tx_Cost', 'Create_Date'
    ]);
  }

  const latestTimestamps = fetchLatestTimestamps(); // Fetch latest timestamps for each wallet and asset

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

function fetchLatestTimestamps() {
  const PROJECT_ID = 'optimum-courier-426820-m4';
  const DATASET_ID = 'ab_crypto';
  const TABLE_ID = 'transactions';
  const QUERY = `
    SELECT
      Wallet_Address,
      Asset_Symbol,
      MAX(Timestamp) as Latest_Timestamp
    FROM
      \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
    GROUP BY
      Wallet_Address,
      Asset_Symbol
  `;

  const request = {
    query: QUERY,
    useLegacySql: false,
    location: 'US' // Specify the dataset location
  };

  try {
    const queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
    const jobId = queryResults.jobReference.jobId;
    let job = BigQuery.Jobs.get(PROJECT_ID, jobId);

    while (job.status.state !== 'DONE') {
      Utilities.sleep(1000);
      job = BigQuery.Jobs.get(PROJECT_ID, jobId);
    }

    const rows = job.statistics.query.outputRows > 0 ? BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId).rows : [];

    if (rows && rows.length > 0) {
      const timestamps = {};
      rows.forEach(row => {
        const walletAddress = row.f[0].v;
        const assetSymbol = row.f[1].v;
        const latestTimestamp = new Date(row.f[2].v).getTime();

        if (!timestamps[walletAddress]) {
          timestamps[walletAddress] = {};
        }

        timestamps[walletAddress][assetSymbol] = latestTimestamp;
      });

      return timestamps;
    } else {
      Logger.log('No data found.');
      return {};
    }
  } catch (e) {
    Logger.log('Error fetching latest timestamps: ' + e.message);
    return {};
  }
}

function fetchWalletData(createDate) {
  var walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallets");
  var walletDataSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallet_Data") ||
                        SpreadsheetApp.getActiveSpreadsheet().insertSheet("Wallet_Data");
  var walletAssetsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallet_Assets") ||
                          SpreadsheetApp.getActiveSpreadsheet().insertSheet("Wallet_Assets");
  var apiKey = '397f3301-24c7-4316-8d7c-cd6f92639c0a'; // Hardcoded API key for demonstration. Replace with actual key from the sheet if necessary.

  var walletAddresses = walletSheet.getRange("A2:A")
                                   .getValues()
                                   .flat()
                                   .filter(String)
                                   .filter(isValidEthereumAddress); // Get all wallet addresses

  if (!createDate) {
    createDate = new Date();
  }  
  
  // Set headers if the sheets are empty
  if (walletDataSheet.getLastRow() === 0) {
    walletDataSheet.appendRow([
      'Wallet_Address', 'Total_Wallet_Balance', 'Total_Realized_PnL', 'Total_Unrealized_PnL', 'Asset_Count',
      '24h_Realized_PnL', '24h_Unrealized_PnL', '7d_Realized_PnL', '7d_Unrealized_PnL',
      '30d_Realized_PnL', '30d_Unrealized_PnL', '1y_Realized_PnL', '1y_Unrealized_PnL', 'Create_Date'
    ]);
  }
  if (walletAssetsSheet.getLastRow() === 0) {
    walletAssetsSheet.appendRow([
      'Wallet_Address', 'Asset_Name', 'Asset_Symbol', 'Asset_ID', 'Realized_PnL', 'Unrealized_PnL',
      'Allocation', 'Price', 'Price_Bought', 'Price_Change_24h', 'Price_Change_1h', 'Total_Invested',
      'Min_Buy_Price', 'Max_Buy_Price', 'Estimated_Balance', 'Token_Balance', 'Create_Date'
    ]);
  }

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

function pushToBigQuery() {
  const PROJECT_ID = 'optimum-courier-426820-m4';
  const DATASET_ID = 'ab_crypto';
  
  const spreadsheetId = '1nquhw_n2hIp6uRYcIncygoTUp9fHD2UzYyoWNkaA4eE'; // Replace with your actual spreadsheet ID
  
  try {
    const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
    
    // Move Wallet_Data to gbq table wallet_data
    moveSheetToBigQuery(spreadsheet, 'Wallet_Data', PROJECT_ID, DATASET_ID, 'wallet_data');
    
    // Move Wallet_Assets to gbq table wallet_assets
    moveSheetToBigQuery(spreadsheet, 'Wallet_Assets', PROJECT_ID, DATASET_ID, 'wallet_assets');
    
    // Move Transactions to gbq table transactions
    moveSheetToBigQuery(spreadsheet, 'Transactions', PROJECT_ID, DATASET_ID, 'transactions');
    
    // Move Zapper_Wallet_Data to gbq table zapper_wallet_data
    moveSheetToBigQuery(spreadsheet, 'Zapper_Wallet_Data', PROJECT_ID, DATASET_ID, 'zapper_wallet_data');
    
    // Move Zapper_Wallet_Assets to gbq table zapper_wallet_assets
    moveSheetToBigQuery(spreadsheet, 'Zapper_Wallet_Assets', PROJECT_ID, DATASET_ID, 'zapper_wallet_assets');
    
  } catch (error) {
    Logger.log('Error: ' + JSON.stringify(error));
    throw new Error('Failed to push data to BigQuery: ' + JSON.stringify(error));
  }
}

function moveSheetToBigQuery(spreadsheet, sheetName, projectId, datasetId, tableId) {
  const sheet = spreadsheet.getSheetByName(sheetName);
  const data = sheet.getDataRange().getValues();

  Logger.log(`Data from Google Sheets (${sheetName}): ` + JSON.stringify(data));

  const rows = [];
  const headers = data[0].map(header => header.replace(/\s+/g, '_')); // Convert headers to use underscores

  for (let i = 1; i < data.length; i++) {
    const row = {};

    for (let j = 0; j < headers.length; j++) {
      // If the data is missing, insert null
      row[headers[j]] = data[i][j] === '' || data[i][j] === null ? null : data[i][j];
    }

    rows.push(row);
  }

  Logger.log(`Prepared rows for BigQuery (${sheetName}): ` + JSON.stringify(rows));

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

  Logger.log(`Job configuration for ${sheetName}: ` + JSON.stringify(job));

  const jsonLines = rows.map(row => JSON.stringify(row)).join('\n');

  Logger.log(`JSON lines for ${sheetName}: ` + jsonLines);

  const blob = Utilities.newBlob(jsonLines, 'application/json');
  const insertJob = BigQuery.Jobs.insert(job, projectId, blob);

  Logger.log(`Job status for ${sheetName}: ` + insertJob.status.state);

  const jobId = insertJob.jobReference.jobId;
  let jobStatus = BigQuery.Jobs.get(projectId, jobId);
  while (jobStatus.status.state === 'RUNNING') {
    Logger.log(`Job status for ${sheetName}: ` + jobStatus.status.state);
    Utilities.sleep(1000); // Wait for 1 second before checking again
    jobStatus = BigQuery.Jobs.get(projectId, jobId);
  }

  if (jobStatus.status.state === 'DONE') {
    if (jobStatus.status.errorResult) {
      Logger.log(`Error for ${sheetName}: ` + jobStatus.status.errorResult.message);
      Logger.log(`Error details for ${sheetName}: ` + JSON.stringify(jobStatus.status.errors));
      throw new Error(`Job failed for ${sheetName}: ` + jobStatus.status.errorResult.message);
    } else {
      const outputRows = jobStatus.statistics.load.outputRows;
      Logger.log(`Job completed successfully for ${sheetName}. Number of records inserted: ${outputRows}`);
    }
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




function fetchZapperData(createDate) {
  var walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Wallets");
  var zapperWalletDataSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Zapper_Wallet_Data") ||
                              SpreadsheetApp.getActiveSpreadsheet().insertSheet("Zapper_Wallet_Data");
  var zapperWalletAssetsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Zapper_Wallet_Assets") ||
                                SpreadsheetApp.getActiveSpreadsheet().insertSheet("Zapper_Wallet_Assets");
  var apiKey = 'YzMyNWU5OWItNzU0ZC00ZmVjLTg1M2QtZjMxOTU2YWNiN2ViOg=='; // Hardcoded API key for demonstration. Replace with actual key from the sheet if necessary.

  var walletAddresses = walletSheet.getRange("A2:A")
                                   .getValues()
                                   .flat()
                                   .filter(String)
                                   .filter(isValidEthereumAddress); // Get all wallet addresses

  if (!createDate) {
    createDate = new Date();
  }

  // Clear existing data if it hasn't been cleared already
  if (zapperWalletDataSheet.getLastRow() === 0) {
    zapperWalletDataSheet.clear();
    zapperWalletDataSheet.appendRow(['Wallet_Address', 'Updated_At', 'Balance_USD', 'Asset_Count', 'Create_Date']);
  }
  if (zapperWalletAssetsSheet.getLastRow() === 0) {
    zapperWalletAssetsSheet.clear();
    zapperWalletAssetsSheet.appendRow([
      'Wallet_Address', 'Updated_At', 'Token_Address',
      'Token_Symbol', 'Token_Decimals', 'Token_Price', 'Token_Balance', 'Token_Balance_USD', 'Create_Date'
    ]);
  }

  walletAddresses.forEach(function(walletAddress) {
    var apiUrl = `https://api.zapper.xyz/v2/balances/tokens?addresses%5B%5D=${walletAddress.toLowerCase()}`;
    var options = {
      "method": "get",
      "headers": {
        "Authorization": `Basic ${apiKey}`,
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
              walletAddress, balanceItem.updatedAt, token.address,
              token.symbol, token.decimals, token.price, token.balance, token.balanceUSD, createDate
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

function isValidEthereumAddress(address) {
  return /^0x[a-fA-F0-9]{40}$/.test(address);
}