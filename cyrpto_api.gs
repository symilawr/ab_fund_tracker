// Run all data fetch functions
function runAllDataFetchFunctions() {
  const createDate = new Date();
  const timeout = 300000; // 5 minutes

  const walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Wallets');
  let walletAddresses = walletSheet.getRange('A2:A').getValues();

  // Flatten and filter non-empty values
  walletAddresses = walletAddresses.flat().filter(String);

  walletAddresses.forEach(walletAddress => {
    if (isValidEthereumAddress(walletAddress)) {
      console.log(`${walletAddress} is a valid ETH address`);
      fetchWalletData(walletAddress, createDate);
      fetchZapperData(walletAddress, createDate);
    } else {
      console.log(`${walletAddress} is not a valid ETH address`);
      fetchHuahuaOsmoData(walletAddress, createDate);
    }
  });
}


// Fetch Mobula transaction data
function fetchMobulaTransactionData(createDate) {
  const API_KEY = getEnvironmentVariable('MOBULA_API_KEY');
  const walletSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Wallets');
  const sheet = getOrCreateSheet('Transactions');

  let walletAddresses = walletSheet.getRange('A2:A').getValues().flat().filter(String).filter(isValidEthereumAddress);

  if (!createDate) {
    createDate = new Date();
  }

  // Clear existing data and set headers
  clearAndSetHeaders(sheet, [
    'Wallet_Address', 'Timestamp', 'Asset_Name', 'Asset_Symbol', 'Asset_Contract', 'Asset_Logo',
    'Type', 'Method_ID', 'Hash', 'Blockchain', 'Amount', 'Amount_USD',
    'To', 'From', 'Block_Number', 'Tx_Cost', 'Create_Date'
  ]);

  const latestTimestamp = fetchLatestTimestamps('transactions');

  walletAddresses.forEach(walletAddress => {
    fetchTransactionsForWallet(walletAddress, latestTimestamp, API_KEY, sheet, createDate, timeout);
  });
}

function fetchTransactionsForWallet(walletAddress, latestTimestamp, API_KEY, sheet, createDate, timeout) {
  let continueFetching = true;
  let lastTimestamp = latestTimestamp ? latestTimestamp : null;

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
        appendTransactionsToSheet(sheet, data.data, walletAddress, createDate);
        lastTimestamp = data.data[data.data.length - 1].timestamp;
        console.log(`Last Timestamp for wallet ${walletAddress}: ${lastTimestamp}`);
      } else {
        console.log(`No transactions found for wallet address: ${walletAddress}`);
        continueFetching = false;
      }

      // Check if there are more pages of transactions to fetch
      if (data && data.pagination) {
        continueFetching = data.pagination.total > data.pagination.limit;
      } else {
        continueFetching = false;
      }

    } catch (e) {
      console.error(`Exception for wallet address ${walletAddress}: ${e.message}`);
      continueFetching = false;
    }

    // Check if the script is close to timing out
    if (new Date().getTime() - createDate.getTime() > timeout) {
      ScriptApp.newTrigger('fetchMobulaTransactionData')
        .timeBased()
        .after(1 * 60 * 1000) // 1 minute later
        .create();
      return;
    }
  }
}

function appendTransactionsToSheet(sheet, transactions, walletAddress, createDate) {
  transactions.forEach(transaction => {
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
      transaction.tx_cost || '', // Handle potential missing tx_cost field
      createDate
    ]);
  });
}

// Fetch Zapper data
function fetchZapperData(walletAddress, createDate) {
  const zapperWalletDataSheet = getOrCreateSheet('Zapper_Wallet_Data');
  const zapperWalletAssetsSheet = getOrCreateSheet('Zapper_Wallet_Assets');
  const API_KEY = getEnvironmentVariable('ZAPPER_API_KEY');

  const apiUrl = `https://api.zapper.xyz/v2/balances/tokens?addresses%5B%5D=${walletAddress.toLowerCase()}`;
  const options = {
    'method': 'get',
    'headers': {
      'Authorization': `Basic ${API_KEY}`
    },
    'muteHttpExceptions': true
  };

  try {
    const response = UrlFetchApp.fetch(apiUrl, options);
    const responseCode = response.getResponseCode();
    if (responseCode == 200) {
      const data = JSON.parse(response.getContentText());
      console.log(`API Response: ${JSON.stringify(data)}`);
      const balanceData = data[walletAddress.toLowerCase()];

      if (balanceData && balanceData.length > 0) {
        const assetCount = balanceData.length;
        zapperWalletDataSheet.appendRow([walletAddress, balanceData[0].updatedAt, balanceData[0].token.balanceUSD, assetCount, createDate]);
        appendZapperAssetsToSheet(zapperWalletAssetsSheet, balanceData, walletAddress, createDate);
      } else {
        console.log(`No balances found for the specified wallet address: ${walletAddress}`);
      }
    } else {
      console.error(`Error: ${responseCode} - ${response.getContentText()}`);
    }
  } catch (e) {
    console.error(`Exception: ${e.message}`);
  }
}

function appendZapperAssetsToSheet(sheet, balanceData, walletAddress, createDate) {
  balanceData.forEach(balanceItem => {
    const token = balanceItem.token;
    sheet.appendRow([
      walletAddress, balanceItem.network, balanceItem.updatedAt, token.id, token.address, token.name, token.symbol,
      token.decimals, token.coingeckoId, token.updatedAt, token.createdAt, token.price,
      token.networkId, token.marketCap, token.priceUpdatedAt, token.balance, token.balanceUSD,
      token.balanceRaw, createDate
    ]);
  });
}

// Fetch ETH prices
function fetchETHPrices() {
  const sheet = getOrCreateSheet('ETH Prices');
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

    // Clear the sheet and set headers
    clearAndSetHeaders(sheet, ['Timestamp', 'Price']);

    // Populate the sheet with data
    if (data && data.data && data.data.price_history && data.data.price_history.length > 0) {
      data.data.price_history.forEach(priceData => {
        sheet.appendRow([
          new Date(priceData[0]),
          priceData[1]
        ]);
      });
    } else {
      console.log('No price data found.');
    }
  } catch (e) {
    console.error(`Exception: ${e.message}`);
  }
}

// Move sheet data to BigQuery
function moveSheetToBigQuery(spreadsheet, sheetName, projectId, datasetId, tableId) {
  removeDuplicateRows(sheetName); // Remove duplicate rows first

  const sheet = spreadsheet.getSheetByName(sheetName);
  const data = sheet.getDataRange().getValues();

  console.log(`Processing data from sheet: ${sheetName}`);

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
          console.error(`Invalid date detected in sheet ${sheetName}, row ${i + 1}, column ${headers[j]}, value: ${cellValue}`);
          throw new Error(`Invalid date detected in sheet ${sheetName}, row ${i + 1}, column ${headers[j]}`);
        }
      }

      // If the data is missing, insert null
      row[fieldName] = cellValue === '' || cellValue === null ? null : cellValue;
    }

    rows.push(row);
  }

  console.log(`Prepared ${rows.length} rows for BigQuery (${sheetName})`);

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

  console.log(`Job configuration for ${sheetName}: ${JSON.stringify(job)}`);

  const chunkSize = 1000; // Define chunk size
  for (let start = 0; start < formattedRows.length; start += chunkSize) {
    const chunk = formattedRows.slice(start, start + chunkSize);
    const jsonLines = chunk.map(row => JSON.stringify(row)).join('\n');
    const blob = Utilities.newBlob(jsonLines, 'application/json');
    
    let insertJob;
    try {
      insertJob = BigQuery.Jobs.insert(job, projectId, blob);
    } catch (error) {
      console.error(`Error inserting job for ${sheetName} chunk starting at ${start}: ${error.message}`);
      throw error;
    }

    console.log(`Job status for ${sheetName} chunk starting at ${start}: ${insertJob.status.state}`);

    const jobId = insertJob.jobReference.jobId;
    let jobStatus = BigQuery.Jobs.get(projectId, jobId);
    while (jobStatus.status.state === 'RUNNING') {
      console.log(`Job status for ${sheetName} chunk starting at ${start}: ${jobStatus.status.state}`);
      Utilities.sleep(1000); // Wait for 1 second before checking again
      jobStatus = BigQuery.Jobs.get(projectId, jobId);
    }

    if (jobStatus.status.state === 'DONE') {
      if (jobStatus.status.errorResult) {
        console.error(`Error for ${sheetName} chunk starting at ${start}: ${jobStatus.status.errorResult.message}`);
        console.error(`Error details for ${sheetName} chunk starting at ${start}: ${JSON.stringify(jobStatus.status.errors)}`);
        throw new Error(`Job failed for ${sheetName} chunk starting at ${start}: ${jobStatus.status.errorResult.message}`);
      } else {
        const outputRows = jobStatus.statistics.load.outputRows;
        console.log(`Job completed successfully for ${sheetName} chunk starting at ${start}. Number of records inserted: ${outputRows}`);
      }
    }
  }
}

// Push all sheets to BigQuery
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
        console.log(`Starting to process sheet: ${sheetName}`);
        moveSheetToBigQuery(spreadsheet, sheetName, PROJECT_ID, DATASET_ID, sheetName.toLowerCase().replace(/\s+/g, '_'));
        console.log(`Finished processing sheet: ${sheetName}`);
      }
    });

  } catch (error) {
    console.error('Error: ' + JSON.stringify(error));
    throw new Error('Failed to push data to BigQuery: ' + JSON.stringify(error));
  }
}

// Utility functions
// Utility function to trigger authorization flow
function authorize() {
  const ui = SpreadsheetApp.getUi();
  ui.alert('This is just to trigger the authorization flow.');
}

// Function to add custom menu items to the Google Sheets UI
function onOpen() {
  SpreadsheetApp.getUi().createMenu('BigQuery')
    .addItem('Push to BigQuery', 'pushToBigQuery')
    .addItem('Authorize', 'authorize')
    .addItem('Fetch Data', 'runAllDataFetchFunctions')
    .addToUi();
}

// Function to set up the environment variables
function setup() {
  setEnvironmentVariables();
}

// Function to validate Ethereum address format
function isValidEthereumAddress(address) {
  return /^0x[a-fA-F0-9]{40}$/.test(address);
}

// Function to fetch the latest timestamps from BigQuery
function fetchLatestTimestamps(tableId) {
  const PROJECT_ID = getEnvironmentVariable('PROJECT_ID');
  const DATASET_ID = getEnvironmentVariable('DATASET_ID');
  const QUERY = `
    SELECT MAX(Timestamp) as Latest_Timestamp
    FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
  `;
  console.log(`Executing query: ${QUERY}`);
  const request = {
    query: QUERY,
    useLegacySql: false,
    location: 'US'
  };

  try {
    const queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
    console.log(`Query results: ${JSON.stringify(queryResults)}`);
    const jobId = queryResults.jobReference.jobId;
    let job = BigQuery.Jobs.get(PROJECT_ID, jobId);

    while (job.status.state !== 'DONE') {
      Utilities.sleep(1000);
      job = BigQuery.Jobs.get(PROJECT_ID, jobId);
    }

    console.log(`Job status: ${job.status.state}`);
    console.log(`Job statistics: ${JSON.stringify(job.statistics)}`);

    const rows = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId).rows;
    console.log(`Query results: ${JSON.stringify(rows)}`);

    if (rows && rows.length > 0) {
      const latestTimestamp = parseFloat(rows[0].f[0].v) * 1000;
      console.log(`Latest timestamp: ${latestTimestamp}`);
      return latestTimestamp;
    } else {
      console.log('No data found.');
      return null;
    }
  } catch (e) {
    console.error(`Error fetching latest timestamps: ${e.message}`);
    return null;
  }
}

// Function to remove duplicate rows from a specified sheet
function removeDuplicateRows(sheetName) {
  const spreadsheetId = '1nquhw_n2hIp6uRYcIncygoTUp9fHD2UzYyoWNkaA4eE';

  try {
    const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
    const sheet = spreadsheet.getSheetByName(sheetName);
    const data = sheet.getDataRange().getValues();

    console.log(`Checking for duplicates in sheet: ${sheetName}`);

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

    rowsToDelete.reverse().forEach(rowIndex => {
      console.log(`Deleting row ${rowIndex} in sheet ${sheetName}`);
      sheet.deleteRow(rowIndex);
    });

    console.log(`Deleted ${rowsToDelete.length} duplicate rows from ${sheetName}`);

  } catch (error) {
    console.error(`Error: ${JSON.stringify(error)}`);
    throw new Error(`Failed to remove duplicate rows: ${JSON.stringify(error)}`);
  }
}

// Function to fetch the schema of a BigQuery table
function fetchTableSchema(projectId, datasetId, tableId) {
  const table = BigQuery.Tables.get(projectId, datasetId, tableId);
  return table.schema.fields;
}

// Utility function to convert a value to float
function convertToFloat(value) {
  return parseFloat(value.toString());
}

// Function to format data according to BigQuery table schema
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

// Fetch wallet data and append to Google Sheets
function fetchWalletData(walletAddress, createDate) {
  const apiKey = getEnvironmentVariable('MOBULA_API_KEY');
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const walletDataSheet = getOrCreateSheet('Wallet_Data');
  const walletAssetsSheet = getOrCreateSheet('Wallet_Assets');

  const apiUrl = `https://api.mobula.io/api/1/wallet/portfolio?wallet=${walletAddress}`;
  const options = {
    headers: {
      Authorization: `Bearer ${apiKey}`
    },
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(apiUrl, options);
    const data = JSON.parse(response.getContentText());

    if (data && data.data) {
      const walletData = data.data;
      const totalPnLHistory = walletData.total_pnl_history || {};
      walletDataSheet.appendRow([
        walletData.wallet,
        walletData.total_wallet_balance,
        walletData.total_realized_pnl,
        walletData.total_unrealized_pnl,
        walletData.assets.length,
        totalPnLHistory['24h']?.realized || '',
        totalPnLHistory['24h']?.unrealized || '',
        totalPnLHistory['7d']?.realized || '',
        totalPnLHistory['7d']?.unrealized || '',
        totalPnLHistory['30d']?.realized || '',
        totalPnLHistory['30d']?.unrealized || '',
        totalPnLHistory['1y']?.realized || '',
        totalPnLHistory['1y']?.unrealized || '',
        createDate
      ]);

      walletData.assets.forEach(asset => {
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
      console.log(`No data found for wallet address: ${walletAddress}`);
    }
  } catch (e) {
    console.error(`Exception for wallet address ${walletAddress}: ${e.message}`);
  }
}

// Fetch Huahua and Osmo data and append to Google Sheets
function fetchHuahuaOsmoData(walletAddress, createDate) {
  const balances = fetchBalances(walletAddress);
  const staking = fetchStaking(walletAddress);
  const rewards = fetchRewards(walletAddress);
  const tokenBalance = balances + staking + rewards;

  const token = walletAddress.includes('osmo') ? 'osmo' : (walletAddress.includes('chihuahua') ? 'huahua' : null);
  if (token) {
    fetchPriceData(walletAddress, token, tokenBalance, createDate);
  }
}

// Fetch balances for a given wallet address
function fetchBalances(walletAddress) {
  const apiUrl = `https://as-proxy.gateway.atomscan.com/${walletAddress.includes('chihuahua') ? 'chihuahua-lcd' : 'osmo-lcd'}/cosmos/bank/v1beta1/balances/${walletAddress}`;

  try {
    const response = UrlFetchApp.fetch(apiUrl, { muteHttpExceptions: true });
    const data = JSON.parse(response.getContentText());

    if (data && data.balances) {
      return data.balances
        .filter(balance => ['uhuahua', 'uosmo'].includes(balance.denom))
        .reduce((total, balance) => total + parseFloat(balance.amount), 0) / 1000000;
    } else {
      console.log(`No balances data found for wallet address: ${walletAddress}`);
      return 0;
    }
  } catch (e) {
    console.error(`Exception for wallet address ${walletAddress}: ${e.message}`);
    return 0;
  }
}

// Fetch staking data for a given wallet address
function fetchStaking(walletAddress) {
  const apiUrl = `https://as-proxy.gateway.atomscan.com/${walletAddress.includes('chihuahua') ? 'chihuahua-lcd' : 'osmo-lcd'}/cosmos/staking/v1beta1/delegations/${walletAddress}`;

  try {
    const response = UrlFetchApp.fetch(apiUrl, { muteHttpExceptions: true });
    const data = JSON.parse(response.getContentText());

    if (data && data.delegation_responses) {
      return data.delegation_responses
        .filter(delegation => ['uhuahua', 'uosmo'].includes(delegation.balance.denom))
        .reduce((total, delegation) => total + parseFloat(delegation.balance.amount), 0) / 1000000;
    } else {
      console.log(`No staking data found for wallet address: ${walletAddress}`);
      return 0;
    }
  } catch (e) {
    console.error(`Exception for wallet address ${walletAddress}: ${e.message}`);
    return 0;
  }
}

// Fetch rewards for a given wallet address
function fetchRewards(walletAddress) {
  const baseUrl = 'https://as-proxy.gateway.atomscan.com/';
  const apiUrl = `${baseUrl}${walletAddress.includes('chihuahua') ? 'chihuahua-lcd' : 'osmo-lcd'}/cosmos/distribution/v1beta1/delegators/${walletAddress}/rewards`;

  try {
    const response = UrlFetchApp.fetch(apiUrl, { muteHttpExceptions: true });
    const data = JSON.parse(response.getContentText());

    if (data && data.total) {
      const totalRewards = data.total
        .filter(reward => ['uhuahua', 'uosmo'].includes(reward.denom))
        .reduce((total, reward) => total + parseFloat(reward.amount), 0) / 1000000;

      return totalRewards;
    } else {
      console.log(`No rewards data found for wallet address: ${walletAddress}`);
      return 0;
    }

  } catch (e) {
    console.error(`Exception for wallet address ${walletAddress}: ${e.message}`);
    return 0;
  }
}

// Fetch price data for a given token and calculate balances
function fetchPriceData(walletAddress, token, tokenBalance, createDate) {
  const apiUrl = 'https://as-proxy.servers.atomscan.com/prices';

  try {
    const response = UrlFetchApp.fetch(apiUrl, { muteHttpExceptions: true });
    const data = JSON.parse(response.getContentText());

    if (data && data[token]) {
      const price = data[token].cmc.quote.USD.price;
      const totalWalletBalance = tokenBalance * price;
      appendDataToSheet(walletAddress, totalWalletBalance, createDate);
      appendPriceDataToSheet(walletAddress, token, data[token].cmc, tokenBalance, createDate);
    } else {
      console.log(`No price data found for token: ${token}`);
    }
  } catch (e) {
    console.error(`Exception for token ${token}: ${e.message}`);
  }
}

// Append fetched data to the "Wallet_Data" sheet
function appendDataToSheet(walletAddress, totalWalletBalance, createDate) {
  const sheet = getOrCreateSheet('Wallet_Data');
  sheet.appendRow([
    walletAddress,
    totalWalletBalance,
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    createDate
  ]);
}

// Append fetched price data to the "Wallet_Assets" sheet
function appendPriceDataToSheet(walletAddress, token, data, tokenBalance, createDate) {
  const sheet = getOrCreateSheet('Wallet_Assets');
  const usdPrice = data.quote.USD.price;
  const estimatedBalance = tokenBalance * usdPrice;
  const usdPrice24hChange = usdPrice - (usdPrice / (1 + (data.quote.USD.percent_change_24h / 100)));
  const usdPrice1hChange = usdPrice - (usdPrice / (1 + (data.quote.USD.percent_change_1h / 100)));

  sheet.appendRow([
    walletAddress,
    data.name,
    data.symbol,
    data.id,
    '',
    '',
    '',
    usdPrice,
    '',
    usdPrice24hChange,
    usdPrice1hChange,
    '',
    '',
    '',
    estimatedBalance,
    tokenBalance,
    createDate
  ]);
}

// Helper functions
function getOrCreateSheet(sheetName) {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(sheetName);
  }
  return sheet;
}

function clearAndSetHeaders(sheet, headers) {
  sheet.clear();
  sheet.appendRow(headers);
}

