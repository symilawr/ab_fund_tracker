# Portfolio Dashboard / Fund Tracker

This script automates the process of fetching wallet data, transaction data, and other cryptocurrency-related data from various APIs and pushing it to Google BigQuery. The script is designed to run within a Google Sheets environment.

Open source Fund Administration Software for tracking portfolios with real-time pricing data and analytics.

## Functions Overview

### `runAllDataFetchFunctions`
This is the main function that triggers data fetching processes. It checks the time elapsed to ensure the function completes within a timeout of 5 minutes.

### `fetchMobulaTransactionData(createDate)`
Fetches transaction data from the Mobula API for each wallet address listed in the "Wallets" sheet and appends the data to the "Transactions" sheet. The function continues to fetch data until there are no more pages of transactions or the script reaches the timeout limit.

#### Parameters
- `createDate`: The date and time when the data fetch process started.

### `fetchLatestTimestamps(tableId)`
Fetches the latest transaction timestamps for each wallet and asset from BigQuery. This helps in fetching only the new transactions since the last recorded timestamp.

#### Parameters
- `tableId`: The ID of the table to fetch timestamps from.

### `fetchWalletData(walletAddress, createDate)`
Fetches wallet portfolio data from the Mobula API and appends it to the "Wallet_Data" and "Wallet_Assets" sheets. The function processes each wallet address listed in the "Wallets" sheet.

#### Parameters
- `walletAddress`: The wallet address to fetch data for.
- `createDate`: The date and time when the data fetch process started.

### `pushToBigQuery()`
Moves data from various sheets in the Google Spreadsheet to corresponding tables in BigQuery. It logs the number of records inserted.

### `moveSheetToBigQuery(spreadsheet, sheetName, projectId, datasetId, tableId)`
Helper function to move data from a specified sheet to a BigQuery table. It handles JSON conversion and the job configuration for BigQuery insertion.

#### Parameters
- `spreadsheet`: The Google Spreadsheet object.
- `sheetName`: The name of the sheet to move data from.
- `projectId`: The Google Cloud project ID.
- `datasetId`: The BigQuery dataset ID.
- `tableId`: The BigQuery table ID.

### `authorize()`
Triggers the authorization flow required for accessing Google APIs.

### `onOpen()`
Adds a custom menu to the Google Spreadsheet UI for easy access to the functions Push to BigQuery and Authorize.

### `fetchZapperData(walletAddress, createDate)`
Fetches wallet data from the Zapper API and appends it to the "Zapper_Wallet_Data" and "Zapper_Wallet_Assets" sheets. The function processes each wallet address listed in the "Wallets" sheet.

#### Parameters
- `walletAddress`: The wallet address to fetch data for.
- `createDate`: The date and time when the data fetch process started.

### `isValidEthereumAddress(address)`
Validates if a given string is a valid Ethereum address.

#### Parameters
- `address`: The Ethereum address to validate.

### `fetchETHPrices()`
Fetches Ethereum prices from the Mobula API and updates the "crypto_prices" sheet in the Google Spreadsheet.

### `removeDuplicateRows(sheetName)`
Removes duplicate rows from a specified sheet.

#### Parameters
- `sheetName`: The name of the sheet to remove duplicates from.

### `fetchTableSchema(projectId, datasetId, tableId)`
Fetches the schema of a BigQuery table.

#### Parameters
- `projectId`: The Google Cloud project ID.
- `datasetId`: The BigQuery dataset ID.
- `tableId`: The BigQuery table ID.

### `convertToFloat(value)`
Converts a value to float.

#### Parameters
- `value`: The value to convert.

### `formatDataAccordingToSchema(data, schema)`
Formats data according to BigQuery table schema.

#### Parameters
- `data`: The data to format.
- `schema`: The schema to format data against.

### `mapAndTransferTransactions()`
Maps and transfers transactions from the "Transactions" sheet to the "Trade Log" sheet, ensuring data is processed correctly.

### `getEthPriceClosestToTimestamp(date)`
Fetches the closest ETH price to the given timestamp from BigQuery.

#### Parameters
- `date`: The date to find the closest ETH price for.

## Usage Instructions

**Setup the Google Sheets:**
- Create sheets named "Wallets", "Transactions", "Wallet_Data", "Wallet_Assets", "Zapper_Wallet_Data", and "Zapper_Wallet_Assets" in your Google Spreadsheet.
- List the wallet addresses in the "Wallets" sheet starting from cell A2.

**Authorize the Script:**
- Open the Google Spreadsheet.
- Go to the custom menu `BigQuery` and click on Authorize to authorize the script to access Google APIs.

**Run the Data Fetch Functions:**
- From the custom menu `BigQuery`, click on `Push to BigQuery` to run the data fetch and push the data to `BigQuery`.

## Notes
- Ensure the Google Cloud project ID, dataset ID, and table IDs are correctly specified in the script.
- Replace the hardcoded API keys with actual keys or retrieve them securely from the sheet if necessary.
- The script includes error handling and logging to help troubleshoot any issues that may arise during the data fetch and push processes.
