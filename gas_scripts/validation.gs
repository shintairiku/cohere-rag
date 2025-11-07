/**
 * @fileoverview 検索トリガー用のデータ検証関連ユーティリティ。
 */

/**
 * 新しいトリガー候補値でデータ検証を構築します。
 * @return {GoogleAppsScript.Spreadsheet.DataValidation}
 */
function buildTriggerValidation_() {
  return SpreadsheetApp.newDataValidation()
    .requireValueInList(Config.TRIGGER_OPTIONS, true)
    .setAllowInvalid(false)
    .build();
}

/**
 * プラットフォームシートのD列に最新のデータ検証を適用します。
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 */
function applyTriggerValidationToSheet_(sheet) {
  const startRow = 2;
  const numRows = sheet.getMaxRows() - startRow + 1;
  if (numRows <= 0) {
    return;
  }
  const range = sheet.getRange(startRow, Config.PLATFORM.SEARCH_TRIGGER_COL, numRows, 1);
  range.setDataValidation(buildTriggerValidation_());
}

/**
 * すべてのプラットフォームシートにトリガー用データ検証を適用します。
 */
function applyTriggerValidationToPlatformSheets_() {
  const spreadsheet = SpreadsheetApp.getActive();
  if (!spreadsheet) {
    return;
  }
  const sheets = spreadsheet.getSheets();
  for (const sheet of sheets) {
    if (sheet.getName().startsWith(Config.PLATFORM.SHEET_PREFIX)) {
      applyTriggerValidationToSheet_(sheet);
    }
  }
}
