/**
 * @OnlyCurrentDoc
 */

// --- 設定項目 ---
const API_BASE_URL = "https://cohere-rag-742231208085.asia-northeast1.run.app"; 
const COMPANY_LIST_SHEET_NAME = "会社一覧";
// (検索シート用の設定は変更なし)
const COLUMN_QUERY = 1;
const COLUMN_TRIGGER = 3;
const COLUMN_RESULT_START = 4;
const TRIGGER_TEXT_SIMILAR = "類似画像検索";
const TRIGGER_TEXT_RANDOM = "ランダム画像検索";
const TRIGGER_TEXT_NOT_EXECUTED = "未実行";
const STATUS_TEXT_EXECUTING = "検索中...";
const STATUS_TEXT_COMPLETE = "実行完了";
// --- 設定項目ここまで ---


function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('✨画像検索メニュー')
    .addItem('1. 選択行の会社の画像をベクトル化', 'callVectorizeApiForSelectedRow')
    .addSeparator()
    .addItem('（管理者用）空のUUIDを一括生成', 'fillEmptyUuids')
    .addToUi();
}

function onEdit(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;
  const sheetName = sheet.getName();

  if (sheetName === COMPANY_LIST_SHEET_NAME && range.getColumn() === 2 && range.getRow() > 1) {
    const companyName = range.getValue();
    const uuidCell = sheet.getRange(range.getRow(), 1);
    if (companyName && uuidCell.isBlank()) {
      uuidCell.setValue(Utilities.getUuid());
    }
    return;
  }

  if (sheetName !== COMPANY_LIST_SHEET_NAME && range.getColumn() === COLUMN_TRIGGER && range.getRow() > 1) {
    const triggerValue = e.value;
    const row = range.getRow();
    if (triggerValue === TRIGGER_TEXT_NOT_EXECUTED) {
      clearPreviousResults(sheet, row);
      return;
    }
    if (triggerValue === TRIGGER_TEXT_SIMILAR || triggerValue === TRIGGER_TEXT_RANDOM) {
      performSearch(sheet, row, triggerValue);
    }
  }
}

// --- ここから修正 ---
function callVectorizeApiForSelectedRow() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  if (sheet.getName() !== COMPANY_LIST_SHEET_NAME) {
    SpreadsheetApp.getUi().alert(`この操作は「${COMPANY_LIST_SHEET_NAME}」シートでのみ実行できます。`);
    return;
  }
  const activeCell = sheet.getActiveCell();
  const activeRow = activeCell.getRow();
  if (activeRow < 2) {
    SpreadsheetApp.getUi().alert("ヘッダー行より下の、対象の会社の行を選択してください。");
    return;
  }

  // 選択行からUUIDとDrive URLを取得
  const rowData = sheet.getRange(activeRow, 1, 1, 3).getValues()[0];
  const uuid = rowData[0];      // A列
  const companyName = rowData[1]; // B列
  const driveUrl = rowData[2];  // C列

  if (!uuid || !driveUrl) {
    SpreadsheetApp.getUi().alert("選択された行にUUIDまたはDrive URLが見つかりません。");
    return;
  }

  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(`「${companyName}」の画像のベクトル化を開始しますか？`, ui.ButtonSet.YES_NO);

  if (response == ui.Button.YES) {
    try {
      // 取得した情報をAPIに渡す
      callVectorizeApi(uuid, driveUrl);
      ui.alert(`「${companyName}」のベクトル化処理を開始しました。\n処理には数分かかる場合があります。`);
    } catch (e) {
      Logger.log(e);
      ui.alert(`API呼び出し中にエラーが発生しました。\n\nエラー内容:\n${e.message}`);
    }
  }
}

function callVectorizeApi(uuid, driveUrl) {
  const url = `${API_BASE_URL}/vectorize`;
  // ペイロードにdrive_urlを追加
  const payload = {
    uuid: uuid,
    drive_url: driveUrl 
  };
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  Logger.log(`Requesting to ${url} with payload: ${JSON.stringify(payload)}`);
  const response = UrlFetchApp.fetch(url, options);
  const responseCode = response.getResponseCode();
  const responseBody = response.getContentText();
  Logger.log(`Response Code: ${responseCode}`);
  Logger.log(`Response Body: ${responseBody}`);
  if (responseCode !== 202) {
    throw new Error(`APIリクエストに失敗しました (HTTP ${responseCode}): ${responseBody}`);
  }
}
// --- ここまで修正 ---

// ... (以降の検索関連、UUID生成関連の関数は変更なし) ...
function clearPreviousResults(sheet, row) {
  sheet.getRange(row, COLUMN_RESULT_START, 1, 15).clearContent();
}

function performSearch(sheet, row, trigger) {
  const statusCell = sheet.getRange(row, COLUMN_TRIGGER);
  const query = sheet.getRange(row, COLUMN_QUERY).getValue();
  
  if (trigger === TRIGGER_TEXT_SIMILAR && !query) {
    statusCell.setValue("エラー: クエリが空です");
    return;
  }
  
  clearPreviousResults(sheet, row);
  statusCell.setValue(STATUS_TEXT_EXECUTING);
  SpreadsheetApp.flush();

  try {
    const uuid = getUuidForSheet(sheet);
    if (!uuid) {
      throw new Error("このシートに対応する企業が見つかりません。");
    }

    const results = callSearchApi(uuid, query, trigger);
    
    if (results && results.length > 0) {
      writeResultsToSheet(sheet, row, results);
      statusCell.setValue(STATUS_TEXT_COMPLETE);
    } else {
      statusCell.setValue("結果なし");
    }
  } catch (e) {
    statusCell.setValue(`エラー: ${e.message.substring(0, 100)}`);
    Logger.log(`[performSearch] Error: ${e.toString()}`);
  }
}

function writeResultsToSheet(sheet, row, results) {
  const isRandom = (sheet.getRange(row, COLUMN_TRIGGER).getValue() === TRIGGER_TEXT_RANDOM);
  const outputRow = [];

  for (let i = 0; i < 5; i++) {
    if (results[i]) {
      const fileUrl = results[i].filepath;
      const displayText = results[i].filename || fileUrl;
      const richText = SpreadsheetApp.newRichTextValue().setText(displayText).setLinkUrl(fileUrl).build();
      
      outputRow.push(richText);
      outputRow.push(isRandom ? "" : results[i].similarity.toFixed(4));
      outputRow.push(false);
    } else {
      outputRow.push("", "", "");
    }
  }
  sheet.getRange(row, COLUMN_RESULT_START, 1, 15).setValues([outputRow]);
}

function callSearchApi(uuid, query, trigger) {
  const topK = 5;
  const url = `${API_BASE_URL}/search?uuid=${encodeURIComponent(uuid)}&q=${encodeURIComponent(query)}&top_k=${topK}&trigger=${encodeURIComponent(trigger)}`;
  const options = {
    method: 'get',
    muteHttpExceptions: true
  };
  Logger.log(`Requesting to ${url}`);
  const response = UrlFetchApp.fetch(url, options);
  const responseCode = response.getResponseCode();
  const responseBody = response.getContentText();
  Logger.log(`Response Code: ${responseCode}`);
  if (responseCode === 200) {
    const json = JSON.parse(responseBody);
    return json.results || [];
  } else {
    throw new Error(`APIリクエスト失敗 (HTTP ${responseCode})`);
  }
}

function getUuidForSheet(sheet) {
  const companyListSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(COMPANY_LIST_SHEET_NAME);
  if (!companyListSheet) {
    throw new Error(`「${COMPANY_LIST_SHEET_NAME}」シートが見つかりません。`);
  }
  const sheetUrl = SpreadsheetApp.getActiveSpreadsheet().getUrl() + "#gid=" + sheet.getSheetId();
  const data = companyListSheet.getDataRange().getValues();
  for (let i = 1; i < data.length; i++) {
    const uuid = data[i][0];
    const companySheetUrl = data[i][3];
    if (companySheetUrl === sheetUrl) {
      return uuid;
    }
  }
  return null;
}

function fillEmptyUuids() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(COMPANY_LIST_SHEET_NAME);
  if (!sheet) {
    SpreadsheetApp.getUi().alert(`シート "${COMPANY_LIST_SHEET_NAME}" が見つかりません。`);
    return;
  }
  const dataRange = sheet.getDataRange();
  const values = dataRange.getValues();
  let updatedCount = 0;
  for (let i = 1; i < values.length; i++) {
    const uuid = values[i][0];
    const companyName = values[i][1];
    if (companyName && !uuid) {
      sheet.getRange(i + 1, 1).setValue(Utilities.getUuid());
      updatedCount++;
    }
  }
  if (updatedCount > 0) {
    SpreadsheetApp.getUi().alert(`${updatedCount}件のUUIDを生成しました。`);
  } else {
    SpreadsheetApp.getUi().alert('UUIDが空の行は見つかりませんでした。');
  }
}
