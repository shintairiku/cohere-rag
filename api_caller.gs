/**
 * @OnlyCurrentDoc
 *
 * このスクリプトは、スプレッドシートから画像検索APIを呼び出すためのものです。
 * 検索は各企業シートのC列「実行状況」の値を変更することでトリガーされます。
 * UUIDの自動生成やベクトル化の呼び出し機能も含まれています。
 */

// --- 設定項目 ---
// ご自身のCloud RunサービスのURLに書き換えてください (末尾に / は不要です)
const API_BASE_URL = "https://cohere-rag-742231208085.asia-northeast1.run.app"; 

const COMPANY_LIST_SHEET_NAME = "会社一覧";

// 検索シート用の設定
const COLUMN_QUERY = 1;          // A列: 検索クエリ
const COLUMN_TRIGGER = 3;        // C列: 実行状況（トリガー）
const COLUMN_RESULT_START = 4;   // D列: 結果出力の開始列

const TRIGGER_TEXT_SIMILAR = "類似画像検索";
const TRIGGER_TEXT_RANDOM = "ランダム画像検索";
const TRIGGER_TEXT_NOT_EXECUTED = "未実行";
const STATUS_TEXT_EXECUTING = "検索中...";
const STATUS_TEXT_COMPLETE = "実行完了";
// --- 設定項目ここまで ---


/**
 * スプレッドシートを開いたときにカスタムメニューを追加します。
 * 検索実行はonEditトリガーに変更したため、メニューからは削除しました。
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('✨画像検索メニュー')
    .addItem('1. 選択行の会社の画像をベクトル化', 'callVectorizeApiForSelectedRow')
    .addSeparator()
    .addItem('（管理者用）空のUUIDを一括生成', 'fillEmptyUuids')
    .addToUi();
}

/**
 * スプレッドシートが編集されたときに自動実行される関数。
 * 編集内容に応じて、UUID生成または画像検索を実行します。
 * @param {Object} e - イベントオブジェクト
 */
function onEdit(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;
  const sheetName = sheet.getName();

  // --- ルート1: 「会社一覧」シートでのUUID自動生成 ---
  if (sheetName === COMPANY_LIST_SHEET_NAME && range.getColumn() === 2 && range.getRow() > 1) {
    const companyName = range.getValue();
    const uuidCell = sheet.getRange(range.getRow(), 1);
    if (companyName && uuidCell.isBlank()) {
      uuidCell.setValue(Utilities.getUuid());
    }
    return; // UUID生成後は処理を終了
  }

  // --- ルート2: 企業別シートでの検索トリガー ---
  if (sheetName !== COMPANY_LIST_SHEET_NAME && range.getColumn() === COLUMN_TRIGGER && range.getRow() > 1) {
    const triggerValue = e.value;
    const row = range.getRow();

    // 「未実行」が選択された場合は結果をクリアして終了
    if (triggerValue === TRIGGER_TEXT_NOT_EXECUTED) {
      clearPreviousResults(sheet, row);
      return;
    }
    
    // それ以外のトリガーテキストが選択された場合
    if (triggerValue === TRIGGER_TEXT_SIMILAR || triggerValue === TRIGGER_TEXT_RANDOM) {
      performSearch(sheet, row, triggerValue);
    }
  }
}

/**
 * 以前の検索結果をクリアする関数
 */
function clearPreviousResults(sheet, row) {
  sheet.getRange(row, COLUMN_RESULT_START, 1, 15).clearContent();
}

/**
 * APIを呼び出して検索を実行する関数
 */
function performSearch(sheet, row, trigger) {
  const statusCell = sheet.getRange(row, COLUMN_TRIGGER);
  const query = sheet.getRange(row, COLUMN_QUERY).getValue();
  
  // 類似画像検索でクエリが空の場合はエラー
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

/**
 * APIから取得した結果をシートに書き込む関数
 */
function writeResultsToSheet(sheet, row, results) {
  const isRandom = (sheet.getRange(row, COLUMN_TRIGGER).getValue() === TRIGGER_TEXT_RANDOM);
  const outputRow = [];

  for (let i = 0; i < 5; i++) {
    if (results[i]) {
      const fileUrl = results[i].filepath;
      const displayText = results[i].filename || fileUrl;
      const richText = SpreadsheetApp.newRichTextValue().setText(displayText).setLinkUrl(fileUrl).build();
      
      // 画像パス(リッチテキスト), 類似度, 使用チェック(FALSE)
      outputRow.push(richText);
      outputRow.push(isRandom ? "" : results[i].similarity.toFixed(4));
      outputRow.push(false);
    } else {
      outputRow.push("", "", "");
    }
  }
  // D列から15列分 (3列 * 5件) の範囲に結果を書き込み
  sheet.getRange(row, COLUMN_RESULT_START, 1, 15).setValues([outputRow]);
}


/**
 * searchエンドポイントを呼び出すコア関数。
 */
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


/**
 * メニューから「ベクトル化」が選択されたときに実行される関数。
 */
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
  const uuid = sheet.getRange(activeRow, 1).getValue();
  if (!uuid) {
    SpreadsheetApp.getUi().alert("選択された行にUUIDが見つかりません。先にUUIDを生成してください。");
    return;
  }
  const companyName = sheet.getRange(activeRow, 2).getValue();
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(`「${companyName}」の画像のベクトル化を開始しますか？`, ui.ButtonSet.YES_NO);

  if (response == ui.Button.YES) {
    try {
      callVectorizeApi(uuid);
      ui.alert(`「${companyName}」のベクトル化処理を開始しました。\n処理には数分かかる場合があります。`);
    } catch (e) {
      Logger.log(e);
      ui.alert(`API呼び出し中にエラーが発生しました。\n\nエラー内容:\n${e.message}`);
    }
  }
}

/**
 * vectorizeエンドポイントを呼び出すコア関数。
 */
function callVectorizeApi(uuid) {
  const url = `${API_BASE_URL}/vectorize`;
  const payload = { uuid: uuid };
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

/**
 * 現在開いているシートに対応する企業のUUIDを「会社一覧」シートから検索して返す。
 */
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

/**
 * メニューから手動で空のUUIDをすべて埋める関数。
 */
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
