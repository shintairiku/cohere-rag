/**
 * @fileoverview 企業別・類似画像検索プロダクトのGoogle Apps Script
 * 機能：
 * 1. カスタムメニューの作成 (ベクトル化実行)
 * 2. 会社一覧シートへの企業追加時にUUIDを自動生成
 * 3. 各企業シートでの検索トリガー(handleSheetEdit)によるAPI呼び出し
 */

// --- ユーザー設定項目 ---
// ご自身のCloud Runサービス(main-service)のURLに書き換えてください (末尾に / は不要です)
const API_BASE_URL = "https://cohere-rag-742231208085.asia-northeast1.run.app"; 
// --- ユーザー設定項目ここまで ---


// --- グローバル設定 ---
// 「会社一覧」シートに関する設定
const COMPANY_LIST_SHEET_NAME = "会社一覧";
const COMPANY_LIST_UUID_COL = 1;          // A列: UUID
const COMPANY_LIST_NAME_COL = 2;          // B列: 会社名
const COMPANY_LIST_DRIVE_URL_COL = 3;     // C列: GoogleドライブURL

// 各企業シートに関する設定
const PLATFORM_SHEET_PREFIX = "platform-";
const SEARCH_QUERY_COL = 1;               // A列: 検索クエリ
const SEARCH_TRIGGER_COL = 3;             // C列: 実行状況 (トリガー)
const SEARCH_RESULT_START_COL = 4;        // D列: 結果出力の開始列

// トリガーとして認識するテキスト
const TRIGGER_TEXT_SIMILAR = "類似画像検索";
const TRIGGER_TEXT_RANDOM = "ランダム画像検索";
const TRIGGER_TEXT_NOT_EXECUTED = "未実行";


/**
 * スプレッドシートを開いたときにカスタムメニューをUIに追加します。
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('✨画像検索メニュー')
    .addItem('選択行のベクトル化を実行', 'callVectorizeApi')
    .addSeparator()
    .addItem('空のUUIDを一括生成', 'generateUuids')
    .addToUi();
}

/**
 * スプレッドシートが編集されたときに自動的に実行されるトリガー関数。
 * @param {Object} e - イベントオブジェクト
 */
function handleSheetEdit(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;

  // --- 検索シートでの処理 ---
  if (sheet.getName().startsWith(PLATFORM_SHEET_PREFIX) && range.getColumn() === SEARCH_TRIGGER_COL) {
    const triggerValue = e.value;
    const row = range.getRow();
    
    // ヘッダー行は無視
    if (row === 1) return;

    if (triggerValue === TRIGGER_TEXT_SIMILAR || triggerValue === TRIGGER_TEXT_RANDOM) {
      performSearch(sheet, row, triggerValue);
    } else if (triggerValue === TRIGGER_TEXT_NOT_EXECUTED) {
      clearPreviousResults(sheet, row);
    }
  }

  // --- 会社一覧シートでの処理 (UUID自動生成) ---
  if (sheet.getName() === COMPANY_LIST_SHEET_NAME && range.getColumn() === COMPANY_LIST_NAME_COL) {
     const row = range.getRow();
     // ヘッダー行は無視
     if (row === 1) return;
     
     const uuidCell = sheet.getRange(row, COMPANY_LIST_UUID_COL);
     if (!uuidCell.getValue()) {
       uuidCell.setValue(Utilities.getUuid());
     }
  }
}

// =======================================================================
// 画像検索関連の関数
// =======================================================================

/**
 * 画像検索を実行するメイン関数
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @param {number} row - 編集された行
 * @param {string} triggerValue - C列に入力されたトリガーテキスト
 */
function performSearch(sheet, row, triggerValue) {
  const statusCell = sheet.getRange(row, SEARCH_TRIGGER_COL);
  clearPreviousResults(sheet, row);
  statusCell.setValue("検索中...");
  SpreadsheetApp.flush(); // UIを即時更新

  try {
    const companyUuid = getUuidForSheet(sheet);
    if (!companyUuid) {
      throw new Error("「会社一覧」シートで対応する企業が見つかりません。");
    }

    const query = sheet.getRange(row, SEARCH_QUERY_COL).getValue();
    if (triggerValue === TRIGGER_TEXT_SIMILAR && !query) {
      throw new Error("類似画像検索には検索クエリが必要です。");
    }

    const results = callSearchApi(companyUuid, query, triggerValue);
    
    if (results && results.length > 0) {
      writeResultsToSheet(sheet, row, results, triggerValue);
      statusCell.setValue("実行完了");
    } else {
      statusCell.setValue("結果なし");
    }
  } catch (error) {
    statusCell.setValue(`エラー: ${error.message}`);
    Logger.log(`[performSearch] Error: ${error.toString()}`);
  }
}

/**
 * シート名から対応する企業のUUIDを取得する
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @return {string|null} - 企業のUUID、見つからない場合はnull
 */
function getUuidForSheet(sheet) {
  const sheetName = sheet.getName();
  if (!sheetName.startsWith(PLATFORM_SHEET_PREFIX)) {
    return null;
  }
  const companyName = sheetName.substring(PLATFORM_SHEET_PREFIX.length);

  const companyListSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(COMPANY_LIST_SHEET_NAME);
  if (!companyListSheet) {
    SpreadsheetApp.getUi().alert(`エラー: '${COMPANY_LIST_SHEET_NAME}' シートが見つかりません。`);
    return null;
  }
  
  const data = companyListSheet.getRange(2, COMPANY_LIST_NAME_COL, companyListSheet.getLastRow() - 1, 1).getValues();
  const uuids = companyListSheet.getRange(2, COMPANY_LIST_UUID_COL, companyListSheet.getLastRow() - 1, 1).getValues();

  for (let i = 0; i < data.length; i++) {
    if (data[i][0] === companyName) {
      return uuids[i][0];
    }
  }
  return null;
}

/**
 * Cloud Runの検索APIを呼び出す
 * @param {string} uuid - 企業のUUID
 * @param {string} query - 検索クエリ
 * @param {string} trigger - 検索のトリガー種別
 * @return {Array<Object>} - 検索結果の配列
 */
function callSearchApi(uuid, query, trigger) {
  const topK = 5;
  const encodedQuery = encodeURIComponent(query || "");
  const encodedTrigger = encodeURIComponent(trigger);
  
  const apiUrl = `${API_BASE_URL}/search?uuid=${uuid}&q=${encodedQuery}&top_k=${topK}&trigger=${encodedTrigger}`;
  
  const params = {
    method: "get",
    headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
    muteHttpExceptions: true,
  };

  const response = UrlFetchApp.fetch(apiUrl, params);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText();

  if (responseCode === 200) {
    const jsonResponse = JSON.parse(responseText);
    return jsonResponse.results;
  } else {
    Logger.log(`API Error Response (${responseCode}): ${responseText}`);
    throw new Error(`APIエラーが発生しました (コード: ${responseCode})`);
  }
}

/**
 * APIから取得した結果をシートに書き込む
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 * @param {Array<Object>} results - APIからの結果配列
 * @param {string} triggerValue - C列に入力されたトリガーテキスト
 */
function writeResultsToSheet(sheet, row, results, triggerValue) {
  const isRandom = (triggerValue === TRIGGER_TEXT_RANDOM);
  
  // デバッグ: 受信したデータ構造をログ出力
  Logger.log(`writeResultsToSheet called with:`);
  Logger.log(`- results type: ${typeof results}, isArray: ${Array.isArray(results)}`);
  Logger.log(`- results length: ${results ? results.length : 'N/A'}`);
  if (results && results.length > 0) {
    Logger.log(`- First result: ${JSON.stringify(results[0])}`);
    Logger.log(`- filepath type: ${typeof results[0].filepath}`);
    Logger.log(`- filename type: ${typeof results[0].filename}`);
    Logger.log(`- similarity type: ${typeof results[0].similarity}`);
  }
  
  // 各結果ごとに3列（ファイル名、類似度、チェックボックス）を使用
  let col = SEARCH_RESULT_START_COL;
  
  results.forEach((result, index) => {
    if (index >= 5) return; // 最大5件まで
    
    try {
      // 型チェックと安全な文字列変換
      let fileUrl = "";
      let displayText = "不明なファイル";
      
      if (result.filepath && typeof result.filepath === 'string') {
        fileUrl = result.filepath;
      } else if (result.filepath) {
        Logger.log(`Warning: filepath is not a string: ${typeof result.filepath}, value: ${result.filepath}`);
        fileUrl = String(result.filepath);
      }
      
      if (result.filename && typeof result.filename === 'string') {
        displayText = result.filename;
      } else if (result.filename) {
        Logger.log(`Warning: filename is not a string: ${typeof result.filename}, value: ${result.filename}`);
        displayText = String(result.filename);
      } else if (fileUrl) {
        displayText = fileUrl.split('/').pop() || "不明なファイル";
      }
      
      // 1. ファイル名にリンクを設定（RichTextValue）
      const fileNameCell = sheet.getRange(row, col);
      fileNameCell.clear(); // 既存の書式をクリア
      const richText = SpreadsheetApp.newRichTextValue()
        .setText(displayText)
        .setLinkUrl(fileUrl)
        .build();
      fileNameCell.setRichTextValue(richText);
      
      // 2. 類似度スコア（ランダム検索の場合は空）
      const similarityCell = sheet.getRange(row, col + 1);
      similarityCell.clear(); // 既存の書式をクリア
      if (!isRandom && result.similarity !== null && result.similarity !== undefined) {
        // 類似度の型チェック
        if (typeof result.similarity === 'number') {
          similarityCell.setValue(result.similarity.toFixed(4));
        } else if (Array.isArray(result.similarity)) {
          Logger.log(`Error: similarity is an array: ${result.similarity}`);
          similarityCell.setValue("エラー: 配列");
        } else {
          Logger.log(`Warning: similarity is not a number: ${typeof result.similarity}, value: ${result.similarity}`);
          const numValue = parseFloat(result.similarity);
          if (!isNaN(numValue)) {
            similarityCell.setValue(numValue.toFixed(4));
          } else {
            similarityCell.setValue("エラー");
          }
        }
      } else {
        similarityCell.setValue("");
      }
      
      // 3. チェックボックス
      const checkboxCell = sheet.getRange(row, col + 2);
      checkboxCell.clear(); // 既存の書式をクリア
      checkboxCell.insertCheckboxes();
      
      col += 3; // 次の結果は3列後ろ
      
    } catch (error) {
      Logger.log(`Error processing result ${index}: ${error.toString()}`);
      Logger.log(`Result object: ${JSON.stringify(result)}`);
      // エラーが発生した場合でも次の結果を処理するため、エラー表示して継続
      sheet.getRange(row, col).setValue(`エラー: ${error.message}`);
      col += 3;
    }
  });
}


/**
 * 以前の検索結果をクリアする
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 */
function clearPreviousResults(sheet, row) {
  // D列から15列分クリア (5結果 * 3列)
  const range = sheet.getRange(row, SEARCH_RESULT_START_COL, 1, 15);
  range.clear(); // clearContent()ではなくclear()を使用して書式も含めて完全クリア
}


// =======================================================================
// ベクトル化・UUID生成関連の関数
// =======================================================================

/**
 * カスタムメニューから呼び出され、選択行のベクトル化をトリガーする
 */
function callVectorizeApi() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(COMPANY_LIST_SHEET_NAME);
  const activeCell = sheet.getActiveCell();
  
  if (!sheet || sheet.getName() !== COMPANY_LIST_SHEET_NAME) {
    SpreadsheetApp.getUi().alert(`'${COMPANY_LIST_SHEET_NAME}'シートから実行してください。`);
    return;
  }

  const row = activeCell.getRow();
  if (row < 2) {
    SpreadsheetApp.getUi().alert("ヘッダー行ではなく、対象の会社の行を選択してください。");
    return;
  }

  try {
    const uuid = sheet.getRange(row, COMPANY_LIST_UUID_COL).getValue();
    const driveUrl = sheet.getRange(row, COMPANY_LIST_DRIVE_URL_COL).getValue();

    if (!uuid || !driveUrl) {
      throw new Error("UUIDまたはGoogleドライブのURLが空です。");
    }

    const payload = JSON.stringify({
      "uuid": uuid,
      "drive_url": driveUrl
    });

    const params = {
      method: "post",
      contentType: "application/json",
      headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
      payload: payload,
      muteHttpExceptions: true,
    };

    const apiUrl = `${API_BASE_URL}/vectorize`;
    const response = UrlFetchApp.fetch(apiUrl, params);
    const responseCode = response.getResponseCode();

    if (responseCode === 202) {
      SpreadsheetApp.getUi().alert("ベクトル化ジョブの開始をリクエストしました。処理には時間がかかります。");
    } else {
      Logger.log(`API Error Response (${responseCode}): ${response.getContentText()}`);
      throw new Error(`APIエラーが発生しました (コード: ${responseCode})`);
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert(`エラー: ${error.message}`);
    Logger.log(`[callVectorizeApi] Error: ${error.toString()}`);
  }
}

/**
 * 「会社一覧」シートで、UUIDが空の行にUUIDをまとめて生成する
 */
function generateUuids() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(COMPANY_LIST_SHEET_NAME);
  if (!sheet) {
    SpreadsheetApp.getUi().alert(`'${COMPANY_LIST_SHEET_NAME}'シートが見つかりません。`);
    return;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const range = sheet.getRange(2, COMPANY_LIST_UUID_COL, lastRow - 1, COMPANY_LIST_NAME_COL);
  const values = range.getValues();

  let updated = false;
  for (let i = 0; i < values.length; i++) {
    const uuid = values[i][0];
    const companyName = values[i][COMPANY_LIST_NAME_COL - 1];
    if (companyName && !uuid) {
      values[i][0] = Utilities.getUuid();
      updated = true;
    }
  }

  if (updated) {
    range.setValues(values);
    SpreadsheetApp.getUi().alert('空だったUUIDを生成しました。');
  } else {
    SpreadsheetApp.getUi().alert('UUIDが空の行はありませんでした。');
  }
}
