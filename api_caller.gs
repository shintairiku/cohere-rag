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
 * ★★★ 診断のためログ出力を強化 ★★★
 * スプレッドシートが編集されたときに自動的に実行されるトリガー関数。
 * @param {Object} e - イベントオブジェクト
 */
function handleSheetEdit(e) {
  try {
    const sheet = e.source.getActiveSheet();
    const range = e.range;
    const sheetName = sheet.getName();
    const col = range.getColumn();
    const row = range.getRow();
    const value = e.value;

    // イベントの詳細をすべてログに出力して、何が起きているかを確認します
    Logger.log(`handleSheetEdit triggered:`);
    Logger.log(`- Sheet: ${sheetName}`);
    Logger.log(`- Edited Cell: ${range.getA1Notation()}`);
    Logger.log(`- Row: ${row}, Col: ${col}`);
    Logger.log(`- New Value (e.value): ${value}`);
    Logger.log(`- Old Value (e.oldValue): ${e.oldValue}`);

    // --- 検索シートでの処理 ---
    if (sheetName.startsWith(PLATFORM_SHEET_PREFIX) && col === SEARCH_TRIGGER_COL) {
      Logger.log("-> Condition Met: This is a search trigger edit.");
      
      if (row === 1) {
        Logger.log("-> Action: Ignored (Header row).");
        return;
      }

      if (value === TRIGGER_TEXT_SIMILAR || value === TRIGGER_TEXT_RANDOM) {
        Logger.log(`-> Action: Starting search for trigger "${value}" on row ${row}.`);
        performSearch(sheet, row, value);
      } else if (value === TRIGGER_TEXT_NOT_EXECUTED) {
        Logger.log(`-> Action: Clearing results for row ${row}.`);
        clearPreviousResults(sheet, row);
      } else {
        Logger.log(`-> Action: No action taken. Value "${value}" is not a recognized trigger.`);
      }
    }

    // --- 会社一覧シートでの処理 (UUID自動生成) ---
    if (sheetName === COMPANY_LIST_SHEET_NAME && col === COMPANY_LIST_NAME_COL) {
       Logger.log("-> Condition Met: This is a company name edit for UUID generation.");
       if (row === 1) {
         Logger.log("-> Action: Ignored (Header row).");
         return;
       }
       
       const uuidCell = sheet.getRange(row, COMPANY_LIST_UUID_COL);
       if (!uuidCell.getValue()) {
         const newUuid = Utilities.getUuid();
         uuidCell.setValue(newUuid);
         Logger.log(`-> Action: Generated new UUID ${newUuid} for row ${row}.`);
       } else {
         Logger.log("-> Action: No action taken. UUID already exists.");
       }
    }
  } catch (err) {
    Logger.log(`[FATAL ERROR in handleSheetEdit] ${err.toString()}`);
    Logger.log(err.stack);
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
  Logger.log(`[performSearch] Starting search for row ${row}, trigger: ${triggerValue}`);
  
  const statusCell = sheet.getRange(row, SEARCH_TRIGGER_COL);
  
  // プルダウンセルに安全に値を設定する関数
  function safeSetCellValue(cell, value) {
    try {
      Logger.log(`[safeSetCellValue] Attempting to set cell to: "${value}"`);
      // データ検証ルールがある場合、一時的に削除してから値を設定
      const validation = cell.getDataValidation();
      if (validation) {
        Logger.log(`[safeSetCellValue] Cell has data validation, clearing temporarily`);
        cell.clearDataValidations();
        cell.setValue(value);
        cell.setDataValidation(validation);
        Logger.log(`[safeSetCellValue] Successfully set value and restored validation`);
      } else {
        Logger.log(`[safeSetCellValue] Cell has no data validation, setting directly`);
        cell.setValue(value);
        Logger.log(`[safeSetCellValue] Successfully set value directly`);
      }
    } catch (error) {
      Logger.log(`[safeSetCellValue] Warning: Could not set cell value to "${value}": ${error.toString()}`);
      // フォールバック: データ検証を完全に削除してから設定
      try {
        cell.clearDataValidations();
        cell.setValue(value);
        Logger.log(`[safeSetCellValue] Fallback successful`);
      } catch (fallbackError) {
        Logger.log(`[safeSetCellValue] Error: Even fallback method failed: ${fallbackError.toString()}`);
      }
    }
  }
  
  try {
    Logger.log(`[performSearch] Step 1: Clearing previous results`);
    clearPreviousResults(sheet, row);
    Logger.log(`[performSearch] Step 2: Setting status to "検索中..."`);
    safeSetCellValue(statusCell, "検索中...");
    SpreadsheetApp.flush(); // UIを即時更新

    Logger.log(`[performSearch] Step 3: Getting company UUID`);
    const companyUuid = getUuidForSheet(sheet);
    if (!companyUuid) {
      throw new Error("「会社一覧」シートで対応する企業が見つかりません。");
    }
    Logger.log(`[performSearch] Found UUID: ${companyUuid}`);

    Logger.log(`[performSearch] Step 4: Getting search query`);
    const query = sheet.getRange(row, SEARCH_QUERY_COL).getValue();
    Logger.log(`[performSearch] Query: "${query}"`);
    
    if (triggerValue === TRIGGER_TEXT_SIMILAR && !query) {
      throw new Error("類似画像検索には検索クエリが必要です。");
    }

    Logger.log(`[performSearch] Step 5: Calling search API`);
    const results = callSearchApi(companyUuid, query, triggerValue);
    Logger.log(`[performSearch] API returned ${results ? results.length : 0} results`);
    
    if (results && results.length > 0) {
      Logger.log(`[performSearch] Step 6: Writing results to sheet`);
      writeResultsToSheet(sheet, row, results, triggerValue);
      Logger.log(`[performSearch] Step 7: Setting final status`);
      safeSetCellValue(statusCell, "実行完了");
      Logger.log(`[performSearch] Successfully completed search`);
    } else {
      Logger.log(`[performSearch] No results found, setting status`);
      safeSetCellValue(statusCell, "結果なし");
    }
  } catch (error) {
    Logger.log(`[performSearch] ERROR in performSearch: ${error.toString()}`);
    Logger.log(`[performSearch] Error stack: ${error.stack}`);
    safeSetCellValue(statusCell, `エラー: ${error.message}`);
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
 * APIから取得した結果をシートに書き込む（堅牢版）
 * データを一括で書き込んだ後、書式を個別に設定することでエラーを回避します。
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 * @param {Array<Object>} results - APIからの結果配列
 * @param {string} triggerValue - C列に入力されたトリガーテキスト
 */
function writeResultsToSheet(sheet, row, results, triggerValue) {
  Logger.log(`[writeResultsToSheet] Starting to write ${results.length} results to row ${row}`);
  
  try {
    const isRandom = (triggerValue === TRIGGER_TEXT_RANDOM);
    const MAX_RESULTS = 5;

    // 1. 書き込むためのデータ配列と、リンク情報を準備する
    Logger.log(`[writeResultsToSheet] Step 1: Preparing data arrays`);
    const rowData = [];
    const linkInfo = []; // { url: string, text: string, col: number }

    for (let i = 0; i < MAX_RESULTS; i++) {
      if (i < results.length) {
        const result = results[i];
        const fileUrl = result.filepath || "";
        const displayText = result.filename || fileUrl.split('/').pop() || "不明なファイル";
        
        // データ配列にはプレーンテキストを追加
        rowData.push(displayText);
        linkInfo.push({ url: fileUrl, text: displayText, col: SEARCH_RESULT_START_COL + i * 3 });

        // 類似度
        if (!isRandom && result.similarity !== null && typeof result.similarity === 'number') {
          rowData.push(result.similarity.toFixed(4));
        } else {
          rowData.push("");
        }
        
        // チェックボックス用の 'FALSE' を追加
        rowData.push(false);

      } else {
        // 結果が5件未満の場合、空のデータで埋める
        rowData.push("", "", "");
      }
    }

    // 2. 準備したデータを対象範囲に一度に書き込む
    Logger.log(`[writeResultsToSheet] Step 2: Writing data to range`);
    const targetRange = sheet.getRange(row, SEARCH_RESULT_START_COL, 1, MAX_RESULTS * 3);
    Logger.log(`[writeResultsToSheet] Target range: ${targetRange.getA1Notation()}`);
    targetRange.setValues([rowData]);
    Logger.log(`[writeResultsToSheet] Successfully wrote data to range`);

    // 3. リンクとチェックボックスの書式を個別に設定する
    Logger.log(`[writeResultsToSheet] Step 3: Setting links and checkboxes`);
    for (let i = 0; i < MAX_RESULTS; i++) {
      const colOffset = i * 3;
      
      // リンクを設定
      if (i < linkInfo.length && linkInfo[i].url) {
        try {
          const fileNameCell = sheet.getRange(row, linkInfo[i].col);
          const richText = SpreadsheetApp.newRichTextValue()
            .setText(linkInfo[i].text)
            .setLinkUrl(linkInfo[i].url)
            .build();
          fileNameCell.setRichTextValue(richText);
          Logger.log(`[writeResultsToSheet] Set link for result ${i}`);
        } catch (linkError) {
          Logger.log(`[writeResultsToSheet] Warning: Failed to set link for result ${i}: ${linkError.toString()}`);
        }
      }

      // チェックボックスを設定
      if (i < results.length) {
        try {
          const checkboxCell = sheet.getRange(row, SEARCH_RESULT_START_COL + colOffset + 2);
          checkboxCell.insertCheckboxes();
          Logger.log(`[writeResultsToSheet] Set checkbox for result ${i}`);
        } catch (checkboxError) {
          Logger.log(`[writeResultsToSheet] Warning: Failed to set checkbox for result ${i}: ${checkboxError.toString()}`);
        }
      }
    }
    Logger.log(`[writeResultsToSheet] Successfully completed writing results`);
    
  } catch (error) {
    Logger.log(`[writeResultsToSheet] ERROR: ${error.toString()}`);
    Logger.log(`[writeResultsToSheet] Error stack: ${error.stack}`);
    throw error; // エラーを再スローして上位関数でキャッチ
  }
}


/**
 * 以前の検索結果をクリアする
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 */
function clearPreviousResults(sheet, row) {
  Logger.log(`[clearPreviousResults] Clearing results for row ${row}`);
  try {
    // D列から15列分クリア (5結果 * 3列)
    const range = sheet.getRange(row, SEARCH_RESULT_START_COL, 1, 15);
    Logger.log(`[clearPreviousResults] Target range: ${range.getA1Notation()}`);
    
    // 書式、内容、データ検証ルールなどすべてをクリアする
    range.clear({formatOnly: false, contentsOnly: false});
    Logger.log(`[clearPreviousResults] Successfully cleared range`);
  } catch (error) {
    Logger.log(`[clearPreviousResults] ERROR: ${error.toString()}`);
    Logger.log(`[clearPreviousResults] Error stack: ${error.stack}`);
    throw error; // エラーを再スローして上位関数でキャッチ
  }
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

