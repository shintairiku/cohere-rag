/**
 * @fileoverview Image Search System for Company-specific Google Sheets
 * * This Google Apps Script provides functionality for:
 * 1. Custom menu creation for vectorization operations
 * 2. Automatic UUID generation when companies are added
 * 3. Image search triggered by cell edits in company sheets
 * 4. Management of an exclusion list for recently used images
 * * @version 1.7.1
 * @author Claude Code Assistant (with modifications)
 */

/**
 * Configuration object for the application
 */
const Config = {
  // API Configuration - Update this URL to match your Cloud Run service
  API_BASE_URL: "https://cohere-rag-742231208085.asia-northeast1.run.app",

  // Company List Sheet Configuration
  COMPANY_LIST: {
    SHEET_NAME: "会社一覧",
    UUID_COL: 1,        // A列: UUID
    NAME_COL: 2,        // B列: 会社名
    DRIVE_URL_COL: 3,    // C列: GoogleドライブURL
    PRIORITY_COL: 6     // F列: 優先企業リスト (チェックボックス) <- ★★★ 修正箇所 ★★★
  },

  // Platform Sheet Configuration
  PLATFORM: {
    SHEET_PREFIX: "platform-",
    SEARCH_QUERY_COL: 1,      // A列: 検索クエリ
    SEARCH_TRIGGER_COL: 3,    // C列: 実行状況 (トリガー)
    SEARCH_RESULT_START_COL: 4, // D列: 結果出力の開始列
    // D, G, J, M, P -> Filename
    // F, I, L, O, R -> Checkbox for exclusion
    USE_CHECKBOX_COLUMNS: [6, 9, 12, 15, 18]
  },

  // Trigger Text Constants
  TRIGGERS: {
    SIMILAR: "類似画像検索",
    RANDOM: "ランダム画像検索",
    NOT_EXECUTED: "未実行"
  },

  // Exclusion List Configuration (削除予定)
};

/**
 * スプレッドシートを開いたときにカスタムメニューをUIに追加します。
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('✨画像検索メニュー')
    .addItem('選択行のベクトル化を実行', 'callVectorizeApi')
    .addItem('優先企業のベクトル化を一括実行', 'vectorizePriorityCompanies')
    .addSeparator()
    .addItem('空のUUIDを一括生成', 'generateUuids')
    .addToUi();
}

/**
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
    const isChecked = value === "TRUE";

    Logger.log(`handleSheetEdit triggered:`);
    Logger.log(`- Sheet: ${sheetName}, Cell: ${range.getA1Notation()}, New Value: ${value}`);

    // --- 検索シートでの検索トリガー ---
    if (sheetName.startsWith(Config.PLATFORM.SHEET_PREFIX) && col === Config.PLATFORM.SEARCH_TRIGGER_COL && row > 1) {
      Logger.log("-> Condition Met: This is a search trigger edit.");
      if (value === Config.TRIGGERS.SIMILAR || value === Config.TRIGGERS.RANDOM) {
        performSearch(sheet, row, value);
      } else if (value === Config.TRIGGERS.NOT_EXECUTED) {
        clearPreviousResults(sheet, row);
      }
    }

    // --- 会社一覧シートでのUUID自動生成 ---
    else if (sheetName === Config.COMPANY_LIST.SHEET_NAME && col === Config.COMPANY_LIST.NAME_COL && row > 1) {
       Logger.log("-> Condition Met: This is a company name edit for UUID generation.");
       const uuidCell = sheet.getRange(row, Config.COMPANY_LIST.UUID_COL);
       if (!uuidCell.getValue()) {
         uuidCell.setValue(Utilities.getUuid());
         Logger.log(`-> Action: Generated new UUID for row ${row}.`);
       }
    }

    // チェックボックス処理は削除（新仕様では状態を維持）

  } catch (err) {
    Logger.log(`[FATAL ERROR in handleSheetEdit] ${err.toString()}\n${err.stack}`);
    SpreadsheetApp.getUi().alert(`スクリプトエラーが発生しました: ${err.message}`);
  }
}


// =======================================================================
// 画像検索関連の関数
// =======================================================================

/**
 * 画像検索を実行するメイン関数 (エラー修正版)
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @param {number} row - 編集された行
 * @param {string} triggerValue - C列に入力されたトリガーテキスト
 */
function performSearch(sheet, row, triggerValue) {
  Logger.log(`[performSearch] Starting search for row ${row}, trigger: ${triggerValue}`);
  const statusCell = sheet.getRange(row, Config.PLATFORM.SEARCH_TRIGGER_COL);
  const validation = statusCell.getDataValidation();

  try {
    if (validation) {
      statusCell.clearDataValidations();
    }
    
    clearPreviousResults(sheet, row);
    statusCell.setValue("検索中...");
    SpreadsheetApp.flush();

    const companyUuid = getUuidForSheet(sheet);
    if (!companyUuid) throw new Error("「会社一覧」シートで対応する企業が見つかりません。");
    
    const query = sheet.getRange(row, Config.PLATFORM.SEARCH_QUERY_COL).getValue();
    if (triggerValue === Config.TRIGGERS.SIMILAR && !query) {
      throw new Error("類似画像検索には検索クエリが必要です。");
    }

    const companyName = sheet.getName().substring(Config.PLATFORM.SHEET_PREFIX.length);
    const excludeFiles = getExcludeFilesFromCheckboxes(sheet);
    Logger.log(`[performSearch] Excluding ${excludeFiles.length} files from checkboxes for company: ${companyName}`);

    const results = callSearchApi(companyUuid, query, triggerValue, excludeFiles);
    
    writeResultsToSheet(sheet, row, results, triggerValue);
    if (results && results.length > 0) {
      statusCell.setValue("実行完了");
    } else {
      statusCell.setValue("結果なし");
    }
  } catch (error) {
    Logger.log(`[performSearch] ERROR: ${error.toString()}\n${error.stack}`);
    statusCell.setValue(`エラー: ${error.message}`);
  } finally {
    if (validation) {
      statusCell.setDataValidation(validation);
    }
    Logger.log(`[performSearch] Finished search for row ${row}. Validation rule restored.`);
  }
}

/**
 * Cloud Runの検索APIを呼び出す (POSTに変更)
 * @param {string} uuid - 企業のUUID
 * @param {string} query - 検索クエリ
 * @param {string} trigger - 検索のトリガー種別
 * @param {Array<string>} excludeFiles - 除外するファイル名の配列
 * @return {Array<Object>|null} - 検索結果の配列
 */
function callSearchApi(uuid, query, trigger, excludeFiles) {
  const apiUrl = `${Config.API_BASE_URL}/search`;
  const payload = {
    "uuid": uuid,
    "q": query || "",
    "top_k": 5,
    "trigger": trigger,
    "exclude_files": excludeFiles || []
  };
  Logger.log(`[callSearchApi] URL: ${apiUrl}`);
  Logger.log(`[callSearchApi] Payload: ${JSON.stringify(payload)}`);

  const params = {
    method: "post",
    contentType: "application/json",
    headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  const response = UrlFetchApp.fetch(apiUrl, params);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText();

  if (responseCode === 200) {
    return JSON.parse(responseText);
  } else {
    Logger.log(`API Error Response (${responseCode}): ${responseText}`);
    throw new Error(`APIエラーが発生しました (コード: ${responseCode})`);
  }
}

/**
 * 以前の検索結果をクリアする（強化版）
 */
function clearPreviousResults(sheet, row) {
  Logger.log(`[clearPreviousResults] Clearing results for row ${row}`);
  try {
    const MAX_RESULTS = 5;
    const NUM_COLS_PER_RESULT = 3;
    const TOTAL_RESULT_COLS = MAX_RESULTS * NUM_COLS_PER_RESULT;
    const range = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL, 1, TOTAL_RESULT_COLS);
    
    // まず、セル単位でデータ検証をクリア
    for (let col = 0; col < TOTAL_RESULT_COLS; col++) {
      const cell = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL + col);
      try {
        cell.clearDataValidations();
      } catch (e) {
        Logger.log(`[clearPreviousResults] Warning: Could not clear validation for col ${col}: ${e.message}`);
      }
    }
    
    // 次に、範囲全体をクリア
    range.clear();
    // さらに、値を明示的に空文字列で上書き
    const emptyRow = new Array(TOTAL_RESULT_COLS).fill("");
    range.setValues([emptyRow]);
    
    Logger.log(`[clearPreviousResults] Successfully cleared ${TOTAL_RESULT_COLS} columns`);
  } catch (error) {
    Logger.log(`[clearPreviousResults] ERROR: ${error.toString()}`);
    throw error;
  }
}

/**
 * APIから取得した結果をシートに書き込む (v1.7: エラー対策強化版)
 */
function writeResultsToSheet(sheet, row, results, triggerValue) {
  Logger.log(`[writeResultsToSheet v1.7] Starting to write results to row ${row}`);
  try {
    const isRandom = (triggerValue === Config.TRIGGERS.RANDOM);
    const MAX_RESULTS = 5;
    const NUM_COLS_PER_RESULT = 3;
    const TOTAL_RESULT_COLS = MAX_RESULTS * NUM_COLS_PER_RESULT;

    const rowData = [];
    const richTextLinks = [];

    if (!results || !Array.isArray(results)) {
        results = [];
    }

    // データ準備
    for (let i = 0; i < MAX_RESULTS; i++) {
      if (i < results.length) {
        const result = results[i];
        const fileUrl = result.filepath || "";
        const displayText = result.filename || (fileUrl ? fileUrl.split('/').pop() : "") || "不明なファイル";
        
        rowData.push(displayText);
        rowData.push(!isRandom && result.similarity ? result.similarity.toFixed(4) : "");
        rowData.push(""); 

        if (fileUrl) {
          richTextLinks.push({
            col: Config.PLATFORM.SEARCH_RESULT_START_COL + i * NUM_COLS_PER_RESULT,
            text: displayText,
            url: fileUrl
          });
        }
      } else {
        rowData.push("", "", "");
      }
    }

    // セル単位で安全に値を設定
    for (let i = 0; i < TOTAL_RESULT_COLS; i++) {
      const cell = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL + i);
      try {
        // データ検証をクリアしてから値を設定
        cell.clearDataValidations();
        cell.setValue(rowData[i]);
      } catch (e) {
        Logger.log(`[writeResultsToSheet v1.7] Warning: Could not set value for col ${i}: ${e.message}`);
        // フォールバック：空文字列を設定
        try {
          cell.setValue("");
        } catch (e2) {
          Logger.log(`[writeResultsToSheet v1.7] Error: Even empty string failed for col ${i}: ${e2.message}`);
        }
      }
    }
    
    Logger.log("[writeResultsToSheet v1.7] Individual cell write complete.");
    
    // ★★★★★ 重要: ここでflushを呼び出し、シートの更新を確定させる ★★★★★
    SpreadsheetApp.flush();
    Logger.log("[writeResultsToSheet v1.7] Flushed sheet updates.");

    // flush後に書式設定を適用する
    for (const link of richTextLinks) {
      try {
        const cell = sheet.getRange(row, link.col);
        const richText = SpreadsheetApp.newRichTextValue().setText(link.text).setLinkUrl(link.url).build();
        cell.setRichTextValue(richText);
      } catch (e) {
        Logger.log(`[writeResultsToSheet v1.7] Warning: Could not set rich text for ${link.text}: ${e.message}`);
      }
    }
    
    for (let i = 0; i < results.length; i++) {
        if (i < MAX_RESULTS) {
            try {
                const checkCol = Config.PLATFORM.SEARCH_RESULT_START_COL + (i * NUM_COLS_PER_RESULT) + 2;
                const checkCell = sheet.getRange(row, checkCol);
                checkCell.clearDataValidations(); // チェックボックス用セルもクリア
                checkCell.insertCheckboxes();
            } catch (e) {
                Logger.log(`[writeResultsToSheet v1.7] Warning: Could not insert checkbox for result ${i}: ${e.message}`);
            }
        }
    }
    Logger.log(`[writeResultsToSheet v1.7] Applied formats and checkboxes successfully.`);
  } catch (error) {
    Logger.log(`[writeResultsToSheet v1.7] ERROR: ${error.toString()}\n${error.stack}`);
    throw error;
  }
}


// =======================================================================
// 除外リスト管理の関数 (新規追加セクション)
// =======================================================================

// 除外リスト関連の関数は削除（新仕様では不要）

/**
 * 検索結果シートのチェックボックス状態を参照して除外ファイルリストを取得します。
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @returns {string[]} 除外するファイル名の配列
 */
function getExcludeFilesFromCheckboxes(sheet) {
  const excludeFiles = [];
  const lastRow = sheet.getLastRow();
  
  if (lastRow < 2) {
    return excludeFiles;
  }
  
  Logger.log(`[getExcludeFilesFromCheckboxes] Checking checkboxes for rows 2-${lastRow}`);
  
  for (let row = 2; row <= lastRow; row++) {
    for (let i = 0; i < Config.PLATFORM.USE_CHECKBOX_COLUMNS.length; i++) {
      const checkboxCol = Config.PLATFORM.USE_CHECKBOX_COLUMNS[i];
      const fileNameCol = checkboxCol - 2; // ファイル名はチェックボックスの2つ左
      
      try {
        const checkboxCell = sheet.getRange(row, checkboxCol);
        const fileNameCell = sheet.getRange(row, fileNameCol);
        const isChecked = checkboxCell.getValue() === true;
        const fileName = fileNameCell.getRichTextValue().getText();
        
        if (isChecked && fileName) {
          excludeFiles.push(fileName);
          Logger.log(`   Found checked file: ${fileName} at row ${row}, col ${checkboxCol}`);
        }
      } catch (e) {
        // エラーは無視（空セルなど）
      }
    }
  }
  
  Logger.log(`[getExcludeFilesFromCheckboxes] Found ${excludeFiles.length} checked files to exclude`);
  return excludeFiles;
}


// 除外リスト編集関数も削除（新仕様では不要）

// =======================================================================
// ベクトル化・UUID生成関連の関数 (元のコードを維持)
// =======================================================================

function getUuidForSheet(sheet) {
  const sheetName = sheet.getName();
  if (!sheetName.startsWith(Config.PLATFORM.SHEET_PREFIX)) return null;

  const companyName = sheetName.substring(Config.PLATFORM.SHEET_PREFIX.length);
  const companyListSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(Config.COMPANY_LIST.SHEET_NAME);
  if (!companyListSheet) return null;

  const data = companyListSheet.getRange(2, Config.COMPANY_LIST.NAME_COL, companyListSheet.getLastRow(), 1).getValues();
  const uuids = companyListSheet.getRange(2, Config.COMPANY_LIST.UUID_COL, companyListSheet.getLastRow(), 1).getValues();

  for (let i = 0; i < data.length; i++) {
    if (data[i][0] === companyName) {
      return uuids[i][0];
    }
  }
  return null;
}

function callVectorizeApi() {
  const ui = SpreadsheetApp.getUi();
  const activeSheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  if (activeSheet.getName() !== Config.COMPANY_LIST.SHEET_NAME) {
    ui.alert(`'${Config.COMPANY_LIST.SHEET_NAME}'シートから実行してください。`);
    return;
  }
  
  const activeRange = activeSheet.getActiveRange();
  const row = activeRange.getRow();
  if (row < 2) {
    ui.alert("ヘッダー行ではなく、対象の会社の行を選択してください。");
    return;
  }

  try {
    const rowData = activeSheet.getRange(row, 1, 1, 3).getValues()[0];
    const uuid = rowData[Config.COMPANY_LIST.UUID_COL - 1];
    const driveUrl = rowData[Config.COMPANY_LIST.DRIVE_URL_COL - 1];

    if (!uuid || !driveUrl) throw new Error("UUIDまたはGoogleドライブのURLが空です。");

    const payload = JSON.stringify({ "uuid": uuid, "drive_url": driveUrl });
    const params = {
      method: "post",
      contentType: "application/json",
      headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
      payload: payload,
      muteHttpExceptions: true,
    };

    const apiUrl = `${Config.API_BASE_URL}/vectorize`;
    const response = UrlFetchApp.fetch(apiUrl, params);
    const responseCode = response.getResponseCode();

    if (responseCode === 202) {
      ui.alert("ベクトル化ジョブの開始をリクエストしました。処理には時間がかかります。");
    } else {
      Logger.log(`API Error Response (${responseCode}): ${response.getContentText()}`);
      throw new Error(`APIエラーが発生しました (コード: ${responseCode})`);
    }
  } catch (error) {
    ui.alert(`エラー: ${error.message}`);
    Logger.log(`[callVectorizeApi] Error: ${error.toString()}`);
  }
}

/**
 * 優先企業リストのチェックボックスがONの企業を一括でベクトル化する
 */
function vectorizePriorityCompanies() {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(Config.COMPANY_LIST.SHEET_NAME);
  if (!sheet) {
    ui.alert(`'${Config.COMPANY_LIST.SHEET_NAME}'シートが見つかりません。`);
    return;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    ui.alert('ベクトル化対象の企業がありません。');
    return;
  }

  const range = sheet.getRange(2, 1, lastRow - 1, Config.COMPANY_LIST.PRIORITY_COL);
  const values = range.getValues();
  
  const companiesToVectorize = [];
  for (let i = 0; i < values.length; i++) {
    const isPriority = values[i][Config.COMPANY_LIST.PRIORITY_COL - 1];
    if (isPriority === true) {
      const companyName = values[i][Config.COMPANY_LIST.NAME_COL - 1];
      const uuid = values[i][Config.COMPANY_LIST.UUID_COL - 1];
      const driveUrl = values[i][Config.COMPANY_LIST.DRIVE_URL_COL - 1];
      if (companyName && uuid && driveUrl) {
        companiesToVectorize.push({ name: companyName, uuid: uuid, driveUrl: driveUrl });
      }
    }
  }

  if (companiesToVectorize.length === 0) {
    ui.alert('優先企業にチェックが入っている企業がありません。');
    return;
  }

  let successCount = 0;
  let failureCount = 0;
  const errors = [];

  // showModalDialog is deprecated, using a simple toast message instead for broader compatibility.
  SpreadsheetApp.getActiveSpreadsheet().toast(`ベクトル化処理を開始します... (${companiesToVectorize.length}件)`, "処理中", -1);

  // 逐次処理に変更: forループを使用してawaitを含む処理に対応
  for (let i = 0; i < companiesToVectorize.length; i++) {
    const company = companiesToVectorize[i];
    
    // 進捗状況を表示
    SpreadsheetApp.getActiveSpreadsheet().toast(`処理中... (${i + 1}/${companiesToVectorize.length}): ${company.name}`, "処理中", -1);
    
    try {
      const payload = JSON.stringify({ "uuid": company.uuid, "drive_url": company.driveUrl });
      const params = {
        method: "post",
        contentType: "application/json",
        headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
        payload: payload,
        muteHttpExceptions: true,
      };

      const apiUrl = `${Config.API_BASE_URL}/vectorize`;
      const response = UrlFetchApp.fetch(apiUrl, params);
      const responseCode = response.getResponseCode();

      if (responseCode === 202) {
        successCount++;
        Logger.log(`Successfully requested vectorization for ${company.name}`);
      } else {
        failureCount++;
        const errorMessage = `Failed to vectorize ${company.name} (Code: ${responseCode}): ${response.getContentText()}`;
        errors.push(errorMessage);
        Logger.log(errorMessage);
      }
    } catch (error) {
      failureCount++;
      const errorMessage = `Error vectorizing ${company.name}: ${error.message}`;
      errors.push(errorMessage);
      Logger.log(errorMessage);
    }
    
    // 次のリクエストまで3秒待機（Cloud Runへの負荷を軽減）
    // 最後の企業の場合は待機しない
    if (i < companiesToVectorize.length - 1) {
      Utilities.sleep(3000); // 3秒待機
    }
  }

  SpreadsheetApp.getActiveSpreadsheet().toast("一括ベクトル化処理が完了しました。", "完了", 5);
  let resultMessage = `一括ベクトル化処理が完了しました。\n\n成功: ${successCount}件\n失敗: ${failureCount}件`;
  if (failureCount > 0) {
    resultMessage += "\n\nエラー詳細:\n" + errors.join("\n");
  }
  ui.alert(resultMessage);
}


function generateUuids() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(Config.COMPANY_LIST.SHEET_NAME);
  if (!sheet) {
    SpreadsheetApp.getUi().alert(`'${Config.COMPANY_LIST.SHEET_NAME}'シートが見つかりません。`);
    return;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const range = sheet.getRange(2, Config.COMPANY_LIST.UUID_COL, lastRow - 1, Config.COMPANY_LIST.NAME_COL);
  const values = range.getValues();
  let updated = false;

  for (let i = 0; i < values.length; i++) {
    const uuid = values[i][0];
    const companyName = values[i][Config.COMPANY_LIST.NAME_COL - 1];
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