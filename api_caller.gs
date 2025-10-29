/**
 * @fileoverview Image Search System for Company-specific Google Sheets
 * * This Google Apps Script provides functionality for:
 * 1. Custom menu creation for vectorization operations
 * 2. Automatic UUID generation when companies are added
 * 3. Image search triggered by cell edits in company sheets
 * 4. Management of an exclusion list for recently used images
 * * @version 1.8.0
 * @author Claude Code Assistant (with modifications)
 */

/**
 * Configuration object for the application
 */
const Config = {
  // API Configuration - Update this URL to match your Cloud Run service
  // API_BASE_URL: "https://cohere-rag-742231208085.asia-northeast1.run.app",
  API_BASE_URL: "https://cohere-rag-dev-742231208085.asia-northeast1.run.app",

  // Company List Sheet Configuration
  COMPANY_LIST: {
    SHEET_NAME: "会社一覧",
    UUID_COL: 1,        // A列: UUID
    NAME_COL: 2,        // B列: 会社名
    DRIVE_URL_COL: 3,    // C列: GoogleドライブURL
    PRIORITY_COL: 6     // F列: 優先企業リスト (チェックボックス)
  },

  // Platform Sheet Configuration
  PLATFORM: {
    SHEET_PREFIX: "platform-",
    SEARCH_QUERY_COL: 1,        // A列: 検索クエリ
    SEARCH_COUNT_COL: 2,        // B列: 検索表示数 ★新規追加★
    SEARCH_DATE_COL: 3,         // C列: 検索実行日時（B→Cへ）
    SEARCH_TRIGGER_COL: 4,      // D列: 実行状況（C→Dへ）
    SEARCH_RESULT_START_COL: 5, // E列: 結果出力の開始列（D→Eへ）
    // E, H, K, N, Q -> Filename
    // G, J, M, P, S -> Checkbox for exclusion
    USE_CHECKBOX_COLUMNS: [7, 10, 13, 16, 19]  // 各+1
  },

  // Trigger Text Constants
  TRIGGERS: {
    STANDARD: "スタンダード",
    SHUFFLE: "シャッフル",
    RANDOM: "ランダム",
    LEGACY_SIMILAR: "類似画像検索",
    LEGACY_RANDOM: "ランダム画像検索",
    NOT_EXECUTED: "未実行"
  },

  TRIGGER_OPTIONS: ["未実行", "スタンダード", "シャッフル", "ランダム", "実行完了"],

  // Exclusion List Configuration (削除予定)
};

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
 * D列のトリガー値を新仕様に沿って正規化します。
 * 互換のため旧称は新名称へマッピングします。
 * @param {string} triggerValue
 * @return {string|null} スタンダード/シャッフル/ランダムのいずれか。対象外は null。
 */
function normalizeTriggerValue(triggerValue) {
  switch (triggerValue) {
    case Config.TRIGGERS.STANDARD:
    case Config.TRIGGERS.SHUFFLE:
    case Config.TRIGGERS.RANDOM:
      return triggerValue;
    case Config.TRIGGERS.LEGACY_SIMILAR:
      return Config.TRIGGERS.SHUFFLE;
    case Config.TRIGGERS.LEGACY_RANDOM:
      return Config.TRIGGERS.RANDOM;
    default:
      return null;
  }
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
  applyTriggerValidationToPlatformSheets_();
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

    Logger.log(`handleSheetEdit triggered:`);
    Logger.log(`- Sheet: ${sheetName}, Cell: ${range.getA1Notation()}, New Value: ${value}`);

    // --- 検索シートでの検索トリガー ---
    if (sheetName.startsWith(Config.PLATFORM.SHEET_PREFIX) && col === Config.PLATFORM.SEARCH_TRIGGER_COL && row > 1) {
      Logger.log("-> Condition Met: This is a search trigger edit.");
      const normalizedTrigger = normalizeTriggerValue(value);
      if (normalizedTrigger) {
        performSearch(sheet, row, normalizedTrigger);
      } else if (value === Config.TRIGGERS.NOT_EXECUTED) {
        clearPreviousResults(sheet, row);
        // C列の日時もクリア（B→C）
        const dateCell = sheet.getRange(row, Config.PLATFORM.SEARCH_DATE_COL);
        dateCell.setValue("");
        applyTriggerValidationToSheet_(sheet);
      } else if (value && value !== "") {
        Logger.log(`-> Warning: Unsupported trigger value "${value}"`);
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
 * @param {string} triggerValue - D列に入力されたトリガーテキスト
 */
function performSearch(sheet, row, triggerValue) {
  Logger.log(`[performSearch] Starting search for row ${row}, trigger: ${triggerValue}`);
  const statusCell = sheet.getRange(row, Config.PLATFORM.SEARCH_TRIGGER_COL);
  const hadValidation = !!statusCell.getDataValidation();

  try {
    if (hadValidation) {
      statusCell.clearDataValidations();
    }
    
    clearPreviousResults(sheet, row);
    statusCell.setValue("検索中...");
    
    // 検索実行日時をC列に記録（B→C）
    const dateCell = sheet.getRange(row, Config.PLATFORM.SEARCH_DATE_COL);
    const now = new Date();
    const dateString = `${now.getFullYear()}/${String(now.getMonth() + 1).padStart(2, '0')}/${String(now.getDate()).padStart(2, '0')}`;
    dateCell.setValue(dateString);
    
    SpreadsheetApp.flush();

    const companyUuid = getUuidForSheet(sheet);
    if (!companyUuid) throw new Error("「会社一覧」シートで対応する企業が見つかりません。");
    
    const query = sheet.getRange(row, Config.PLATFORM.SEARCH_QUERY_COL).getValue();
    const needsQuery = (triggerValue === Config.TRIGGERS.STANDARD || triggerValue === Config.TRIGGERS.SHUFFLE);
    if (needsQuery && !query) {
      const modeLabel = (triggerValue === Config.TRIGGERS.STANDARD) ? "スタンダード検索" : "シャッフル検索";
      throw new Error(`${modeLabel}には検索クエリが必要です。`);
    }

    // ★検索表示数をB列から取得★
    let topK = sheet.getRange(row, Config.PLATFORM.SEARCH_COUNT_COL).getValue();
    if (typeof topK !== 'number' || topK < 1 || topK > 50) {
      topK = 5; // デフォルト値
      Logger.log(`[performSearch] Invalid top_k value, using default: 5`);
    } else {
      Logger.log(`[performSearch] Using top_k from sheet: ${topK}`);
    }

    Logger.log(`[performSearch] Getting exclusion list from checkboxes...`);
    
    let excludeFiles = [];
    try {
      excludeFiles = getExcludeFilesFromCheckboxes(sheet);
      Logger.log(`[performSearch] Successfully got ${excludeFiles.length} files to exclude`);
    } catch (excludeError) {
      Logger.log(`[performSearch] ERROR in getExcludeFilesFromCheckboxes: ${excludeError.toString()}`);
      excludeFiles = [];
    }

    Logger.log(`[performSearch] Calling API with excludeFiles: ${JSON.stringify(excludeFiles)}`);
    const results = callSearchApi(companyUuid, query, triggerValue, excludeFiles, topK);
    
    writeResultsToSheet(sheet, row, results, triggerValue, topK);
    if (results && results.length > 0) {
      statusCell.setValue("実行完了");
    } else {
      statusCell.setValue("結果なし");
    }
  } catch (error) {
    Logger.log(`[performSearch] ERROR: ${error.toString()}\n${error.stack}`);
    statusCell.setValue(`エラー: ${error.message}`);
  } finally {
    statusCell.setDataValidation(buildTriggerValidation_());
    Logger.log(`[performSearch] Finished search for row ${row}. Validation rule restored.`);
  }
}

/**
 * Cloud Runの検索APIを呼び出す (POSTに変更)
 * @param {string} uuid - 企業のUUID
 * @param {string} query - 検索クエリ
 * @param {string} trigger - 検索のトリガー種別
 * @param {Array<string>} excludeFiles - 除外するファイル名の配列
 * @param {number} topK - 検索結果の取得件数
 * @return {Array<Object>|null} - 検索結果の配列
 */
function callSearchApi(uuid, query, trigger, excludeFiles, topK) {
  const apiUrl = `${Config.API_BASE_URL}/search`;
  
  // 検索シート名でembed-v4.0使用の判定
  const activeSheet = SpreadsheetApp.getActiveSheet();
  const sheetName = activeSheet.getName();
  const useEmbedV4 = sheetName.includes("embed-v4.0");
  
  const payload = {
    "uuid": uuid,
    "q": query || "",
    "top_k": topK,  // ★動的に渡す★
    "trigger": trigger,
    "exclude_files": excludeFiles || [],
    "use_embed_v4": useEmbedV4
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
/**
 * 以前の検索結果をクリアする（エラー修正版）
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @param {number} row - クリア対象の行
 */
function clearPreviousResults(sheet, row) {
  Logger.log(`[clearPreviousResults] Clearing results for row ${row}`);
  try {
    const startCol = Config.PLATFORM.SEARCH_RESULT_START_COL;
    const lastCol = sheet.getLastColumn();

    // クリアする列が存在する場合のみ実行
    if (lastCol >= startCol) {
      const numColsToClear = lastCol - startCol + 1;
      const range = sheet.getRange(row, startCol, 1, numColsToClear);
      
      // データ検証をクリアしてから内容をクリア
      range.clear({ validationsOnly: true });
      range.clearContent();
    }
    
    // C列の検索日時もクリア
    const dateCell = sheet.getRange(row, Config.PLATFORM.SEARCH_DATE_COL);
    dateCell.setValue("");
    
    Logger.log(`[clearPreviousResults] Successfully cleared from column ${startCol} to ${lastCol}.`);
  } catch (error) {
    Logger.log(`[clearPreviousResults] ERROR: ${error.toString()}`);
    throw error;
  }
}

/**
 * APIから取得した結果をシートに書き込む (v1.8: topK対応版)
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @param {number} row - 書き込み対象の行
 * @param {Array<Object>} results - APIから返された検索結果
 * @param {string} triggerValue - 検索トリガーの種別
 * @param {number} topK - 表示する結果の最大数
 */
function writeResultsToSheet(sheet, row, results, triggerValue, topK) {
  Logger.log(`[writeResultsToSheet v1.8] Starting to write results to row ${row}`);
  try {
    const isRandom = (triggerValue === Config.TRIGGERS.RANDOM);
    const MAX_RESULTS = topK || 5;  // ★動的に変更★
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

    if (rowData.length > 0) {
        const resultRange = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL, 1, rowData.length);
        resultRange.setValues([rowData]);
    }
    
    Logger.log("[writeResultsToSheet v1.8] Cell values write complete.");
    
    SpreadsheetApp.flush();
    Logger.log("[writeResultsToSheet v1.8] Flushed sheet updates.");

    // flush後に書式設定を適用する
    for (const link of richTextLinks) {
      try {
        const cell = sheet.getRange(row, link.col);
        const richText = SpreadsheetApp.newRichTextValue().setText(link.text).setLinkUrl(link.url).build();
        cell.setRichTextValue(richText);
      } catch (e) {
        Logger.log(`[writeResultsToSheet v1.8] Warning: Could not set rich text for ${link.text}: ${e.message}`);
      }
    }
    
    for (let i = 0; i < results.length; i++) {
        if (i < MAX_RESULTS) {
            try {
                const checkCol = Config.PLATFORM.SEARCH_RESULT_START_COL + (i * NUM_COLS_PER_RESULT) + 2;
                const checkCell = sheet.getRange(row, checkCol);
                checkCell.insertCheckboxes();
            } catch (e) {
                Logger.log(`[writeResultsToSheet v1.8] Warning: Could not insert checkbox for result ${i}: ${e.message}`);
            }
        }
    }
    Logger.log(`[writeResultsToSheet v1.8] Applied formats and checkboxes successfully.`);
  } catch (error) {
    Logger.log(`[writeResultsToSheet v1.8] ERROR: ${error.toString()}\n${error.stack}`);
    throw error;
  }
}


// =======================================================================
// 除外リスト管理の関数 (新規追加セクション)
// =======================================================================

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
        
        let fileName = "";
        try {
          const richText = fileNameCell.getRichTextValue();
          if (richText) {
            fileName = richText.getText();
          }
        } catch (richTextError) {
          fileName = fileNameCell.getValue() || "";
        }
        
        if (isChecked && fileName && fileName.trim() !== "") {
          excludeFiles.push(fileName.trim());
          Logger.log(`   Found checked file: ${fileName} at row ${row}, col ${checkboxCol}`);
        }
      } catch (e) {
        Logger.log(`   Error processing row ${row}, col ${checkboxCol}: ${e.message}`);
      }
    }
  }
  
  Logger.log(`[getExcludeFilesFromCheckboxes] Found ${excludeFiles.length} checked files to exclude`);
  return excludeFiles;
}


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
    const companyName = rowData[Config.COMPANY_LIST.NAME_COL - 1];
    const useEmbedV4 = companyName && companyName.includes("embed-v4.0");
    const payload = JSON.stringify({ 
      "uuid": uuid, 
      "drive_url": driveUrl,
      "use_embed_v4": useEmbedV4
    });
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
  SpreadsheetApp.getActiveSpreadsheet().toast(`ベクトル化処理を開始します... (${companiesToVectorize.length}件)`, "処理中", -1);

  for (let i = 0; i < companiesToVectorize.length; i++) {
    const company = companiesToVectorize[i];
    SpreadsheetApp.getActiveSpreadsheet().toast(`処理中... (${i + 1}/${companiesToVectorize.length}): ${company.name}`, "処理中", -1);
    try {
      const useEmbedV4 = company.name && company.name.includes("embed-v4.0");
      const payload = JSON.stringify({ 
        "uuid": company.uuid, 
        "drive_url": company.driveUrl,
        "use_embed_v4": useEmbedV4
      });
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
