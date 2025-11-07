/**
 * @fileoverview 画像検索処理および結果描画ロジック。
 */

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
  const searchModel = determineSearchModelForSheet_(sheetName);
  
  const payload = {
    "uuid": uuid,
    "q": query || "",
    "top_k": topK,  // ★動的に渡す★
    "trigger": trigger,
    "exclude_files": excludeFiles || [],
    "use_embed_v4": useEmbedV4,
    "search_model": searchModel
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
 * シート名から検索時に利用するモデルを判定します。
 * @param {string} sheetName - 判定対象のシート名
 * @return {string} - APIに渡すモデル識別子
 */
function determineSearchModelForSheet_(sheetName) {
  if (!sheetName) {
    return "cohere-multilingual-v3.0";
  }
  const normalized = sheetName.toLowerCase();
  if (normalized.includes("vertex")) {
    return "vertex-ai";
  }
  if (normalized.includes("embed-v4.0")) {
    return "cohere-embed-v4.0";
  }
  return "cohere-multilingual-v3.0";
}

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

/**
 * プラットフォームシート名から対応するUUIDを取得します。
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @return {string|null}
 */
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
