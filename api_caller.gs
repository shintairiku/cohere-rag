/**
 * @fileoverview Image Search System for Company-specific Google Sheets
 * * This Google Apps Script provides functionality for:
 * 1. Custom menu creation for vectorization operations
 * 2. Automatic UUID generation when companies are added
 * 3. Image search triggered by cell edits in company sheets
 * 4. Management of an exclusion list for recently used images
 * * @version 1.1.0
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
    DRIVE_URL_COL: 3    // C列: GoogleドライブURL
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

  // Exclusion List Configuration (New)
  EXCLUSION: {
    SHEET_NAME: "除外リスト",
    COMPANY_NAME_COL: 1,     // A列: 企業名
    EXCLUSIONS_COL: 2,        // B列: 除外ファイルリスト（JSON形式）
    LAST_UPDATED_COL: 3,      // C列: 最終更新日
    EDIT_BUTTON_COL: 4,       // D列: 編集ボタン（チェックボックス）
    PERIOD_MONTHS: 2
  }
};


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

    // --- 除外リストへの追加処理 (チェックボックス操作) ---
    else if (isChecked) {
      // シナリオ1: 検索結果シートから除外リストへ追加
      if (sheetName.startsWith(Config.PLATFORM.SHEET_PREFIX) && Config.PLATFORM.USE_CHECKBOX_COLUMNS.includes(col)) {
        Logger.log("-> Condition Met: Add to exclusion list trigger.");
        const fileNameCell = sheet.getRange(row, col - 2); // ファイル名はチェックボックスの2つ左のセル (D, G, J...)
        const fileName = fileNameCell.getRichTextValue().getText(); // リンク付きでもテキストを取得
        
        if (fileName) {
          const companyName = sheetName.substring(Config.PLATFORM.SHEET_PREFIX.length);
          addFileToExclusionList(companyName, fileName);
          range.setValue(false); // 処理後にチェックを外す
          SpreadsheetApp.getActiveSpreadsheet().toast(`「${fileName}」を${companyName}の除外リストに追加しました。`);
        }
      }
      // シナリオ2: 除外リストシートの編集ボタン
      else if (sheetName === Config.EXCLUSION.SHEET_NAME && col === Config.EXCLUSION.EDIT_BUTTON_COL && row > 1) {
        Logger.log("-> Condition Met: Edit exclusion list trigger.");
        range.setValue(false); // チェックを外す
        const companyName = sheet.getRange(row, Config.EXCLUSION.COMPANY_NAME_COL).getValue();
        openExclusionListEditor(companyName, row);
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
 * 画像検索を実行するメイン関数
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象のシート
 * @param {number} row - 編集された行
 * @param {string} triggerValue - C列に入力されたトリガーテキスト
 */
function performSearch(sheet, row, triggerValue) {
  Logger.log(`[performSearch] Starting search for row ${row}, trigger: ${triggerValue}`);
  const statusCell = sheet.getRange(row, Config.PLATFORM.SEARCH_TRIGGER_COL);
  
  function safeSetCellValue(cell, value) {
    try {
        const validation = cell.getDataValidation();
        if (validation) {
            cell.clearDataValidations();
            cell.setValue(value);
            cell.setDataValidation(validation);
        } else {
            cell.setValue(value);
        }
    } catch (e) {
        Logger.log(`[safeSetCellValue] Warning: Could not set cell value. ${e.message}`);
        try {
            cell.setValue(value);
        } catch (e2) {
            Logger.log(`[safeSetCellValue] Error: Fallback failed. ${e2.message}`);
        }
    }
  }
  
  try {
    clearPreviousResults(sheet, row);
    safeSetCellValue(statusCell, "検索中...");
    SpreadsheetApp.flush();

    const companyUuid = getUuidForSheet(sheet);
    if (!companyUuid) throw new Error("「会社一覧」シートで対応する企業が見つかりません。");
    
    const query = sheet.getRange(row, Config.PLATFORM.SEARCH_QUERY_COL).getValue();
    if (triggerValue === Config.TRIGGERS.SIMILAR && !query) {
      throw new Error("類似画像検索には検索クエリが必要です。");
    }

    // ★変更点: 企業名を取得して除外リストを取得
    const companyName = sheet.getName().substring(Config.PLATFORM.SHEET_PREFIX.length);
    const excludeFiles = getActiveExclusionList(companyName);
    Logger.log(`[performSearch] Excluding ${excludeFiles.length} files from search for company: ${companyName}`);

    // ★変更点: API呼び出しに除外リストを渡す
    const results = callSearchApi(companyUuid, query, triggerValue, excludeFiles);
    
    if (results && results.length > 0) {
      writeResultsToSheet(sheet, row, results, triggerValue);
      safeSetCellValue(statusCell, "実行完了");
    } else {
      safeSetCellValue(statusCell, "結果なし");
    }
  } catch (error) {
    Logger.log(`[performSearch] ERROR: ${error.toString()}\n${error.stack}`);
    safeSetCellValue(statusCell, `エラー: ${error.message}`);
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
  const apiUrl = `${Config.API_BASE_URL}/search/`;
  
  const payload = {
    "uuid": uuid,
    "q": query || "",
    "top_k": 5,
    "trigger": trigger,
    "exclude_files": excludeFiles || []
  };

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
    // APIが配列を直接返すことを想定 (main.pyの仕様に合わせる)
    return JSON.parse(responseText); 
  } else {
    Logger.log(`API Error Response (${responseCode}): ${responseText}`);
    throw new Error(`APIエラーが発生しました (コード: ${responseCode})`);
  }
}

/**
 * 以前の検索結果をクリアする
 */
function clearPreviousResults(sheet, row) {
  Logger.log(`[clearPreviousResults] Clearing results for row ${row}`);
  try {
    const range = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL, 1, 15);
    range.clear({formatOnly: false, contentsOnly: false});
    Logger.log(`[clearPreviousResults] Successfully cleared range`);
  } catch (error) {
    Logger.log(`[clearPreviousResults] ERROR: ${error.toString()}`);
    throw error;
  }
}

/**
 * APIから取得した結果をシートに書き込む
 */
function writeResultsToSheet(sheet, row, results, triggerValue) {
  Logger.log(`[writeResultsToSheet] Starting to write ${results.length} results to row ${row}`);
  
  try {
    const isRandom = (triggerValue === Config.TRIGGERS.RANDOM);
    const MAX_RESULTS = 5;
    const rowData = [];
    const linkInfo = [];

    for (let i = 0; i < MAX_RESULTS; i++) {
      if (i < results.length) {
        const result = results[i];
        const fileUrl = result.filepath || "";
        const displayText = result.filename || fileUrl.split('/').pop() || "不明なファイル";
        
        rowData.push(displayText, !isRandom && result.similarity ? result.similarity.toFixed(4) : "", false);
        linkInfo.push({ url: fileUrl, text: displayText, col: Config.PLATFORM.SEARCH_RESULT_START_COL + i * 3 });
      } else {
        rowData.push("", "", "");
      }
    }

    const targetRange = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL, 1, MAX_RESULTS * 3);
    targetRange.setValues([rowData]);

    for (let i = 0; i < MAX_RESULTS; i++) {
      if (i < results.length) {
        const link = linkInfo[i];
        if (link.url) {
          const fileNameCell = sheet.getRange(row, link.col);
          const richText = SpreadsheetApp.newRichTextValue().setText(link.text).setLinkUrl(link.url).build();
          fileNameCell.setRichTextValue(richText);
        }
        const checkboxCell = sheet.getRange(row, Config.PLATFORM.SEARCH_RESULT_START_COL + (i * 3) + 2);
        checkboxCell.insertCheckboxes();
      }
    }
    Logger.log(`[writeResultsToSheet] Successfully completed writing results`);
  } catch (error) {
    Logger.log(`[writeResultsToSheet] ERROR: ${error.toString()}`);
    throw error;
  }
}

// =======================================================================
// 除外リスト管理の関数 (新規追加セクション)
// =======================================================================

/**
 * 指定された企業の除外リストにファイルを追加します。
 * @param {string} companyName - 企業名
 * @param {string} fileName - 追加するファイル名
 */
function addFileToExclusionList(companyName, fileName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(Config.EXCLUSION.SHEET_NAME);

  // 除外リストシートが存在しない場合は作成
  if (!sheet) {
    sheet = ss.insertSheet(Config.EXCLUSION.SHEET_NAME, 0);
    const headers = [["企業名", "除外ファイルリスト", "最終更新日", "編集"]];
    sheet.getRange("A1:D1").setValues(headers).setFontWeight("bold");
    sheet.setColumnWidth(Config.EXCLUSION.COMPANY_NAME_COL, 200);
    sheet.setColumnWidth(Config.EXCLUSION.EXCLUSIONS_COL, 600);
    sheet.setColumnWidth(Config.EXCLUSION.LAST_UPDATED_COL, 150);
    sheet.setColumnWidth(Config.EXCLUSION.EDIT_BUTTON_COL, 80);
    
    // ヘッダー行を固定
    sheet.setFrozenRows(1);
  }

  // 企業の行を検索
  const lastRow = sheet.getLastRow();
  let companyRow = -1;
  
  if (lastRow > 1) {
    const companyNames = sheet.getRange(2, Config.EXCLUSION.COMPANY_NAME_COL, lastRow - 1, 1).getValues().flat();
    companyRow = companyNames.indexOf(companyName) + 2; // +2 because array is 0-indexed and starts from row 2
  }

  const now = new Date();
  
  if (companyRow > 1) {
    // 既存の企業行を更新
    const exclusionsCell = sheet.getRange(companyRow, Config.EXCLUSION.EXCLUSIONS_COL);
    const currentExclusionsJson = exclusionsCell.getValue();
    
    let exclusions = [];
    if (currentExclusionsJson) {
      try {
        const parsedData = JSON.parse(currentExclusionsJson);
        // 配列形式と新しいオブジェクト形式の両方に対応
        if (Array.isArray(parsedData)) {
          exclusions = parsedData.map(f => ({filename: f, date: now.toISOString()}));
        } else if (parsedData.files) {
          exclusions = parsedData.files;
        }
      } catch (e) {
        Logger.log(`Error parsing exclusions for ${companyName}: ${e.message}`);
      }
    }
    
    // 既に存在するかチェック
    if (exclusions.some(f => f.filename === fileName)) {
      SpreadsheetApp.getActiveSpreadsheet().toast(`「${fileName}」は既に${companyName}の除外リストに存在します。`);
      return;
    }
    
    // 新しいファイルを追加
    exclusions.push({
      filename: fileName,
      date: now.toISOString()
    });
    
    // JSON形式で保存
    exclusionsCell.setValue(JSON.stringify({files: exclusions}));
    sheet.getRange(companyRow, Config.EXCLUSION.LAST_UPDATED_COL).setValue(now);
    
  } else {
    // 新しい企業行を追加
    const newExclusions = {
      files: [{
        filename: fileName,
        date: now.toISOString()
      }]
    };
    
    const newRow = [companyName, JSON.stringify(newExclusions), now, false];
    sheet.appendRow(newRow);
    const newRowIndex = sheet.getLastRow();
    sheet.getRange(newRowIndex, Config.EXCLUSION.EDIT_BUTTON_COL).insertCheckboxes();
  }
  
  Logger.log(`Added ${fileName} to exclusion list for company: ${companyName}`);
}

/**
 * 指定された企業の現在有効な（指定期間内の）除外ファイルリストを取得します。
 * @param {string} companyName - 企業名
 * @returns {string[]} 除外するファイル名の配列
 */
function getActiveExclusionList(companyName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(Config.EXCLUSION.SHEET_NAME);
  
  if (!sheet || sheet.getLastRow() < 2) {
    return [];
  }

  // 企業の行を検索
  const lastRow = sheet.getLastRow();
  const companyNames = sheet.getRange(2, Config.EXCLUSION.COMPANY_NAME_COL, lastRow - 1, 1).getValues().flat();
  const companyRow = companyNames.indexOf(companyName) + 2;
  
  if (companyRow < 2) {
    Logger.log(`No exclusion list found for company: ${companyName}`);
    return [];
  }

  // 除外リストのJSONを取得
  const exclusionsJson = sheet.getRange(companyRow, Config.EXCLUSION.EXCLUSIONS_COL).getValue();
  
  if (!exclusionsJson) {
    return [];
  }

  try {
    const parsedData = JSON.parse(exclusionsJson);
    const thresholdDate = new Date();
    thresholdDate.setMonth(thresholdDate.getMonth() - Config.EXCLUSION.PERIOD_MONTHS);

    let activeExclusions = [];
    
    // 新しいオブジェクト形式の処理
    if (parsedData.files && Array.isArray(parsedData.files)) {
      activeExclusions = parsedData.files
        .filter(f => {
          if (!f.date) return true; // 日付がない場合は有効とみなす
          return new Date(f.date) >= thresholdDate;
        })
        .map(f => f.filename);
    }
    // 後方互換性のため配列形式もサポート
    else if (Array.isArray(parsedData)) {
      activeExclusions = parsedData; // 古い形式は全て有効とみなす
    }

    Logger.log(`Found ${activeExclusions.length} active exclusions for company: ${companyName}`);
    return activeExclusions;
    
  } catch (e) {
    Logger.log(`Error parsing exclusion list for ${companyName}: ${e.message}`);
    return [];
  }
}


/**
 * 除外リスト編集用のダイアログを開きます
 * @param {string} companyName - 企業名
 * @param {number} row - 編集する行番号
 */
function openExclusionListEditor(companyName, row) {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(Config.EXCLUSION.SHEET_NAME);
  
  if (!sheet) {
    ui.alert('エラー', '除外リストシートが見つかりません。', ui.ButtonSet.OK);
    return;
  }
  
  const exclusionsJson = sheet.getRange(row, Config.EXCLUSION.EXCLUSIONS_COL).getValue();
  let exclusions = [];
  
  try {
    const parsedData = JSON.parse(exclusionsJson);
    if (parsedData.files && Array.isArray(parsedData.files)) {
      exclusions = parsedData.files;
    } else if (Array.isArray(parsedData)) {
      exclusions = parsedData.map(f => ({filename: f, date: new Date().toISOString()}));
    }
  } catch (e) {
    Logger.log(`Error parsing exclusions: ${e.message}`);
  }
  
  // 除外リストを表示用に整形
  let message = `企業: ${companyName}\n\n現在の除外ファイル:\n`;
  
  if (exclusions.length === 0) {
    message += '（なし）';
  } else {
    exclusions.forEach((file, index) => {
      const date = file.date ? new Date(file.date).toLocaleDateString('ja-JP') : '日付不明';
      message += `${index + 1}. ${file.filename} (追加日: ${date})\n`;
    });
  }
  
  message += '\n削除したいファイル番号をカンマ区切りで入力してください。\n例: 1,3,5\n全て削除する場合は「all」と入力してください。';
  
  const response = ui.prompt('除外リスト編集', message, ui.ButtonSet.OK_CANCEL);
  
  if (response.getSelectedButton() === ui.Button.OK) {
    const input = response.getResponseText().trim();
    
    if (input === '') {
      return;
    }
    
    if (input.toLowerCase() === 'all') {
      // 全て削除
      exclusions = [];
      ui.alert('完了', `${companyName}の除外リストを全てクリアしました。`, ui.ButtonSet.OK);
    } else {
      // 指定された番号のファイルを削除
      const indices = input.split(',').map(s => parseInt(s.trim()) - 1).filter(i => !isNaN(i) && i >= 0 && i < exclusions.length);
      
      if (indices.length > 0) {
        // インデックスを降順でソートして後ろから削除
        indices.sort((a, b) => b - a);
        const removedFiles = [];
        
        indices.forEach(i => {
          removedFiles.push(exclusions[i].filename);
          exclusions.splice(i, 1);
        });
        
        ui.alert('完了', `以下のファイルを除外リストから削除しました:\n${removedFiles.join('\n')}`, ui.ButtonSet.OK);
      }
    }
    
    // 更新されたリストを保存
    const updatedJson = JSON.stringify({files: exclusions});
    sheet.getRange(row, Config.EXCLUSION.EXCLUSIONS_COL).setValue(updatedJson);
    sheet.getRange(row, Config.EXCLUSION.LAST_UPDATED_COL).setValue(new Date());
  }
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

  const data = companyListSheet.getRange(2, Config.COMPANY_LIST.NAME_COL, companyListSheet.getLastRow() - 1, 1).getValues();
  const uuids = companyListSheet.getRange(2, Config.COMPANY_LIST.UUID_COL, companyListSheet.getLastRow() - 1, 1).getValues();

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

    const apiUrl = `${Config.API_BASE_URL}/vectorize/`;
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

