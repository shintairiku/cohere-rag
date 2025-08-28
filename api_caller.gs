//Triggers.gs

/**
 * @fileoverview Cloud Runでデプロイした画像検索APIをGoogleスプレッドシートから呼び出すスクリプト
 */

// --- 設定 ---
// const CLOUD_RUN_URL = "https://image-search-service-742231208085.asia-northeast1.run.app";
const CLOUD_RUN_URL = "https://cohere-rag-742231208085.asia-northeast1.run.app";
const COLUMN_QUERY = 1;          // A列: 検索クエリ
const COLUMN_TRIGGER = 3;        // C列: 実行トリガー（プルダウン）
const COLUMN_RESULT_START = 4;   // D列: 最初の結果出力先
const TRIGGER_TEXT_SIMILAR = "類似画像検索";
const TRIGGER_TEXT_RANDOM = "ランダム画像検索";
const TRIGGER_TEXT_NOT_EXECUTED = "未実行"; // ★「未実行」の選択肢を追加
// --- 設定ここまで ---


/**
 * スプレッドシートが編集されたときにトリガーによって呼び出されるメイン関数 (★★ この関数を修正しました ★★)
 * @param {Object} e - イベントオブジェクト
 */
function handleSheetEdit(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;

  // C列以外が編集された場合は何もしない
  if (range.getColumn() !== COLUMN_TRIGGER) {
    return;
  }
  
  const editedValue = e.value;
  const editedRow = range.getRow();

  if (editedValue === TRIGGER_TEXT_NOT_EXECUTED) {
    // 以前の結果だけをクリアして、処理を終了する
    clearPreviousResults(sheet, editedRow);
    return; // API呼び出しは行わない
  }
  
  // 「未実行」以外が選択された場合は、APIを呼び出す
  performSearch(sheet, editedRow, editedValue);
}

/**
 * 以前の検索結果をクリアする共通関数
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 */
function clearPreviousResults(sheet, row) {
  const resultRange = sheet.getRange(row, COLUMN_RESULT_START, 1, 15);
  resultRange.clearContent(); 
}

/**
 * 検索を実行する (以前のperformSimilarSearchから名前を変更)
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 * @param {string} value - トリガーの値
 */
function performSearch(sheet, row, value) {
  const statusCell = sheet.getRange(row, COLUMN_TRIGGER);
  const query = sheet.getRange(row, COLUMN_QUERY).getValue();
  
  if (value === TRIGGER_TEXT_SIMILAR && !query) {
    statusCell.setValue("エラー: クエリが空です");
    return;
  }
  
  clearPreviousResults(sheet, row);
  statusCell.setValue("検索中...");
  SpreadsheetApp.flush();

  try {
    const apiUrl = `${CLOUD_RUN_URL}/search?q=${encodeURIComponent(query)}&top_k=5&trigger=${encodeURIComponent(value)}`;
    const response = UrlFetchApp.fetch(apiUrl, {'muteHttpExceptions': true});
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    if (responseCode === 200) {
      const jsonResponse = JSON.parse(responseText);
      const results = jsonResponse.results;
      
      if (results && results.length > 0) {
        statusCell.setValue("実行完了");
        const isRandom = (value === TRIGGER_TEXT_RANDOM);
        writeResultsToSheet(sheet, row, results, isRandom);
      } else {
        statusCell.setValue("結果なし");
      }
    } else {
      statusCell.setValue(`APIエラー(${responseCode})`);
      Logger.log(`API Error Response: ${responseText}`);
    }
  } catch (error) {
    statusCell.setValue("スクリプトエラー");
    Logger.log(`[performSearch] Error: ${error.toString()}`);
  }
}

/**
 * APIから取得した結果をシートに書き込む共通関数
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - 対象シート
 * @param {number} row - 対象行
 * @param {Array<Object>} results - APIからの結果配列
 * @param {boolean} isRandom - ランダム検索かどうか
 */
function writeResultsToSheet(sheet, row, results, isRandom) {
  results.forEach((result, i) => {
    const imagePathCell = sheet.getRange(row, COLUMN_RESULT_START + (i * 3));
    
    const fileUrl = result.filepath; 
    const displayText = result.filename || fileUrl;
    
    const richText = SpreadsheetApp.newRichTextValue()
      .setText(displayText)
      .setLinkUrl(fileUrl)
      .build();
      
    imagePathCell.setRichTextValue(richText);

    if (!isRandom) {
      const similarityCell = sheet.getRange(row, COLUMN_RESULT_START + 1 + (i * 3));
      similarityCell.setValue(result.similarity.toFixed(4));
    }
  });
}

/**
 * @OnlyCurrentDoc
 *
 * スプレッドシートが編集されたときに自動的に実行されるトリガー。
 * B列（会社名）に値が入力され、かつA列（uuid）が空の場合にUUIDを自動生成します。
 */
function onEdit(e) {
  const sheetName = "会社一覧"; // 対象のシート名
  const companyColumn = 2;   // 監視する列（B列: 会社名）
  const uuidColumn = 1;      // UUIDを書き込む列（A列）

  const range = e.range;
  const sheet = range.getSheet();

  // 編集されたシートが対象か、編集が単一セルか、対象列かを確認
  if (sheet.getName() === sheetName && range.getNumColumns() === 1 && range.getColumn() === companyColumn) {
    const editedRow = range.getRow();
    // ヘッダー行（1行目）は無視
    if (editedRow > 1) {
      const companyName = range.getValue();
      const uuidCell = sheet.getRange(editedRow, uuidColumn);

      // 会社名が入力されていて、かつUUIDセルがまだ空の場合のみUUIDを生成
      if (companyName && uuidCell.isBlank()) {
        const newUuid = Utilities.getUuid();
        uuidCell.setValue(newUuid);
      }
    }
  }
}

/**
 * スプレッドシートを開いたときにカスタムメニューを追加する関数。
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('カスタムメニュー')
    .addItem('空のUUIDを一括生成', 'fillEmptyUuids')
    .addToUi();
}

/**
 * 「会社一覧」シートにあるデータのうち、会社名が入力済みでUUIDが空の行すべてにUUIDを生成する関数。
 * メニューから手動で実行します。
 */
function fillEmptyUuids() {
  const sheetName = "会社一覧";
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  
  if (!sheet) {
    ui.alert(`シート "${sheetName}" が見つかりません。`);
    return;
  }

  const uuidColumn = 1;   // A列
  const companyColumn = 2; // B列
  const dataRange = sheet.getDataRange();
  const values = dataRange.getValues();

  let updatedCount = 0;
  // ヘッダー行をスキップしてループ (i=1から)
  for (let i = 1; i < values.length; i++) {
    const uuid = values[i][uuidColumn - 1];
    const companyName = values[i][companyColumn - 1];

    // 会社名の列に値があり、かつUUIDの列が空の場合
    if (companyName && !uuid) {
      const newUuid = Utilities.getUuid();
      sheet.getRange(i + 1, uuidColumn).setValue(newUuid); // getRangeは1から始まるため i + 1
      updatedCount++;
    }
  }
  
  if (updatedCount > 0) {
    ui.alert(`${updatedCount}件のUUIDを生成しました。`);
  } else {
    ui.alert('UUIDが空の行は見つかりませんでした。');
  }
}
