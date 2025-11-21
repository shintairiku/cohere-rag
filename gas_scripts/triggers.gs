/**
 * @fileoverview onOpenや編集トリガーなど、ユーザー操作の入口となる処理を担当します。
 */

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
 * スプレッドシートを開いたときにカスタムメニューをUIに追加します。
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('✨画像検索メニュー')
    .addItem('優先企業の変更通知を登録', 'registerDriveWatchForPriorityCompanies')
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
