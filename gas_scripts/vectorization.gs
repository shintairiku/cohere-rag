/**
 * 優先企業に対してDrive変更通知チャネルの登録を行い、初回はベクトル化も実行する。
 */
function registerDriveWatchForPriorityCompanies() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(Config.COMPANY_LIST.SHEET_NAME);
  if (!sheet) {
    ui.alert(`'${Config.COMPANY_LIST.SHEET_NAME}'シートが見つかりません。`);
    return;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) {
    ui.alert('処理対象の企業がありません。');
    return;
  }

  const dataRange = sheet.getRange(2, 1, lastRow - 1, Config.COMPANY_LIST.PRIORITY_COL);
  const values = dataRange.getValues();

  let hasPriority = false;
  let successCount = 0;
  let vectorizeCount = 0;
  let failureCount = 0;
  const errors = [];

  ss.toast('優先企業の変更通知登録を開始します...', '処理中', -1);

  for (let i = 0; i < values.length; i++) {
    const rowNumber = i + 2;
    const row = values[i];
    const isPriority = row[Config.COMPANY_LIST.PRIORITY_COL - 1];
    if (isPriority !== true) {
      continue;
    }

    hasPriority = true;
    const companyName = row[Config.COMPANY_LIST.NAME_COL - 1];
    const driveUrl = row[Config.COMPANY_LIST.DRIVE_URL_COL - 1];
    let uuid = row[Config.COMPANY_LIST.UUID_COL - 1];

    if (!companyName || !driveUrl) {
      failureCount++;
      errors.push(`Row ${rowNumber}: 会社名またはドライブURLが空です。`);
      continue;
    }

    try {
      if (!uuid) {
        uuid = Utilities.getUuid();
        sheet.getRange(rowNumber, Config.COMPANY_LIST.UUID_COL).setValue(uuid);
      }

      const useEmbedV4 = companyName.indexOf("embed-v4.0") !== -1;
      const watchResult = registerDriveWatch_(uuid, driveUrl, companyName, useEmbedV4);
      successCount++;

      if (watchResult && watchResult.is_new_channel) {
        try {
          triggerVectorizeJob_(uuid, driveUrl, useEmbedV4);
          vectorizeCount++;
        } catch (vectorErr) {
          errors.push(`Row ${rowNumber} (${companyName}): ベクトル化に失敗しました - ${vectorErr.message}`);
        }
      }
    } catch (err) {
      failureCount++;
      errors.push(`Row ${rowNumber} (${companyName || '未設定'}): ${err.message}`);
    }
  }

  ss.toast('優先企業の変更通知登録が完了しました。', '完了', 5);

  if (!hasPriority) {
    ui.alert('優先企業にチェックが入っている企業がありません。');
    return;
  }

  let message = `変更通知登録 成功: ${successCount}件\nベクトル化実行: ${vectorizeCount}件\n失敗: ${failureCount}件`;
  if (errors.length > 0) {
    message += `\n\n詳細:\n${errors.join("\n")}`;
  }
  ui.alert(message);
}

/**
 * Drive変更通知チャネルを登録するAPIを呼び出す。
 * @return {Object} APIのレスポンスオブジェクト
 */
function registerDriveWatch_(uuid, driveUrl, companyName, useEmbedV4) {
  const payload = JSON.stringify({
    uuid: uuid,
    drive_url: driveUrl,
    company_name: companyName || '',
    use_embed_v4: useEmbedV4
  });
  const params = {
    method: "post",
    contentType: "application/json",
    headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
    payload: payload,
    muteHttpExceptions: true,
  };
  const apiUrl = `${Config.API_BASE_URL}/drive/watch`;
  const response = UrlFetchApp.fetch(apiUrl, params);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText() || "";

  if (responseCode >= 200 && responseCode < 300) {
    if (!responseText) {
      return { is_new_channel: true };
    }
    try {
      return JSON.parse(responseText);
    } catch (parseError) {
      throw new Error("APIレスポンスの解析に失敗しました。");
    }
  }

  throw new Error(`Drive watch APIエラー (コード: ${responseCode}) ${responseText}`);
}

/**
 * Cloud Runのベクトル化ジョブを直接呼び出す。
 */
function triggerVectorizeJob_(uuid, driveUrl, useEmbedV4) {
  const payload = JSON.stringify({
    uuid: uuid,
    drive_url: driveUrl,
    use_embed_v4: useEmbedV4
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
    return true;
  }
  const responseText = response.getContentText() || "";
  throw new Error(`ベクトル化APIエラー (コード: ${responseCode}) ${responseText}`);
}

/**
 * 優先企業の設定をAPIに送信し、drive-watch-statesへ保存させる。
 */
function savePriorityCompanyStates() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(Config.COMPANY_LIST.SHEET_NAME);
  if (!sheet) {
    ui.alert(`'${Config.COMPANY_LIST.SHEET_NAME}'シートが見つかりません。`);
    return;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) {
    ui.alert('優先企業にチェックが入った行がありません。');
    return;
  }

  const range = sheet.getRange(2, 1, lastRow - 1, Config.COMPANY_LIST.PRIORITY_COL);
  const values = range.getValues();
  const companies = [];
  const errors = [];

  for (let i = 0; i < values.length; i++) {
    const rowNumber = i + 2;
    const row = values[i];
    const isPriority = row[Config.COMPANY_LIST.PRIORITY_COL - 1];
    if (isPriority !== true) {
      continue;
    }

    let uuid = row[Config.COMPANY_LIST.UUID_COL - 1];
    const companyName = row[Config.COMPANY_LIST.NAME_COL - 1];
    const driveUrl = row[Config.COMPANY_LIST.DRIVE_URL_COL - 1];
    if (!companyName || !driveUrl) {
      errors.push(`Row ${rowNumber}: 会社名またはドライブURLが空です。`);
      continue;
    }
    if (!uuid) {
      uuid = Utilities.getUuid();
      sheet.getRange(rowNumber, Config.COMPANY_LIST.UUID_COL).setValue(uuid);
    }
    const useEmbedV4 = companyName.indexOf("embed-v4.0") !== -1;
    companies.push({
      uuid: uuid,
      company_name: companyName,
      drive_url: driveUrl,
      use_embed_v4: useEmbedV4
    });
  }

  if (companies.length === 0) {
    ui.alert('優先企業にチェックが入っている有効な行がありません。');
    return;
  }

  try {
    const result = syncCompanyStates_(companies);
    let message = `保存に成功: ${result.saved_count || 0}件`;
    if ((result.error_count || 0) > 0) {
      message += `\n失敗: ${result.error_count}件`;
      if (result.errors && result.errors.length) {
        const detail = result.errors.slice(0, 5).map(err => `${err.uuid || 'unknown'}: ${err.error || err}`);
        message += `\n例: ${detail.join(", ")}`;
      }
    }
    if (errors.length > 0) {
      message += `\nスキップ: ${errors.length}件`;
    }
    ui.alert(message);
  } catch (err) {
    ui.alert(`保存に失敗しました: ${err.message}`);
  }
}

function syncCompanyStates_(companies) {
  const payload = JSON.stringify({ companies: companies });
  const params = {
    method: "post",
    contentType: "application/json",
    headers: { "Authorization": "Bearer " + ScriptApp.getIdentityToken() },
    payload: payload,
    muteHttpExceptions: true,
  };
  const apiUrl = `${Config.API_BASE_URL}/drive/company-states`;
  const response = UrlFetchApp.fetch(apiUrl, params);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText() || "";
  if (responseCode >= 200 && responseCode < 300) {
    try {
      return JSON.parse(responseText);
    } catch (err) {
      return { saved_count: companies.length, error_count: 0 };
    }
  }
  throw new Error(`APIエラー (コード: ${responseCode}) ${responseText}`);
}
