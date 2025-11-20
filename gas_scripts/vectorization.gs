/**
 * @fileoverview ベクトル化リクエストとUUID生成系の処理。
 */

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
