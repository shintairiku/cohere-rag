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
