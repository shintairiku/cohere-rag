/**
 * @fileoverview 共通設定オブジェクト。
 * Google Apps Script 全体から参照される定数をまとめています。
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
