
import os
import pandas as pd

def process_form(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        all_sheets = []
        for sheet in xls.sheet_names:
            df = xls.parse(sheet)
            if df.shape[0] < 5:
                continue
            roles = df.iloc[3:, 0].reset_index(drop=True)
            data = df.iloc[3:, 1:]
            data.columns = df.iloc[0, 1:]
            data = data.reset_index(drop=True)
            data.index = [f"row_{i}" for i in range(len(data))]
            melted = data.T.reset_index()
            melted.columns = ['聚會名稱'] + list(roles)
            melted['來源檔案'] = os.path.basename(file_path)
            all_sheets.append(melted)
        return pd.concat(all_sheets, ignore_index=True) if all_sheets else pd.DataFrame()
    except Exception as e:
        print(f"❌ 錯誤於 {file_path}: {e}")
        return pd.DataFrame()

def read_forms_from_folder(folder):
    all_data = []
    for fname in os.listdir(folder):
        if fname.endswith(".xlsx") and not fname.startswith("~"):
            full_path = os.path.join(folder, fname)
            df = process_form(full_path)
            if not df.empty:
                all_data.append(df)

    # 新增欄位一致過濾
    if not all_data:
        return pd.DataFrame()
    first_cols = all_data[0].columns.tolist()
    all_data = [df for df in all_data if df.columns.tolist() == first_cols]

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
