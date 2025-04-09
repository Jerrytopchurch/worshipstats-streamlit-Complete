import pandas as pd
from collections import Counter, defaultdict

def split_names(value):
    if pd.isna(value): return []
    return [n.strip() for n in str(value).split("/") if n.strip() not in ["", "NaN", "暫停"]]

def flatten_people(df):
    melted = df.drop(columns=['聚會名稱', '來源檔案'], errors='ignore')
    names = melted.values.flatten()
    all_names = []
    for cell in names:
        all_names.extend(split_names(cell))
    return Counter(all_names)

def extract_month(filename):
    for month in ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']:
        if month in filename:
            return month
    return "未知"

def calculate_statistics(df, weights):
    bonus_rate = weights.get("MD/BL/VL 加權倍數", 1.0)

    counts = flatten_people(df)
    df_people = pd.DataFrame(counts.items(), columns=["姓名", "總次數"])

    type_keywords = {
        "禱告會": ["禱告會"],
        "主日崇拜": ["三民早堂", "美河堂"],
        "青年主日": ["青年主日"],
        "QQ堂": ["QQ", "大Q"],
        "英文崇拜": ["英文崇拜"],
        "早上飽": ["早上飽"]
    }

    source_counter = defaultdict(lambda: defaultdict(float))
    monthly_counter = defaultdict(lambda: defaultdict(lambda: {"次數": 0, "加權": 0.0}))

    for _, row in df.iterrows():
        gathering = str(row["聚會名稱"])
        source = row.get("來源檔案", "")
        month = extract_month(source)
        match_type = None
        for t, keys in type_keywords.items():
            if any(k in gathering for k in keys):
                match_type = t
                break
        if not match_type:
            continue

        base_weight = weights.get(match_type, 1)
        cols = row.drop(labels=["聚會名稱", "來源檔案"], errors='ignore')

        for col_name, cell in cols.items():
            final_weight = base_weight * bonus_rate if any(k in col_name for k in ['MD', 'Band Leader', 'Vocal Leader']) else base_weight
            for n in split_names(cell):
                source_counter[n][match_type] += final_weight
                monthly_counter[n][month]["次數"] += 1
                monthly_counter[n][month]["加權"] += final_weight

    # 加權來源明細表
    raw_df = pd.DataFrame.from_dict(source_counter, orient='index').fillna(0).astype(float)
    source_df = raw_df.copy()
    for col in source_df.columns:
        if col in weights:
            source_df[col] *= weights[col]
    source_df = source_df.round(2)
    source_df["加權總分"] = source_df.sum(axis=1)
    source_df = source_df.reset_index().rename(columns={"index": "姓名"})
    raw_df = raw_df.reset_index().rename(columns={"index": "姓名"})
    source_df = pd.merge(raw_df, source_df, on="姓名", suffixes=("_原始", "_加權"))

    # 總表與篩選
    df_people = pd.merge(df_people, source_df[["姓名", "加權總分"]], on="姓名", how="left")
    df_people = df_people.rename(columns={"加權總分": "加權分數"})

    median = df_people["總次數"].median()
    potential = df_people[(df_people["總次數"] <= median) & (df_people["總次數"] >= 2)].copy()
    heavy = df_people[(df_people["加權分數"] > df_people["加權分數"].quantile(0.9)) | (df_people["總次數"] > 15)].copy()

    # 月份統計
    all_months = sorted({m for person in monthly_counter.values() for m in person})
    month_rows = []
    for name in monthly_counter:
        row = {"姓名": name}
        total_cnt, total_weight = 0, 0
        for m in all_months:
            info = monthly_counter[name].get(m, {"次數": 0, "加權": 0})
            row[f"{m}_次數"] = info["次數"]
            row[f"{m}_加權"] = round(info["加權"], 2)
            total_cnt += info["次數"]
            total_weight += info["加權"]
        row["總次數"] = total_cnt
        row["總加權"] = round(total_weight, 2)
        month_rows.append(row)
    month_df = pd.DataFrame(month_rows)

    return df_people, potential, heavy, source_df, month_df
