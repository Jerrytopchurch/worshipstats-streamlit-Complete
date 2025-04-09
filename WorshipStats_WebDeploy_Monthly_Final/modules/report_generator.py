
import pandas as pd

def export_summary(monthly_df, summary_df, output_dir):
    with pd.ExcelWriter(f"{output_dir}/統計總報表.xlsx") as writer:
        monthly_df.to_excel(writer, sheet_name="服事次數", index=False)
        summary_df.to_excel(writer, sheet_name="加權統計", index=False)

    print("✅ 統計報表已輸出到 output_reports")
