
import os

def export_reports(stats_df, potential_df, heavy_df, folder):
    os.makedirs(folder, exist_ok=True)
    stats_df.to_excel(os.path.join(folder, "總統計報表.xlsx"), index=False)
    potential_df.to_excel(os.path.join(folder, "潛力人員清單.xlsx"), index=False)
    heavy_df.to_excel(os.path.join(folder, "負擔過重人員清單.xlsx"), index=False)
