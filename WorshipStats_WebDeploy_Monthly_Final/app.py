
import streamlit as st
import pandas as pd
import io
from modules.form_reader import read_forms_from_folder
from modules.stat_calculator import calculate_statistics
from modules.report_generator import export_reports
import os
import tempfile

st.set_page_config(page_title="WorshipStats Web", layout="centered")
st.title("📊 WorshipStats 敬拜服事報表系統")

st.markdown("上傳你的服事表（.xlsx 檔），我會幫你統計每位同工的服事次數與加權分數：")

uploaded_files = st.file_uploader("上傳一份或多份服事表：", type="xlsx", accept_multiple_files=True)

st.sidebar.header("⚙️ 權重設定")
weights = {
    "主日崇拜": st.sidebar.slider("主日崇拜", 1, 5, 3),
    "青年主日": st.sidebar.slider("青年主日", 1, 5, 3),
    "禱告會": st.sidebar.slider("禱告會", 1, 5, 2),
    "英文崇拜": st.sidebar.slider("英文崇拜", 1, 3, 1),
    "大Q": st.sidebar.slider("大Q", 1, 3, 1),
    "QQ堂": st.sidebar.slider("QQ堂", 1, 3, 1),
    "早上飽": st.sidebar.slider("早上飽", 1, 5, 2),
    "MD/BL/VL 加權倍數": st.sidebar.slider("MD/BL/VL 加成倍率", 1.0, 3.0, 1.5, 0.1)
}

if uploaded_files:
    temp_dir = tempfile.mkdtemp()
    saved_paths = []
    for uploaded_file in uploaded_files:
        save_path = os.path.join(temp_dir, uploaded_file.name)
        with open(save_path, "wb") as f:
            f.write(uploaded_file.read())
        saved_paths.append(save_path)

    all_data = pd.concat([read_forms_from_folder(os.path.dirname(path)) for path in saved_paths], ignore_index=True)

    if all_data.empty:
        st.warning("找不到有效資料，請確認表單格式無誤。")
    else:
        st.success("✅ 表單成功解析！開始分析...")

        stats_df, potential_df, heavy_df, source_df = calculate_statistics(all_data, weights)

        st.subheader("📄 統計報表預覽")
        sort_option = st.selectbox("排序依據：", ["總次數", "加權分數"], index=0)
        stats_df_sorted = stats_df.sort_values(by=sort_option, ascending=False)
        st.dataframe(stats_df_sorted, use_container_width=True)

        with st.expander("🌱 潛力人員清單"):
            st.dataframe(potential_df, use_container_width=True)

        with st.expander("🔥 負擔過重人員清單"):
            st.dataframe(heavy_df, use_container_width=True)

        with st.expander("📘 CL3：加權來源明細"):
    st.dataframe(source_df, use_container_width=True)
    st.dataframe(source_df, use_container_width=True)

    st.dataframe(source_df, use_container_width=True)

with st.expander("📆 CL4：每月服事統計"):
    st.dataframe(month_df, use_container_width=True)
            st.dataframe(source_df, use_container_width=True)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            stats_df_sorted.to_excel(writer, sheet_name="統計總表", index=False)
            potential_df.to_excel(writer, sheet_name="潛力人員", index=False)
            heavy_df.to_excel(writer, sheet_name="負擔人員", index=False)
            source_df.to_excel(writer, sheet_name="加權明細", index=False)
    month_df.to_excel(writer, sheet_name="月份統計", index=False)

        st.download_button(
            label="📥 下載統計報表 Excel",
            data=output.getvalue(),
            file_name="WorshipStats_統計報表.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("請上傳 .xlsx 表單開始使用 🙌")
