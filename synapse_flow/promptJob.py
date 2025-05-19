from dagster import op, job, In, Out
import pandas as pd

@op
def read_csv_file(context, file_path: str) -> pd.DataFrame:
    print(f"[ReadCsvOp] 开始读取 Csv 文件: {file_path}")
    df = pd.read_csv(file_path, encoding="utf-8")  # 或 encoding="gbk" 根据文件实际编码
    print(f"[ReadCsvOp] 读取完成:\n{df.to_string()}")
    return df

@op
def read_excel_file(context, file_path: str) -> pd.DataFrame:
    context.log.info(f"[ReadExcelOp] 开始读取 Excel 文件: {file_path}")
    df = pd.read_excel(file_path)  # 直接用 read_excel，支持 xlsx 文件
    context.log.info(f"[ReadExcelOp] 读取完成，数据预览:\n{df.head().to_string()}")
    return df
@op
def process_data(context, df: pd.DataFrame) -> dict:
    context.log.info(f"[ProcessDataOp] 收到数据，行数: {len(df)}")
    # 做一些处理，比如统计、过滤、转换等
    summary = {
        "row_count": len(df),
        "columns": df.columns.tolist(),
    }
    context.log.info(f"[ProcessDataOp] 处理完成，摘要: {summary}")
    return summary

@job
def promptJobPipeLine():
    # 这里实际调用时需要传 file_path 参数
    df = read_excel_file()
    process_data(df)
