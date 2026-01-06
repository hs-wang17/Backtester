import json
import pandas as pd
import os
from pathlib import Path


def json_to_dataframe():
    """
    将 /home/haris/project/backtester/para_optimizer_ef/scores 目录下的JSON文件转换为DataFrame
    """
    # 设置JSON文件目录路径
    json_dir = Path("/home/haris/project/backtester/para_optimizer_ef/scores")

    # 获取所有JSON文件
    json_files = list(json_dir.glob("*.json"))

    if not json_files:
        print("未找到JSON文件")
        return None

    print(f"找到 {len(json_files)} 个JSON文件:")
    for file in json_files:
        print(f"  - {file.name}")

    all_data = []

    # 处理每个JSON文件
    for json_file in json_files:
        print(f"\n处理文件: {json_file.name}")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # 每个JSON文件包含多个回测结果
            for item in data:
                # 提取参数信息
                parameters = item.get("parameters", {})

                # 提取回测信息（通常只有一个，但以防万一有多个）
                backtest_info_list = item.get("backtest_info", [])
                if backtest_info_list:
                    backtest_info = backtest_info_list[0]  # 取第一个回测结果
                else:
                    backtest_info = {}

                # 合并所有数据到一个字典中
                row_data = {}

                # 添加参数列（添加前缀以区分）
                for param_name, param_value in parameters.items():
                    row_data[f"param_{param_name}"] = param_value

                # 添加基本回测指标列
                basic_metrics = [
                    "年化收益",
                    "年化波动",
                    "夏普比率",
                    "累计收益",
                    "最大回撤",
                    "平均回撤",
                    "胜率(天)",
                    "超额年化收益",
                    "超额年化波动",
                    "信息比率",
                    "超额累计收益",
                    "超额最大回撤",
                    "超额平均回撤",
                    "超额胜率(天)",
                ]

                for metric in basic_metrics:
                    if metric in backtest_info:
                        row_data[metric] = backtest_info[metric]

                # 添加逐年数据列
                yearly_metrics = ["逐年超额年化收益", "逐年超额年化波动", "逐年信息比率"]

                for metric in yearly_metrics:
                    if metric in backtest_info:
                        yearly_data = backtest_info[metric]
                        for year, value in yearly_data.items():
                            # 根据metric类型添加适当的前缀
                            if "收益" in metric:
                                prefix = "超额年化收益"
                            elif "波动" in metric:
                                prefix = "超额年化波动"
                            elif "信息比率" in metric:
                                prefix = "信息比率"

                            row_data[f"{prefix}_{year}"] = value

                # 添加文件名作为标识
                row_data["source_file"] = json_file.name

                all_data.append(row_data)

        except Exception as e:
            print(f"处理文件 {json_file.name} 时出错: {e}")
            continue

    if not all_data:
        print("没有成功处理任何数据")
        return None

    # 创建DataFrame
    df = pd.DataFrame(all_data)

    print(f"\n成功创建DataFrame，包含 {len(df)} 行和 {len(df.columns)} 列")
    print(f"列名: {list(df.columns)}")

    # 显示基本统计信息
    print(f"\n数据预览:")
    print(df.head())

    print(f"\n基本统计信息:")
    print(f"总记录数: {len(df)}")
    print(f"来源文件数: {df['source_file'].nunique()}")

    # 显示数值列的基本统计
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        print(f"\n数值列统计:")
        print(df[numeric_cols].describe())

    return df


def save_dataframe(df, output_file="/home/haris/project/backtester/para_optimizer_ef/scores/backtest_results.xlsx"):
    """
    将DataFrame保存到Excel文件
    """
    if df is None:
        print("DataFrame为空，无法保存")
        return

    try:
        # 保存为Excel文件
        df.to_excel(output_file, index=False, engine="openpyxl")
        print(f"\nDataFrame已保存到: {output_file}")

        # 同时保存为CSV文件（更大兼容性）
        csv_file = output_file.replace(".xlsx", ".csv")
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print(f"DataFrame已保存到: {csv_file}")

    except Exception as e:
        print(f"保存文件时出错: {e}")


if __name__ == "__main__":
    # 转换JSON文件为DataFrame
    df = json_to_dataframe()

    if df is not None:
        # 保存结果
        save_dataframe(df, "/home/haris/project/backtester/para_optimizer_ef/scores/backtest_results.xlsx")

        print(f"\n转换完成！")
        print(f"处理了 {len(df)} 条回测记录")
        print(f"包含 {len([col for col in df.columns if col.startswith('param_')])} 个参数")
        print(f"包含 {len([col for col in df.columns if not col.startswith('param_') and col != 'source_file'])} 个性能指标")
