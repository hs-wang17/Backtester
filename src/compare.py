import os
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 设置绘图风格
plt.style.use('seaborn-v0_8-darkgrid') 
plt.rcParams.update({"font.sans-serif": ["WenQuanYi Micro Hei"], "axes.unicode_minus": False, "font.size": 12})

def plot_strategy_comparison(file_paths, title, save_dir, date_str, support_type):
    """
    绘制策略超额净值比较图
    
    参数:
    file_paths: 要绘制的文件路径列表
    title: 图表标题
    save_dir: 保存图表的目录
    date_str: 日期字符串
    support_type: support类型 (5或7)
    """
    # 确保保存目录存在
    os.makedirs(save_dir, exist_ok=True)
    
    # 1. 第一遍遍历：读取所有数据并确定共同的"最晚起始点"
    data_frames = {}
    start_dates = []
    
    for path in file_paths:
        # 读取数据，确保索引是 datetime
        df = pd.read_csv(path, index_col=0)
        df.index = pd.to_datetime(df.index.astype(str), format='%Y%m%d')
        df = df.sort_index()
        # 存储并记录起始日期
        label_name = os.path.basename(path).replace("_rel_nv.csv", "")
        data_frames[label_name] = df
        start_dates.append(df.index[0])
    # 找到所有曲线中，最晚开始的那个日期
    latest_start_date = max(start_dates)
    print(f"所有曲线将统一从 {latest_start_date.date()} 开始对比")
    
    # 2. 第二遍遍历：截断、归一化并绘图
    plt.figure(figsize=(12, 6))
    ax = plt.gca()
    
    # 设置日期显示格式
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    
    legend_labels = ["早盘策略(20日)", "早盘策略(10日)", "午盘策略(20日)", "午盘策略(10日)"]
    
    for i, (label, df) in enumerate(data_frames.items()):
        # 只保留最晚起始日期之后的数据
        df_sliced = df[df.index >= latest_start_date].copy()
        
        if not df_sliced.empty:
            # 获取第一行（即共同起始点）的值
            initial_val = df_sliced.iloc[0]
            
            # 归一化：当前值 / 初始值 (使起始点全部变为 1.0)
            df_sliced['norm_nv'] = df_sliced / initial_val
            
            # 绘图
            plt.plot(df_sliced.index, df_sliced['norm_nv'], label=legend_labels[i], linewidth=1.5)
    
    # 图表装饰
    plt.axhline(y=1.0, color='black', linestyle='-', alpha=0.3) # 画一条 1.0 的基准线
    plt.title(title, fontsize=14)
    plt.xlabel('日期', fontsize=12)
    plt.ylabel('超额净值', fontsize=12)
    
    # 处理图例：如果标签太长，可以只显示中间的日期部分
    plt.legend(bbox_to_anchor=(1, 1), loc='upper left', fontsize=12)
    plt.gcf().autofmt_xdate()
    plt.grid(True)
    plt.tight_layout()
    
    # 保存图表
    save_path = os.path.join(save_dir, f"{date_str}_trade_support_{support_type}.pdf")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"图表已保存到: {save_path}")
    
    plt.show()

def main():
    target_dir_list = ["/home/haris/mymodel/backtests", "/home/haris/mymodel_noon/backtests", "/home/haris/mymodel_10/backtests", "/home/haris/mymodel_noon_10/backtests"]
    suffix = '_rel_nv.csv'
    result_files = []
    
    for target_dir in target_dir_list:
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                if file.endswith(suffix):
                    full_path = os.path.join(root, file)
                    result_files.append(full_path)
    result_files.sort()
    
    date_groups = defaultdict(list)
    for path in result_files:
        filename = os.path.basename(path)
        date_str = filename.split('period_')[1][:8]
        date_groups[date_str].append(path)
        
    latest_date = max(date_groups.keys())
    latest_files = date_groups[latest_date]
    
    support_groups = {
        "support5": [],
        "support7": []
    }
    
    for path in latest_files:
        if "trade_support5" in path:
            support_groups["support5"].append(path)
        elif "trade_support7" in path:
            support_groups["support7"].append(path)
    
    # 绘制support5的图表
    if support_groups["support5"]:
        plot_strategy_comparison(
            support_groups["support5"], 
            "策略超额净值比较 (基于Trade Support 5)",
            "/home/haris/mymodel_compare",
            latest_date,
            5
        )
    
    # 绘制support7的图表
    if support_groups["support7"]:
        plot_strategy_comparison(
            support_groups["support7"], 
            "策略超额净值比较 (基于Trade Support 7)",
            "/home/haris/mymodel_compare",
            latest_date,
            7
        )

if __name__ == "__main__":
    main()
