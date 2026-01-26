"""
绘制Barra因子相关系数随时间变化的曲线图
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import matplotlib.font_manager as fm

# 查找系统中可用的中文字体
def get_chinese_font():
    """获取系统中可用的中文字体"""
    # 常见中文字体名称(按优先级排序)
    chinese_font_names = [
        'Noto Sans CJK SC',     # 思源黑体
        'Noto Sans CJK TC',
        'Noto Serif CJK SC',    # 思源宋体
        'Noto Serif CJK TC',
        'WenQuanYi Micro Hei',  # 文泉驿微米黑
        'WenQuanYi Zen Hei',    # 文泉驿正黑
        'AR PL UMing CN',       # 文鼎PL简中明体
        'AR PL UKai CN',        # 文鼎PL简中楷体
        'SimHei',               # 黑体
        'SimSun',               # 宋体
        'Microsoft YaHei',      # 微软雅黑
        'STHeiti',              # 华文黑体
        'STSong',               # 华文宋体
    ]

    # 获取系统所有字体
    available_fonts = set([f.name for f in fm.fontManager.ttflist])

    # 查找第一个可用的中文字体
    for font_name in chinese_font_names:
        if font_name in available_fonts:
            return font_name

    return None

# 设置中文字体
chinese_font = get_chinese_font()
if chinese_font:
    plt.rcParams['font.sans-serif'] = [chinese_font]
    print(f"使用中文字体: {chinese_font}")
else:
    # 如果没有找到中文字体，使用英文标签
    print("警告: 未找到中文字体，将使用英文标签")
    USE_ENGLISH = True

plt.rcParams['axes.unicode_minus'] = False

# 读取数据
df = pd.read_csv('barra_correlation_check.csv')

# 将日期列转换为datetime格式
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

# 自动检测因子列表(排除date和n_stocks列)
factors = [col for col in df.columns if col not in ['date', 'n_stocks']]
print(f"\n自动检测到 {len(factors)} 个因子: {', '.join(sorted(factors))}")

# 标签字典(中英文)
USE_ENGLISH = chinese_font is None
labels = {
    'title': 'Barra Factor Correlation Over Time' if USE_ENGLISH else 'Barra因子相关系数随时间变化',
    'date': 'Date' if USE_ENGLISH else '日期',
    'correlation': 'Correlation' if USE_ENGLISH else '相关系数',
    'factor_corr': '{} Factor Correlation' if USE_ENGLISH else '{} 因子相关系数',
    'all_factors': 'All Factors Comparison' if USE_ENGLISH else '所有因子相关系数对比',
}

# 计算子图布局
n_factors = len(factors)
if n_factors <= 4:
    nrows, ncols = 2, 2
elif n_factors <= 6:
    nrows, ncols = 2, 3
elif n_factors <= 9:
    nrows, ncols = 3, 3
elif n_factors <= 12:
    nrows, ncols = 3, 4
elif n_factors <= 16:
    nrows, ncols = 4, 4
else:
    nrows, ncols = 5, 4

# 创建图表
fig, axes = plt.subplots(nrows, ncols, figsize=(ncols*5, nrows*4))
fig.suptitle(labels['title'], fontsize=16, fontweight='bold')

# 将axes转换为一维数组
axes_flat = axes.flatten() if n_factors > 1 else [axes]

# 绘制每个因子的时间序列
for idx, factor in enumerate(sorted(factors)):
    if idx >= len(axes_flat):
        break

    ax = axes_flat[idx]

    if factor in df.columns:
        ax.plot(df['date'], df[factor], linewidth=1.5, color=f'C{idx % 10}', label=factor)
        ax.set_title(labels['factor_corr'].format(factor), fontsize=12, fontweight='bold')
        ax.set_xlabel(labels['date'], fontsize=10)
        ax.set_ylabel(labels['correlation'], fontsize=10)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(loc='best')

        # 设置日期格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # 设置y轴范围
        y_min = df[factor].min()
        y_max = df[factor].max()
        if not (pd.isna(y_min) or pd.isna(y_max)):
            ax.set_ylim([y_min * 0.95, y_max * 1.02])

# 隐藏多余的子图
for idx in range(n_factors, len(axes_flat)):
    axes_flat[idx].set_visible(False)

plt.tight_layout()
plt.savefig('barra_correlation_trends.png', dpi=300, bbox_inches='tight')
print("图表已保存为: barra_correlation_trends.png")

# 创建第二张图:所有因子对比
fig2, ax2 = plt.subplots(figsize=(14, 8))
fig2.suptitle(labels['all_factors'], fontsize=16, fontweight='bold')

for idx, factor in enumerate(sorted(factors)):
    if factor in df.columns:
        ax2.plot(df['date'], df[factor], linewidth=1.5, label=factor, alpha=0.8, marker='o', markersize=1)

ax2.set_xlabel(labels['date'], fontsize=12)
ax2.set_ylabel(labels['correlation'], fontsize=12)
ax2.grid(True, alpha=0.3, linestyle='--')
ax2.legend(loc='best', fontsize=9, ncol=2)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('barra_correlation_comparison.png', dpi=300, bbox_inches='tight')
print("图表已保存为: barra_correlation_comparison.png")

# 创建第三张图:单独绘制每个因子的详细趋势
labels2 = {
    'title': 'Barra Factor Correlation Detailed Trends' if USE_ENGLISH else 'Barra因子相关系数详细趋势',
    'mean': 'Mean' if USE_ENGLISH else '均值',
    'std': 'Std' if USE_ENGLISH else '标准差',
    'min': 'Min' if USE_ENGLISH else '最小值',
    'max': 'Max' if USE_ENGLISH else '最大值',
}

fig3, axes3 = plt.subplots(n_factors, 1, figsize=(14, n_factors*2.5))
fig3.suptitle(labels2['title'], fontsize=16, fontweight='bold')

# 处理单个因子的情况
axes3_list = [axes3] if n_factors == 1 else axes3

for idx, factor in enumerate(sorted(factors)):
    ax = axes3_list[idx]

    if factor in df.columns:
        ax.plot(df['date'], df[factor], linewidth=2, color=f'C{idx % 10}', marker='o',
                markersize=1, alpha=0.7)
        ax.fill_between(df['date'], df[factor], alpha=0.2, color=f'C{idx % 10}')

        ax.set_title(labels['factor_corr'].format(factor), fontsize=12, fontweight='bold')
        ax.set_ylabel(labels['correlation'], fontsize=10)
        ax.grid(True, alpha=0.3, linestyle='--')

        # 添加统计信息
        mean_val = df[factor].mean()
        std_val = df[factor].std()
        min_val = df[factor].min()
        max_val = df[factor].max()

        stats_text = f'{labels2["mean"]}: {mean_val:.4f}\n{labels2["std"]}: {std_val:.4f}\n{labels2["min"]}: {min_val:.4f}\n{labels2["max"]}: {max_val:.4f}'
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                fontsize=9, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 设置日期格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # 添加水平参考线
        ax.axhline(y=mean_val, color='red', linestyle='--', linewidth=1, alpha=0.5, label=labels2['mean'])
        ax.legend(loc='upper right', fontsize=8)

axes3_list[-1].set_xlabel(labels['date'], fontsize=10)

plt.tight_layout()
plt.savefig('barra_correlation_detailed.png', dpi=300, bbox_inches='tight')
print("详细图表已保存为: barra_correlation_detailed.png")

# 打印统计摘要
print("\n=== Barra因子相关系数统计摘要 ===")
print(df[factors].describe())

# 计算相关系数的变化趋势
print("\n=== 因子相关系数变化趋势 ===")
for factor in factors:
    first_val = df[factor].iloc[0]
    last_val = df[factor].iloc[-1]
    change = last_val - first_val
    change_pct = (change / first_val) * 100
    print(f"{factor:12s}: 起始={first_val:.4f}, 最新={last_val:.4f}, 变化={change:+.4f} ({change_pct:+.2f}%)")

plt.show()
