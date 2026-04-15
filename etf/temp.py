import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import warnings
import logging
import sys
from datetime import datetime

warnings.filterwarnings("ignore")

# 设置中文显示
plt.rcParams.update({"font.sans-serif": ["WenQuanYi Micro Hei"], "axes.unicode_minus": False, "font.size": 12})


class ETFPortfolioCalculator:
    def __init__(self, strategy_file_path, nav_data_path, commission_rate=0.0, init_capital=1e8, output_dir="/home/haris/project/etf/output"):
        """
        初始化ETF组合计算器

        参数:
        strategy_file_path: 策略Excel文件路径
        nav_data_path: 净值数据文件夹路径
        commission_rate: 手续费率，默认为0.0
        init_capital: 初始资金量，默认为1e8（1亿）
        """
        self.strategy_file_path = strategy_file_path
        self.nav_data_path = nav_data_path
        self.commission_rate = commission_rate
        self.init_capital = init_capital
        self.output_dir = output_dir
        self.strategy_data = None
        self.nav_data = {}
        self.trading_days = []
        self.portfolio_nav = None
        self.etf_shares = {}

        # 设置日志系统
        self.setup_logging()

    def setup_logging(self):
        """设置日志系统"""
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)

        # 生成日志文件名（带时间戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.output_dir, f"etf_analysis_{timestamp}.log")

        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)],  # 同时输出到控制台
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info("=" * 60)
        self.logger.info("ETF轮动策略组合净值分析系统启动")
        self.logger.info(f"日志文件: {log_file}")
        self.logger.info("=" * 60)

    def load_strategy_data(self):
        """加载策略数据"""
        self.logger.info("加载策略数据...")
        self.strategy_data = pd.read_excel(self.strategy_file_path)

        # 按日期正序排列
        self.strategy_data = self.strategy_data.sort_values("date").reset_index(drop=True)
        self.strategy_data = self.strategy_data[["date", "code"]].copy()
        self.strategy_data["date"] = self.strategy_data["date"].astype(str)

        self.logger.info(f"策略数据加载完成，共{len(self.strategy_data)}条记录")
        self.logger.info(f"日期范围: {self.strategy_data['date'].min()} 至 {self.strategy_data['date'].max()}")
        self.logger.info(f"初始资金量: {self.init_capital:,.0f} 元")

    def get_trading_days(self):
        """获取所有交易日列表"""
        self.logger.info("获取交易日列表...")
        csv_files = [f for f in os.listdir(self.nav_data_path) if f.endswith(".csv")]
        trading_days = [f.replace(".csv", "") for f in csv_files]
        self.trading_days = sorted(trading_days)
        self.logger.info(f"共找到{len(self.trading_days)}个交易日")

    def load_nav_data(self):
        """加载净值数据"""
        self.logger.info("加载净值数据...")
        for trading_day in self.trading_days:
            file_path = os.path.join(self.nav_data_path, f"{trading_day}.csv")
            try:
                df = pd.read_csv(file_path)
                df = df[["ts_code", "accum_nav"]].copy()
                df["accum_nav"] = pd.to_numeric(df["accum_nav"], errors="coerce")
                self.nav_data[trading_day] = df
            except Exception as e:
                self.logger.error(f"加载{trading_day}的净值数据失败: {e}")

        self.logger.info("净值数据加载完成")

    def get_next_trading_day(self, current_date):
        """获取下一个交易日"""
        # 处理日期格式：确保是字符串格式
        if isinstance(current_date, (int, np.integer)):
            current_str = str(current_date)
        elif isinstance(current_date, str):
            current_str = current_date
        else:
            current_str = current_date.strftime("%Y%m%d")

        if current_str not in self.trading_days:
            # 如果当前日期不是交易日，找到下一个交易日
            for day in self.trading_days:
                if day > current_str:
                    return day
            return None

        current_index = self.trading_days.index(current_str)
        if current_index + 1 < len(self.trading_days):
            return self.trading_days[current_index + 1]
        return None

    def calculate_portfolio_nav(self):
        """计算组合净值"""
        self.logger.info("计算组合净值...")
        portfolio_results = []

        # 获取策略中的唯一日期
        strategy_dates = sorted(self.strategy_data["date"].unique())

        # 初始化第一天的ETF份额
        first_date = strategy_dates[0]
        first_holdings = self.strategy_data[self.strategy_data["date"] == first_date]
        first_etf_codes = first_holdings["code"].tolist()

        # 计算每只ETF的初始投入资金和份额（全部资金等权重投资）
        for etf_code in first_etf_codes:
            weight = 1.0 / len(first_etf_codes)  # 等权重
            etf_capital = self.init_capital * weight  # 每只ETF的初始投入

            # 获取第一个交易日的净值计算份额
            first_trade_day = self.get_next_trading_day(first_date)
            if first_trade_day and first_trade_day in self.nav_data:
                first_day_nav = self.nav_data[first_trade_day]
                etf_nav_data = first_day_nav[first_day_nav["ts_code"] == etf_code]
                if len(etf_nav_data) > 0:
                    nav_value = etf_nav_data["accum_nav"].iloc[0]
                    if pd.notna(nav_value) and nav_value > 0:
                        self.etf_shares[etf_code] = etf_capital / nav_value
                        self.logger.info(
                            f"{first_date}策略，{first_trade_day}交易日 - ETF {etf_code}: 初始投入 {etf_capital:,.0f} 元, 净值 {nav_value:.4f}, 份额 {self.etf_shares[etf_code]:.0f}"
                        )

        # 记录前一日的ETF组合
        prev_etf_codes = set(first_etf_codes)

        # 创建策略日期到ETF组合的映射
        strategy_etf_map = {}
        for date in strategy_dates:
            holdings = self.strategy_data[self.strategy_data["date"] == date]
            strategy_etf_map[date] = set(holdings["code"].tolist())

        # 获取所有需要处理的交易日（从第一个策略日期开始的所有交易日）
        start_trade_day = self.get_next_trading_day(first_date)
        if start_trade_day is None:
            self.logger.error(f"错误: 找不到{first_date}之后的交易日")
            self.portfolio_nav = pd.DataFrame(portfolio_results)
            return

        # 获取从第一个交易日开始的所有交易日
        all_relevant_trade_days = []
        start_index = self.trading_days.index(start_trade_day)
        for i in range(start_index, len(self.trading_days)):
            all_relevant_trade_days.append(self.trading_days[i])

        # 遍历所有相关交易日
        for trade_day in all_relevant_trade_days:
            # 获取当日净值数据
            if trade_day not in self.nav_data:
                self.logger.warning(f"警告: {trade_day} 的净值数据不存在，跳过")
                continue

            day_nav = self.nav_data[trade_day]

            # 找到当前交易日对应的策略日期（最近的策略日期）
            current_strategy_date = None
            current_etf_codes = prev_etf_codes  # 默认保持之前的ETF组合

            for strategy_date in sorted(strategy_dates):
                strategy_trade_day = self.get_next_trading_day(strategy_date)
                if strategy_trade_day and trade_day >= strategy_trade_day:
                    current_strategy_date = strategy_date
                    current_etf_codes = strategy_etf_map[strategy_date]
                else:
                    break

            # 检查是否有ETF组合变化
            if prev_etf_codes != current_etf_codes and current_strategy_date is not None:
                # 有组合变化，需要重新平衡
                self.logger.info(f"{trade_day}交易日 - 检测到ETF组合变化，重新平衡投资组合")

                # 计算当前总资产（基于当前持有的ETF在当日的净值）
                total_assets = 0
                for etf_code in prev_etf_codes:
                    if etf_code in self.etf_shares:
                        etf_nav_data = day_nav[day_nav["ts_code"] == etf_code]
                        if len(etf_nav_data) > 0:
                            nav_value = etf_nav_data["accum_nav"].iloc[0]
                            if pd.notna(nav_value) and nav_value > 0:
                                total_assets += self.etf_shares[etf_code] * nav_value

                # 重新分配总资产到当前ETF组合（等权重）
                self.etf_shares.clear()

                if current_etf_codes:
                    etf_capital_each = total_assets / len(current_etf_codes)

                    for etf_code in current_etf_codes:
                        etf_nav_data = day_nav[day_nav["ts_code"] == etf_code]
                        if len(etf_nav_data) > 0:
                            nav_value = etf_nav_data["accum_nav"].iloc[0]
                            if pd.notna(nav_value) and nav_value > 0:
                                self.etf_shares[etf_code] = etf_capital_each / nav_value
                                self.logger.info(
                                    f"{trade_day}交易日 - ETF {etf_code}: 重新投入 {etf_capital_each:,.0f} 元, 净值 {nav_value:.4f}, 份额 {self.etf_shares[etf_code]:.0f}"
                                )

                # 更新前一日的ETF组合
                prev_etf_codes = current_etf_codes

            # 计算组合市值（基于当前持有的ETF份额）
            portfolio_value = 0
            valid_etfs = 0

            for etf_code in current_etf_codes:
                if etf_code in self.etf_shares:
                    etf_nav_data = day_nav[day_nav["ts_code"] == etf_code]
                    if len(etf_nav_data) > 0:
                        nav_value = etf_nav_data["accum_nav"].iloc[0]
                        if pd.notna(nav_value) and nav_value > 0:
                            etf_value = self.etf_shares[etf_code] * nav_value
                            portfolio_value += etf_value
                            valid_etfs += 1

            if valid_etfs > 0:
                # 计算组合净值（标准化为1.0基准）
                portfolio_nav_value = portfolio_value / self.init_capital

                # 判断是否有交易操作
                if current_strategy_date is not None:
                    # 检查这个交易日是否是某个策略的执行日
                    strategy_trade_day = self.get_next_trading_day(current_strategy_date)
                    if trade_day == strategy_trade_day:
                        # 这是策略执行日，记录交易信息
                        prev_holdings = self.strategy_data[self.strategy_data["date"] == current_strategy_date]
                        prev_codes = set(prev_holdings["code"].tolist()) if len(prev_holdings) > 0 else set()

                        if prev_etf_codes != current_etf_codes:
                            etfs_to_sell = prev_etf_codes - current_etf_codes
                            etfs_to_buy = current_etf_codes - prev_etf_codes
                            action_type = []
                            if etfs_to_sell:
                                action_type.append(f"卖出{len(etfs_to_sell)}只")
                            if etfs_to_buy:
                                action_type.append(f"买入{len(etfs_to_buy)}只")
                            action_desc = "、".join(action_type)
                            self.logger.info(
                                f"{current_strategy_date}策略，{trade_day}交易日 - {action_desc}，当日净值: {portfolio_nav_value:.4f}，市值: {portfolio_value:,.0f}元"
                            )
                        else:
                            self.logger.info(
                                f"{current_strategy_date}策略，{trade_day}交易日 - 继续持有{len(current_etf_codes)}只ETF，当日净值: {portfolio_nav_value:.4f}，市值: {portfolio_value:,.0f}元"
                            )
                    else:
                        # 这是无交易的净值更新日
                        self.logger.info(
                            f"{trade_day}交易日 - 无交易，持有{len(current_etf_codes)}只ETF，当日净值: {portfolio_nav_value:.4f}，市值: {portfolio_value:,.0f}元"
                        )
                else:
                    # 这是无交易的净值更新日
                    self.logger.info(
                        f"{trade_day}交易日 - 无交易，持有{len(current_etf_codes)}只ETF，当日净值: {portfolio_nav_value:.4f}，市值: {portfolio_value:,.0f}元"
                    )

                portfolio_results.append(
                    {
                        "strategy_date": current_strategy_date if current_strategy_date else "",
                        "trade_date": trade_day,
                        "portfolio_value": portfolio_value,  # 实际市值
                        "portfolio_nav": portfolio_nav_value,  # 标准化净值
                        "etf_count": valid_etfs,
                        "total_etfs": len(current_etf_codes),
                        "absolute_return": portfolio_value - self.init_capital,  # 绝对收益
                        "return_rate": ((portfolio_value - self.init_capital) / self.init_capital) * 100,  # 收益率
                        "has_trading": current_strategy_date is not None and trade_day == self.get_next_trading_day(current_strategy_date),  # 是否有交易
                    }
                )

        self.portfolio_nav = pd.DataFrame(portfolio_results)
        self.logger.info(
            f"组合净值计算完成，共{len(self.portfolio_nav)}个交易日，其中{len(self.portfolio_nav[self.portfolio_nav['has_trading']==True])}个交易日有交易"
        )

    def normalize_portfolio_nav(self, base_value=1.0):
        """标准化组合净值，以第一个交易日为基准"""
        if self.portfolio_nav is None or len(self.portfolio_nav) == 0:
            self.logger.error("错误: 没有有效的组合净值数据")
            return

        # 以第一个交易日为基准值1.0
        first_nav = self.portfolio_nav["portfolio_nav"].iloc[0]
        if first_nav > 0:
            self.portfolio_nav["normalized_nav"] = self.portfolio_nav["portfolio_nav"] / first_nav * base_value
        else:
            self.logger.warning("警告: 第一个交易日的净值为0，无法标准化")
            self.portfolio_nav["normalized_nav"] = self.portfolio_nav["portfolio_nav"]

    def calculate_performance_stats(self):
        """计算绩效统计"""
        if self.portfolio_nav is None or len(self.portfolio_nav) == 0:
            return None

        nav_series = self.portfolio_nav["normalized_nav"]

        # 计算收益率
        returns = nav_series.pct_change().dropna()

        # 计算统计指标
        total_return = (nav_series.iloc[-1] / nav_series.iloc[0] - 1) * 100
        annual_return = ((nav_series.iloc[-1] / nav_series.iloc[0]) ** (252 / len(nav_series)) - 1) * 100
        volatility = returns.std() * np.sqrt(252) * 100
        max_drawdown = ((nav_series / nav_series.expanding().max() - 1).min()) * 100
        sharpe_ratio = annual_return / volatility if volatility > 0 else 0

        # 计算绝对收益
        final_value = self.portfolio_nav["portfolio_value"].iloc[-1]
        absolute_return = final_value - self.init_capital
        absolute_return_rate = (absolute_return / self.init_capital) * 100

        stats = {
            "初始资金": f"{self.init_capital:,.0f}",
            "最终市值": f"{final_value:,.0f}",
            "绝对收益": f"{absolute_return:,.0f}",
            "绝对收益率": f"{absolute_return_rate:.2f}%",
            "总收益率": f"{total_return:.2f}%",
            "年化收益率": f"{annual_return:.2f}%",
            "年化波动率": f"{volatility:.2f}%",
            "最大回撤": f"{max_drawdown:.2f}%",
            "夏普比率": f"{sharpe_ratio:.3f}",
            "交易天数": len(nav_series),
            "起始日期": self.portfolio_nav["trade_date"].iloc[0],
            "结束日期": self.portfolio_nav["trade_date"].iloc[-1],
        }

        return stats

    def plot_nav_curve(self, save_path=None, show_absolute_value=False):
        """绘制净值曲线"""
        if self.portfolio_nav is None or len(self.portfolio_nav) == 0:
            self.logger.error("错误: 没有有效的组合净值数据")
            return

        plt.figure(figsize=(12, 8))

        # 转换交易日期为datetime格式用于绘图
        trade_dates = pd.to_datetime(self.portfolio_nav["trade_date"], format="%Y%m%d")

        if show_absolute_value:
            # 绘制实际市值曲线
            plt.plot(trade_dates, self.portfolio_nav["portfolio_value"], linewidth=2, color="blue", label="ETF组合市值（元）")
            plt.ylabel("市值（元）", fontsize=12)
            title = "ETF轮动策略组合市值曲线"
        else:
            # 绘制标准化净值曲线
            plt.plot(trade_dates, self.portfolio_nav["normalized_nav"], linewidth=2, color="blue", label="ETF组合净值")
            plt.ylabel("标准化净值", fontsize=12)
            title = "ETF轮动策略组合净值曲线"

        plt.title(title, fontsize=16, fontweight="bold")
        plt.xlabel("交易日", fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend()

        # 格式化x轴日期显示
        plt.xticks(rotation=45)

        # 添加统计信息
        stats = self.calculate_performance_stats()
        if stats:
            if show_absolute_value:
                info_text = f"初始资金: {stats['初始资金']}\n绝对收益: {stats['绝对收益']}\n绝对收益率: {stats['绝对收益率']}"
            else:
                info_text = f"总收益率: {stats['总收益率']}\n年化收益率: {stats['年化收益率']}\n最大回撤: {stats['最大回撤']}"
            plt.text(0.02, 0.98, info_text, transform=plt.gca().transAxes, verticalalignment="top", bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8))

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            self.logger.info(f"净值曲线已保存至: {save_path}")

        plt.show()

    def save_results(self, output_path):
        """保存计算结果"""
        if self.portfolio_nav is None:
            self.logger.error("错误: 没有计算结果可保存")
            return

        # 保存净值数据
        self.portfolio_nav.to_csv(output_path, index=False, encoding="utf-8-sig")
        self.logger.info(f"计算结果已保存至: {output_path}")

        # 保存绩效统计
        stats = self.calculate_performance_stats()
        if stats:
            stats_df = pd.DataFrame(list(stats.items()), columns=["指标", "值"])
            stats_path = output_path.replace(".csv", "_stats.csv")
            stats_df.to_csv(stats_path, index=False, encoding="utf-8-sig")
            self.logger.info(f"绩效统计已保存至: {stats_path}")

    def run_analysis(self, show_absolute_value=False):
        """运行完整分析"""
        self.logger.info("=" * 60)
        self.logger.info("开始ETF轮动策略组合净值分析（含初始资金量计算）")
        self.logger.info("=" * 60)

        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)

        # 加载数据
        self.load_strategy_data()
        self.get_trading_days()
        self.load_nav_data()

        # 计算组合净值
        self.calculate_portfolio_nav()
        self.normalize_portfolio_nav()

        # 显示绩效统计
        stats = self.calculate_performance_stats()
        if stats:
            self.logger.info("\n" + "=" * 60)
            self.logger.info("绩效统计:")
            self.logger.info("=" * 60)
            for key, value in stats.items():
                self.logger.info(f"{key}: {value}")

        # 绘制净值曲线
        plot_path = os.path.join(self.output_dir, "portfolio_nav_curve.png")
        self.plot_nav_curve(save_path=plot_path, show_absolute_value=show_absolute_value)

        # 保存结果
        results_path = os.path.join(self.output_dir, "portfolio_nav_results.csv")
        self.save_results(results_path)

        self.logger.info("=" * 60)
        self.logger.info("分析完成!")
        self.logger.info("=" * 60)

        return self.portfolio_nav, stats


def main():
    """主函数"""
    # 创建计算器实例
    calculator = ETFPortfolioCalculator(
        strategy_file_path="/home/haris/project/etf/data/ETF轮动策略底层3-GF.xlsx",
        nav_data_path="/home/haris/project/etf/data/daily_nav",
        commission_rate=0.0,  # 手续费率设为0.0
        init_capital=1e8,  # 1亿初始资金
    )

    # 运行分析
    portfolio_nav, stats = calculator.run_analysis(show_absolute_value=False)  # 显示标准化净值曲线

    return calculator, portfolio_nav, stats


if __name__ == "__main__":
    calculator, portfolio_nav, stats = main()
