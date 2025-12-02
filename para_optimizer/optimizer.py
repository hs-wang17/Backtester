import numpy as np
import pandas as pd
from typing import Callable, Dict, Any, Tuple
from skopt import gp_minimize
from skopt.space import Real
from skopt.utils import use_named_args
from skopt.callbacks import CheckpointSaver
import json
from datetime import datetime
from param_manager import ParamManager


class HyperparameterOptimizer:
    def __init__(self, backtest_func: Callable, param_manager: ParamManager, n_calls: int = 50, n_random_starts: int = 10, random_state: int = 42):
        """
        超参数优化器

        Args:
            backtest_func: 回测函数，接受参数字典，返回回测结果字典
            param_manager: 参数管理器
            n_calls: 总优化次数
            n_random_starts: 随机初始点数量
            random_state: 随机种子
        """
        self.backtest_func = backtest_func
        self.param_manager = param_manager
        self.n_calls = n_calls
        self.n_random_starts = n_random_starts
        self.random_state = random_state

        # 定义搜索空间
        self.space = [
            Real(*param_manager.param_ranges["CITIC_LIMIT"], name="CITIC_LIMIT"),
            Real(*param_manager.param_ranges["CMVG_LIMIT"], name="CMVG_LIMIT"),
            Real(*param_manager.param_ranges["STK_HOLD_LIMIT"], name="STK_HOLD_LIMIT"),
            Real(*param_manager.param_ranges["OTHER_LIMIT"], name="OTHER_LIMIT"),
            Real(*param_manager.param_ranges["STK_BUY_R"], name="STK_BUY_R"),
            Real(*param_manager.param_ranges["TURN_MAX"], name="TURN_MAX"),
            Real(*param_manager.param_ranges["MEM_HOLD"], name="MEM_HOLD"),
        ]

        self.best_score = -np.inf
        self.best_params = None
        self.best_info = None
        self.history = []

    def objective(self, params: list) -> float:
        """
        目标函数：最大化信息比率，同时满足约束条件
        惩罚项确保超额收益不减小、超额波动不增大
        """
        # 将参数列表转换为字典
        param_dict = {
            "CITIC_LIMIT": params[0],
            "CMVG_LIMIT": params[1],
            "STK_HOLD_LIMIT": params[2],
            "OTHER_LIMIT": params[3],
            "STK_BUY_R": params[4],
            "TURN_MAX": params[5],
            "MEM_HOLD": params[6],
        }

        try:
            # 运行回测
            self.param_manager.set_params(param_dict)
            result = self.backtest_func()

            # 获取指标
            info = result["info"]
            ir = info.get("信息比率", 0)
            ex_ret = info.get("超额年化收益", 0)
            ex_std = info.get("超额年化波动", 0)

            # 确保指标是标量值，不是Series或其他类型
            if isinstance(ir, (pd.Series, pd.DataFrame)):
                ir = float(ir.iloc[0]) if len(ir) > 0 else 0
            if isinstance(ex_ret, (pd.Series, pd.DataFrame)):
                ex_ret = float(ex_ret.iloc[0]) if len(ex_ret) > 0 else 0
            if isinstance(ex_std, (pd.Series, pd.DataFrame)):
                ex_std = float(ex_std.iloc[0]) if len(ex_std) > 0 else 0

            ir = float(ir) if pd.notnull(ir) else 0
            ex_ret = float(ex_ret) if pd.notnull(ex_ret) else 0
            ex_std = float(ex_std) if pd.notnull(ex_std) else 0

            # 记录历史
            record = {"params": param_dict.copy(), "ir": ir, "ex_ret": ex_ret, "ex_std": ex_std, "timestamp": datetime.now().isoformat()}
            self.history.append(record)

            # 基础分数
            score = ir

            # 添加约束条件惩罚项（软约束）
            # 先检查 self.best_info 是否存在且不是空的
            if self.best_info is not None and isinstance(self.best_info, dict) and len(self.best_info) > 0:
                # 获取基准值，确保是标量
                base_ex_ret = self.best_info.get("超额年化收益", 0)
                base_ex_std = self.best_info.get("超额年化波动", 0)

                # 确保基准值也是标量
                if isinstance(base_ex_ret, (pd.Series, pd.DataFrame)):
                    base_ex_ret = float(base_ex_ret.iloc[0]) if len(base_ex_ret) > 0 else 0
                if isinstance(base_ex_std, (pd.Series, pd.DataFrame)):
                    base_ex_std = float(base_ex_std.iloc[0]) if len(base_ex_std) > 0 else 0

                base_ex_ret = float(base_ex_ret) if pd.notnull(base_ex_ret) else 0
                base_ex_std = float(base_ex_std) if pd.notnull(base_ex_std) else 0

                # 约束1：超额收益不减小（如果减小则惩罚）
                if ex_ret < base_ex_ret:
                    penalty = (base_ex_ret - ex_ret) * 10
                    score -= penalty

                # 约束2：超额波动不增大（如果增大则惩罚）
                if ex_std > base_ex_std:
                    penalty = (ex_std - base_ex_std) * 5
                    score -= penalty

            # 更新最佳结果 - 简化逻辑，只存储标量值
            if ir > self.best_score:
                self.best_score = ir
                self.best_params = param_dict.copy()

                # 只存储标量值，不存储可能包含Series的info字典
                self.best_info = {
                    "信息比率": ir,
                    "超额年化收益": ex_ret,
                    "超额年化波动": ex_std,
                    # 可以添加其他需要的标量指标
                    "年化收益": float(info.get("年化收益", 0)) if pd.notnull(info.get("年化收益", 0)) else 0,
                    "最大回撤": float(info.get("最大回撤", 0)) if pd.notnull(info.get("最大回撤", 0)) else 0,
                }

                # 保存最佳参数
                self.param_manager.save_params(
                    {
                        "信息比率": ir,
                        "超额年化收益": ex_ret,
                        "超额年化波动": ex_std,
                        "优化时间": datetime.now().isoformat(),
                        # 只保存标量值
                        **{
                            k: float(v) if isinstance(v, (int, float, np.number)) else str(v)
                            for k, v in info.items()
                            if not isinstance(v, (pd.Series, pd.DataFrame, dict, list))
                        },
                    }
                )

            print(f"参数: {param_dict}")
            print(f"信息比率: {ir:.4f}, 超额收益: {ex_ret:.4f}, 超额波动: {ex_std:.4f}, 分数: {score:.4f}")
            print("-" * 50)

            # 返回负分数用于最小化
            return -score

        except Exception as e:
            print(f"回测失败: {str(e)[:200]}")  # 只显示前200个字符
            import traceback

            traceback.print_exc()  # 打印完整的错误堆栈
            # 返回一个很差的值
            return 1000.0

    def optimize(self):
        """执行贝叶斯优化"""
        print("开始贝叶斯优化...")
        print(f"参数空间: {[dim.name for dim in self.space]}")
        print(f"总迭代次数: {self.n_calls}")
        print(f"随机初始点: {self.n_random_starts}")
        print("=" * 60)

        # 定义带命名参数的包装函数
        @use_named_args(self.space)
        def wrapped_objective(**params):
            param_list = [params[name] for name in sorted(params.keys())]
            return self.objective(param_list)

        # 创建检查点保存回调
        checkpoint_saver = CheckpointSaver("./checkpoint.pkl", store_objective=False)

        # 执行贝叶斯优化
        result = gp_minimize(
            func=wrapped_objective,
            dimensions=self.space,
            n_calls=self.n_calls,
            n_random_starts=self.n_random_starts,
            random_state=self.random_state,
            callback=[checkpoint_saver],
            verbose=True,
        )

        print("\n" + "=" * 60)
        print("优化完成！")
        print(f"最佳信息比率: {self.best_score:.4f}")
        print(f"最佳参数: {self.best_params}")

        # 保存优化历史
        self.save_history()

        return result

    def save_history(self):
        """保存优化历史"""
        history_file = f"optimization_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(self.history, f, indent=4, ensure_ascii=False)
        print(f"优化历史已保存到 {history_file}")

    def get_best_result(self) -> Dict[str, Any]:
        """获取最佳结果"""
        return {"score": self.best_score, "params": self.best_params, "info": self.best_info}
