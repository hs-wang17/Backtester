import numpy as np
import pandas as pd
import traceback
from typing import Callable, Dict, Any, Tuple
from skopt import gp_minimize
from skopt.space import Real
from skopt.utils import use_named_args
from skopt.callbacks import CheckpointSaver
import json
from datetime import datetime
from param_manager import ParamManager
import config as config
import os


class HyperparameterOptimizer:
    def __init__(self, backtest_func: Callable, param_manager: ParamManager, n_calls: int = 50, n_random_starts: int = 10, random_state: int = 42):
        """
        Hyperparameter Optimizer using Bayesian Optimization.

        Args:
            backtest_func: backtest function
            param_manager: parameter manager
            n_calls: number of optimization calls
            n_random_starts: number of random starts
            random_state: random state for reproducibility
            penalty_weight_ret: penalty weight for return
            penalty_weight_std: penalty weight for standard deviation
            space: search space
            best_score: best score
            best_params: best parameters
            best_info: best information
            history: history of optimization
        """
        self.backtest_func = backtest_func
        self.param_manager = param_manager
        self.n_calls = n_calls
        self.n_random_starts = n_random_starts
        self.random_state = random_state
        self.penalty_weight_ret = 1.0e3
        self.penalty_weight_std = 5.0e2

        # define search space
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
        objective function for optimization.
        maximize information ratio while satisfying constraints.
        penalty terms ensure excess return does not decrease and excess std does not increase.
        Args:
            params: list of parameters
        Returns:
            float: information ratio
        """
        # map params to param dict
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
            # backtest with current parameters
            self.param_manager.set_params(param_dict)
            result = self.backtest_func()
            info = result["info"]
            ir = info.get("信息比率", 0)
            ex_ret = info.get("超额年化收益", 0)
            ex_std = info.get("超额年化波动", 0)
            if isinstance(ir, (pd.Series, pd.DataFrame)):
                ir = float(ir.iloc[0]) if len(ir) > 0 else 0
            if isinstance(ex_ret, (pd.Series, pd.DataFrame)):
                ex_ret = float(ex_ret.iloc[0]) if len(ex_ret) > 0 else 0
            if isinstance(ex_std, (pd.Series, pd.DataFrame)):
                ex_std = float(ex_std.iloc[0]) if len(ex_std) > 0 else 0
            ir = float(ir) if pd.notnull(ir) else 0
            ex_ret = float(ex_ret) if pd.notnull(ex_ret) else 0
            ex_std = float(ex_std) if pd.notnull(ex_std) else 0

            # history record
            record = {"params": param_dict.copy(), "ir": ir, "ex_ret": ex_ret, "ex_std": ex_std, "timestamp": datetime.now().isoformat()}
            self.history.append(record)

            # base score
            score = ir

            # check if best_info is available
            if self.best_info is not None and isinstance(self.best_info, dict) and len(self.best_info) > 0:
                # get base excess return and std
                base_ex_ret = self.best_info.get("超额年化收益", 0)
                base_ex_std = self.best_info.get("超额年化波动", 0)
                if isinstance(base_ex_ret, (pd.Series, pd.DataFrame)):
                    base_ex_ret = float(base_ex_ret.iloc[0]) if len(base_ex_ret) > 0 else 0
                if isinstance(base_ex_std, (pd.Series, pd.DataFrame)):
                    base_ex_std = float(base_ex_std.iloc[0]) if len(base_ex_std) > 0 else 0
                base_ex_ret = float(base_ex_ret) if pd.notnull(base_ex_ret) else 0
                base_ex_std = float(base_ex_std) if pd.notnull(base_ex_std) else 0

                # constrain 1: excess return does not decrease (penalty if decreases)
                if ex_ret < base_ex_ret:
                    penalty = (base_ex_ret - ex_ret) * self.penalty_weight_ret
                    score -= penalty
                # constrain 2: excess std does not increase (penalty if increases)
                if ex_std > base_ex_std:
                    penalty = (ex_std - base_ex_std) * self.penalty_weight_std
                    score -= penalty

            # update best if improved
            if score > self.best_score:
                self.best_score = score
                self.best_params = param_dict.copy()
                self.best_info = {
                    "信息比率": ir,
                    "超额年化收益": ex_ret,
                    "超额年化波动": ex_std,
                    "年化收益": float(info.get("年化收益", 0)) if pd.notnull(info.get("年化收益", 0)) else 0,
                    "最大回撤": float(info.get("最大回撤", 0)) if pd.notnull(info.get("最大回撤", 0)) else 0,
                }

                # save best params
                self.param_manager.save_params(
                    {
                        "信息比率": ir,
                        "超额年化收益": ex_ret,
                        "超额年化波动": ex_std,
                        "优化时间": datetime.now().isoformat(),
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

            # return negative score for minimization
            return -score

        except Exception as e:
            print(f"回测失败: {str(e)}")
            traceback.print_exc()
            return 1000.0

    def optimize(self):
        """conduct optimization using Bayesian Optimization"""
        print("开始贝叶斯优化...")
        print(f"参数空间: {[dim.name for dim in self.space]}")
        print(f"总迭代次数: {self.n_calls}")
        print(f"随机初始点: {self.n_random_starts}")
        print("=" * 60)

        # define the objective function with named args
        @use_named_args(self.space)
        def wrapped_objective(**params):
            # param_list = [params[name] for name in sorted(params.keys())]
            param_list = [params[name] for name in params.keys()]
            return self.objective(param_list)

        # create checkpoint saver
        checkpoint_saver = CheckpointSaver("./checkpoint.pkl", store_objective=False)

        # run optimization
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

        # save history
        self.save_history()

        return result

    def save_history(self):
        """save optimization history to JSON file"""
        history_file = os.path.join(config.BASE_DIR, f"results/optimization_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(self.history, f, indent=4, ensure_ascii=False)
        print(f"优化历史已保存到 {history_file}")

    def get_best_result(self) -> Dict[str, Any]:
        """get the best optimization result"""
        return {"score": self.best_score, "params": self.best_params, "info": self.best_info}
