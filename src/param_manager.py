import json
import os
from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass
class HyperParams:
    CITIC_LIMIT: float = 0.06  # 行业限制 (范围0-2)
    CMVG_LIMIT: float = 0.2  # 市值限制 (范围0-2)
    STK_HOLD_LIMIT: float = 0.0106  # 个股持仓限制 (范围0-0.02)
    OTHER_LIMIT: float = 1.08  # 其他指标限制 (范围0-2)
    STK_BUY_R: float = 0.0072  # 个股买入比例 (范围0.001-0.02)
    TURN_MAX: float = 0.09  # 个股最大买入比例 (范围0.03-0.2)
    MEM_HOLD: float = 0.0  # 成员股持仓限制 (范围0-0.4)


class ParamManager:
    """hyperparameter manager for portfolio optimization"""

    def __init__(self, param_file: str = None):
        import src.config as config

        self.param_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../para_optimizer_gp/results/",
            f"optimal_params_trade_support{config.TRADE_SUPPORT}.json"
        )
        self.param_file = os.path.abspath(self.param_file)

        self.params = HyperParams()
        self.eps = 1e-8
        if config.TRADE_SUPPORT == 5:
            self.param_ranges = {
                "CITIC_LIMIT": (self.eps, 2.0 - self.eps),
                "CMVG_LIMIT": (self.eps, 2.0 - self.eps),
                "STK_HOLD_LIMIT": (0.001 + self.eps, 0.02 - self.eps),
                "OTHER_LIMIT": (self.eps, 2.0 - self.eps),
                "STK_BUY_R": (0.001 + self.eps, 0.02 - self.eps),
                "TURN_MAX": (0.03 + self.eps, 0.2 - self.eps),
                "MEM_HOLD": (self.eps, 0.4 - self.eps),
            }
        elif config.TRADE_SUPPORT == 7:
            self.param_ranges = {
                "CITIC_LIMIT": (self.eps, 0.5 - self.eps),
                "CMVG_LIMIT": (self.eps, 0.5 - self.eps),
                "STK_HOLD_LIMIT": (0.001 + self.eps, 0.02 - self.eps),
                "OTHER_LIMIT": (self.eps, 0.5 - self.eps),
                "STK_BUY_R": (0.001 + self.eps, 0.02 - self.eps),
                "TURN_MAX": (0.03 + self.eps, 0.2 - self.eps),
                "MEM_HOLD": (self.eps, 0.4 - self.eps),
            }

    def set_params(self, params_dict: Dict[str, float]):
        for key, value in params_dict.items():
            if hasattr(self.params, key):
                setattr(self.params, key, value)

    def get_param_dict(self) -> Dict[str, float]:
        return asdict(self.params)

    def save_params(self, info: Dict[str, Any] = None):
        data = {"params": self.get_param_dict(), "info": info or {}}
        file_dir = os.path.dirname(self.param_file)
        if file_dir and not os.path.exists(file_dir):
            os.makedirs(file_dir, exist_ok=True)
        with open(self.param_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"参数已保存到 {self.param_file}")

    def load_params(self):
        if os.path.exists(self.param_file):
            with open(self.param_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.set_params(data["params"])
            print(f"参数已从 {self.param_file} 加载")
            return data.get("info", {})
        return None
