import json
import os
from dataclasses import dataclass, asdict
from typing import Dict, Any
import numpy as np


@dataclass
class HyperParams:
    """超参数类"""

    CITIC_LIMIT: float = 0.06  # 行业限制 (范围0-2)
    CMVG_LIMIT: float = 0.2  # 市值限制 (范围0-2)
    STK_HOLD_LIMIT: float = 0.0106  # 个股持仓限制 (范围0-0.02)
    OTHER_LIMIT: float = 1.08  # 其他指标限制 (范围0-2)
    STK_BUY_R: float = 0.0072  # 个股买入比例 (范围0-0.02)
    TURN_MAX: float = 0.09  # 个股最大买入比例 (范围0-0.2)
    MEM_HOLD: float = 0.0  # 成员股持仓限制 (范围0-0.4)


class ParamManager:
    def __init__(self, param_file="optimal_params.json"):
        self.param_file = param_file
        self.params = HyperParams()
        self.param_ranges = {
            "CITIC_LIMIT": (0.0, 2.0),
            "CMVG_LIMIT": (0.0, 2.0),
            "STK_HOLD_LIMIT": (0.0, 0.02),
            "OTHER_LIMIT": (0.0, 2.0),
            "STK_BUY_R": (0.0, 0.02),
            "TURN_MAX": (0.0, 0.2),
            "MEM_HOLD": (0.0, 0.4),
        }

    def set_params(self, params_dict: Dict[str, float]):
        """设置参数"""
        for key, value in params_dict.items():
            if hasattr(self.params, key):
                setattr(self.params, key, value)

    def get_params(self) -> HyperParams:
        """获取当前参数"""
        return self.params

    def get_param_dict(self) -> Dict[str, float]:
        """获取参数字典"""
        return asdict(self.params)

    def save_params(self, info: Dict[str, Any] = None):
        """保存参数到文件"""
        data = {"params": self.get_param_dict(), "info": info or {}}
        with open(self.param_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"参数已保存到 {self.param_file}")

    def load_params(self):
        """从文件加载参数"""
        if os.path.exists(self.param_file):
            with open(self.param_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.set_params(data["params"])
            print(f"参数已从 {self.param_file} 加载")
            return data.get("info", {})
        return None

    def get_param_bounds(self) -> list:
        """获取参数边界"""
        bounds = []
        param_dict = self.get_param_dict()
        for param_name in param_dict.keys():
            bounds.append(self.param_ranges[param_name])
        return bounds

    def dict_to_vector(self, param_dict: Dict[str, float]) -> np.ndarray:
        """将字典转换为向量"""
        return np.array([param_dict[key] for key in sorted(param_dict.keys())])

    def vector_to_dict(self, vector: np.ndarray) -> Dict[str, float]:
        """将向量转换为字典"""
        keys = sorted(self.get_param_dict().keys())
        return {key: float(vector[i]) for i, key in enumerate(keys)}
