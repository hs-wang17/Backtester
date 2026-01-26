"""
Barra因子命名映射字典
数据库因子名 -> 本地因子名
"""

FACTOR_NAME_MAPPING = {
    # 完全匹配
    "BETA": "BETA",
    "GROWTH": "GROWTH",
    "LEVERAGE": "LEVERAGE",
    "MOMENTUM": "MOMENTUM",
    "SIZE": "SIZE",
    # 模糊匹配
    "DIVYILD": "DIVIDEND-YIELD",  # 相似度: 0.67
    "EARNQLTY": "EARNINGS-QUALITY",  # 相似度: 0.67
    "EARNYILD": "EARNINGS-YIELD",  # 相似度: 0.73
    "INVSQLTY": "INVESTMENT-QUALITY",  # 相似度: 0.62
    "LIQUIDTY": "LIQUIDITY",  # 相似度: 0.94
    "LTREVRSL": "LONG-TERM-REVERSAL",  # 相似度: 0.62
    "MIDCAP": "MID-CAP",  # 相似度: 0.92
    "PROFIT": "PROFITABILITY",  # 相似度: 0.63
    "BTOP": "BOOK-TO-PRICE",  # 相似度: 0.62
    "EARNVAR": "EARNINGS-VARIABILITY",  # 相似度: 0.62
    "RESVOL": "RESIDUAL-VOLATILITY",  # 相似度: 0.62
}

# 数据库因子列表
DB_FACTORS = [
    "ANALSENTI",
    "Agriculture",
    "Automobiles",
    "BETA",
    "BTOP",
    "Banks",
    "BasicChemicals",
    "BuildMater",
    "COUNTRY",
    "CateringTourism",
    "Coal",
    "Computers",
    "Conglomerates",
    "Construction",
    "ConsumerServices",
    "DIVYILD",
    "Defense",
    "DiverseFinan",
    "EARNQLTY",
    "EARNVAR",
    "EARNYILD",
    "ElectronicCompon",
    "Electronics",
    "FoodBeverages",
    "GROWTH",
    "HealthCare",
    "HomeAppliances",
    "ID",
    "INDMOM",
    "INVSQLTY",
    "LEVERAGE",
    "LIQUIDTY",
    "LTREVRSL",
    "LightIndustry",
    "MIDCAP",
    "MOMENTUM",
    "Machinery",
    "Media",
    "NonbankFinan",
    "NonferrousMetals",
    "PROFIT",
    "Petroleum",
    "PowerEquip",
    "PowerEquipNewEnergy",
    "PowerUtilities",
    "RESVOL",
    "RealEstate",
    "RetailTrade",
    "SEASON",
    "SIZE",
    "STREVRSL",
    "Steel",
    "Telecoms",
    "TextileGarment",
    "Transportation",
    "UPDATE_TIME",
]

# 本地因子列表
LOCAL_FACTORS = [
    "BETA",
    "BOOK-TO-PRICE",
    "DIVIDEND-YIELD",
    "EARNINGS-QUALITY",
    "EARNINGS-VARIABILITY",
    "EARNINGS-YIELD",
    "GROWTH",
    "INVESTMENT-QUALITY",
    "LEVERAGE",
    "LIQUIDITY",
    "LONG-TERM-REVERSAL",
    "MID-CAP",
    "MOMENTUM",
    "PROFITABILITY",
    "RESIDUAL-VOLATILITY",
    "SIZE",
]

# 数据库中未映射的因子
UNMAPPED_DB_FACTORS = [
    "ANALSENTI",
    "Agriculture",
    "Automobiles",
    "Banks",
    "BasicChemicals",
    "BuildMater",
    "COUNTRY",
    "CateringTourism",
    "Coal",
    "Computers",
    "Conglomerates",
    "Construction",
    "ConsumerServices",
    "Defense",
    "DiverseFinan",
    "ElectronicCompon",
    "Electronics",
    "FoodBeverages",
    "HealthCare",
    "HomeAppliances",
    "ID",
    "INDMOM",
    "LightIndustry",
    "Machinery",
    "Media",
    "NonbankFinan",
    "NonferrousMetals",
    "Petroleum",
    "PowerEquip",
    "PowerEquipNewEnergy",
    "PowerUtilities",
    "RealEstate",
    "RetailTrade",
    "SEASON",
    "STREVRSL",
    "Steel",
    "Telecoms",
    "TextileGarment",
    "Transportation",
    "UPDATE_TIME",
]

# 本地未映射的因子
UNMAPPED_LOCAL_FACTORS = []
