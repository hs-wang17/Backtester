import os
import polars as pl


PATH_DAILY_DATA = "/mnt/raid0/nfs_readonly/data_frames"
PATH_FD_DATA = "/mnt/raid0/user_data/data_frames"
PATH_FAC_ROOT = "/mnt/raid0/user_data/DailyFactors/barra"
FAC_CLASS_NAMES = ""
FEATHER_INDEX_NAME = "__index_level_0__"
PARAM_LIST1 = ""
PARAM_LIST2 = ""
PARAM_LIST3 = ""
PARAM_LIST4 = ""
MV_NAME_LIST = ""
PB_NAME_LIST = ""
INDEX_LIST = ""
BASEFACTOR2SHEET = ""
MYSQL_URI = "mysql://readonly_user:readonly_user@119.253.67.3:3306/mydb"


def load_id_dict() -> dict[str, str]:
    """
    Load PARTY_ID -> TICKER_SYMBOL mapping from MySQL.

    This is intentionally lazy (not executed at import time) so unit tests and
    offline workflows don't require network/DB access.
    """
    id_symbol = pl.read_database_uri(
        "select PARTY_ID,TICKER_SYMBOL from md_security where (EXCHANGE_CD='XSHG' or EXCHANGE_CD='XSHE') and ASSET_CLASS='E' and (LEFT(TICKER_SYMBOL,1)='0' or LEFT(TICKER_SYMBOL,1)='3' or LEFT(TICKER_SYMBOL,1)='6')",
        uri=MYSQL_URI,
        engine="connectorx",
    )
    return dict(zip(id_symbol["PARTY_ID"], id_symbol["TICKER_SYMBOL"]))


ID_DICT: dict[str, str] = (
    load_id_dict() if os.getenv("BARRA_LOAD_ID_DICT") == "1" else {}
)


tables = ["fdmt_cf_n_q_pit", "vw_fdmt_bs_new", "fdmt_is_n_q_pit"] + [
    "fdmt_der_ttm_pit",
    "fdmt_indi_ps_ttm_pit",
    "fdmt_indi_gh_ttm_pit",
    "fdmt_indi_rtn_ttmpit",
    "fdmt_indi_trnovr_ttm_pit",
    "fdmt_indi_lqd_ttmpit",
    "fdmt_indi_cashttmpit",
]

support_cols = [
    "id",
    "party_id",
    "symbol",
    "exchange_cd",
    "act_pubtime",
    "publish_date",
    "end_date_rep",
    "end_date",
    "report_type",
    "fiscal_period",
    "merged_flag",
    "accouting_standards",
    "currency_cd",
    "industry_category",
    "update_time",
    "tmstamp",
    "is_calc",
    "is_new",
]

valid_cols_bs = [
    "cash_c_equiv",
    "trading_fa",
    "nr_ar",
    "notes_receiv",
    "ar",
    "prepayment",
    "oth_receiv_total",
    "int_receiv",
    "div_receiv",
    "oth_receiv",
    "inventories",
    "nca_within_1y",
    "oth_ca",
    "t_ca",
    "avail_for_sale_fa",
    "lt_receiv",
    "lt_equity_invest",
    "invest_real_estate",
    "fixed_assets_total",
    "fixed_assets",
    "fixed_assets_disp",
    "cip_total",
    "cip",
    "const_materials",
    "intan_assets",
    "r_d",
    "goodwill",
    "lt_amor_exp",
    "defer_tax_assets",
    "oth_nca",
    "t_nca",
    "t_assets",
    "st_borr",
    "trading_fl",
    "np_ap",
    "notes_payable",
    "ap",
    "advance_receipts",
    "payroll_payable",
    "taxes_payable",
    "oth_payable_total",
    "int_payable",
    "div_payable",
    "oth_payable",
    "ncl_within_1y",
    "oth_cl",
    "t_cl",
    "lt_borr",
    "bond_payable",
    "lt_payable_total",
    "lt_payable",
    "specific_payables",
    "estimated_liab",
    "defer_revenue",
    "defer_tax_liab",
    "oth_ncl",
    "t_ncl",
    "t_liab",
    "paid_in_capital",
    "capital_reser",
    "treasury_share",
    "oth_compre_income",
    "surplus_reser",
    "retained_earnings",
    "t_equity_attr_p",
    "minority_int",
    "t_sh_equity",
    "t_liab_equity",
]

valid_cols_cf = [
    "c_fr_sale_g_s",
    "refund_of_tax",
    "c_fr_oth_operate_a",
    "c_inf_fr_operate_a",
    "c_paid_g_s",
    "c_paid_to_for_empl",
    "c_paid_for_taxes",
    "c_paid_for_oth_op_a",
    "c_outf_operate_a",
    "n_cf_operate_a",
    "proc_sell_invest",
    "gain_invest",
    "disp_fix_assets_oth",
    "n_disp_subs_oth_biz_c",
    "c_fr_oth_invest_a",
    "c_inf_fr_invest_a",
    "pur_fix_assets_oth",
    "c_paid_invest",
    "n_c_paid_acquis",
    "c_paid_oth_invest_a",
    "c_outf_fr_invest_a",
    "n_cf_fr_invest_a",
    "c_fr_cap_contr",
    "c_fr_mino_s_subs",
    "c_fr_borr",
    "c_fr_oth_finan_a",
    "c_inf_fr_finan_a",
    "c_paid_for_debts",
    "c_paid_div_prof_int",
    "div_prof_subs_mino_s",
    "c_paid_oth_finan_a",
    "c_outf_fr_finan_a",
    "n_cf_fr_finan_a",
    "forex_effects",
    "n_change_in_cash",
    "n_ce_beg_bal",
    "n_ce_end_bal",
]

valid_cols_is = [
    "t_revenue",
    "revenue",
    "t_cogs",
    "cogs",
    "biz_tax_surchg",
    "sell_exp",
    "admin_exp",
    "r_d_exp",
    "finan_exp",
    "invest_income",
    "a_j_invest_income",
    "f_value_chg_gain",
    "assets_impair_loss",
    "operate_profit",
    "noperate_income",
    "noperate_exp",
    "nca_disploss",
    "t_profit",
    "income_tax",
    "n_income",
    "n_income_attr_p",
    "minority_gain",
    "oth_compr_income",
    "t_compr_income",
    "compr_inc_attr_p",
    "compr_inc_attr_m_s",
]

valid_cols_der = [
    "t_fixed_assets",
    "int_free_cl",
    "int_free_ncl",
    "int_cl",
    "int_debt",
    "n_debt",
    "n_tan_assets",
    "work_capital",
    "n_work_capital",
    "ic",
    "t_re",
    "gross_profit",
    "opa_profit",
    "val_chg_profit",
    "n_int_exp",
    "ebit",
    "ebitda",
    "ebiat",
    "nr_profit_loss",
    "ni_attr_p_cut",
    "fcff",
    "fcfe",
    "da",
]

valid_cols_ps = [
    "eps",
    "t_rev_ps",
    "rev_ps",
    "op_ps",
    "ebit_ps",
    "n_cf_oper_a_ps",
    "n_c_in_cash_ps",
    "fcff_ps",
    "fcfe_ps",
]

valid_cols_gh = [
    "t_revenue_yoy",
    "revenue_yoy",
    "oper_profit_yoy",
    "t_profit_yoy",
    "ni_yoy",
    "ni_attr_p_yoy",
    "ni_attr_p_cut_yoy",
    "roe_yoy",
    "n_cf_opa_yoy",
    "n_cf_opa_ps_yoy",
]

valid_cols_rtn = [
    "gross_margin",
    "np_margin",
    "roe",
    "roe_a",
    "roe_cut",
    "roa",
    "roa_ebit",
    "roic",
]

valid_cols_trnovr = [
    "fa_turnover",
    "tfa_turnover",
    "ca_turnover",
    "ta_turnover",
    "inven_turnover",
    "days_inven",
    "ar_turnover",
    "days_ar",
    "oper_cycle",
    "ap_turnover",
    "days_ap",
]

valid_cols_lqd = [
    "op_cl",
    "op_tl",
    "ebitda_tl",
    "ebitda_id",
    "n_cf_opa_cl",
    "n_cf_opa_liab",
    "n_cf_opa_id",
    "n_cf_opa_nd",
    "n_cf_opa_ncl",
    "n_cf_nfa_cl",
    "n_cf_nfa_liab",
    "times_inte_ebit",
    "times_inte_ebitda",
    "times_inte_cf",
]

valid_cols_cash = [
    "ar_r",
    "adv_r_r",
    "cfsgs_r",
    "n_cf_opa_tr",
    "n_cf_opa_r",
    "n_cf_opa_opap",
    "n_cf_opa_op",
    "p_fixa_o_da",
    "c_rcvry_a",
    "n_cf_opa_propt",
    "n_cf_ia_propt",
    "n_cf_fa_propt",
]

TABLES = [
    "vw_fdmt_bs_new",  # balance sheet pit
    "fdmt_cf_n_ttmp",  # cash flow ttm pit
    "fdmt_is_n_ttmp",  # income statement
    "mkt_div_yield",
    "con_sec_coredata",
    "con_sec_coredata_2",
    "con_sec_corederi",
    "con_sec_corederi_2",
    "equ_free_shares",
    "fdmt_main_data_pit",
    "fdmt_bs_n_qa_pit",
    "fdmt_md_n_ttmp",
    "fdmt_indi_ps_ttm_pit",
    "fdmt_indi_trnovr_ttm_pit",
    "fdmt_indi_rtn_ttmpit",
    "mkt_equd",
]

# Canonical (post-normalization) column requirements per fin table.
# Used by:
# - `get_fdmt_data_from_mysql(..., colnames=...)` to fetch a minimal subset
# - `tests/check_fin_data.py` to verify cached fin tables contain all columns needed
#   by factor calculations.
FIN_TABLE_REQUIRED_COLS: dict[str, list[str]] = {
    # market tables
    "mkt_div_yield": ["sec_id", "trade_date", "div_rate_l12m"],
    "mkt_equd": ["sec_id", "trade_date", "pe"],
    # consensus tables
    "con_sec_coredata": ["sec_id", "rep_fore_date", "fore_year", "con_eps"],
    "con_sec_coredata_2": [
        "sec_id",
        "rep_fore_date",
        "fore_year",
        "con_profit",
        "con_income",
        "con_div",
        "con_oc",
        "con_ocf",
        "con_na",
        "con_ebt",
        "con_oprofit",
    ],
    "con_sec_corederi": ["sec_id", "rep_fore_date", "fore_year", "con_profit_cgr2y"],
    "con_sec_corederi_2": ["sec_id", "rep_fore_date", "fore_year", "con_dyr"],
    # equity
    "equ_free_shares": ["sec_id", "publish_date", "free_shares"],
    # fundamentals
    "fdmt_main_data_pit": ["sec_id", "publish_date", "end_date", "int_debt", "da"],
    "fdmt_bs_n_qa_pit": [
        "sec_id",
        "publish_date",
        "end_date",
        "t_assets",
        "t_liab",
        "cash_c_equiv",
        "t_ncl",
        "t_sh_equity",
    ],
    "fdmt_cf_n_ttmp": [
        "sec_id",
        "publish_date",
        "end_date",
        "n_cf_operate_a",
        "n_cf_fr_invest_a",
        "n_cf_fr_finan_a",
    ],
    "fdmt_is_n_ttmp": [
        "sec_id",
        "publish_date",
        "end_date",
        "n_income",
        "revenue",
        "cogs",
    ],
    "fdmt_md_n_ttmp": [
        "sec_id",
        "publish_date",
        "end_date",
        "ebitda",
        "ebit",
        "cp_exp",
    ],
    "fdmt_indi_ps_ttm_pit": ["sec_id", "publish_date", "end_date", "eps", "rev_ps"],
    "fdmt_indi_trnovr_ttm_pit": ["sec_id", "publish_date", "ta_turnover"],
    "fdmt_indi_rtn_ttmpit": ["sec_id", "publish_date", "gross_margin", "roa"],
    # currently unused by factors, but kept in TABLES
    "vw_fdmt_bs_new": ["sec_id", "publish_date", "end_date"],
}

TABLE_2_COL = {
    "vw_fdmt_bs_new": valid_cols_bs,
    "fdmt_cf_n_q_pit": valid_cols_cf,
    "fdmt_is_n_q_pit": valid_cols_is,
    "fdmt_der_ttm_pit": valid_cols_der,
    "fdmt_indi_ps_ttm_pit": valid_cols_ps,
    "fdmt_indi_gh_ttm_pit": valid_cols_gh,
    "fdmt_indi_rtn_ttmpit": valid_cols_rtn,
    "fdmt_indi_trnovr_ttm_pit": valid_cols_trnovr,
    "fdmt_indi_lqd_ttmpit": valid_cols_lqd,
    "fdmt_indi_cashttmpit": valid_cols_cash,
    "fdmt_is_n_ttmp": ["PARTY_ID", "PUBLISH_DATE", "END_DATE", "REVENUE"],
}
