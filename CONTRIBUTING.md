# CONTRIBUTING.md

感谢你对本项目的关注与贡献！本指南说明怎样高效、规范地为本仓库提交 issue、功能建议、代码改动与文档更新。请在贡献前阅读，能大幅加快合并流程并减少往返沟通。

---

# 贡献总览 🚀

* 报告 Bug → 使用 `Issues`（清晰复现步骤 + 最小示例）
* 提出新功能或改进 → 使用 `Issues` 并标注 `enhancement`
* 代码变更 → 发起 Pull Request（PR）
* 文档改进 → 直接提交 PR（若只改文档，可在 PR 中简短说明）
* 大型/破坏性改动 → 先在 Issue 里讨论设计方案

---

# 提交 Issue（问题与建议）🐞

请遵循以下要点，便于维护者快速定位问题或评估建议：

1. 标题要简洁、准确。示例：`backtest: 无法识别 scores_path 参数`
2. 描述必须包含：

   * 复现步骤（最好能贴出命令行、运行环境、最小可复现代码或数据示例）
   * 预期行为与实际行为
   * 错误日志/Traceback（若有，请贴完整关键部分）
   * Python 版本、主要依赖版本（pandas/numpy/cvxpy 等）
3. 若涉及数据或结果，尽量提供匿名或合成的小样本数据。
4. 对于新功能建议，说明动机、预期设计、兼容性影响与替代方案。

---

# 提交代码（Pull Request）🔧

## 分支与提交规范

* 从 `main` 创建分支，分支命名规则：`feat/描述`、`fix/描述`、`refactor/描述`、`doc/描述`。示例：`feat/args-config-injection`。
* 每个 PR 应专注于单一目的（bug 修复或单个功能改进）。
* 使用清晰的 commit 信息，推荐使用 [Conventional Commits] 风格：

  * `feat: 添加 args 支持覆盖 config.SCORES_PATH`
  * `fix: 修复 http2 配置设置错误`
  * `docs: 更新 README 中的运行示例`
* 代码应尽量原子化，便于 review。

## 代码风格与质量

* Python 版本：**3.12**
* 格式化工具：**black**（请在提交前运行）
* import 排序：**isort**
* 静态检查：**flake8**（建议通过 CI 检查）
* 类型注解：代码中应尽可能添加类型注解（PEP 484）
* 函数/类需有 docstring（简要说明输入、输出与副作用）
* 遵循现有代码风格，变量命名语义化、避免魔法数字

## 测试

* 若改动影响计算逻辑或关键函数，请添加或修改单元测试（优先使用 pytest）。
* 在本地运行全部测试并确保通过后再提交 PR：

  ```bash
  pytest -q
  ```
* CI（若配置）会在 PR 时自动运行测试与静态检查，请依据 CI 报告修正问题。

## PR 描述模板（建议）

PR 标题遵循 commit 风格。PR 描述至少包含：

* 变更摘要（简要）
* 解决的 Issue（若有，写 `Closes #<issue>`）
* 变更类型：`bugfix` / `feature` / `breaking`
* 回归风险与影响范围（如需迁移/数据格式变化）
* 本地自测步骤与结果（包括关键命令）
* 依赖变更（新增/更新第三方库）

示例：

```
feat: 支持通过 --scores_path 覆盖 config.SCORES_PATH

Closes #12

变更摘要：
- 在 config.py 添加 update_from_args
- 在 run.py 中解析 args 并注入到 config

本地测试：
python run.py --scores_path /path/to/scores.csv
结果：成功加载并生成 /results/backtests/<strategy>.html/.pdf
```

---

# 本地开发与运行 🔁

建议开发流程：

1. 创建并激活虚拟环境（推荐 venv 或 conda）

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. 在 `config.py` 中设置本地数据路径（或使用命令行参数注入）
3. 运行回测示例：

   ```bash
   python run.py --scores_path "/home/haris/results/predictions/StockPredictor_20251119_043804_combined_predictions.csv"
   ```
4. 运行测试：

   ```bash
   pytest -q
   ```

---

# 数据与隐私注意事项 🔒

* 请勿提交包含敏感或受限数据（公司私有数据、个人身份信息、API key 等）。
* 若需要说明数据结构，请使用小型合成样本或把示例数据做脱敏处理。
* 对于可能暴露数据源路径的改动，请确保路径为示例或相对路径。

---

# CI / 自动化建议（供维护者参考）⚙️

在仓库推荐配置（若尚未配置）：

* GitHub Actions：

  * job: format（black, isort）
  * job: lint（flake8）
  * job: test（pytest）
* PR 合并时启用 `Require status checks to pass`

---

# 代码审查与合并流程 ✅

* 每个 PR 至少需要一位审阅者批准。
* 审阅关注点：

  * 功能正确性与边界条件
  * 可读性与文档齐全性
  * 单元测试覆盖关键逻辑
  * 无明显性能/资源泄露问题
* 通过后由维护者合并；合并时建议使用“Squash and merge”以保持 main 的整洁提交历史（依据项目偏好）。

---

# 常见问题 Q&A ❓

* Q：如何将命令行参数注入 config？
  A：在 `run.py` 中使用 `argparse` 解析参数，并调用 `config.update_from_args(args)` 或者将参数传入 `run_backtest(args)`。推荐做法是让 `config` 提供覆盖函数（见 README 示例）。

* Q：提交大文件导致 push 失败怎么办？
  A：请使用 Git LFS 或把大文件上传到外部存储并在 repo 中放示例/小样本。

---

# 联系方式与贡献声明 🤝

欢迎通过 Issues 提问或提交 PR。对重大设计变更请提前在 Issue 中讨论以获得维护者意见。感谢你的贡献！

---

如果你希望，我可以直接生成一个 `PULL_REQUEST_TEMPLATE.md` 与 `ISSUE_TEMPLATE.md`（包含上述必填字段），或把上述 CI/格式化脚本（black/isort/flake8 配置、GitHub Actions）也一并帮你写好，告诉我你想要哪些自动化工具，我会直接给出文件内容。
