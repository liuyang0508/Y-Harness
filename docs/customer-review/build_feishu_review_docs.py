#!/usr/bin/env python3
"""Create the five Beijing Kede aquaculture client-review documents in Feishu."""

from __future__ import annotations

import json
import re
import subprocess
from collections import OrderedDict


FOLDER_TOKEN = "XHzlfrMcdl0g3idtnpfcVd26nUp"


def run_cli(args: list[str]) -> dict:
    proc = subprocess.run(
        ["lark-cli", *args],
        check=False,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(args[:3])}\n{proc.stdout}\n{proc.stderr}")
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"non-json output: {proc.stdout}\n{proc.stderr}") from exc
    if not payload.get("ok"):
        raise RuntimeError(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def create_document(title: str, sections: OrderedDict[str, str]) -> dict:
    skeleton = "\n".join(f"<h1>{heading}</h1>" for heading in sections)
    created = run_cli(
        [
            "docs",
            "+create",
            "--as",
            "user",
            "--title",
            title,
            "--parent-token",
            FOLDER_TOKEN,
            "--content",
            skeleton,
        ]
    )
    data = created["data"]
    doc = data.get("document", data)
    doc_id = doc.get("document_id") or doc.get("doc_token") or doc.get("token")
    url = doc.get("url") or f"https://my.feishu.cn/docx/{doc_id}"
    if not doc_id:
        match = re.search(r"/docx/([A-Za-z0-9]+)", url or "")
        if match:
            doc_id = match.group(1)
    if not doc_id:
        raise RuntimeError(f"cannot locate document id: {created}")

    outline = run_cli(
        [
            "docs",
            "+fetch",
            "--as",
            "user",
            "--doc",
            doc_id,
            "--scope",
            "outline",
            "--detail",
            "with-ids",
            "--max-depth",
            "1",
        ]
    )
    content = outline["data"]["document"]["content"]
    heading_ids = {
        re.sub(r"<[^>]+>", "", label): block_id
        for block_id, label in re.findall(r'<h1 id="([^"]+)">(.*?)</h1>', content, flags=re.S)
    }
    for heading, fragment in sections.items():
        block_id = heading_ids.get(heading)
        if not block_id:
            raise RuntimeError(f"heading id missing: {heading} in {title}")
        run_cli(
            [
                "docs",
                "+update",
                "--as",
                "user",
                "--doc",
                doc_id,
                "--command",
                "block_insert_after",
                "--block-id",
                block_id,
                "--content",
                fragment,
            ]
        )
    return {"title": title, "document_id": doc_id, "url": url}


BUSINESS = OrderedDict(
    {
        "1. 文档定位与评审结论": """
<callout emoji="📌" background-color="light-blue" border-color="blue">
  <p><b>评审结论：</b>现有会议纪要、业务资料、思维导图、字段草案和 IoT 截图，已经足以确定产品方向、业务闭环和 POC 范围；尚不足以冻结生产接口、容量和诊断准确率。本文用于业务对齐，不替代接口契约、技术设计和上线审批。</p>
</callout>
<table>
  <thead><tr><th background-color="light-gray">项目</th><th background-color="light-gray">内容</th></tr></thead>
  <tbody>
    <tr><td>文档状态</td><td>客户评审版 V1.0，日期 2026-08-02</td></tr>
    <tr><td>已核验输入</td><td>2026-07-29 腾讯会议纪要与转写；40 个知识文件；18,828 个思维导图节点；27 张业务字段工作表；16 张 IoT 页面截图</td></tr>
    <tr><td>当前数据边界</td><td>尚无可持续调用的原始 IoT/ERP 数据接口；POC 采用契约驱动、可复现的受控 Mock，后续以真实样本替换</td></tr>
    <tr><td>方案主线</td><td>围绕“鱼—水—设备—人—任务—知识”形成从发现、诊断、处置、验证到沉淀的闭环</td></tr>
  </tbody>
</table>
<p>本文将“事实、建议、待确认”分开表达。任何来源于容量假设、接口假设或效果目标的内容，在客户确认前均不作为合同承诺。</p>
""",
        "2. 业务背景、问题与建设目标": """
<p>客户现有平台已覆盖 ERP、物联网、设备状态、报警与可视化，但数据、知识和任务仍分散在不同系统。现场人员面对“吃料下降、死亡上升、水质波动、设备异常”等问题时，需要在思维导图、SOP、论文、个人经验、会议结论和实时数据之间人工拼接，难以稳定复现诊断过程。</p>
<p>本项目不是建设一个通用聊天机器人，而是建设可追溯的智慧渔业业务智能体：它先确认用户、场区、车间、系统、池塘、批次和时间窗，再按业务问题调用知识、图谱、IoT、ERP 和任务工具，输出证据充分、边界明确的判断，并将处置结果回收为案例。</p>
<h2>2.1 核心业务痛点</h2>
<ul>
  <li>知识量大但格式不统一：38 万字思维导图、SOP、论文、经验和会议记录难以直接供一线检索。</li>
  <li>传统向量检索容易“语义相似但条件不匹配”，论文又可能陈旧或脱离当前鱼种、阶段和系统工况。</li>
  <li>生产判断依赖最近 3—5 天 IoT 趋势、ERP 事件和现场观察，单独看知识文档无法排除原因。</li>
  <li>语音或自然语言录入可降低现场成本，但写错池塘、字段、单位或时间会产生直接业务风险。</li>
  <li>会议、任务和效果数据没有稳定回流，导致同类问题重复讨论、经验无法跨周期复用。</li>
</ul>
<h2>2.2 建设目标</h2>
<ol>
  <li seq="auto">把企业知识转成可检索、可引用、可审核、可版本化的知识与诊断图谱。</li>
  <li seq="auto">把知识推理与池塘实时/历史数据结合，形成“候选原因—证据验证—处置建议”的诊断链路。</li>
  <li seq="auto">提供自然语言 BI、报告、任务、录入和会议沉淀能力，并保留每一步来源和回执。</li>
  <li seq="auto">通过评测、专家审核和效果对比持续校准，不允许系统未经治理自行修改生产规则。</li>
</ol>
""",
        "3. 产品范围、角色与路线": """
<h2>3.1 使用角色</h2>
<table>
  <thead><tr><th background-color="light-gray">角色</th><th background-color="light-gray">主要诉求</th><th background-color="light-gray">关键约束</th></tr></thead>
  <tbody>
    <tr><td>一线养殖人员</td><td>快速问答、语音录入、任务执行、现场复核</td><td>入口简单；默认继承当前池塘；写入前确认</td></tr>
    <tr><td>技术人员/专家</td><td>诊断、冲突裁决、SOP 与案例审核</td><td>能查看证据路径、适用条件和历史结果</td></tr>
    <tr><td>生产负责人</td><td>跨池塘 BI、风险排序、计划和复盘</td><td>指标口径统一；数据可对账</td></tr>
    <tr><td>知识管理员</td><td>知识摄取、版本、标签、审核与发布</td><td>发布与编辑分权；全程留痕</td></tr>
    <tr><td>平台/运维人员</td><td>接口、权限、模型、策略、告警和审计</td><td>租户隔离、可回滚、可观测</td></tr>
  </tbody>
</table>
<h2>3.2 产品路线</h2>
<table>
  <thead><tr><th background-color="light-gray">阶段</th><th background-color="light-gray">范围</th><th background-color="light-gray">退出条件</th></tr></thead>
  <tbody>
    <tr><td>P0 方案与契约</td><td>冻结实体、字段、接口、知识治理、黄金题和安全边界</td><td>五份方案评审通过；POC 输入齐备</td></tr>
    <tr><td>P1 隔离 POC</td><td>受控 Mock；诊断问答、BI、语音预览、会议/任务沉淀；首批知识图谱</td><td>端到端回放、拒答和引用指标达标</td></tr>
    <tr><td>P2 真实数据联调</td><td>接入脱敏 IoT/ERP 样本；只读查询；指标对账；专家校准</td><td>数据口径一致，真实样本回归通过</td></tr>
    <tr><td>P3 试运行</td><td>受控写入、审批、任务闭环、灰度用户与运维保障</td><td>SLO、审计、回滚、灾备演练通过</td></tr>
    <tr><td>P4 规模化</td><td>多场区、多批次、更多视觉/会议能力和经营分析</td><td>容量、成本和运营机制稳定</td></tr>
  </tbody>
</table>
<p><b>非一期范围：</b>未经审批的自动投药、自动设备控制、依据单一模型直接改变生产参数、无来源的“自我进化”、全量实时屏幕理解。</p>
""",
        "4. 业务架构与能力域": """
<whiteboard type="mermaid">
flowchart LR
  U[养殖人员 技术专家 管理者] --> C[Web 移动端 语音 会议]
  C --> A[智慧渔业智能体]
  A --> D[诊断与问答]
  A --> B[BI与智能报告]
  A --> R[结构化录入]
  A --> T[任务与计划]
  A --> K[知识与案例]
  D --> E[知识 诊断图谱 IoT ERP]
  B --> E
  R --> P[预览 审批 幂等写入]
  T --> F[执行 回执 效果对比]
  F --> K
  K --> G[审核 发布 评测]
</whiteboard>
<p>业务架构由六个能力域组成：智能诊断、生产分析、现场录入、任务协同、会议沉淀和知识治理。共同底座不是一个大知识库，而是统一的身份与作用域、业务实体、证据对象、任务状态和审核策略。</p>
<h2>4.1 六类问题的闭环解法</h2>
<table>
  <thead><tr><th background-color="light-gray">业务问题</th><th background-color="light-gray">解决机制</th><th background-color="light-gray">验收证据</th></tr></thead>
  <tbody>
    <tr><td>垂直智慧渔业知识</td><td>保真原文、语义 Claim、术语表、图谱和场景化 Skill；按鱼种、阶段、系统、指标过滤</td><td>答案引用到原文节点与适用条件</td></tr>
    <tr><td>来源权重与知识冲突</td><td>按场景匹配、生产验证、时效、可追溯、多源印证动态评分；冲突并列并触发审核</td><td>能解释为何采用或保留某条结论</td></tr>
    <tr><td>会议与知识结合</td><td>逐字稿提取决定、问题、任务和待验证结论；关联已有知识与池塘上下文；审核后发布</td><td>会议结论可追溯至原文和发布记录</td></tr>
    <tr><td>任务执行与效果对比</td><td>任务绑定池塘、问题、基线窗口、动作、负责人和观察窗口；执行后计算前后差异</td><td>任务有回执、有结果、有复盘</td></tr>
    <tr><td>IoT 与生产数据联动</td><td>基于作用域调用 3—5 天或业务指定窗口；对齐事件、单位和质量标记；确定性计算</td><td>数据可回查源系统并完成指标对账</td></tr>
    <tr><td>跨会议、跨任务案例沉淀</td><td>以问题—上下文—证据—动作—结果—复盘为 Case；审核后进入案例库与评测集</td><td>相似事件可复用且不越过权限边界</td></tr>
  </tbody>
</table>
""",
        "5. 全量业务旅程": """
<table>
  <thead><tr><th background-color="light-gray">编号</th><th background-color="light-gray">旅程</th><th background-color="light-gray">关键输入</th><th background-color="light-gray">业务输出</th><th background-color="light-gray">风险门禁</th></tr></thead>
  <tbody>
    <tr><td>AQ-JR-001</td><td>异常诊断与追问</td><td>问题、池塘、3—5 天 IoT、ERP 事件、现场观察</td><td>候选原因、证据、排除项、处置建议</td><td>缺池塘/关键数据则追问；高风险建议转专家</td></tr>
    <tr><td>AQ-JR-002</td><td>自然语言 BI</td><td>指标、范围、时间、口径</td><td>表格、图表、结论与口径说明</td><td>只调用白名单查询和确定性公式</td></tr>
    <tr><td>AQ-JR-003</td><td>智能报告</td><td>九大业务维度、日报/周报/月报模板</td><td>风险、经营、计划和异常摘要</td><td>数据缺失显式标注，不补造数值</td></tr>
    <tr><td>AQ-JR-004</td><td>语音/文本生产录入</td><td>身份、池塘、时间、字段、数值</td><td>结构化预览、确认、源系统回执</td><td>写入必须验证、确认、幂等和审计</td></tr>
    <tr><td>AQ-JR-005</td><td>会议分析</td><td>录制、逐字稿、纪要、参会人</td><td>决定、任务、争议、待验证知识</td><td>转写与屏幕内容分开；发布前审核</td></tr>
    <tr><td>AQ-JR-006</td><td>知识冲突处理</td><td>相互冲突的 Claim 与适用条件</td><td>差异解释、暂行结论、审核任务</td><td>不能消解时保留冲突，不强行统一</td></tr>
    <tr><td>AQ-JR-007</td><td>任务执行与效果复盘</td><td>诊断、动作、基线、观察窗口</td><td>任务、前后对比、复盘与案例</td><td>因果与相关分开表达</td></tr>
    <tr><td>AQ-JR-008</td><td>知识摄取与发布</td><td>文档、SOP、思维导图、经验、案例</td><td>版本化知识、图谱、索引与发布记录</td><td>来源、权限、审核和适用域齐备</td></tr>
  </tbody>
</table>
<p>上述八条旅程共享同一状态机和证据模型，不能分别建设成八个互不相通的“机器人”。首个演示故事线选 AQ-JR-001，但验收与架构覆盖全部旅程。</p>
""",
        "6. 关键业务链路": """
<h2>6.1 异常诊断链路</h2>
<whiteboard type="mermaid">
sequenceDiagram
  participant U as 使用者
  participant A as Agent
  participant C as 作用域服务
  participant K as 知识与图谱
  participant D as IoT与ERP
  participant E as 专家
  U->>A: 3号池吃料下降，怎么回事
  A->>C: 解析用户 池塘 批次 时间窗 权限
  C-->>A: 已确认作用域或歧义
  A->>K: 检索候选原因与判断依据
  A->>D: 查询近5天水质 投喂 死亡 设备事件
  K-->>A: 原因路径与知识证据
  D-->>A: 生产事实与质量标记
  A-->>U: 原因排序 排除项 缺口 建议与引用
  alt 高风险或证据冲突
    A->>E: 提交上下文包和证据摘要
    E-->>A: 审核结论
  end
</whiteboard>
<h2>6.2 语音录入链路</h2>
<whiteboard type="mermaid">
sequenceDiagram
  participant U as 一线人员
  participant A as Agent
  participant V as 校验与策略
  participant R as ERP
  U->>A: 我今天给3号池投喂80公斤
  A->>V: 解析身份 池塘 时间 数值 单位 字段
  V-->>A: 结构化预览与风险提示
  A-->>U: 展示待写入内容
  U->>A: 确认提交
  A->>R: 幂等写入
  R-->>A: 业务单号与版本回执
  A-->>U: 提交结果
</whiteboard>
<h2>6.3 会议—任务—知识闭环</h2>
<p>会议产物进入原始证据层后，系统提取决定、任务、争议、假设和待验证结论；每一项必须绑定来源段落、责任人、计划时间和关联对象。确定的任务进入任务系统，执行结果回收后形成 Case；未经审核的观点只作为候选，不直接发布为企业知识。会中共享屏幕若没有独立录屏或关键帧，不得假设逐字稿已经包含屏幕内容。</p>
""",
        "7. 运营治理、人工介入与责任": """
<h2>7.1 何时交由人工</h2>
<ul>
  <li>存在用药、停食、转鱼、设备控制、删除/修改生产记录等高影响动作。</li>
  <li>池塘、批次、单位、时间窗或用户权限不能唯一确定。</li>
  <li>关键传感器缺失、质量异常、数据新鲜度不足，无法支持结论。</li>
  <li>多个高分证据冲突，或建议超出已审核 SOP 的适用范围。</li>
  <li>疑似重大病害、高死亡、缺氧、停电等 P0/P1 事件。</li>
</ul>
<h2>7.2 人工接管包</h2>
<p>系统向专家或负责人提交的问题不能只是聊天文本，必须包含用户身份与权限、池塘/批次/时间窗、问题摘要、已调用工具、数据质量、候选原因、证据与冲突、建议动作、待裁决问题和 Trace ID。专家结论写回后保留审核人、时间、适用条件和版本。</p>
<h2>7.3 后管平台</h2>
<p>需要后管平台支撑知识摄取与发布、图谱校正、Skill 版本、策略阈值、工具权限、接口健康、评测集、问题回放和审计。专家负责知识与诊断规则；一线业务同学可维护术语、表单模板和低风险流程；平台管理员维护模型、连接器与权限。发布生产 Skill 或 Policy 必须经过评审和灰度。</p>
""",
        "8. 验收标准、客户输入与会议议程": """
<h2>8.1 POC 业务验收</h2>
<table>
  <thead><tr><th background-color="light-gray">维度</th><th background-color="light-gray">建议门槛</th><th background-color="light-gray">说明</th></tr></thead>
  <tbody>
    <tr><td>作用域正确</td><td>黄金题池塘/批次解析 100%</td><td>不确定时必须追问，不能猜池塘</td></tr>
    <tr><td>证据可追溯</td><td>事实性结论引用覆盖率不低于 95%</td><td>引用可回到原文或源系统查询</td></tr>
    <tr><td>数值正确</td><td>确定性指标与基准脚本一致率 100%</td><td>FCR、死亡率、存塘等不得由 LLM 心算</td></tr>
    <tr><td>诊断质量</td><td>Top-3 原因覆盖率目标不低于 85%</td><td>以客户专家共同标注题为准，POC 后冻结</td></tr>
    <tr><td>安全</td><td>越权与未确认写入 0 次</td><td>高风险用例必须拒绝或转人工</td></tr>
    <tr><td>可复现</td><td>同版本输入可回放</td><td>固定 Mock、知识、Skill、模型和工具版本</td></tr>
  </tbody>
</table>
<h2>8.2 客户下一批输入</h2>
<ul>
  <li>场区—车间—系统—池塘—批次—设备的主数据与唯一标识。</li>
  <li>IoT/ERP 接口说明、字段字典、单位、频率、时区、权限和脱敏规则。</li>
  <li>指标公式、阈值和版本；知识审核人、冲突裁决人和发布流程。</li>
  <li>20—50 个黄金问题、20—50 个真实口语问题、10 个必须拒答/转人工问题。</li>
  <li>部署网络、安全合规、模型出网、并发、数据量、RPO/RTO 和预算约束。</li>
</ul>
<h2>8.3 本次客户会议需要形成的决定</h2>
<ol>
  <li seq="auto">确认首批用户、场区/池塘范围和三条 POC 主链路。</li>
  <li seq="auto">确认 POC 接口最小集、接口责任人和样本交付日期。</li>
  <li seq="auto">确认知识审核人与指标口径负责人。</li>
  <li seq="auto">确认部署模式、模型数据边界和验收题组织方式。</li>
</ol>
""",
    }
)


INTERFACES = OrderedDict(
    {
        "1. 文档定位与接口结论": """
<callout emoji="❗" background-color="light-yellow" border-color="orange">
  <p>本文是客户技术团队的接口对接底稿。当前已确认“需要连接客户私域数据，可由客户提供 API/MCP/CLI，或由项目方基于客户数据源封装 Connector”；尚未拿到正式接口文档、连续 IoT 数据和 ERP 数据。未经本清单确认，生产方案不得直接读取主库，更不得写库。</p>
</callout>
<p>接口分为 POC 最小集和生产完整集。POC 可使用脱敏样本与 Mock；生产必须冻结认证、数据字典、主键、时区、幂等、错误码、审计和容量。</p>
""",
        "2. 交互全景与责任边界": """
<whiteboard type="mermaid">
flowchart LR
  UI[Web 移动端 语音 会议] --> GW[客户接入网关与SSO]
  GW --> AG[Agent平台]
  AG --> MCP[MCP与Connector网关]
  MCP --> IOT[IoT平台]
  MCP --> ERP[ERP生产系统]
  MCP --> ALM[报警与任务]
  MCP --> DOC[知识与会议平台]
  AG --> MG[模型网关]
  AG --> GOV[审核 审计 评测]
  IOT -->|只读事实| AG
  ERP -->|查询与受控写入回执| AG
</whiteboard>
<table>
  <thead><tr><th background-color="light-gray">责任方</th><th background-color="light-gray">主要责任</th></tr></thead>
  <tbody>
    <tr><td>客户业务团队</td><td>业务口径、主数据、样本、验收题、审核人和流程确认</td></tr>
    <tr><td>客户技术团队</td><td>接口/API 网关、账号权限、网络、字段字典、错误码、测试环境和数据对账</td></tr>
    <tr><td>项目实施团队</td><td>Connector/MCP 封装、Schema、上下文、工具策略、评测、部署与可观测</td></tr>
    <tr><td>联合责任</td><td>生产写入、权限模型、SLO、灾备、验收基线和变更审批</td></tr>
  </tbody>
</table>
""",
        "3. POC 最小接口集": """
<table>
  <thead><tr><th background-color="light-gray">ID</th><th background-color="light-gray">接口/样本</th><th background-color="light-gray">最小字段</th><th background-color="light-gray">用途</th><th background-color="light-gray">当前状态</th></tr></thead>
  <tbody>
    <tr><td>POC-01</td><td>用户与角色样本</td><td>user_id、role、tenant_id、pond_scope</td><td>权限与池塘解析</td><td>待客户提供</td></tr>
    <tr><td>POC-02</td><td>主数据样本</td><td>site/workshop/system/pond/device/cycle/batch 唯一 ID 与别名</td><td>实体映射</td><td>存在字段草案，未冻结</td></tr>
    <tr><td>POC-03</td><td>IoT 3—5 天样本</td><td>pond_id、device_id、metric、value、unit、event_time、quality</td><td>诊断与趋势</td><td>目前仅 16 张截图</td></tr>
    <tr><td>POC-04</td><td>ERP 3—5 天样本</td><td>投放、投喂、死亡、打样、分池、用药、任务事件</td><td>事件对齐和指标计算</td><td>目前仅字段设计与示例</td></tr>
    <tr><td>POC-05</td><td>指标口径</td><td>FCR、死亡率、存塘、生物量、投喂率公式及版本</td><td>确定性计算</td><td>部分资料已有，待业务签字</td></tr>
    <tr><td>POC-06</td><td>知识文件与权限</td><td>文件、版本、来源、适用域、审核状态</td><td>知识摄取</td><td>首批 40 文件已提供</td></tr>
    <tr><td>POC-07</td><td>黄金问题与答案</td><td>问题、作用域、必需证据、可接受结论、拒答条件</td><td>评测</td><td>待联合标注</td></tr>
  </tbody>
</table>
<p>如果生产接口尚未准备好，POC-03/04 可先交付脱敏 CSV/JSON；要求字段、关系和数据质量与未来接口一致，避免演示 Mock 无法替换。</p>
""",
        "4. 身份、权限与主数据接口": """
<table>
  <thead><tr><th background-color="light-gray">ID</th><th background-color="light-gray">方向</th><th background-color="light-gray">接口</th><th background-color="light-gray">关键要求</th><th background-color="light-gray">阶段</th></tr></thead>
  <tbody>
    <tr><td>IAM-01</td><td>客户→Agent</td><td>SSO/OIDC/CAS 登录</td><td>稳定 subject、tenant、角色；令牌校验与注销</td><td>联调/生产</td></tr>
    <tr><td>IAM-02</td><td>客户→Agent</td><td>组织/角色查询</td><td>部门、岗位、状态、角色版本</td><td>联调/生产</td></tr>
    <tr><td>IAM-03</td><td>客户→Agent</td><td>对象 ACL</td><td>用户可访问的场区、池塘、任务与数据范围</td><td>POC 可 Mock，生产必需</td></tr>
    <tr><td>MDM-01</td><td>客户→Agent</td><td>场区层级</td><td>tenant/site/workshop/system/pond 的主键、名称、别名、状态</td><td>POC 必需</td></tr>
    <tr><td>MDM-02</td><td>客户→Agent</td><td>养殖周期与批次</td><td>cycle_id、batch_id、species、stage、start/end、pond 关系</td><td>POC 必需</td></tr>
    <tr><td>MDM-03</td><td>客户→Agent</td><td>设备与测点</td><td>device_id、point_id、type、pond/system、metric、unit、状态</td><td>POC 必需</td></tr>
    <tr><td>MDM-04</td><td>双向</td><td>主数据变更通知</td><td>版本号、变更时间、删除标记、全量对账</td><td>生产</td></tr>
  </tbody>
</table>
<p><b>池塘识别顺序：</b>可信页面/告警绑定对象 → 已确认会话状态 → 用户文本实体 → 用户可访问列表消歧。用户名或自然语言不能直接当 pond_id。跨池塘问题必须显式列出作用域。</p>
""",
        "5. IoT 与设备数据接口": """
<table>
  <thead><tr><th background-color="light-gray">ID</th><th background-color="light-gray">接口</th><th background-color="light-gray">关键输入/输出</th><th background-color="light-gray">模式</th><th background-color="light-gray">验收</th></tr></thead>
  <tbody>
    <tr><td>IOT-01</td><td>最新观测查询</td><td>pond/device/metric → value、unit、event_time、quality</td><td>REST/gRPC，只读</td><td>与页面同一时点对账</td></tr>
    <tr><td>IOT-02</td><td>时序窗口查询</td><td>范围、指标、from/to、粒度 → 原始或聚合序列</td><td>REST/gRPC，只读</td><td>3—5 天窗口无漏点、单位一致</td></tr>
    <tr><td>IOT-03</td><td>设备状态与运行参数</td><td>启停、电流、负载、模式、状态、最后心跳</td><td>只读</td><td>设备 ID 可映射池塘/系统</td></tr>
    <tr><td>IOT-04</td><td>报警事件</td><td>alarm_id、type、severity、object、start/end、ack、handler</td><td>查询+Webhook</td><td>重复/恢复事件语义明确</td></tr>
    <tr><td>IOT-05</td><td>实时订阅</td><td>metric/event topic、offset、delivery time</td><td>MQTT/Kafka/Webhook</td><td>重连、重放、去重可验证</td></tr>
    <tr><td>IOT-06</td><td>数据质量</td><td>缺失、漂移、离群、校准、传感器故障</td><td>查询或伴随字段</td><td>坏数据不进入高置信结论</td></tr>
  </tbody>
</table>
<p>首批建议覆盖溶氧、温度、pH、液位、浊度、ORP 及关键设备开关/故障。客户截图显示约分钟级观测和设备故障记录，但截图不能替代接口契约，也不能证明连续性、单位和测点映射。</p>
""",
        "6. ERP、生产记录与指标接口": """
<h2>6.1 只读接口</h2>
<table>
  <thead><tr><th background-color="light-gray">ID</th><th background-color="light-gray">业务对象</th><th background-color="light-gray">最小字段</th><th background-color="light-gray">用途</th></tr></thead>
  <tbody>
    <tr><td>ERP-R01</td><td>投放/分池/销售</td><td>batch、from/to pond、quantity、weight、spec、event_time</td><td>批次与存塘变化</td></tr>
    <tr><td>ERP-R02</td><td>死亡记录</td><td>pond、batch、count、weight、reason、event_time、version</td><td>死亡率与风险</td></tr>
    <tr><td>ERP-R03</td><td>投喂记录</td><td>pond、feed、amount、meal、behavior_score、operator、time</td><td>摄食与 FCR</td></tr>
    <tr><td>ERP-R04</td><td>打样/生长</td><td>sample_count、length、weight、method、time</td><td>生物量和生长曲线</td></tr>
    <tr><td>ERP-R05</td><td>用药与病检</td><td>symptom、drug、dose、duration、diagnosis、result</td><td>诊断、风险和复发</td></tr>
    <tr><td>ERP-R06</td><td>任务/巡检/维保</td><td>task、object、status、owner、deadline、result</td><td>事件解释和闭环</td></tr>
    <tr><td>ERP-R07</td><td>库存/成本</td><td>material、stock、usage、price、energy、labor</td><td>报告和经营分析</td></tr>
    <tr><td>METRIC-01</td><td>指标计算</td><td>metric_id、formula_version、inputs、result、unit、window</td><td>FCR、死亡率、存塘等确定性计算</td></tr>
  </tbody>
</table>
<h2>6.2 受控写入接口</h2>
<table>
  <thead><tr><th background-color="light-gray">ID</th><th background-color="light-gray">接口</th><th background-color="light-gray">控制要求</th></tr></thead>
  <tbody>
    <tr><td>ERP-W01</td><td>record.validate</td><td>校验对象、字段、单位、时间、权限、业务规则；只返回预览</td></tr>
    <tr><td>ERP-W02</td><td>record.commit</td><td>必须携带用户确认令牌、幂等键、原数据版本和审计主体</td></tr>
    <tr><td>ERP-W03</td><td>record.correct/cancel</td><td>修改/删除需要原因、审批、乐观锁和原值留痕</td></tr>
    <tr><td>ERP-W04</td><td>effect.status</td><td>查询最终状态，处理超时、未知结果和人工对账</td></tr>
  </tbody>
</table>
<p>若客户只能提供数据库访问，POC 可使用只读副本/视图和白名单账号；必须提供表结构、字段字典、主键/外键、数据量、更新时间与维护窗口。生产写入必须走服务 API，不接受 Agent 直写业务库。</p>
""",
        "7. 报警、任务、知识、会议与通知接口": """
<table>
  <thead><tr><th background-color="light-gray">ID</th><th background-color="light-gray">接口</th><th background-color="light-gray">说明</th><th background-color="light-gray">阶段</th></tr></thead>
  <tbody>
    <tr><td>ALM-01</td><td>alarm.query/ack/close</td><td>查询、确认和关闭告警；关闭为写操作，需权限和回执</td><td>联调/生产</td></tr>
    <tr><td>TSK-01</td><td>task.create/update/query</td><td>诊断建议形成任务；绑定池塘、基线、观察窗和负责人</td><td>POC 可 Mock</td></tr>
    <tr><td>KB-01</td><td>document.list/download</td><td>文件、版本、来源、权限和更新事件</td><td>POC/生产</td></tr>
    <tr><td>KB-02</td><td>knowledge.review/publish</td><td>候选 Claim、冲突、审核意见、发布版本和撤回</td><td>平台自建</td></tr>
    <tr><td>MTG-01</td><td>record/transcript/minutes</td><td>腾讯会议可通过 MCP/API 读取录制、逐字稿和 AI 纪要；需授权</td><td>可选</td></tr>
    <tr><td>MTG-02</td><td>screen/keyframe artifact</td><td>共享屏幕分析需另有录屏/关键帧/附件；不把逐字稿等同屏幕内容</td><td>二期可选</td></tr>
    <tr><td>NTF-01</td><td>message.send/callback</td><td>飞书/企微/短信等通知，含发送回执和用户动作回调</td><td>联调/生产</td></tr>
    <tr><td>RPT-01</td><td>report.export/archive</td><td>日报、周报、月报导出与归档</td><td>POC/生产</td></tr>
  </tbody>
</table>
""",
        "8. 通用接口契约、安全与验收": """
<h2>8.1 通用请求信封</h2>
<pre lang="json" caption="建议字段"><code>{
  "request_id": "...",
  "tenant_id": "...",
  "actor": {"id":"...","roles":["..."]},
  "scope": {"site_id":"...","pond_id":"...","cycle_id":"..."},
  "event_time": "ISO-8601",
  "idempotency_key": "...",
  "schema_version": "v1"
}</code></pre>
<h2>8.2 必须统一的技术条款</h2>
<ul>
  <li>认证：短期令牌、服务身份、证书/密钥轮换、最小权限和出站白名单。</li>
  <li>时间：Asia/Shanghai 展示，服务端使用带时区 ISO-8601；区分 event_time、ingested_at 和 updated_at。</li>
  <li>一致性：查询返回 source_version；写入使用幂等键、乐观锁、最终状态查询和回执。</li>
  <li>质量：单位、精度、缺失、重复、迟到、异常、传感器校准和数据来源必须可识别。</li>
  <li>错误：HTTP/业务错误码、可重试标志、限流、超时、部分成功和用户可读提示。</li>
  <li>审计：记录主体、租户、对象、接口、参数摘要、结果摘要、策略决定和 Trace ID；敏感字段脱敏。</li>
</ul>
<h2>8.3 单接口验收包</h2>
<p>每个接口交付 OpenAPI/Proto/Schema、沙箱地址、鉴权方式、示例请求响应、字段字典、枚举与错误码、限流/SLA、幂等说明、数据样本、对账脚本和负责人。仅提供 URL 或数据库账号不视为接口交付完成。</p>
""",
        "9. 客户对接清单与冻结门禁": """
<table>
  <thead><tr><th background-color="light-gray">确认项</th><th background-color="light-gray">客户建议责任人</th><th background-color="light-gray">交付物</th><th background-color="light-gray">阻塞阶段</th></tr></thead>
  <tbody>
    <tr><td>接口总负责人及各系统 Owner</td><td>技术负责人</td><td>RACI 与联络清单</td><td>联调</td></tr>
    <tr><td>部署网络、域名、证书、白名单</td><td>基础设施/安全</td><td>网络拓扑与开通单</td><td>环境部署</td></tr>
    <tr><td>SSO、角色和池塘 ACL</td><td>IAM/业务</td><td>授权模型和测试账号</td><td>真实用户测试</td></tr>
    <tr><td>IoT/ERP API 与字段</td><td>平台研发</td><td>接口文档、样本、沙箱和对账人</td><td>真实数据联调</td></tr>
    <tr><td>指标公式和阈值</td><td>生产/技术专家</td><td>版本化口径并签字</td><td>BI/诊断验收</td></tr>
    <tr><td>知识权限和审核</td><td>知识负责人</td><td>来源、使用范围、审核/发布流程</td><td>知识上线</td></tr>
    <tr><td>规模、SLO、RPO/RTO</td><td>业务/运维</td><td>容量输入和服务等级</td><td>资源采购</td></tr>
  </tbody>
</table>
<callout emoji="🏁" background-color="light-green" border-color="green"><p><b>生产设计冻结门禁：</b>主数据键、权限模型、核心 API、公式版本、规模/SLO 和部署边界全部确认；真实样本完成对账；写入和回滚演练通过。</p></callout>
""",
    }
)


TECHNICAL = OrderedDict(
    {
        "1. 总体判断与设计原则": """
<callout emoji="📌" background-color="light-blue" border-color="blue"><p><b>总体判断：</b>Y-Harness 可作为通用 Agent Runtime，但智慧渔业落地还需要 Aquaculture Domain Pack、Embedding Host、业务连接器和治理后台。系统应按“Agent = LLM × Harness × Domain Pack × Governed Data”建设；LLM 负责语义理解和表达，不负责身份授权、数值计算、最终事实或生产写入。</p></callout>
<ul>
  <li>证据优先：答案由可追溯 Claim、IoT/ERP 事实和确定性计算支撑。</li>
  <li>作用域优先：先确认 tenant、pond、cycle、batch 和 time range，再检索和调用工具。</li>
  <li>读写分离：查询可自动执行；外部写入必须预览、策略判断、人工确认、幂等提交和回执。</li>
  <li>版本优先：知识、图谱、Skill、Policy、Prompt、模型、接口 Schema 和评测集均可回滚。</li>
  <li>渐进落地：先 Mock 验证链路，再真实数据对账，最后开放受控写入。</li>
</ul>
""",
        "2. 产品与技术总体架构": """
<h2>2.1 产品架构</h2>
<whiteboard type="mermaid">
flowchart TB
  CH[Web 移动端 IM 语音 会议] --> EX[体验层]
  EX --> AP[Agent应用层]
  AP --> Q[诊断问答]
  AP --> BI[BI与报告]
  AP --> WR[录入与任务]
  AP --> KM[知识与案例]
  Q --> HR[Y-Harness运行层]
  BI --> HR
  WR --> HR
  KM --> HR
  HR --> DP[智慧渔业Domain Pack]
  DP --> CT[Context Intent Policy Skills]
  DP --> RG[Hybrid RAG Graph Memory]
  DP --> TL[Tools MCP Connectors]
  CT --> DS[客户IoT ERP 主数据]
  RG --> KS[知识 Evidence Case]
  TL --> DS
  HR --> GV[Eval Audit Observability Admin]
</whiteboard>
<h2>2.2 技术架构</h2>
<whiteboard type="mermaid">
flowchart LR
  G[API Gateway SSO] --> H[Embedding Host]
  H --> Y[Y-Harness Runtime]
  Y --> C[Context Resolver]
  Y --> O[Agent Orchestrator]
  Y --> P[Policy Approval]
  O --> R[Retrieval Rerank Graph]
  O --> M[Model Gateway]
  O --> T[Tool MCP Gateway]
  R --> PG[(PostgreSQL pgvector FTS)]
  R --> OB[(Object Evidence Store)]
  T --> I[IoT Connector]
  T --> E[ERP Connector]
  T --> X[Meeting Task Notification]
  Y --> AM[Agent-Memory-Hub Adapter]
  Y --> EV[Eval Trace Audit]
</whiteboard>
<p>POC 阶段优先采用 PostgreSQL + pgvector + 全文索引承载结构化、向量和关键词检索，图谱以关系表/图查询层实现；达到规模或复杂度门槛后再拆分 OpenSearch、专用向量库或图数据库，避免为演示过早采购多套集群。</p>
""",
        "3. Agent 组件、职责与业务用途": """
<table>
  <thead><tr><th background-color="light-gray">组件</th><th background-color="light-gray">实现责任</th><th background-color="light-gray">业务用途</th><th background-color="light-gray">不得承担</th></tr></thead>
  <tbody>
    <tr><td>LLM</td><td>意图候选、问题改写、结构化抽取、规划、解释与表达</td><td>理解现场口语，组合已验证证据</td><td>生成身份、权限、实时值、公式结果和回执</td></tr>
    <tr><td>Y-Harness Runtime</td><td>Agent Loop、上下文编译、Tool 调度、状态、策略钩子、Trace</td><td>保证所有旅程走统一受控链路</td><td>自动理解渔业实体语义</td></tr>
    <tr><td>Embedding Host</td><td>请求信封、渠道适配、认证主体、页面上下文、流式响应</td><td>把当前用户和页面事实可靠注入</td><td>把可伪造文本当权威身份</td></tr>
    <tr><td>Domain Pack</td><td>实体、词典、诊断图谱、指标、Skills、Policies、Verifiers</td><td>把通用 Harness 变成智慧渔业 Agent</td><td>绕过 Core 的安全与状态机制</td></tr>
    <tr><td>Intent Router</td><td>规则优先、LLM 分类、置信和风险路由</td><td>区分诊断、BI、录入、任务、会议、知识维护</td><td>仅凭一个分类分数自动执行高风险动作</td></tr>
    <tr><td>Context Resolver</td><td>实体解析、时间解析、权限裁剪、歧义和缺口</td><td>确定是哪个池塘、批次、设备和时间窗</td><td>在多候选时替用户选择</td></tr>
    <tr><td>Agentic RAG</td><td>混合召回、图谱扩展、Rerank、Claim 聚合与深读</td><td>把论文、SOP、经验与当前问题对齐</td><td>把相似文本当成已验证事实</td></tr>
    <tr><td>Tool/MCP</td><td>Schema、鉴权、超时、幂等、错误与证据回执</td><td>查询 IoT/ERP、写记录、建任务、读会议</td><td>把 MCP 会话当作租户隔离</td></tr>
    <tr><td>Memory</td><td>会话状态、用户偏好、案例、决策与可检索长期记忆</td><td>跨会议/任务复用，并保持来源</td><td>把所有聊天永久保存或自动升格为知识</td></tr>
    <tr><td>Policy/HITL</td><td>Allow/Ask/Deny、审批、风险与责任人路由</td><td>控制用药、停食、写入、发布等动作</td><td>让 Prompt 代替强制策略</td></tr>
    <tr><td>Eval/Verifier</td><td>离线评测、在线检查、回放、数值和引用校验</td><td>证明“准确、安全、可复现”</td><td>只看用户点赞率</td></tr>
  </tbody>
</table>
""",
        "4. Context、意图与作用域治理": """
<h2>4.1 Context Package</h2>
<p>上下文不是把全部历史对话交给模型，而是由系统提取、校验、裁剪和编译。身份、ACL、当前页面对象、写权限和原始工具回执保留在模型外作为硬控制；模型只获得完成任务所需的业务上下文。</p>
<pre lang="json" caption="Context Package 核心结构"><code>{
  "authority": {"actor_id":"...","tenant_id":"...","roles":["..."]},
  "interaction": {"channel":"web","page":"pond-dashboard","alarm_id":"..."},
  "resolved_scope": {"pond_id":"pond-03","cycle_id":"...","time_range":{"from":"...","to":"..."}},
  "intent": {"name":"diagnose_anomaly","confidence":0.91,"risk":"medium"},
  "entities": [{"type":"metric","id":"dissolved_oxygen","status":"verified"}],
  "constraints": {"data_origin":"mock","write_allowed":false},
  "open_questions": [],
  "evidence_refs": ["iot://...","knowledge://..."],
  "provenance": {"resolver_version":"aqua-context-v1"}
}</code></pre>
<h2>4.2 提取和校验链路</h2>
<ol>
  <li seq="auto">从认证网关和页面元数据取得不可伪造的主体与对象候选。</li>
  <li seq="auto">规则/词典解析明确表达，LLM 按 Schema 提取意图、实体、时间和指标候选。</li>
  <li seq="auto">Entity Resolver 映射主数据 ID、别名和父子关系，并按 ACL 裁剪。</li>
  <li seq="auto">Ambiguity Resolver 对多候选、跨池塘和缺少时间窗发起最小追问。</li>
  <li seq="auto">Context Budgeter 按相关性、可信度、新鲜度、必要性和 Token 成本排序。</li>
  <li seq="auto">Y-Harness 编译 Skill、状态、记忆、证据和有限会话历史，并记录取舍 Trace。</li>
</ol>
<h2>4.3 意图识别</h2>
<p>采用“确定性入口/指令规则 → 轻量分类 → LLM 结构化补全 → 风险路由”的分层设计。输出不只有 intent，还包括 required_slots、resolved_scope、risk、candidate_tools 和 need_confirmation。诊断和 BI 可自动查询；写入、发布、用药、设备控制必须经过 Policy。</p>
""",
        "5. 知识库、诊断图谱与 Agentic RAG": """
<h2>5.1 知识构建流水线</h2>
<whiteboard type="mermaid">
flowchart LR
  S[文档 SOP 论文 思维导图 会议 经验] --> I[登记来源 版本 权限 适用域]
  I --> P[解析 标题 表格 公式 图片]
  P --> C[语义切分 Claim 条件 证据 结论]
  C --> G[实体关系与诊断路径]
  C --> X[全文与向量索引]
  G --> R[专家审核]
  X --> R
  R --> V[发布版本]
  V --> E[Eval与在线反馈]
  E --> I
</whiteboard>
<p>思维导图的 18,828 个节点全部进入原始保真层，保留父子路径、顺序和原始文本；再异步抽取症状、指标、原因、证据、动作、风险、对象和适用条件。图谱抽取失败不得丢失原始节点。PDF、SOP、个人经验和会议结论按统一 Claim 模型表达。</p>
<h2>5.2 Agentic RAG</h2>
<p>标准链路为：查询理解 → tenant/pond/species/stage/time 过滤 → BM25/全文召回 → 向量召回 → 图谱邻域扩展 → 去重和 Claim 聚合 → Cross-encoder/LLM Rerank → 证据装配 → 必要时深读 → 生成 → 引用与数值验证。向量召回解决现场口语和文档表达差异，全文检索保证术语、编号和精确短语，图谱补充多跳因果路径。</p>
<h2>5.3 动态证据与冲突</h2>
<p>来源类型不是固定等级。证据评分由场景匹配、生产验证、时效、可追溯、可复现、多源印证、数据质量和冲突惩罚组成。一个与当前池塘、鱼种和阶段高度匹配、经多个周期验证的个人经验，可以高于年代久远且场景不符的论文。冲突按 Claim 比较适用条件；不能消解时并列展示并转专家。</p>
""",
        "6. Tools、MCP、Skills 与业务连接器": """
<h2>6.1 MCP 封装原则</h2>
<ul>
  <li>每个 Tool 声明版本化输入/输出 Schema、读写属性、风险等级、超时、重试、幂等、错误码和审计字段。</li>
  <li>客户已有 API/MCP/CLI 时做适配；只有数据库时优先只读视图/副本，生产写入由客户业务服务 API 承担。</li>
  <li>MCP Gateway 负责注册、鉴权、限流和 Trace；凭证、会话、缓存必须按租户分区。</li>
  <li>工具输出转为 Connector Evidence，包含 source、resource、version、observed_at、quality 和回执。</li>
</ul>
<h2>6.2 建议 Skills</h2>
<table>
  <thead><tr><th background-color="light-gray">Skill</th><th background-color="light-gray">用途</th><th background-color="light-gray">关键工具</th></tr></thead>
  <tbody>
    <tr><td>pond-scope-resolution</td><td>识别池塘/批次/时间窗并消歧</td><td>context.resolve_scope、masterdata.query</td></tr>
    <tr><td>aquaculture-diagnosis</td><td>异常诊断与追问</td><td>knowledge.search、graph.diagnose、iot.query、erp.query</td></tr>
    <tr><td>water-quality-analysis</td><td>水质趋势、阈值和关联</td><td>iot.window、metric.calculate</td></tr>
    <tr><td>feeding-analysis</td><td>投喂、摄食、FCR 与生物量</td><td>erp.feed、erp.sample、metric.calculate</td></tr>
    <tr><td>device-troubleshooting</td><td>设备故障排查</td><td>iot.device、alarm.query、knowledge.search</td></tr>
    <tr><td>natural-language-bi</td><td>指标查询、图表和口径说明</td><td>metric.catalog、analytics.query</td></tr>
    <tr><td>production-recording</td><td>语音/文本录入</td><td>record.validate、record.preview、record.commit</td></tr>
    <tr><td>meeting-to-action</td><td>会议结论、任务和候选知识</td><td>meeting.read、task.create、knowledge.submit</td></tr>
    <tr><td>knowledge-governance</td><td>摄取、冲突、审核和发布</td><td>knowledge.ingest/review/publish</td></tr>
    <tr><td>case-review</td><td>任务效果对比与案例沉淀</td><td>task.query、metric.compare、case.persist</td></tr>
  </tbody>
</table>
""",
        "7. Memory、Policy、人工与后管平台": """
<h2>7.1 Agent-Memory-Hub 适配</h2>
<p>Memory 分为会话状态、用户/角色偏好、任务状态、案例记忆和长期知识候选。Agent-Memory-Hub 负责跨 Agent 的可检索记忆条目与原始会话证据分层：原始 transcript 进入 source/evidence；decision、fact、signal、episode、artifact、handoff 等经过策略后进入 memory items。所有条目携带 tenant、scope、sensitivity、source、validity 和版本。</p>
<p>低峰期可做对话摘要、反馈聚合和候选记忆生成，但不得自动修改生产 Skill、Policy 或已发布知识。候选必须通过去重、敏感性检查、适用域校验和人工审核，再进入可影响决策的层级。</p>
<h2>7.2 Policy 与人工介入</h2>
<table>
  <thead><tr><th background-color="light-gray">风险等级</th><th background-color="light-gray">示例</th><th background-color="light-gray">策略</th></tr></thead>
  <tbody>
    <tr><td>低</td><td>查询公开知识、只读统计、报告草稿</td><td>Allow；保留引用与 Trace</td></tr>
    <tr><td>中</td><td>诊断建议、跨池塘对比、低质量数据</td><td>Allow with warning 或 Ask；显示不确定性</td></tr>
    <tr><td>高</td><td>生产写入、任务发布、知识发布、用药/停食建议</td><td>Ask；指定审批人；确认后提交</td></tr>
    <tr><td>禁止</td><td>越权查询、直写数据库、无证据自动控制设备</td><td>Deny；记录审计</td></tr>
  </tbody>
</table>
<h2>7.3 后管能力</h2>
<p>后管平台至少包含：知识源与版本、图谱与冲突、Skill/Prompt/Policy 发布、工具注册与凭证、角色与审批流、评测集与问题回放、接口健康、模型配额、审计和告警。角色分为编辑、审核、发布、平台管理员和审计员，避免同一人无约束完成全流程。</p>
""",
        "8. Eval、稳定性、安全与可观测": """
<h2>8.1 评测体系</h2>
<ul>
  <li>数据/契约测试：字段、单位、时间、枚举、主键、质量标记和公式。</li>
  <li>检索测试：Recall@K、MRR、nDCG、来源/适用域过滤和冲突覆盖。</li>
  <li>回答测试：事实正确、引用覆盖、数值一致、完整性、表达和校准。</li>
  <li>Agent 测试：意图/池塘解析、Tool 选择、参数、审批、幂等、异常和拒答。</li>
  <li>在线评测：采纳率、专家修订率、任务完成率、事故/越权、成本和时延。</li>
</ul>
<h2>8.2 稳定性</h2>
<p>入口限流、请求预算、模型和工具超时、指数退避、熔断、隔离仓、幂等键、Outbox/Effect Receipt、异步长任务、结果缓存与版本绑定共同保障稳定性。外部数据不可用时，系统返回“缺少哪些数据、当前可提供什么”，不得用旧值伪装实时答案。</p>
<h2>8.3 安全</h2>
<p>采用租户/池塘双重数据围栏、最小权限、密钥托管、传输与存储加密、敏感字段脱敏、审计不可抵赖、依赖与镜像扫描、Prompt Injection 防护和工具参数白名单。检索结果仍需 ACL 过滤，模型不能扩大访问范围。</p>
<h2>8.4 可观测</h2>
<p>每次运行记录 request/trace、主体、作用域、意图、上下文取舍、检索候选、证据、模型/Prompt/Skill 版本、工具参数摘要、策略决定、时延、Token、成本和最终回执。支持按一次对话完整回放。</p>
""",
        "9. 端到端时序与数据模型": """
<h2>9.1 统一运行时序</h2>
<whiteboard type="mermaid">
sequenceDiagram
  participant U as Channel
  participant H as Embedding Host
  participant Y as Y-Harness
  participant C as Context Resolver
  participant R as RAG Graph
  participant T as Tool Gateway
  participant P as Policy
  participant V as Verifier
  U->>H: Request Envelope
  H->>C: 身份 页面 文本 状态
  C-->>H: Context Package
  H->>Y: Invocation
  Y->>R: 检索和证据装配
  Y->>T: 查询生产事实
  R-->>Y: Claims与引用
  T-->>Y: Connector Evidence
  Y->>P: 动作与风险决策
  P-->>Y: Allow Ask Deny
  Y->>V: 答案 数值 引用 作用域
  V-->>Y: 通过或修正
  Y-->>U: 响应 追问 审批或异步任务
</whiteboard>
<h2>9.2 核心实体</h2>
<p>Tenant、Site、Workshop、System、Pond、Device、Metric、Observation、CultureCycle、Batch、StockEvent、FeedEvent、MortalityEvent、SampleEvent、MedicationEvent、Alarm、Task、KnowledgeSource、Claim、Evidence、Conflict、Case、Approval、EffectReceipt、EvalCase 和 Trace 构成统一领域模型。所有业务事实至少包含 tenant_id、object_id、event_time、source_version 和 data_origin。</p>
""",
        "10. 实施节奏、交付物与风险边界": """
<table>
  <thead><tr><th background-color="light-gray">阶段</th><th background-color="light-gray">建议周期</th><th background-color="light-gray">核心工作</th><th background-color="light-gray">门禁</th></tr></thead>
  <tbody>
    <tr><td>方案冻结</td><td>1—2 周</td><td>接口、数据、权限、知识、评测、部署与 RACI</td><td>五份文档评审完成</td></tr>
    <tr><td>POC 构建</td><td>3—5 周</td><td>全量旅程骨架、首个诊断故事线、Mock Connector、知识摄取、Eval</td><td>端到端回放与安全门禁通过</td></tr>
    <tr><td>真实数据联调</td><td>2—4 周</td><td>IoT/ERP、指标对账、真实样本校准、只读灰度</td><td>接口/数据质量验收</td></tr>
    <tr><td>试运行与上线</td><td>2—4 周</td><td>受控写入、后管、压测、安全、灾备、培训与切换</td><td>上线评审与回滚演练</td></tr>
  </tbody>
</table>
<p>周期从客户输入齐备时起算；接口、真实数据、部署环境或审核人员延迟会顺延对应阶段。当前结论只能证明方案可实施和 POC 链路可验证，不能提前承诺生产诊断准确率、连续数据质量或硬件采购量。</p>
""",
    }
)


RESOURCES = OrderedDict(
    {
        "1. 采购结论与容量边界": """
<callout emoji="❗" background-color="light-yellow" border-color="orange"><p><b>采购结论：</b>目前缺少正式用户数、并发、IoT 点位/频率、知识增长、SLO、RPO/RTO 和私有模型要求，不能给出“最终采购数量”。本清单给出可启动的基线配置、扩容阈值和压测门禁；生产采购应在真实样本联调与压测后冻结。</p></callout>
<table>
  <thead><tr><th background-color="light-gray">环境</th><th background-color="light-gray">容量包络（方案假设）</th><th background-color="light-gray">用途</th></tr></thead>
  <tbody>
    <tr><td>POC</td><td>5—20 用户、并发不高于 5、Mock/脱敏样本、非 HA</td><td>验证旅程、知识和接口契约</td></tr>
    <tr><td>预发</td><td>50—100 用户、并发 10—20、镜像生产拓扑</td><td>联调、回归、压测和故障演练</td></tr>
    <tr><td>生产基线</td><td>100—300 用户、并发 30—50、Agent 请求峰值约 5 RPS</td><td>首期上线容量包络，并非客户事实</td></tr>
  </tbody>
</table>
""",
        "2. 容量估算方法": """
<h2>2.1 需要客户提供的输入</h2>
<ul>
  <li>场区/池塘/设备/测点数量、采样间隔、日增量、保留年限、是否需要在本系统镜像。</li>
  <li>用户数、峰值并发、日问答数、长任务比例、报告批次和会议处理量。</li>
  <li>知识文档总量、字符数、图片/音视频体量、更新频率、索引版本保留数。</li>
  <li>可用性、p95/p99 时延、RPO/RTO、备份保留、合规和模型出网限制。</li>
</ul>
<h2>2.2 计算公式</h2>
<pre lang="text" caption="容量估算"><code>IoT 日记录数 = 设备数 × 每设备测点数 × 86400 ÷ 采样间隔秒
IoT 年原始容量 = 日记录数 × 单条编码字节 × 365 × 索引系数 × 副本系数
知识切片数 = 有效字符数 ÷ 平均切片字符数 × 版本保留系数
向量容量 = 切片数 × 向量维度 × 每维字节 × 索引放大系数 × 副本系数
模型日 Token = 日请求数 × 平均输入 Token + 日请求数 × 平均输出 Token
并发需求 ≈ 峰值到达率 × 平均处理时长</code></pre>
<p>IoT 默认保留在客户数据平台，本系统按窗口查询并只保存证据摘要；只有客户要求镜像时才采购时序存储。这样可显著降低重复存储和数据一致性成本。</p>
""",
        "3. POC 与预发资源建议": """
<h2>3.1 POC 起步配置</h2>
<table>
  <thead><tr><th background-color="light-gray">资源</th><th background-color="light-gray">建议配置</th><th background-color="light-gray">用途</th><th background-color="light-gray">依据</th></tr></thead>
  <tbody>
    <tr><td>应用/Agent Runtime</td><td>1 台 4C8G</td><td>API、Y-Harness、Domain Pack</td><td>并发不高于 5，可单机恢复</td></tr>
    <tr><td>异步 Worker</td><td>1 台 4C8G</td><td>知识摄取、会议、评测、报告</td><td>与交互流量隔离</td></tr>
    <tr><td>PostgreSQL + pgvector</td><td>1 台 4C16G，SSD 200GB</td><td>元数据、状态、全文/向量和证据</td><td>首批知识和 Mock 足够</td></tr>
    <tr><td>Redis</td><td>1 台 2C4G</td><td>缓存、短状态、限流</td><td>非 HA POC</td></tr>
    <tr><td>对象存储</td><td>500GB</td><td>原文件、解析产物、评测附件</td><td>含版本与余量</td></tr>
    <tr><td>可观测</td><td>1 台 4C8G，SSD 200GB</td><td>日志、Trace、指标</td><td>保留 14—30 天</td></tr>
  </tbody>
</table>
<h2>3.2 预发配置</h2>
<table>
  <thead><tr><th background-color="light-gray">资源</th><th background-color="light-gray">建议配置</th><th background-color="light-gray">说明</th></tr></thead>
  <tbody>
    <tr><td>Gateway/应用</td><td>2 × 4C8G</td><td>验证滚动发布和单节点故障</td></tr>
    <tr><td>Agent/Worker</td><td>2 × 4C8G</td><td>交互与异步任务分离</td></tr>
    <tr><td>PostgreSQL</td><td>1 × 8C32G，SSD 500GB</td><td>可选只读副本，验证备份恢复</td></tr>
    <tr><td>Redis</td><td>1 × 4C8G</td><td>压测和故障降级</td></tr>
    <tr><td>对象存储</td><td>1TB</td><td>版本、录制/附件按需纳入</td></tr>
    <tr><td>可观测</td><td>2 × 4C16G，SSD 500GB</td><td>压测期延长日志和 Trace 保留</td></tr>
  </tbody>
</table>
""",
        "4. 生产基线与可选扩展": """
<h2>4.1 首期生产基线</h2>
<table>
  <thead><tr><th background-color="light-gray">资源</th><th background-color="light-gray">建议配置</th><th background-color="light-gray">高可用/用途</th></tr></thead>
  <tbody>
    <tr><td>负载均衡/API Gateway</td><td>2 × 4C8G 或托管服务</td><td>双实例、TLS、WAF、限流</td></tr>
    <tr><td>Embedding Host</td><td>3 × 4C8G</td><td>多可用区、无状态扩容</td></tr>
    <tr><td>Y-Harness/Agent Runtime</td><td>3 × 8C16G</td><td>交互式 Agent Loop</td></tr>
    <tr><td>异步 Worker</td><td>3 × 8C16G</td><td>知识、报告、会议、Eval</td></tr>
    <tr><td>PostgreSQL HA</td><td>主备各 8C32G，SSD 各 500GB</td><td>状态、元数据、证据、pgvector；每日备份</td></tr>
    <tr><td>Redis HA</td><td>3 × 4C8G</td><td>哨兵/集群、缓存与限流</td></tr>
    <tr><td>消息队列</td><td>3 × 4C8G 或托管</td><td>异步任务、事件和重试</td></tr>
    <tr><td>对象存储</td><td>2TB 起，版本/生命周期</td><td>原始资料、产物、附件和备份</td></tr>
    <tr><td>可观测平台</td><td>3 × 8C32G，SSD 各 1TB 或托管</td><td>指标、日志、Trace、告警</td></tr>
  </tbody>
</table>
<h2>4.2 条件触发型资源</h2>
<table>
  <thead><tr><th background-color="light-gray">组件</th><th background-color="light-gray">触发条件</th><th background-color="light-gray">建议起配</th></tr></thead>
  <tbody>
    <tr><td>OpenSearch</td><td>知识切片超过约 200 万、复杂全文过滤或 pg FTS p95 不达标</td><td>3 × 8C32G，SSD 各 1TB</td></tr>
    <tr><td>专用向量库</td><td>向量超过约 500 万且过滤/并发无法满足</td><td>3 × 8C32G，按索引实测扩盘</td></tr>
    <tr><td>图数据库</td><td>图谱多跳查询成为核心且关系表方案 p95 不达标</td><td>3 × 8C32G，SSD 各 500GB</td></tr>
    <tr><td>时序数据库</td><td>需要镜像 IoT 或客户平台无法提供窗口聚合</td><td>按日记录数与保留期另算</td></tr>
    <tr><td>GPU 推理节点</td><td>禁止外部模型且确定模型/量化/吞吐后</td><td>通过目标模型基准测试确定，不先按品牌下单</td></tr>
  </tbody>
</table>
""",
        "5. 模型、软件与安全资源": """
<h2>5.1 模型服务</h2>
<p>一期建议使用受管模型 API，经 Model Gateway 完成供应商路由、脱敏、配额、缓存、降级和版本锁定，不采购 GPU。至少准备主/备两类文本模型、Embedding 模型和可选 Rerank；ASR 用于语音录入，OCR/视觉仅在附件和截图场景按需启用。</p>
<p>Token 预算按“快问答、复杂诊断、会议/报告”三类拆分，分别限制输入、输出、工具轮次和最大成本。连续 30 天记录请求量、Token、缓存命中和单位成功任务成本后，再决定包量或私有化。</p>
<h2>5.2 软件与基础能力</h2>
<ul>
  <li>Kubernetes 或客户现有容器平台、镜像仓库、CI/CD、配置中心和服务发现。</li>
  <li>WAF、负载均衡、DNS/证书、VPN/专线或内网互通、堡垒机和出站代理。</li>
  <li>KMS/Secrets、数据库审计、主机/容器安全、漏洞扫描和制品签名。</li>
  <li>PostgreSQL、Redis、消息队列、对象存储和可观测平台优先复用客户现有托管服务。</li>
</ul>
""",
        "6. 压测、容量门禁与验收": """
<h2>6.1 工作负载模型</h2>
<table>
  <thead><tr><th background-color="light-gray">场景</th><th background-color="light-gray">占比建议</th><th background-color="light-gray">重点指标</th></tr></thead>
  <tbody>
    <tr><td>知识问答</td><td>35%</td><td>首 Token、总时延、引用覆盖、缓存</td></tr>
    <tr><td>IoT/ERP 诊断</td><td>30%</td><td>工具时延、并发、降级、证据完整</td></tr>
    <tr><td>BI/报告</td><td>15%</td><td>查询与计算正确、异步完成时间</td></tr>
    <tr><td>录入/任务</td><td>10%</td><td>确认、幂等、回执、未知结果对账</td></tr>
    <tr><td>会议/知识摄取</td><td>10%</td><td>队列积压、吞吐、失败重试和成本</td></tr>
  </tbody>
</table>
<h2>6.2 压测轮次</h2>
<ol>
  <li seq="auto">单组件基准：检索、数据库、模型网关、Connector 和指标计算。</li>
  <li seq="auto">端到端阶梯：1×、2×、3×目标并发，记录 p50/p95/p99、错误率、队列和资源。</li>
  <li seq="auto">8 小时稳定性与 24 小时长稳测试，检查内存、连接、缓存、队列和日志增长。</li>
  <li seq="auto">故障注入：模型超时、IoT 不可用、数据库切换、Redis/队列节点故障、网络抖动。</li>
  <li seq="auto">恢复演练：备份恢复、跨区切换、重复写入、未知结果对账和版本回滚。</li>
</ol>
<p><b>建议门槛：</b>API 可用性不低于 99.9%；非模型接口 p95 不高于 500ms；问答首 Token p95 不高于 3s；端到端错误率低于 1%；写入重复副作用为 0。最终指标由双方在规模输入后确认。</p>
""",
        "7. 采购冻结条件与成本控制": """
<ul>
  <li>生产采购前必须完成真实样本容量测量、目标并发压测、30 天模型 Token 估算和备份恢复测算。</li>
  <li>优先复用客户数据库、对象存储、Kubernetes、监控和安全服务；新增组件需说明不可复用的原因。</li>
  <li>搜索/向量/图数据库采用阶段门槛拆分，不在 POC 同时采购三套集群。</li>
  <li>计算节点按无状态水平扩容；存储预留 30% 容量与一年增长；日志和 Trace 分层保留。</li>
  <li>模型设置路由、缓存、Token 上限、批处理和离线低峰队列，以“单次成功业务任务成本”而非单 Token 价格评估。</li>
</ul>
<callout emoji="🏁" background-color="light-green" border-color="green"><p>采购清单冻结所需客户输入：部署模式、可复用资源、用户与并发、IoT/知识数据量、模型边界、SLO、RPO/RTO、保留期和预算上限。</p></callout>
""",
    }
)


DEPLOYMENT = OrderedDict(
    {
        "1. 部署目标、边界与推荐模式": """
<callout emoji="📌" background-color="light-blue" border-color="blue"><p><b>推荐模式：</b>客户私有数据面 + 受控模型出口。业务主数据、IoT/ERP、知识原文、状态、证据和审计部署在客户网络；模型调用经 Model Gateway 脱敏、白名单和审计。若客户禁止出网，再切换为全私有模型部署并重新评估 GPU、性能和运维。</p></callout>
<p>部署分为开发、POC、预发和生产，使用同一逻辑架构、不同安全与可靠性配置。当前未确认客户云平台、网络和合规要求，因此本文提供标准拓扑和两种替代模式，不假设已有环境能力。</p>
<table>
  <thead><tr><th background-color="light-gray">模式</th><th background-color="light-gray">数据边界</th><th background-color="light-gray">优点</th><th background-color="light-gray">适用条件</th></tr></thead>
  <tbody>
    <tr><td>推荐：私有数据面+受控模型</td><td>业务数据留在客户侧，最小上下文出站</td><td>落地快、模型效果好、GPU 成本低</td><td>允许合规模型出口</td></tr>
    <tr><td>全私有化</td><td>数据和模型均在客户侧</td><td>边界最清晰</td><td>预算、GPU、模型许可和运维具备</td></tr>
    <tr><td>混合 SaaS</td><td>部分 Agent 服务在云端</td><td>运维轻</td><td>客户接受数据分类与跨网调用</td></tr>
  </tbody>
</table>
""",
        "2. 环境与网络拓扑": """
<whiteboard type="mermaid">
flowchart TB
  subgraph UZ[用户接入区]
    U[Web 移动端 IM] --> W[WAF LB API Gateway]
  end
  subgraph AZ[Agent应用区]
    W --> H[Embedding Host]
    H --> Y[Y-Harness Runtime]
    Y --> WK[Async Workers]
    Y --> MG[Model Gateway]
  end
  subgraph DZ[数据与知识区]
    Y --> PG[(PostgreSQL pgvector)]
    Y --> RD[(Redis)]
    WK --> MQ[(Message Queue)]
    WK --> OS[(Object Storage)]
  end
  subgraph IZ[客户集成区]
    Y --> TG[Tool MCP Gateway]
    TG --> IOT[IoT API]
    TG --> ERP[ERP API]
    TG --> EXT[Alarm Task Meeting]
  end
  subgraph GZ[治理运维区]
    Y --> OB[Logs Metrics Traces]
    Y --> IAM[IAM Policy Audit Secrets]
  end
  MG -->|受控出站或内网| LLM[LLM Embedding Rerank]
</whiteboard>
<ul>
  <li>生产数据库、缓存、队列和对象存储不得暴露公网；管理端经 VPN/堡垒机访问。</li>
  <li>Agent 到 IoT/ERP 只通过客户 API 网关或受控 Connector 子网；出站域名/IP 白名单化。</li>
  <li>生产、预发和 POC 使用独立账号、网络、数据库、密钥、索引和对象存储桶。</li>
  <li>时间同步、DNS、证书、代理和日志出口作为部署前置条件纳入验收。</li>
</ul>
""",
        "3. 组件部署与配置基线": """
<table>
  <thead><tr><th background-color="light-gray">组件</th><th background-color="light-gray">部署形态</th><th background-color="light-gray">状态/扩缩容</th><th background-color="light-gray">关键配置</th></tr></thead>
  <tbody>
    <tr><td>API Gateway</td><td>双实例/托管</td><td>无状态</td><td>TLS、WAF、SSO、限流、请求大小、Trace</td></tr>
    <tr><td>Embedding Host</td><td>Kubernetes Deployment</td><td>无状态水平扩容</td><td>渠道适配、身份信封、流式连接</td></tr>
    <tr><td>Y-Harness Runtime</td><td>Deployment</td><td>会话状态外置</td><td>模型、Tool、Skill、Policy、Budget 版本</td></tr>
    <tr><td>Domain Pack</td><td>版本化制品/镜像</td><td>灰度与回滚</td><td>实体、图谱、指标、Skills、Verifiers</td></tr>
    <tr><td>Workers</td><td>Deployment/Job</td><td>按队列扩缩容</td><td>知识、会议、报告、Eval、重试隔离</td></tr>
    <tr><td>MCP/Connector Gateway</td><td>隔离 Deployment</td><td>按系统/租户分区</td><td>凭证、超时、熔断、Schema、幂等</td></tr>
    <tr><td>PostgreSQL</td><td>托管或 HA</td><td>主备/备份</td><td>加密、连接池、PITR、慢查询、pgvector</td></tr>
    <tr><td>Redis/Queue</td><td>HA/托管</td><td>节点故障切换</td><td>TTL、持久化、死信、重复消费</td></tr>
    <tr><td>Object Storage</td><td>私有桶</td><td>版本和生命周期</td><td>原文、解析产物、附件、备份、加密</td></tr>
    <tr><td>Observability</td><td>独立集群/托管</td><td>分层保留</td><td>日志、指标、Trace、成本、安全告警</td></tr>
  </tbody>
</table>
""",
        "4. 身份、安全、密钥与数据治理": """
<h2>4.1 身份与权限</h2>
<p>用户身份由客户 SSO 提供，服务身份使用短期凭证；授权同时校验 tenant、角色、资源和动作。池塘 ACL 在检索前、工具调用前和结果输出前分别执行。后台维护采用编辑、审核、发布、平台管理和审计分权。</p>
<h2>4.2 数据安全</h2>
<ul>
  <li>数据按公开、内部、敏感、严格敏感分级；Prompt 只包含最小必要字段。</li>
  <li>TLS 传输、数据库/对象存储加密、备份加密、KMS/Secrets 轮换，禁止密钥进入镜像和日志。</li>
  <li>模型出口前做字段白名单、脱敏和内容审计；禁止发送客户未授权的原文、人员信息和商业数据。</li>
  <li>知识摄取执行文件安全扫描、格式限制、Prompt Injection 检测、ACL 和来源标记。</li>
  <li>日志/Trace 不记录令牌和完整敏感正文；需要取证时通过受控 Evidence Store 回读。</li>
</ul>
<h2>4.3 供应链安全</h2>
<p>镜像使用固定 digest，依赖生成 SBOM 并进行漏洞扫描；制品签名后进入仓库。开源数据库、RAG 组件和模型许可需在上线前完成法务与安全评审。</p>
""",
        "5. CI/CD、版本与发布流程": """
<whiteboard type="mermaid">
flowchart LR
  C[代码 知识 Skill Policy Prompt] --> B[构建 测试 扫描]
  B --> A[制品与版本清单]
  A --> D[开发环境]
  D --> P[POC回放]
  P --> S[预发回归 压测 安全]
  S --> G[变更审批]
  G --> R[生产灰度]
  R --> M[监控与验收]
  M -->|通过| F[全量发布]
  M -->|失败| RB[回滚镜像 配置 知识 模型]
</whiteboard>
<p>一次发布必须生成 Release Manifest：代码镜像、数据库迁移、Domain Pack、Knowledge Snapshot、Graph、Skill、Policy、Prompt、模型、Connector Schema 和 Eval Dataset 的版本。禁止只回滚代码而保留不兼容的知识或接口配置。</p>
<h2>5.1 发布门禁</h2>
<ul>
  <li>单元、契约、端到端、Eval、安全扫描和迁移演练通过。</li>
  <li>生产写操作默认关闭，通过功能开关按角色/池塘灰度。</li>
  <li>重大知识/策略变更由业务审核人和技术发布人双签。</li>
  <li>灰度期间比较错误率、p95、专家修订率、Tool 失败和成本。</li>
</ul>
""",
        "6. 数据初始化、迁移与真实数据切换": """
<ol>
  <li seq="auto">导入场区、池塘、设备、批次和指标主数据，执行唯一键与关系校验。</li>
  <li seq="auto">导入知识原文，生成解析产物、Claim、索引和图谱候选；专家审核后发布 Snapshot。</li>
  <li seq="auto">加载 POC Mock，并在 data_origin 标注 mock；完成八条旅程回放。</li>
  <li seq="auto">接入脱敏真实样本，双跑 Mock 与真实 Connector，比较字段、公式、时间窗和结果。</li>
  <li seq="auto">真实数据通过对账后关闭对应 Mock；历史 Trace 保留原数据来源。</li>
  <li seq="auto">生产切换先只读，再开放任务，最后开放经审批的记录写入。</li>
</ol>
<p>迁移脚本必须可重入、可断点、可校验和可回滚。原始文件校验哈希、索引版本、失败记录和重跑结果进入迁移报告。</p>
""",
        "7. 高可用、备份、灾备与降级": """
<table>
  <thead><tr><th background-color="light-gray">对象</th><th background-color="light-gray">保护措施</th><th background-color="light-gray">降级策略</th></tr></thead>
  <tbody>
    <tr><td>应用/Agent</td><td>多实例、探针、滚动发布、自动扩缩容</td><td>切换备用模型；复杂任务转异步</td></tr>
    <tr><td>PostgreSQL</td><td>主备、PITR、每日全量+连续日志、季度恢复演练</td><td>只读模式；暂停写入</td></tr>
    <tr><td>Redis/队列</td><td>HA、持久化、死信、消费幂等</td><td>绕过非关键缓存；任务排队</td></tr>
    <tr><td>知识索引</td><td>Snapshot、可重建、版本指针</td><td>回退上一稳定版本</td></tr>
    <tr><td>IoT/ERP</td><td>超时、重试、熔断、健康检查</td><td>明确提示数据不可用，不伪装实时结论</td></tr>
    <tr><td>模型服务</td><td>主备路由、配额、限流、缓存</td><td>降级模型或模板化答复；禁止高风险动作</td></tr>
  </tbody>
</table>
<p>建议生产基线目标：RPO 不高于 15 分钟、RTO 不高于 2 小时；最终以客户业务影响分析确认。恢复后必须校验待提交 Effect、任务队列、索引版本和审计连续性。</p>
""",
        "8. 可观测、SLO 与运维流程": """
<h2>8.1 SLI/SLO</h2>
<ul>
  <li>服务：可用性、请求成功率、首 Token、端到端 p95/p99、队列时延。</li>
  <li>模型：超时、拒绝、Token、缓存、降级率、单位成功任务成本。</li>
  <li>检索：Recall、空召回、过滤后候选、Rerank 时延、引用缺失。</li>
  <li>工具：各 Connector 成功率、超时、熔断、未知写结果、对账积压。</li>
  <li>业务：作用域纠错、专家修订、任务采纳/完成、知识审核积压。</li>
</ul>
<h2>8.2 告警与值守</h2>
<p>P0 包括越权、重复写入、审计中断、数据库不可用和严重数据泄露；P1 包括模型/核心 Connector 大面积失败、队列持续积压和引用覆盖异常。每级告警定义 Owner、响应时间、升级路径、Runbook 和复盘模板。</p>
<h2>8.3 运维后台</h2>
<p>后台显示环境/版本、接口健康、模型额度、任务队列、知识发布、策略命中、Eval 走势、数据质量、审计搜索和回放入口。业务运营与基础设施告警分开，避免一线人员看到技术噪声。</p>
""",
        "9. 安装、切换、回滚与验收": """
<h2>9.1 标准实施步骤</h2>
<ol>
  <li seq="auto">完成网络、账号、证书、域名、KMS、镜像仓库和基础服务检查。</li>
  <li seq="auto">部署数据库、缓存、队列、对象存储和可观测，验证备份恢复。</li>
  <li seq="auto">部署 Gateway、Embedding Host、Y-Harness、Workers、Domain Pack 和 Model Gateway。</li>
  <li seq="auto">配置 SSO、ACL、Connector、Skill、Policy、Knowledge Snapshot 和 Eval。</li>
  <li seq="auto">执行 smoke、契约、端到端、权限、故障、压测和安全测试。</li>
  <li seq="auto">生产灰度：只读用户 → 试点池塘 → 任务 → 经审批写入。</li>
</ol>
<h2>9.2 回滚</h2>
<p>触发条件包括错误率/时延越界、越权、重复写入、核心指标对账失败、专家修订率异常或数据质量失控。回滚按功能开关、模型/Prompt、Domain Pack/知识、应用镜像、数据库迁移顺序执行；外部写入通过 Effect Receipt 对账，不做盲目反向写。</p>
<h2>9.3 交付与验收材料</h2>
<ul>
  <li>部署拓扑、资源清单、配置矩阵、网络/端口/域名表、账号权限表。</li>
  <li>镜像与 SBOM、Release Manifest、数据库迁移、备份恢复和灾备报告。</li>
  <li>接口联调、压测、安全、Eval、回放、灰度和切换报告。</li>
  <li>监控大盘、告警规则、Runbook、应急通讯录、培训和运维交接。</li>
</ul>
""",
        "10. 待客户确认与实施前置": """
<table>
  <thead><tr><th background-color="light-gray">确认项</th><th background-color="light-gray">所需产物</th><th background-color="light-gray">不确认的影响</th></tr></thead>
  <tbody>
    <tr><td>云/机房/容器平台</td><td>资源区域、K8s/VM、托管服务清单</td><td>无法冻结拓扑和采购</td></tr>
    <tr><td>网络与出站</td><td>安全域、白名单、专线/VPN、代理、DNS/证书</td><td>无法联调模型和客户系统</td></tr>
    <tr><td>模型部署边界</td><td>可用模型、数据出网、内容安全、预算</td><td>无法确定 GPU/API 和性能</td></tr>
    <tr><td>身份和合规</td><td>SSO、RBAC/ABAC、日志/数据保留、密级</td><td>无法进行真实用户测试</td></tr>
    <tr><td>SLO 与灾备</td><td>可用性、p95/p99、RPO/RTO、维护窗口</td><td>无法冻结 HA 与备份</td></tr>
    <tr><td>系统接口和数据量</td><td>API、点位、频率、日增量、峰值并发</td><td>无法完成容量和压测设计</td></tr>
  </tbody>
</table>
<callout emoji="🏁" background-color="light-green" border-color="green"><p>实施启动条件：五份文档评审通过、接口责任人到位、POC 样本与验收题齐备、测试环境和模型边界确认。生产上线条件另行通过安全、容量、数据对账和回滚门禁。</p></callout>
""",
    }
)


DOCS = [
    ("北京科德智慧渔业智能体｜业务方案（客户评审版 V1.0）", BUSINESS),
    ("北京科德智慧渔业智能体｜接口交互清单（客户确认版 V1.0）", INTERFACES),
    ("北京科德智慧渔业智能体｜技术方案（客户评审版 V1.0）", TECHNICAL),
    ("北京科德智慧渔业智能体｜资源采购清单（容量规划版 V1.0）", RESOURCES),
    ("北京科德智慧渔业智能体｜部署方案（实施评审版 V1.0）", DEPLOYMENT),
]


def main() -> None:
    results = []
    for title, sections in DOCS:
        result = create_document(title, sections)
        results.append(result)
        print(json.dumps(result, ensure_ascii=False))
    print(json.dumps({"folder_token": FOLDER_TOKEN, "documents": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
