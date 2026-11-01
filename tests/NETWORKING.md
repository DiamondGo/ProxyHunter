# 异步网络与应用集成契约

网络层与应用层的完整 asyncio 迁移均已集成。本文保留网络层的精确兼容契约和原始回归记录，并补充全应用生命周期、测试入口与验证边界；不再是未完成集成的交接清单。

## 原始回归基线

在修改任何源文件前，已运行原始同步实现上的纯逻辑测试：17 passed in 0.27s。

命令：`.venv/bin/python -m pytest tests/test_regression_baseline.py -q`

涵盖模型标识/序列化/时间戳/代理 URL、去重、代理池轮询及候选去重、连续失败阈值、成功清零、过期结果、动态阈值、两类抓取解析和回退选择/DNS URL。

原版 ProxyScrape 的解析器在同步网络生成器内部，基线用内存响应代替传输。迁移后将相同案例改为直接调用提取后的同步解析器，断言未改变。

## 公共集成接口

以下网络接口都必须等待；纯解析、去重、回退候选选择、模型方法和代理池方法仍同步。

- [`scrape_all(sources, pages=3, protocols=None, fallback_proxies=None) -> list[Proxy]`](../proxyhunter/scrapers/__init__.py:17)：参数不变，顺序抓取，隔离未知/失败来源。
- [`BaseScraper.fetch(fallback_proxies=None) -> list[Proxy]`](../proxyhunter/scrapers/base.py:11)：两类具体抓取器均为异步列表返回，不是异步生成器。构造参数不变。两类同步解析器分别接受 HTML，或文本和协议。
- [`get_with_fallback(session, url, *, params=None, timeout=15, fallback_proxies=None, max_fallback_attempts=3) -> BufferedResponse`](../proxyhunter/scrapers/fallback.py:124)：传入调用方拥有的 aiohttp 会话，直接尝试后最多三个不同回退代理；不在函数内筛选/排序候选。返回前读取并释放响应。回退会话独立且由本函数关闭。
- [`BufferedResponse(status, body, encoding="utf-8")`](../proxyhunter/scrapers/fallback.py:86)：脱离连接的响应，状态、兼容状态别名、文本、字节正文以及 JSON 解析均同步访问；无需关闭。不是 Requests 或 aiohttp 原始响应。
- [`fill_missing_geo(proxies) -> None`](../proxyhunter/geolocate.py:16)：异步、原地修改，仍只补缺失国家的代理，顺序分批请求。
- [`ProxyValidator(timeout=8.0, workers=50, secondary_check=True, secondary_url="http://www.gstatic.com/generate_204", geo_verify_via_proxy=False)`](../proxyhunter/validator.py:35)：构造不做 I/O。
- [`ProxyValidator.validate_all(proxies) -> list[Proxy]`](../proxyhunter/validator.py:67)：首次非空调用异步获取公共 IP，一次生命周期只尝试一次；返回原对象、包含死亡结果，结果按输入顺序。每次调用最多创建工作协程数个任务，不按候选数量创建任务。取消会取消并等待所有工作协程。实例应在同一事件循环使用。
- [`connect_via_upstream(upstream, target_host, target_port, timeout=10.0) -> tuple[StreamReader, StreamWriter]`](../proxyhunter/forwarder/tunnel.py:37)：成功后调用方拥有流；握手失败或取消时自行清理。
- [`relay(a_reader, a_writer, b_reader, b_writer, idle_timeout=120.0) -> int`](../proxyhunter/forwarder/tunnel.py:100)：背压、双向共享空闲期限、正常 EOF 半关闭；返回成功写入并完成排空的双向总字节数。异常重置会中止双向转发并返回此前计数，不承诺远端应用已消费数据。取消清理后抛出带已转发计数的取消异常子类，供转发器统计使用。
- [`LocalHttpProxy(pool, settings, host, port)`](../proxyhunter/forwarder/http_proxy.py:42) 和 [`LocalSocks5Proxy(pool, settings, host, port)`](../proxyhunter/forwarder/socks_proxy.py:30)：构造不绑定、不创建线程。均继承异步 [`start() -> self`](../proxyhunter/forwarder/tunnel.py:191) 与 [`shutdown() -> None`](../proxyhunter/forwarder/tunnel.py:224)，生命周期操作串行化，可重复调用。启动后地址和端口为真实绑定值，支持端口 0。关闭会结束所有已接受处理协程和连接；旧的线程启动方法已删除。

## 必须保留的网络语义

- HTTP 和 HTTPS 协议标签均表示明文 HTTP 上游代理；HTTPS 能力通过 CONNECT 验证，不是 TLS-to-proxy。
- HTTP 请求中的 SOCKS4 使用本地 DNS；验证器 SOCKS5 使用本地 DNS；抓取回退 SOCKS5h 使用远端 DNS；原始 TCP 转发 SOCKS4a/SOCKS5 使用远端 DNS。
- 主验证请求不跟随重定向，保留 Requests 的小于 400 为成功语义；二次检查包含 generate_204 时要求精确 204，否则同样按小于 400 判断。抓取、公共 IP 和 geo 跟随重定向。连接/读超时分开，不增加整个响应的总期限。
- 匿名级别、最小成功主请求延迟、非 443 CONNECT 限制探测、通过代理出口确认 geo 均保留；错误 JSON 的结构经过检查。
- 每个被验证代理拥有独立会话，回退代理之间不共享连接或 Cookie。单响应正文上限为 16 MiB。
- HTTP CONNECT 和 SOCKS 握手只读取自己的头部，早到的隧道数据/正文保留在流缓冲区，不会丢失。
- 重试仅在连接建立阶段，禁止在已发客户端成功响应或已写应用请求后重试。代理池成功依据实际中继负载，不把握手或初始 HTTP 头部记作成功；取消前的负载也能正确记账。
- 同步设置接口继续读取原有 timeout 和 pool_max_retries（默认重试 3 次）。额外可选设置为 forward_idle_timeout（默认 120 秒）、forward_max_connections（默认 256）；未在设置文件提供这些扩展项时默认值照常生效。

## 可复用本地夹具

[`network_fixtures.py`](network_fixtures.py) 提供 HTTP/TLS 站点工厂、可跟踪任务的原始 TCP 服务器，以及真实 HTTP CONNECT、SOCKS4(a)、SOCKS5 代理工厂。支持拒绝、停滞、同包 CONNECT 负载、远端名称映射和自定义 HTTP 响应。所有监听地址均为回环地址，代理和服务器拥有并清理自己创建的任务/连接。

异步测试的统一期限为 8 秒；测试夹具关闭也有有界的流关闭等待。测试证书/私钥只用于本地 HTTPS，绝不是生产凭据。无公共免费代理依赖。

## 网络组件阶段的历史验证

测试运行环境：Python 3.12.13。源文件另通过 Python 3.10 语法解析检查；未在真实 Python 3.10 解释器下执行测试。

网络迁移阶段的严格测试命令：

`PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest tests/ -q -W error`

网络阶段最终连续两轮：155 passed；其中覆盖率运行 3.32 秒，全部警告作为错误的运行 2.18 秒。该阶段指定网络源文件语句覆盖率 98%，验证器、地理补全及回退模块均为 100%。这些数字仅是历史组件基线，当前全应用验证如下。

覆盖率运行在上一命令基础上使用下列选项，且覆盖率文件写入临时目录：

`COVERAGE_FILE=/tmp/proxyhunter-network.coverage PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest tests/ -q -W error::ResourceWarning --cov=proxyhunter.validator --cov=proxyhunter.geolocate --cov=proxyhunter.scrapers --cov=proxyhunter.forwarder.tunnel --cov=proxyhunter.forwarder.http_proxy --cov=proxyhunter.forwarder.socks_proxy --cov-report=term-missing`

静态检查使用开发依赖中的 Ruff，限定指定的网络源文件及测试；检查通过。差异空白检查通过。AST 检查确认所有负责源文件能按 Python 3.10 语法解析，且不再导入 Requests、PySocks、阻塞服务器或线程模块。

## 已完成的应用集成

1. 抓取、验证、geo 的所有应用调用点均等待异步接口。CLI 顶层只运行一次事件循环，提供异步入口供嵌入；模块入口传播退出码。导出文件格式和所有命令行参数保留。
2. [`ProxyStore`](../proxyhunter/store.py) 与 [`SettingsStore`](../proxyhunter/settings.py) 构造无 I/O，加载/保存/刷新异步。设置更新/初始化异步，设置读取与存储查询、代理池操作仍同步。旧 JSON 直接加载；可变默认值与快照隔离。存储的进程内删除版本令牌配合条件写入，防止删除之前的验证/抓取旧结果复活记录，而之后的新抓取仍可重新发现代理；令牌不写入磁盘。
3. [`JSONFile`](../proxyhunter/persistence.py) 在工作线程进行文件访问与序列化。同目录临时文件 + 原子替换；同一事件循环、规范化路径内的读写/刷新按次序执行。取消已启动保存时等待工作线程完成，防止晚到旧写入覆盖后续保存。不是跨进程锁/事务，不解析符号链接别名，也不会合并独立存储实例的快照。
4. [`JobRunner`](../proxyhunter/jobs.py) 使用有强引用的受管理任务，原子地接受至多一个抓取/全量重验证；状态字段、摘要、错误、缓存和限额行为保留。[`Scheduler`](../proxyhunter/scheduler.py) 使用异步生命周期，保留四种操作、动态设置与持久化上次运行时间。
5. [`create_app`](../proxyhunter/webapp.py) 是 aiohttp 应用工厂，保留全部浏览器路径、JSON、UI 和静态资源。手动验证在完成后返回，长网络请求不阻塞 `/api/job`。错误 JSON、类型和配置返回 JSON 400。监听设置保存后待重启生效，其余设置用于后续作业/请求。
6. [`ApplicationServer`](../proxyhunter/lifecycle.py) / 异步服务工厂拥有设置、状态、调度、作业、两个转发器和 Web 请求。支持实际本地端口 0、关闭与异步上下文管理器。外层运行器支持停止事件、启动钩子、重启回调注入及 SIGINT/SIGTERM；部分启动失败和取消同样清理。API 重启先写完响应，关闭监听/连接、等待作业和请求、排空保存后，再保留原始进程参数重新执行；不在任意延时任务中执行进程替换。成功启动的服务在修改者全部停止后，还会最终保存状态与设置/实际选池，补回等锁时被取消的保存；最终保存失败阻止重新执行，但不会跳过剩余清理。部分启动失败则不对未初始化的数据做最终保存。
7. 运行依赖已迁移至 aiohttp、aiohttp-socks、python-socks[asyncio] 和 BeautifulSoup，无 Requests、Flask 或 PySocks。仅磁盘/序列化工作显式移交默认执行器，没有旧式线程网络任务或服务器。

## 全应用验证与边界

```bash
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest -q -W error
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest -q -W error --cov=proxyhunter --cov-report=term-missing
.venv/bin/ruff check proxyhunter tests
.venv/bin/ruff format --check proxyhunter tests
```

新增测试按责任分布在 [`test_persistence.py`](test_persistence.py)、[`test_jobs_scheduler.py`](test_jobs_scheduler.py)、[`test_webapp.py`](test_webapp.py)、[`test_cli.py`](test_cli.py)、[`test_lifecycle.py`](test_lifecycle.py)、[`test_integration.py`](test_integration.py) 和 [`test_application_concurrency.py`](test_application_concurrency.py)。全应用端到端覆盖 API → 本地真实 HTML/文本抓取 → HTTP/可信本地 HTTPS 验证 → JSON 入库 → API 选池 → 两类转发器传输二进制载荷；包括磁盘恢复、真实阻塞网络请求时的状态响应，以及活跃载荷隧道/后台任务在停止和重启时的清理。没有全局网络替身或公网代理依赖。

审查中发现并先写失败回归后修复两个应用并发问题：等锁保存被取消时，关闭只有刷新屏障而漏存最新内存变更；删除完成后，旧验证结果又把记录写回。新增 14 项关闭/最终保存用例和 13 项版本检查/真实网络并发用例，原测试断言未弱化，网络模块无需修复。

当前严格全套 **659 passed**；全包覆盖包含 **1967 条语句、476 个分支出口**，语句覆盖 **98.53%**，分支覆盖 **96.01%**，语句/分支合计 **98.04%**。未覆盖项主要包括原网络层的少量协议错误/异常关闭路径、临时文件清理自身失败的日志分支、抽象抓取方法和部分防御性竞态。模块入口虽经真实子进程退出码测试，但子进程不在本次父进程覆盖率采集中，因此显示为 0%，不是未测试的退出行为。详细逐行缺口以覆盖率报告为准。

最终独立验收在 Python 3.12.13 与实际 Python 3.10.20 上运行，严格全套均为 **659 passed**。Python 3.12 的干净虚拟环境仅按新开发依赖安装，确认没有 Requests、Flask、PySocks 后再次全套通过。Python 3.10 验收纠正了三处测试的跨版本假设：在整个测试外层限制期限，避免旧版 wait_for 提前调度作业；在跨 Task 边界前检查取消原因链；显式接受旧版独立的 asyncio 超时异常。原有载荷、清理、写入次序及失败原因断言保留，没有为通过测试修改运行时逻辑。上述覆盖率采集来自 Python 3.12。

新增 [`GitHub Actions`](../.github/workflows/tests.yml) 的 Python 3.10/3.12 测试矩阵，可在推送和拉取请求时持续运行相同验证；具体运行状态以仓库 Actions 页面为准，本地通过不等于远端 CI 已通过。实际 reexec 在测试中被注入回调/替身，未替换 pytest 进程。确定性本地测试不证明公网代理可靠性或所有操作系统行为。

普通 HTTP 请求采用 `Connection: close`；经 SOCKS 重写针对单个请求/目标连接，不提供跨目标持久连接或完整 HTTP 流水线重写。HTTPS 使用 CONNECT；SOCKS5 仅支持原有 no-auth CONNECT，不含 UDP/BIND/认证扩展。这些限制是保留的能力边界，不是测试证明已经实现的功能。
