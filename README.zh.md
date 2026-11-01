语言：[English](README.md) | **中文**

# ProxyHunter — 异步 Python 代理工具

ProxyHunter 是一个**基于 asyncio 的异步 Python 工具**，用于免费代理抓取、验证和本地转发。它以异步方式从多个免费代理网站获取列表，并发验证代理的可用性、速度、地理位置、HTTPS 支持情况和匿名度，还提供本地 Web 控制台，可以直接把验证通过的代理设为本地 HTTP/SOCKS5 转发代理的上游。

## 功能特性

- 异步优先架构：网络请求、验证、Web 控制台、转发和调度共享一个受管理的 asyncio 事件循环，支持有界验证并发和转发背压
- 从 [freeproxy.world](https://www.freeproxy.world/) 和 [proxyscrape.com](https://proxyscrape.com/) 抓取代理列表
- 并发验证代理的存活状态、延迟、HTTPS 支持、匿名度(是否泄露真实 IP),并通过二次站点交叉验证减少误判
- 自动补全代理的地理位置(国家/城市/ISP)
- 本地 Web 控制台(默认端口 9527):浏览、筛选、排序代理列表,手动触发抓取/验证,管理转发池
- 本地转发代理:HTTP(默认端口 9528)和 SOCKS5(默认端口 9529),自动在转发池中的代理之间做负载均衡和故障转移
- 定时任务(均可在设置页开关和配置间隔):
  1. 定期全量抓取并验证
  2. 定期将延迟最短的代理加入转发池
  3. 转发池代理数量不足时自动补充延迟最低的代理
  4. 定期重新验证已知代理
- 界面支持中英文切换(设置页可选语言,资源文件独立,便于以后扩展更多语言)
- 所有设置持久化保存到本地 JSON 文件

## 目录

- [ProxyHunter — 异步 Python 代理工具](#proxyhunter--异步-python-代理工具)
  - [功能特性](#功能特性)
  - [目录](#目录)
  - [安装](#安装)
  - [快速开始](#快速开始)
  - [使用方法](#使用方法)
    - [方式一:一次性抓取并输出结果文件](#方式一一次性抓取并输出结果文件)
    - [方式二:只重新验证已知代理](#方式二只重新验证已知代理)
    - [方式三:启动本地 Web 控制台 + 转发代理](#方式三启动本地-web-控制台--转发代理)
  - [设置与定时任务](#设置与定时任务)
  - [数据文件说明](#数据文件说明)
  - [常见问题](#常见问题)
  - [异步架构与生命周期](#异步架构与生命周期)
    - [转发能力边界](#转发能力边界)
  - [开发与测试](#开发与测试)

## 安装

需要 Python 3.10 及以上版本。运行依赖为 aiohttp、aiohttp-socks、python-socks 的 asyncio 扩展和 BeautifulSoup；不再使用 Requests、Flask 或 PySocks。
克隆仓库需要 Git，安装依赖和访问代理列表/验证服务需要可用的网络。以下命令在终端执行，无需单独编译软件或构建前端。

```bash
git clone https://github.com/DiamondGo/ProxyHunter.git
cd ProxyHunter
python3 -m venv .venv
source .venv/bin/activate   # Windows 上使用: .venv\Scripts\activate
python -m pip install -r requirements.txt
```

Windows PowerShell 中可将创建、激活环境和安装依赖的步骤替换为：

```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

不方便激活环境时，Linux/macOS 可用 `.venv/bin/python`，Windows 可用 `.venv\Scripts\python.exe` 直接代替 `python`。使用 `python --version` 确认版本至少为 3.10。后续示例假设已激活环境，且当前目录为项目根目录。

## 快速开始

如果希望让浏览器或其他本地应用通过代理上网，建议按下面的完整流程操作：

1. 在一个终端启动控制台和两个本地转发器：

   ```bash
   python -m proxyhunter --serve
   ```

   保持进程运行。启动服务**不会自动抓取或填充首次创建的转发池**；定时任务默认全部关闭。
2. 打开 **http://127.0.0.1:9527**。默认 README 为英文，但新控制台默认使用中文。可在 **设置 → 界面设置 → 界面语言** 中选择中英文并保存；已有设置会保留之前的语言。
3. 点击“**发起全量抓取+验证**”（英文界面为 **Full Scrape + Validate**），等待进度面板完成。首次运行需要至少一个列表来源及验证服务可访问。免费代理大量失效是常态，不保证每次都能找到可用代理。
4. 在“全部代理”中使用筛选器挑选存活代理，访问 HTTPS 站点时优先选择支持 HTTPS 的代理，并按延迟排序。勾选一条或多条记录后，点击“**加入转发池**”。**仅勾选复选框不会启用转发。**
5. 切换到“转发池”，确认本地转发代理状态显示至少一个**可用**代理。后续启动会恢复已保存的选池，但旧代理可能需要重新验证。
6. 将应用的 HTTP/HTTPS 代理设置为主机 **127.0.0.1**、端口 **9528**，或将 SOCKS5 代理设为主机 **127.0.0.1**、端口 **9529**，无需填写代理账号密码。**9527 是控制台端口，不是流量代理端口。**

向池中加入可用代理后，在**另一个终端**中任选下面一条命令验证转发：

```bash
# 通过本地 HTTP 代理的 CONNECT 隧道访问 HTTPS
curl --connect-timeout 10 --max-time 30 --proxy http://127.0.0.1:9528 "https://api.ipify.org?format=json"
# 通过本地 SOCKS5 代理访问，目标域名交给代理端解析
curl --connect-timeout 10 --max-time 30 --proxy socks5h://127.0.0.1:9529 "https://api.ipify.org?format=json"
```

示例需要 curl；Windows PowerShell 建议使用 `curl.exe`，避免旧版别名。成功时会返回检测到的出口 IP，但成功响应并不证明匿名，透明代理仍可能泄露真实 IP。失败时应先重验或更换上游。池为空或全部失效时，**不会自动改为本机直连**。

ProxyHunter 是应用级 TCP 代理，**不是 VPN 或全局隧道**；只有明确配置代理的应用会使用它，不支持 UDP。在服务终端按 **Ctrl+C** 可正常停止。设置、已知代理和选池会保存，内存中的转发统计在重启后清零。

## 使用方法

请选择一种模式：一次性抓取、重验已有代理，或长期运行控制台。前两种批处理模式导出结果后退出；控制台任务更新状态文件，但**不会自动生成批处理导出文件**。

### 方式一:一次性抓取并输出结果文件

```bash
python -m proxyhunter --pages 3 --output-dir ./output
```

运行结束后,`./output` 目录下会生成:

- `proxies_valid.json` — 完整字段(延迟、地理位置、HTTPS 支持、匿名度等)
- `proxies_valid.csv`
- `proxies_valid.txt` — 每行一个 `protocol://ip:port`,可直接用作 `--proxy` 参数

常用参数:

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `--sources` | `freeproxy_world`、`proxyscrape`，或用逗号组合；`all` 选择两者 | `all` |
| `--pages` | freeproxy.world 抓取页数 | `3` |
| `--protocols` | proxyscrape.com 请求的协议 | `http,socks4,socks5` |
| `--workers` | 异步代理验证的最大并发数，不是操作系统线程数 | `50` |
| `--timeout` | 单个请求超时(秒) | `8` |
| `--limit` | 限制新验证数量，不包含复用缓存的记录；小规模试跑可设正数 | 不限 |
| `--no-secondary-check` | 跳过二次检查，可能增加误判 | 不传此开关时启用检查 |
| `--no-geo-lookup` | 跳过直连的地理位置自动补全 | 不传此开关时启用查询 |
| `--geo-verify-via-proxy` | 通过每个代理出口确认国家/城市/ISP，额外发送请求 | 关闭 |
| `--output-dir` | 结果输出目录 | `./output` |
| `--state-file` | 已知代理的持久化状态文件 | `./proxyhunter_state.json` |
| `--recheck-after` | 复用近期验证结果的时间窗口(小时) | `6` |

小规模强制重新验证，以及额外验证真实出口位置的示例：

```bash
python -m proxyhunter --sources proxyscrape --protocols http --workers 20 --timeout 5 --limit 50 --recheck-after 0 --no-geo-lookup --output-dir ./output/quick
python -m proxyhunter --pages 5 --geo-verify-via-proxy --output-dir ./output/detailed
```

`--pages` 只影响 freeproxy.world；`--protocols` 只控制 proxyscrape.com 的请求，**不会全局过滤其他来源的协议**。存活和死亡结果默认都会缓存六小时：近期存活结果直接复用，近期死亡结果跳过验证。抓取时使用 `--recheck-after 0` 可强制检查新抓取的全部候选。`--limit` 不限制抓取数量，也不限制复用缓存后最终输出的总数。

`--timeout` 针对单次连接/读取等待，不是整项任务的总期限；每个代理可能需要多次请求。提高并发不一定更快，也可能触发上游或验证站点限流。直连地理补全会将代理 IP 发送给 ip-api.com；出口位置验证会额外通过每个存活代理请求一次。关闭直连地理补全，不会取消另外显式开启的出口位置验证。

### 方式二:只重新验证已知代理

```bash
python -m proxyhunter --revalidate
```

会跳过抓取,直接重新验证 `--state-file` 中已记录的所有代理。
可使用 `--min-age 6` 只重新验证缓存年龄至少 6 小时的代理；默认 `--min-age 0` 不限制年龄。
所有模式都支持 `-v` / `--verbose`，完整参数见 `python -m proxyhunter --help`。

```bash
python -m proxyhunter --revalidate --min-age 6 --state-file ./proxyhunter_state.json --output-dir ./output/rechecked
python -m proxyhunter --revalidate --min-age 0 --workers 20 --timeout 10 -v
```

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `--revalidate` | 不抓取，重验状态文件中的记录，包括之前失效的代理 | 关闭 |
| `--min-age` | 重验模式只检查缓存年龄至少达到指定小时数的记录 | `0`，不限年龄 |
| `-v` / `--verbose` | 启用诊断日志 | 关闭 |
| `-h` / `--help` | 显示所有参数后退出 | — |

重验模式使用 `--min-age`，不受抓取模式的 `--recheck-after` 影响；不抓取时，来源、页数和来源协议参数不生效。状态文件为空或不存在时没有可重验记录。退出码 **0** 只表示处理完成，不代表一定有存活代理；**1** 表示没有候选或发生运行错误，**2** 表示参数解析失败，**130** 表示批处理被中断。正常停止服务也可能返回 0。实际可用数量应以摘要及输出文件为准。

### 方式三:启动本地 Web 控制台 + 转发代理

```bash
python -m proxyhunter --serve
```

启动后:

- Web 控制台: `http://127.0.0.1:9527`
- 本地 HTTP 转发代理: `127.0.0.1:9528`
- 本地 SOCKS5 转发代理: `127.0.0.1:9529`

在控制台中可以:发起全量抓取、验证选中代理、把代理加入/移出转发池、配置定时任务、切换界面语言,以及修改抓取/网络等设置。设置会保存到 `--settings-file`(默认 `./proxyhunter_settings.json`),网络相关设置(监听地址/端口)修改后需要重启服务才能生效,其余设置保存后立即生效。
设置文件首次创建时由命令行参数初始化；之后以文件为准，命令行参数不会覆盖已有设置。抓取作业使用启动时的设置快照，后续作业读取新设置。

常用参数:

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `--serve` | 持续运行控制台、调度器和两个本地转发器 | 关闭 |
| `--ui-host` / `--ui-port` | Web 控制台监听地址/端口 | `127.0.0.1` / `9527` |
| `--forward-host` | 本地转发代理监听地址 | `127.0.0.1` |
| `--http-proxy-port` | 本地 HTTP 转发代理端口 | `9528` |
| `--socks-port` | 本地 SOCKS5 转发代理端口 | `9529` |
| `--settings-file` | 设置持久化文件路径 | `./proxyhunter_settings.json` |

之后把本地应用的代理设置为 `http://127.0.0.1:9528`(HTTP/HTTPS)或 `socks5://127.0.0.1:9529`,即可通过转发池中的代理出网。
三个监听器默认均仅绑定回环地址。控制台和转发器没有认证，不应通过修改监听地址直接暴露到公网；免费上游也不能视为可信服务。

希望使用其他端口和独立数据、而不更改已有配置时，可以运行：

```bash
python -m proxyhunter --serve --settings-file ./proxyhunter_alternate_settings.json --state-file ./proxyhunter_alternate_state.json --ui-port 9537 --http-proxy-port 9538 --socks-port 9539
```

请使用新的设置文件名进行首次初始化。若指定文件已经存在，应在“设置”中修改端口并保存，再点击“立即重启”，而不是期待命令行覆盖文件。更改控制台端口后需用新地址打开页面。`--state-file` 每次运行仍会决定状态路径；相对路径以当前工作目录为准。不要让多个实例同时操作同一组状态和设置文件。

## 设置与定时任务

修改“设置”后需点击“保存设置”。抓取/验证配置用于后续任务，已运行任务保留启动时的设置快照；转发池失败阈值和重试次数用于后续转发决策。监听地址/端口需要“立即重启”或停止后重新启动才能生效。重启会关闭活跃连接，使用代理的应用可能需要重连。

四类定时任务**初始均关闭**，且仅在 `--serve` 进程运行时执行：

| 任务 | 预设间隔 | 开启后的行为 |
| --- | --- | --- |
| 全量抓取与验证 | 24 小时 | 发现代理候选，并按当前缓存策略验证 |
| 定期加入低延迟代理 | 1 小时 | 将延迟最低的前 **5** 个存活代理合并进现有池，自动去重 |
| 转发池自动补充 | 1 小时 | 从尚未在池中的存活记录中补充，尝试维持至少 **3** 个可用代理 |
| 全量重验已知代理 | 12 小时 | 重验全部已知代理，不受抓取缓存窗口影响 |

间隔、加入数量和最低池容量均可调整。自动入池默认只挑选支持 HTTPS 的代理，可关闭对应开关。自动入池操作依赖已有可用记录，不保证能补满池，也不会自动复活已失败的池内记录。全量抓取和定时重验互斥，忙碌时不会重复启动。调度约每五秒检查一次到期任务，不是精确的墙上时钟定时；从未运行的任务开启后可能在下次检查立即执行。上次运行时间会跨重启保存。关闭定时开关只阻止后续触发，不取消已开始的任务。

**转发池恢复及操作区别：**

- 上游默认连续转发失败 **10 次**后会被跳过；默认最多换 **3 个额外的不同上游**重试，候选足够时合计最多尝试四个。只在尚未确认客户端连接/发送业务请求的安全阶段重试，不会在任意业务数据发出后盲目重放。
- “验证选中项”会真实发起检查，成功时清除池内失败标记；“启用”仅重置失败跟踪、允许再次尝试，不代表验证通过。
- “从池移除”或“清空转发池”保留已知代理记录；“删除选中代理”同时删除记录及池成员关系，后续抓取仍可能重新发现该代理。
- 代理的“存活”验证状态和转发池的“失败”状态不是同一个指标；缓存为存活的代理仍可能在真实转发中失败。“仅 443”表示 CONNECT 只能连接 443 端口，不能将该上游用于任意 SOCKS5 目标端口。

## 数据文件说明

`proxyhunter_state.json`(已知代理及验证结果)和 `proxyhunter_settings.json`(控制台设置)默认写在当前工作目录,属于运行时个人数据,已加入 [`.gitignore`](.gitignore)。旧 JSON 结构可以直接加载，无需迁移。损坏的内容会记录警告并使用有效记录/默认值；目录、权限等文件系统错误会明确上报。自定义状态/设置路径的父目录应已存在；导出目录会自动递归创建。

批处理导出文件**只包含存活结果**，按实测延迟排序；状态文件也会保留死亡结果，供缓存和后续重验使用。JSON 包含协议、地址、来源、`alive`、`latency_ms`、`supports_https`、`https_only`、`anonymity`、UTC 时间 `checked_at`，以及可获得的国家/城市/ISP。CSV 包含对应导出列，TXT 每行一个代理 URL。`https_only` 来自非 443 CONNECT 探测，也可能未知；匿名度可为 `elite`、`anonymous`、`transparent` 或 `unknown`。这些是当次检测结果，不是在线率、带宽、安全性或隐私保证。

建议在服务停止时备份状态和设置文件。使用原文件可恢复已知结果和选池；改用不同文件名可建立独立配置。项目没有专门用于导入任意代理列表的命令行接口。

## 常见问题

| 现象 | 排查方法 |
| --- | --- |
| 缺少依赖、导入失败 | 确认安装与运行使用同一个虚拟环境解释器，重新执行 `python -m pip install -r requirements.txt`，并检查 Python 至少为 3.10。 |
| 控制台打不开、连接被拒绝 | 保持 `--serve` 运行，查看启动日志；使用控制台而非转发端口，已有设置时以实际保存的端口为准。 |
| 端口已被占用 | 停止占用进程或更换端口；已有设置文件应在设置页修改并重启，或使用新的设置文件名。 |
| 代理返回 502、池内无可用代理 | 完成抓取后，明确将存活代理加入池并检查可用数量；重验或更换失效上游，不会自动回退本机直连。 |
| 抓取不到候选 | 检查来源站点连通性；直连失败后会最多尝试三个已验证且合适的代理，但全新空状态没有回退候选。可尝试其他来源或修复网络，不保证能自动解决首次连通问题。 |
| 没有新验证、输出为空 | 近期失效结果可能被缓存；抓取可试 `--recheck-after 0`，重验保存记录可试 `--revalidate --min-age 0`。也可能全部候选确实已失效。 |
| 服务模式参数似乎未生效 | 已有设置文件优先，应在控制台修改或使用新文件；另一个终端执行批处理也不会更改正在运行的进程。 |
| 验证缓慢、HTTPS 失败 | 单个代理可能进行多次有超时的请求；先减小 `--limit`、采用适度 `--workers`、调整 `--timeout`，检查验证服务连通性；选择支持 HTTPS 的代理并注意仅 443 限制，不要通过关闭 TLS 校验掩盖错误。 |
| 状态或设置无法保存 | 检查写入权限、父目录是否存在及是否有多个实例共用文件；保留备份，原子写入不是跨进程协调机制。 |
| 重启后页面无法自动恢复 | 修改过控制台地址/端口时，手动打开新地址；查看终端中的启动或保存错误。 |

诊断时可在原命令后加 `--verbose`。日志可能包含代理地址、目标主机和网络信息，分享前应脱敏。不要经不可信免费代理传输敏感明文数据，未认证的监听器应保持回环绑定。

## 异步架构与生命周期

- CLI 顶层只创建一个受管理的 asyncio 事件循环。抓取、验证、地理补全、aiohttp 控制台、HTTP/SOCKS5 转发和调度均在同一循环运行，没有线程式网络服务器或网络线程池。
- 后台抓取与全量重验证互斥，由受管理任务保留强引用。手动验证接口仍在完成后返回完整结果；长网络请求不会阻塞 `/api/job`。四类定时操作保留各自的持久化上次运行时间。
- 删除记录使用进程内版本检查，阻止删除之前已启动的验证/抓取把旧结果重新写回；之后的新抓取仍可重新发现该代理，不改变磁盘 JSON 结构。
- **磁盘工作仍使用工作线程**：读文件、JSON 编解码和导出通过 asyncio 的默认执行器移出事件循环。这不是网络线程模型。内存查询、同步设置读取及代理池操作留在事件循环中。
- 状态/设置写入使用独立快照，按同一事件循环中的规范化文件路径串行化，同目录临时文件写完后原子替换。取消时先等待已经启动的磁盘工作结束，再释放写入次序，避免旧写入覆盖新保存。它不是跨进程数据库锁，也不合并多个进程的修改。
- SIGINT、SIGTERM、取消、部分启动失败及 API 重启都会关闭监听器，停止调度，取消并等待作业/请求/连接，排空保存并关闭 Web 资源。重启接口先发送 JSON，再在外层生命周期完成清理后使用原始进程参数重新执行。
- 对成功启动的服务，关闭时在所有修改者停止后，额外保存最终状态、当前设置与实际选池，避免保存等锁时被取消导致内存更改丢失。未成功启动时不覆盖尚未正确加载的数据；最终保存失败会阻止重新执行，但仍完成其余资源清理。
- 嵌入式调用可使用异步 [`start_server()`](proxyhunter/lifecycle.py) 并最终等待返回对象的关闭方法，或使用异步上下文管理器；支持非持久化端口 0 覆盖。异步 [`run_server()`](proxyhunter/lifecycle.py) 可注入停止事件、启动钩子和重启回调，测试不会替换测试进程。已有事件循环中使用 [`async_main()`](proxyhunter/cli.py)，不要再次运行同步 CLI 入口。
- 存储构造函数不进行 I/O；调用方必须等待加载/保存/刷新。设置读取与存储查询保持同步，详情见 [`settings.py`](proxyhunter/settings.py)、[`store.py`](proxyhunter/store.py)。刷新只等待已排队保存，不自动保存尚未提交的内存变化。

### 转发能力边界

普通 HTTP 使用 `Connection: close`；通过 SOCKS 上游时只重写单个请求/目标，不支持跨目标持久连接或完整 HTTP 流水线重写。HTTPS 使用 CONNECT，`http` / `https` 上游标签都表示明文 HTTP 代理而非 TLS-to-proxy。SOCKS5 仅支持无认证 CONNECT，不支持 UDP、BIND 或认证扩展。负载计数表示已写入并排空的数据，不保证远端应用已消费。详细 DNS、超时及重试契约见 [`tests/NETWORKING.md`](tests/NETWORKING.md)。

## 开发与测试

在已激活的环境中安装 [开发依赖](requirements-dev.txt)：

```bash
python -m pip install -r requirements-dev.txt
```

下列命令从项目根目录执行；单元测试和集成测试均不依赖公网免费代理：

```bash
# 全套严格测试：单元、网络组件与完整应用
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest -q -W error
# 应用单元/API 测试
.venv/bin/python -m pytest tests/test_persistence.py tests/test_jobs_scheduler.py tests/test_webapp.py tests/test_cli.py tests/test_application_concurrency.py -q
# 真实本地套接字的生命周期与端到端测试
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest tests/test_lifecycle.py tests/test_integration.py -q -W error
# 整个包的语句与分支覆盖率（不只网络层）
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest -q -W error --cov=proxyhunter --cov-report=term-missing
.venv/bin/ruff check proxyhunter tests
.venv/bin/ruff format --check proxyhunter tests
```

本地 HTTP、TLS、CONNECT、SOCKS4/5 服务由 [`tests/network_fixtures.py`](tests/network_fixtures.py) 管理；[`tests/certs`](tests/certs) 的证书/私钥**仅用于测试**，不是生产凭据。测试包含原始 JSON 兼容、原子写失败/取消顺序、作业与调度、全部 API、CLI 子进程退出码、真实抓取→HTTP/HTTPS 验证→入库→选池→两类转发器载荷、删除与旧验证并发、启动失败和重启清理。Python 3.10.20 与 Python 3.12.13 的严格全套均为 659 项通过；全包语句覆盖 98.53%，分支覆盖 96.01%，合计 98.04%（Python 3.12）。另外已在仅安装新依赖的干净环境复跑通过，确认不依赖旧同步库。缺口详见 [`tests/NETWORKING.md`](tests/NETWORKING.md)。公网可靠性、真实操作系统进程替换及不同平台行为不由本地测试保证。

[`GitHub Actions`](.github/workflows/tests.yml) 在推送或拉取请求时，使用 Python 3.10/3.12 运行严格全套、覆盖率、静态与依赖检查。具体运行状态以仓库的 Actions 页面为准。
