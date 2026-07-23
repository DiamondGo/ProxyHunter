语言: 中文 | [English](README.en.md)

# ProxyHunter

ProxyHunter 是一个免费代理抓取与验证工具。它会从多个免费代理网站抓取代理列表,并发验证每个代理的可用性、速度、地理位置、HTTPS 支持情况和匿名度,还提供一个本地 Web 控制台,可以直接把验证通过的代理设为本地 HTTP/SOCKS5 转发代理的上游。

## 功能特性

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

## 安装

需要 Python 3.10 及以上版本。

```bash
git clone https://github.com/DiamondGo/ProxyHunter.git
cd ProxyHunter
python3 -m venv venv
source venv/bin/activate   # Windows 上使用: venv\Scripts\activate
pip install -r requirements.txt
```

## 使用方法

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
| `--sources` | 数据源,逗号分隔或 `all` | `all` |
| `--pages` | freeproxy.world 抓取页数 | `3` |
| `--protocols` | proxyscrape.com 请求的协议 | `http,socks4,socks5` |
| `--workers` | 并发验证数 | `50` |
| `--timeout` | 单个请求超时(秒) | `8` |
| `--limit` | 限制验证数量(用于测试) | 不限 |
| `--no-secondary-check` | 关闭二次站点交叉验证 | 开启 |
| `--no-geo-lookup` | 关闭地理位置自动补全 | 开启 |
| `--output-dir` | 结果输出目录 | `./output` |
| `--state-file` | 已知代理的持久化状态文件 | `./proxyhunter_state.json` |
| `--recheck-after` | 复用近期验证结果的时间窗口(小时) | `6` |

### 方式二:只重新验证已知代理

```bash
python -m proxyhunter --revalidate
```

会跳过抓取,直接重新验证 `--state-file` 中已记录的所有代理。

### 方式三:启动本地 Web 控制台 + 转发代理

```bash
python -m proxyhunter --serve
```

启动后:

- Web 控制台: `http://127.0.0.1:9527`
- 本地 HTTP 转发代理: `127.0.0.1:9528`
- 本地 SOCKS5 转发代理: `127.0.0.1:9529`

在控制台中可以:发起全量抓取、验证选中代理、把代理加入/移出转发池、配置定时任务、切换界面语言,以及修改抓取/网络等设置。设置会保存到 `--settings-file`(默认 `./proxyhunter_settings.json`),网络相关设置(监听地址/端口)修改后需要重启服务才能生效,其余设置保存后立即生效。

常用参数:

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `--ui-host` / `--ui-port` | Web 控制台监听地址/端口 | `127.0.0.1` / `9527` |
| `--forward-host` | 本地转发代理监听地址 | `127.0.0.1` |
| `--http-proxy-port` | 本地 HTTP 转发代理端口 | `9528` |
| `--socks-port` | 本地 SOCKS5 转发代理端口 | `9529` |
| `--settings-file` | 设置持久化文件路径 | `./proxyhunter_settings.json` |

之后把本地应用的代理设置为 `http://127.0.0.1:9528`(HTTP/HTTPS)或 `socks5://127.0.0.1:9529`,即可通过转发池中的代理出网。

## 数据文件说明

`proxyhunter_state.json`(已知代理及验证结果)和 `proxyhunter_settings.json`(控制台设置)默认写在项目根目录,属于运行时个人数据,已加入 `.gitignore`,不会被提交到仓库。

