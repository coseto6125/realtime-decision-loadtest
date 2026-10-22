# 即時決策架構壓測報告

這份報告用實測數字檢查本 repo 的架構：在設計負載下撐不撐得住，故障時是不是只會少做。
每一節列出指令、時長、負載、客戶端與伺服器端延遲、錯誤、資源與不變式結果；原始輸出在 `results/<測試名>/`。
主機識別資訊（既有服務的名稱、使用者、路徑）只放在不提交的 `results/private/`，本文只寫彙總值。

## 摘要

- 設計負載撐得住（T1，10 分鐘，每秒約 155 events、17 decisions）：collector 伺服器端 p99 2.1 ms；事件從進入到 Context 套用 p99 4.0 ms；
  `/decide` 伺服器端 p95 89 ms、p99 155 ms，幾乎全是模型 mock 的延遲，Decision 的 Postgres 寫入 p99 3.0 ms；錯誤 0；
  被接受的事件加上伺服器端儲值事件，與 Context 套用的筆數完全相等。
- 在已執行的注入與模擬依賴下，故障時只會少做（T3 七種注入）：模型全逾時、Context 停機 60 秒、Valkey 暫停 10 秒、事件重送、
  webhook 重送（含 1% 錯簽章）、兩個 scheduler、偽造客戶端儲值，16 條不變式全部 0 違反；每一種故障的結果都是不行動、晚一點做或重試成功，沒有多發優惠或重複處理。
  全部 39 輪測試結束後的不變式檢查都是 0。
- 容量（T4，修正後）：在主機紅線（load 3.5）停止前，到約 14.5 倍基準（每秒 247 次 decide、約 2,300 events）都沒有錯誤；
  14.5 倍是觀察到的下限：主機守衛先停止了 k6，服務本身的飽和點沒有量到。最先飽和的是 lt-decision（0.44 / 0.5 vCPU，p95 從約 100 ms 升到 174 ms），快取層 Valkey 只用 0.27 核。
  第一次 T4 在約 13 倍時出現的 1.04% 錯誤已證實是容器開檔上限 1024，並加上 collector 非阻塞連線池的放大；兩者都已修正並重現驗證。
- 尖峰（T5，目標 10 倍 60 秒，實際約 9.5 倍，k6 有 427 個 session 沒啟動）：0 錯誤，decide p99 最高 155 ms，尖峰結束後 10 秒內恢復。
- 快取層（T6）：Valkey 8.1 與 Redis 7.2 幾乎一樣；這種以 Lua 為主的負載下 Dragonfly 每事件的 CPU 是 Valkey 的 3.5 到 4 倍，
  在 8,000 events/s 由 store 自己先滿。維持 Valkey。
- 只用 Postgres（T6b）：tuned 設計（HOT 狀態列 + 按日分割的只追加事件表 + 依 txid 視界前進的 cursor）在 4,800 events/s 內符合
  Context 讀取 p95 20 ms 的預算，9,600 失敗；naive 的 jsonb 重寫在 2,400 內符合、4,800 失敗。設計尖峰是 150 events/s。
- 實作優化：從 v1 到最終版，collector 與 context 的 CPU 秒 / 千事件各少 24% 與 25%（0.0885、0.1324）。同樣工作的 Rust collector
  是 0.0416；改寫的損益兩平點以公式與 Fargate task 級距表示，在事件量超過約 1,700 events/s 之前不會讓任何 task 變小。
  Sanic 原生 server 比 Granian（ASGI）省 13% 到 29% CPU；valkey-glide 在高負載時尾端延遲比 redis-py 好 3 到 7 倍。
- 模型呼叫（T7）：呼叫本身在 Decision 的 Python 端幾乎不花 CPU（Python 側可見的 rnet 與 JSON frame 合計 < 0.5% 忙碌樣本，rnet 的 Rust 執行緒沒有歸屬）；
  保持連線時 TLS 只多 2% CPU；每次呼叫都新建 TLS 連線時，每千次模型呼叫多約 1 CPU 秒（約每次呼叫 1 ms CPU）與約 1.7 到 3.4 ms 的 p50 延遲。逾時會真的中止請求並釋放連線。斷路器在模型全逾時時把 `/decide` p95 從 155 ms 降到 4.4 ms。
- Postgres 設定：調校設定比預設多 55% 到 70% TPS，全部來自 autovacuum 的兩個參數（新表很快拿到統計，快取的 generic plan
  從 seq scan 改成索引）；builtin C.UTF-8 定序讓冪等查詢少 30%；wal_compression 量不出效果所以移除；外鍵在 Postgres 滿載時花 15% 吞吐量，
  但 T4 時 Postgres 只用 20% 配額，所以保留。
- 測試抓到的真 bug：XACK 與 EVALSHA 放在同一個 pipeline，Valkey 重啟後會遺失事件（已修正：XACK 移進腳本）。
- 長時間（T2，60 分鐘）：557,222 筆事件，六個 Python 服務的記憶體斜率都在 ±0.3 MB / 小時內，伺服器端 p99 與 T1 相同；Valkey 的 stream 在第 55 分鐘到達 MAXLEN 後由修剪維持，used_memory 162 MB。

## 環境

| 項目 | 內容 |
|---|---|
| 主機 | OCI Ampere A1 ARM, 4 OCPU, 24 GB（Ubuntu 24.04；Docker 29.5.2、Compose 5.1.4） |
| 主機上的既有服務 | 11 個容器與主機上的資料庫，測試前 load 約 0.1 到 0.5、可用記憶體約 19 GB；全程未觸碰 |
| 受測系統 | cores 0,1（`cpuset: "0,1"`）。API 服務各 0.5 vCPU / 1 GB；lt-redis 1 vCPU / 1 GB；lt-pg 1 vCPU / 2 GB |
| 壓測端 | T1、T2、T3、T7（1x）在本機 WSL 跑 k6 2.3.0（官方 linux-amd64 靜態檔，sha256 已比對 release checksums），經 SSH tunnel 打 127.0.0.1 的服務埠；T4、T5、ramp 與 T7（5x、13x）在主機上跑 `grafana/k6:2.3.0` 容器 lt-k6，釘 core 2；T6、T6b 的 driver 是 lt-bench 容器，釘 core 2 |
| 紅線守衛 | `scripts/remote/guard.sh` 每 2 秒檢查：1 分鐘 load > 3.5 或可用記憶體 < 6 GB 就停止壓測端；每個測試開始前等 load 降到 2.0 以下 |
| 軟體 | Python 3.13（`python:3.13-slim` arm64）；Sanic 25.12.1、msgspec 0.22.0、psqlpy 0.12.1、valkey-glide 2.5.3、redis-py 8.1.0 + hiredis 3.4.2、rnet 2.4.2、uvloop 0.23.0、xxhash 4.0.1、cachebox 6.2.7；Valkey 8.1.4（`valkey/valkey:8.1.4-alpine`）；Postgres 18.6（`postgres:18.6`，Debian）；Rust 1.97.1 |
| 時鐘壓縮 | 「隔天」= 30 秒，狀態逾時 = 10 秒，「一週」= 210 秒 |
| 每個服務的 process 數 | 1：每個容器只有 0.5 vCPU，多開 process 只會互搶同一份配額 |

## 模擬了什麼

- System One（意圖模型）：`lt-model` 是 mock，輸入 state、question、options，輸出 probs 與 latency_ms；機率是請求內容的雜湊，
  可重現；延遲是 lognormal（中位數 40 ms、p95 120 ms），逾時率、錯誤率與延遲分布可以從 `POST /admin/faults` 調整。
- 付款：`lt-payments` 是既有付款後端的 stub（不在範圍內），模擬架構依賴的保證：只有驗證過的事件、以金流交易 ID 去重、
  兌付在加點的同一個交易內。它送出的伺服器端事件型別是 `recharge`（`src=server`），代表一筆已完成的儲值（設計上的事件名稱是 `recharge_completed`）。
- 推播服務與點數系統：outbox 由 scheduler 標記為已送出（模擬 relay）；推播同意用 user_id 雜湊模擬 90% 同意。
- 長期特徵（常看的主播、常上線時段）：用雜湊模擬；資料倉儲不在範圍內。
- App：k6 產生合成 session（含儲值中斷序列），批次送事件，在 login、watch_30min、leave_room、cancel_recharge 同步呼叫 `/decide`。
- 既有的 10,000 名付費者（u1 到 u10000）預先寫進 payments，讓「已付費者拿不到加贈」可以被檢驗。

## 原型

| 服務 | 做什麼 | 關鍵做法 |
|---|---|---|
| lt-collector | `POST /events` 收一批事件 | msgspec 整批一次解碼（有壞事件才逐筆），壞的進 `events:quarantine`；好的轉成 msgpack 陣列，用一個 EVALSHA 在 Lua 裡整批 XADD（`MAXLEN ~ 500000`） |
| lt-context | consumer group 建 Context | XREADGROUP COUNT 500；每筆一個 EVALSHA：去重（每人最近 128 個 event_id 的 zset）、狀態更新、最近 50 筆、7 天 TTL、XACK 全在同一個腳本；重啟先讀自己的 pending；XAUTOCLAIM 收回閒置 30 秒的 pending；每秒掃「付款中」超過 N 秒（測試 N = 10）的人，送狀態逾時 Trigger 給 Decision，送失敗會重送 |
| lt-decision | `POST /decide` | 冪等（行程內 single-flight + `decisions.event_id` 唯一鍵）；Context 讀取 0.3 秒逾時；policy 版本 5 秒快取；arm = xxh3(user_id + salt)；模型 150 ms 預算，逾時、錯誤或斷路器打開就用政策預設；護欄（Context 不在或過時、已付款、iOS 只推 App Store 方案、每 session 與每週頻率上限，Redis 失效就不行動）；Decision、outbox、next-day job 用一個語句寫入 |
| lt-model | System One mock | 見「模擬了什麼」 |
| lt-payments | 既有付款後端的 stub | HMAC-SHA256 驗章；交易 ID 唯一鍵去重；同一交易內兌付（首次儲值、資格有效、未兌付，加上每人 advisory lock）；commit 後送伺服器端儲值事件，失敗由 sweeper 補送 |
| lt-scheduler | 隔天決策（壓縮時鐘） | `FOR UPDATE SKIP LOCKED` 批次搶 job；依 user_id 排序加鎖避免死鎖；holdout、已付款、曾付費、終生一次、推播同意、每日一則；兩個 replica 可同時跑 |

實作過程中由測試抓到並修正的問題：

| 問題 | 怎麼發現 | 修正 |
|---|---|---|
| XACK 和 EVALSHA 放在同一個非交易 pipeline：Valkey 重啟後腳本快取消失，腳本回 NOSCRIPT 沒有套用，事件卻被確認掉而遺失 | `docker restart lt-redis` 測試（`results/script_restart/run.txt`，修正前重啟後送出的 2 筆中遺失 1 筆） | XACK 移進套用腳本、放在每個 return 前 |
| 延遲直方圖的溢位桶回傳桶上界而不是實際最大值 | 單元測試 | 溢位桶回傳 max |
| psqlpy 0.12 沒有 `RustPSQLDriverPyBaseError` | 部署後啟動失敗 | 改用 `psqlpy.exceptions.Error` |
| 容器開檔上限 1024，約 1,000 條客戶端連線時服務開始拒絕連線，collector 的非阻塞連線池同時丟 MaxConnectionsError | T4 錯誤的根因分析 | `nofile 65536`、BlockingConnectionPool |
| rnet `no_keepalive=True` 不會停止連線重用 | T7 的 TIME_WAIT 計數 | 測「每次新連線」時改用 `pool_max_idle_per_host=0` |

## T1 基準負載（10 分鐘）

指令：`LT_SSH=<alias> scripts/run_test.sh t1_baseline -e DURATION=10m`（k6 在本機 WSL，經 SSH tunnel 打 127.0.0.1 的服務埠）。
負載：每秒 5.3 個 session（每個 session 約 29 個事件、3.2 次 decide），穩態約每秒 155 events、17 decisions，
另有 context 自己送出的狀態逾時 Trigger。結果在 `results/t1_baseline/`。

客戶端（k6，端到端）

| 端點 | 次數 | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- |
| events | 10498 | 39.3 | 42.05 | 74.93 | 83.25 |
| decide | 10287 | 42.18 | 125.8 | 190.37 | 238.46 |
| webhook | 887 | 79.72 | 83.6 | 88 | 227.16 |

錯誤 0%。k6 計數：3,181 個 session、92,951 筆事件、10,287 次 decide、887 次 webhook，事件遺失 0。

伺服器端（服務內的處理時間）

| 操作 | 次數 | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- |
| collector /events | 10498 | 0.691 | 1.456 | 2.073 | 10.93 |
| decision /decide | 10603 | 3.318 | 89.482 | 154.954 | 198.938 |
| decision PG write (1 statement, commit) | 10603 | 1.704 | 2.242 | 2.95 | 194.498 |
| decision Context load | 10603 | 0.505 | 0.91 | 1.456 | 13.742 |
| decision model call | 4070 | 42.472 | 122.462 | 151.154 | 151.154 |
| payments /webhook | 887 | 4.912 | 6.723 | 12.107 | 150.102 |
| payments PG transaction | 887 | 2.522 | 3.589 | 6.215 | 122.358 |
| event ingest to Context applied | 93838 | 2.073 | 3.068 | 4.037 | 15 |
| context apply batch | 11378 | 1.107 | 1.514 | 2.073 | 8.16 |
| scheduler claim batch | 290 | 5.746 | 10.349 | 12.107 | 16.228 |

| 容器 | process CPU 秒 | cgroup CPU 秒 | CPU 秒 / 千事件 | 記憶體起始 MB | 記憶體峰值 MB |
| --- | --- | --- | --- | --- | --- |
| lt-collector | 8.23 | 8.23 | 0.0885 | 33.96 | 36.20 |
| lt-context | 12.42 | 12.42 | 0.1324 | 41.43 | 42.47 |
| lt-decision | 20.97 | 20.97 |  | 36.32 | 40.68 |
| lt-model | 2.05 | 2.05 |  | 31.02 | 31.73 |
| lt-payments | 2.47 | 2.47 |  | 35.51 | 37.82 |
| lt-pg |  | 25.51 |  | 126.36 | 180.21 |
| lt-redis |  | 16.05 |  | 4.17 | 36.81 |
| lt-scheduler | 1.09 | 1.09 |  | 34.68 | 34.94 |



測試前主機：`existing_containers=11 load="1.47 1.99 1.72" mem_available_gib=19.3`
測試後主機：`existing_containers=11 load="0.47 0.73 1.12" mem_available_gib=19.2`

重點：
- 實際吞吐：10 分鐘 92,951 筆事件（約 155 events/s），伺服器端 10,603 次 decide（含 316 次狀態逾時 Trigger，約 17.7 次 / 秒）。
- 每一筆被接受的事件都剛好套用一次：collector 接受 92,951 筆，加上付款 stub 送出的 887 筆伺服器端儲值事件，
  context 套用 93,838 筆，兩數相等；stream lag 0。
- `/decide` 的伺服器端 p95 89 ms、p99 155 ms 幾乎全是模型 mock 的延遲（lognormal p95 120 ms，150 ms 逾時）；
  不呼叫模型的 Trigger（login、watch_30min）中位數 3.3 ms。Decision 的 Postgres 寫入（Decision、outbox、next-day job
  一個語句、一次 commit）p99 2.95 ms。
- 客戶端多出的約 38 ms 是 WSL 到主機的 SSH tunnel 往返，不是服務時間。
- 不變式 16 條全部 0 違反（`results/t1_baseline/invariants.txt`）。

## T1-R：Rust 參考 Collector（5 分鐘）

`rust-collector/` 用 axum + tokio（1 個 worker thread）、sonic-rs、fred（pool 2、pipeline）、rmp-serde，
release profile `lto = "fat"`、`codegen-units = 1`、`panic = "abort"`，toolchain 1.97.1。編譯在主機上用
`lt-rust-build` 容器（釘 core 2）跑 `./Dockerfile` 的 builder 階段，再用 `Dockerfile.runtime` 包成 distroless 映像檔
（`docker build` 無法釘 CPU，所以拆成兩步；`scripts/build_rust.sh`）。換上去的方式是 `compose.rust.yaml`，同樣的 port、cpuset、0.5 CPU、1 GB。
指令：`scripts/restack.sh -- -f compose.yaml -f compose.rust.yaml` 後 `scripts/run_test.sh t1r_rust -e DURATION=5m`。

| 指標 | Python（T1，v3 最終版） | Rust（T1-R） |
|---|---|---|
| CPU 秒 / 千事件 | 0.0885 | 0.0416 |
| 伺服器端 `/events` p50 / p95 / p99 ms | 0.691 / 1.456 / 2.073 | 0.467 / 1.151 / 1.638 |
| 客戶端 `/events` p50 / p95 / p99 ms | 39.30 / 42.05 / 74.93 | 38.97 / 45.70 / 76.74 |
| RSS MB | 36 | 9.4 |
| 映像檔大小 | 420 MB（lt-app，六個服務共用） | 50.9 MB |
| 寫入 stream 的方式 | 一個 EVALSHA 寫整批 | 每事件一個 XADD 的 pipeline（依規格） |
| 單獨 ramp 的上限（0.5 CPU） | 見下 | 見下 |

lt-context 處理結果相同：Rust collector 接受 46,495 筆，加上 462 筆伺服器端儲值事件，context 套用 46,957 筆，
等於 stream 的 entries-read，lag 0；16 條不變式 0 違反。Rust collector 用 pipeline 寫，consumer 每次讀到的批次較小，
所以 context 的 CPU 秒 / 千事件是 0.1808，高於 Python Lua 整批寫入時的 0.1324：collector 寫入的形狀會影響下游。

### Collector 單獨 ramp（k6 在主機 core 2，只打 `/events`，每批 10 筆）

指令：`K6_MODE=host K6_SCRIPT=k6/events_ramp.js scripts/run_test.sh ramp_rust -e PEAK_RPS=2000`（Python 同理，`ramp_python`）。
每 10 秒一列的完整表在 `scripts/windows.py results/ramp_*/remote/k6_points.csv.gz` 的輸出。

| 每秒請求（x10 事件） | Rust 客戶端 p95 ms | Python 客戶端 p95 ms |
|---|---|---|
| 220 | 1.3 | 1.5 |
| 550 | 6.3 | 7.4 |
| 880 | 27.9 | 49.4 |
| 1,045 | 59.4 | 76.4 |
| 1,125 | 74.5 | 90.0 |
| 1,290 | 93.5 | （已停止） |
| 1,480 | 169.8 | |
| 1,440（p95 過 500 ms） | 577.3 | |

| 結束原因與當時資源 | Rust | Python |
|---|---|---|
| 結束原因 | k6 閾值：p95 > 500 ms（約第 230 秒，約 1,440 req/s） | 紅線守衛：主機 1 分鐘 load 3.57 > 3.5（約第 160 秒） |
| collector CPU（上限 0.5） | 0.29 | 0.40 |
| Valkey CPU（上限 1.0） | 0.77 | 0.99，開始回 503（254 / 86,185 = 0.29%） |
| context CPU（上限 0.5） | 0.33 到 0.45，落後 343,694 筆 | 0.23 到 0.42，落後 27,840 筆 |
| k6（core 2） | 0.98 核，壓測端飽和 | 0.66 核 |
| 伺服器端 `/events` p95 / p99 ms（整輪） | 17.2 / 36.3 | 33.6 / 53.7 |

讀法：
- Rust collector 在 0.5 CPU 上限內沒有跑到自己的上限：它只用到 0.29 核，客戶端延遲上升是 core 2 上的 k6 已經吃滿 0.98 核。
  所以 Rust 的上限只能寫成「至少約 14,500 events/s」。
- Python collector 在約 11,000 events/s 時用到 0.40 核，先撞到的是 Valkey 的 1 vCPU（0.99），接著主機 load 觸發紅線停止。
  Python collector 本身的上限因此也沒有量到；以 0.0885 CPU 秒 / 千事件推算，0.5 核約可處理 5,600 events/s 的純 CPU 預算，
  但實測在 11,000 events/s 時只用 0.40 核，表示高負載下批次變大、每筆成本下降，線性推算偏保守。
- 兩次 ramp 都看到同一個下一層瓶頸：context consumer（0.5 核）跟不上，Rust 那輪落後 343,694 筆，stream 長度已頂到
  MAXLEN 500,000。落後量超過 MAXLEN 時，未消費的事件會被修剪掉，這是事件遺失的路徑；見 T4 的討論與 MAXLEN 的取捨。

## 實作優化

每個 A/B 都在同一台主機、同樣的 cpuset 與 cpus 上限、T1 負載（每秒約 155 events、17 decisions）下各跑 3 分鐘，
CPU 用各服務自己的 `getrusage`（`/stats` 的 `cpu_s`，自 reset 起算），延遲用服務內的直方圖。
每個服務一個 worker process：每個容器只有 0.5 vCPU，第二個 process 只會互搶同一份配額。

### 每個角色的套件與 A/B 數字

| 角色 | 採用 | 對照 | 量測 | 結論 |
|---|---|---|---|---|
| Postgres client | psqlpy 0.12.1（Rust） | 未另測 | | 沿用；單一語句 CTE 寫 Decision + outbox + job，一次 round trip、一次 commit |
| 序列化 | msgspec 0.22（Struct，JSON 只在 HTTP 邊界，stream 與 outbox 用 msgpack） | v1：stream 直接存原始 JSON | collector 0.1227 對 0.1225 CPU-s / 千事件 | 量不出差別：msgpack 編碼的成本被「整批一次 decode」的快速路徑抵掉 |
| 事件迴圈 | uvloop 0.23（Sanic 自動採用；driver 用 `uvloop.run`） | | | 沿用 |
| Valkey client（Context Builder 熱路徑） | valkey-glide 2.5.3（Rust core） | redis-py 8.1 + hiredis | 見下表 | glide 勝，熱路徑改用 glide；管理用迴圈（autoclaim、逾時、gauge）留在 redis-py |
| Collector 寫入 stream | 一個 EVALSHA 寫整批（Lua 內 XADD） | 每個事件一個 XADD 的 pipeline | collector 0.1190 對 0.0930；context 0.1828 對 0.1394 | Lua 整批勝：client 端只打包一個命令；而且 10 筆原子寫入讓 consumer 每次讀到更大的批次（apply 批次 6,056 對 3,388） |
| HTTP server | Sanic 25.12 原生 server | Granian 2.8.4（Rust），ASGI 介面 | collector 3.41 對 4.39 CPU 秒（+29%）、decision 6.70 對 7.60（+13%）、RSS 多 8 到 9 MB，延遲相同 | Sanic 勝，維持原設計。Granian 的 ASGI 轉接層比 Sanic 自己的 httptools 路徑貴；RSGI 介面要改寫應用，未測 |
| HTTP client（decision 到模型、context 到 decision） | rnet 2.4.2（Rust），持久 client | v1 用 aiohttp | 未單獨量測 | `resp.status_code.is_success()`、`await resp.bytes()`；150 ms 逾時用 `asyncio.timeout` 取消 |
| 分組雜湊 | xxhash `xxh3_64_intdigest(user_id + salt)` | blake2b | 每次 33 ns 對 224 ns（本機 x86，百萬次） | 跨 process 穩定（無 seed） |
| 行程內快取 | cachebox TTLCache（policy，5 秒）、Cache（Lua SHA） | v1 用 dict + 背景刷新 | | 沿用 |

client A/B 細節（`bench/cache_bench.py --client both`，同一組 op mix，driver 在 core 2，對 lt-redis）：

| 每秒事件 | client | CPU-s / 千事件 | apply pipeline p99 ms | XADD pipeline p99 ms | 讀 Context p99 ms | 落後 tick |
|---|---|---|---|---|---|---|
| 150 | redis-py + hiredis | 0.338 | 1.64 | 4.04 | 2.84 | 0 |
| 150 | glide | 0.327 | 1.35 | 3.19 | 2.73 | 0 |
| 2,000 | redis-py + hiredis | 0.248 | 1.35 | 2.95 | 2.73 | 0 |
| 2,000 | glide | 0.240 | 1.30 | 3.32 | 3.19 | 0 |
| 6,000 | redis-py + hiredis | 0.154 | 12.11 | 238.54 | 258.01 | 371 |
| 6,000 | glide | 0.147 | 3.59 | 34.91 | 34.91 | 49 |

低負載時兩者 CPU 只差 3%；接近 driver 單核飽和（6,000 events/s，driver 用掉 0.9 核）時，glide 的尾端延遲小 3 到 7 倍。

### CPU 秒 / 千事件（T1）

| 版本 | 內容 | lt-collector | lt-context |
|---|---|---|---|
| v1 | JSON 直存 stream、redis-py pipeline、blake2b、aiohttp | 0.1227 | 0.1857 |
| v2 | msgpack stream、xxhash、rnet、cachebox | 0.1225 | 0.1863 |
| v2 + glide | context 熱路徑改 glide | 0.1190 | 0.1828 |
| v3（最終） | collector 一個 EVALSHA 寫整批 | 0.0930 | 0.1394 |

v1 到 v3：collector 少 24%，context 少 25%。

### Profile（py-spy `--native --idle`，250 Hz，T1 負載下 120 秒）

分類：最內層 Python frame 是事件迴圈的 `run_forever`（epoll 等待）算閒置；最內層是 Python frame 或 CPython C-API（`Py*`、`_Py*`）
算 Python 直譯器；其餘（C/Rust 擴充、libuv/uvloop、libc 系統呼叫如 socket write）算 native。py-spy 看不到核心內的堆疊，
系統呼叫的時間落在 libc 的進入點上。py-spy 本身跑在同一個 cgroup，所以 profile 那一輪的 cgroup CPU 會灌水，CPU 數字一律取未剖析的那一輪。

| 執行 | 服務 | Python 樣本 | native 樣本 | 閒置樣本 | Python 佔忙碌 CPU |
|---|---|---|---|---|---|
| v1（100 Hz，無 --idle） | lt-collector | 34 | 54 | 220 | 38.6% |
| v1 | lt-context | 113 | 88 | 273 | 56.2% |
| v2 | lt-collector | 259 | 317 | 29,104 | 45.0% |
| v2 | lt-context | 654 | 600 | 28,165 | 52.2% |
| v3（最終） | lt-collector | 198 | 374 | 29,149 | 34.6% |
| v3（最終） | lt-context | 411 | 450 | 28,813 | 47.7% |

前五名熱點（自身時間，樣本數）：

| 執行 | lt-collector | lt-context |
|---|---|---|
| v1 | libc socket 寫入 28；redis-py `pack_command` 9；httptools `http1_request_header` 5；Sanic `receive_body` 3；libuv `uv__io_poll_check` 2 | libc socket 寫入 60；`pack_command` 9；`apply` 7；redis-py `encode` 7；`execute_command` 6 |
| v2 | libc socket 寫入 124；`http1_request_header` 51；`pack_command` 32；`encode` 20；`receive_body` 13 | libc socket 寫入 376；`pack_command` 64；`apply` 41；hiredis `read_response` 35；`asyncio.timeouts.reschedule` 35 |
| v3 | libc socket 寫入 140；`http1_request_header` 59；Sanic `handle_request` 22；Sanic `http1` 13；`reschedule` 9 | libc socket 寫入 139；`consume` 54；`apply` 48；cffi `from_buffer` 39；glide `convert_commands_to_c_batch_info` 31 |

v3 的 collector 已經沒有 redis-py 的打包成本進前五名；剩下的是 HTTP 解析（native）與 socket 系統呼叫。
context 的 Python 成本移到 glide 的 cffi 參數轉換與自己的 `apply` 迴圈。

### 改寫成 Rust 或 Go 的損益兩平點

CPU 秒 / 千事件本身不能決定要不要改寫；它要乘上事件量、可省下的比例和單價，再和改寫加維護的成本比。

1. Python frame 佔比（v3 profile，忙碌樣本）：lt-collector 198 / 572 = 34.6%，lt-context 411 / 861 = 47.7%。
   這是「只省掉直譯器」的估計上限。
2. 絕對值（T1，10 分鐘）：lt-collector 0.0885、lt-context 0.1324 CPU 秒 / 千事件。
3. 實測的 Rust 對照（T1-R）：同樣工作的 Rust collector 是 0.0416。實際可省 Δ = 0.0885 − 0.0416 = 0.0469 ms CPU / 事件，
   比 Python 佔比推得的 0.0885 × 34.6% = 0.0306 ms 還多，因為 Rust 版同時省掉了部分 HTTP 解析與系統呼叫成本。
   以下用實測的 Δ 取代 Python 佔比的估計。

公式（P 是 Fargate 每 vCPU 小時的單價，留作輸入；M 是每月要攤回的改寫加維護成本，同一貨幣）：

- 省下的核心數 = E × Δ，E 為每秒事件數，Δ = 0.0469 × 10⁻³ CPU 秒 / 事件
- 每月省下 = E × Δ × 730 小時 × P = E × 0.0342 × P
- 損益兩平事件量 E* = M / (0.0342 × P)

例：設計尖峰 E = 150 events/s，每月省 5.1 vCPU 小時（0.007 個 vCPU）。M 每多 1 個 P（也就是每月多 1 vCPU 小時的成本），
E* 就多 29 events/s。

另一個門檻比公式更早出現：Fargate 的 task 以 0.25 vCPU 為最小級距，省下的 CPU 要讓 task 降一級才變成錢。
Python collector 在 0.25 vCPU 內、以 60% 使用率為上限，約可處理 0.25 × 0.6 / 0.0885 × 1000 = 1,695 events/s（以 T1 的單位成本，
高負載時批次變大，實際更多）；設計尖峰 150 events/s 只用到 0.013 vCPU。在事件量超過約 1,700 events/s、需要第二個
0.25 vCPU 級距之前，改寫 collector 不會讓任何 task 變小。lt-context 的 Python 佔比較高（47.7%），但它的成本有一半在
glide 的 cffi 轉換與 Valkey 本身，改寫前應先用同樣的方法量 Rust 對照。

## Postgres 設定（PG 18.6）

映像檔用 `postgres:18.6`（Debian 版，`pg_config --configure` 含 `--with-liburing`），資料目錄掛在
`/var/lib/postgresql`（PG18 映像檔改放 `/var/lib/postgresql/18/docker`）。叢集以
`--locale-provider=builtin --builtin-locale=C.UTF-8` 初始化，文字鍵逐位元組比較。設定檔是
`pg/postgresql.conf`，每個參數一行理由。lt-pg 上限 1 vCPU / 2 GB、cores 0,1。

### A/B 一：T1 負載（每組 3 分鐘，全新叢集）

指令：`LT_SSH=<alias> scripts/pg_matrix.sh`（內部呼叫 `scripts/restack.sh` 與 `scripts/run_test.sh <名稱> -e DURATION=3m -e RATE=5.3`）。

| 組別 | 設定 | 定序 | io_method | decide PG 寫入 p99 ms | `/decide` 伺服器 p95 ms | 冪等查詢 mean ms | WAL bytes（3 分鐘） | checkpoint | lt-pg CPU s |
|---|---|---|---|---|---|---|---|---|---|
| pg_a_default | 預設 | libc en_US.utf8 | worker | 3.068 | 89.482 | 0.034 | 5,938,965 | 0 | 8.63 |
| pg_b_libc | 調校 | libc en_US.utf8 | worker | 3.068 | 93.061 | 0.031 | 5,773,632 | 0 | 9.22 |
| pg_c_tuned | 調校 | builtin C.UTF-8 | worker | 3.318 | 89.482 | 0.023 | 5,552,850 | 0 | 8.86 |
| pg_d_iouring | 調校 | builtin C.UTF-8 | io_uring | 3.191 | 89.482 | 0.023 | 5,735,345 | 0 | 9.92 |

T1 時 Postgres 只用到約 4% 的 1 vCPU（每組約 4 分鐘、9 CPU 秒），`/decide` 的 p95 由模型 mock 的延遲決定（lognormal p95 約 120 ms），
所以 commit p99 與 `/decide` p95 的差異都在雜訊內。唯一穩定的差異是定序：冪等查詢
`SELECT body FROM decisions WHERE event_id = $1` 的平均時間 libc 0.031 到 0.034 ms，builtin 0.023 ms，約少 30%。
四組的 16 條不變式全部 0 違反。

### A/B 二：讓 Postgres 滿載（pgbench，每組 60 秒）

T1 量不出設定差異，所以另外用 pgbench 重播 decide 熱路徑（冪等查詢 + Decision/outbox/next-day job 單一語句寫入，
`bench/pgbench_decide.sql`），8 個 client，`-M prepared`，從 core 2 的 `lt-pgbench` 容器打，應用服務停止。
每組都是全新叢集。指令：`scripts/pg_bench_matrix.sh`、`scripts/pg_ablation.sh`；百分位數由 `scripts/pgbench_pct.py` 從逐筆 log 算出。

| 組別 | TPS | p50 ms | p95 ms | p99 ms | WAL bytes / 交易 |
|---|---|---|---|---|---|
| 預設（第 1 次） | 1,038 | 2.57 | 44.47 | 68.93 | |
| 預設（第 2 次） | 1,015 | 2.56 | 48.06 | 68.47 | 1,228 |
| 預設 + shared_buffers 512MB | 1,005 | 2.63 | 47.30 | 69.12 | |
| 調校，libc（第 1 次） | 1,606 | 2.46 | 18.28 | 52.65 | |
| 調校，libc（第 2 次） | 1,604 | 2.44 | 17.87 | 52.08 | |
| 調校，builtin（第 1 次） | 1,547 | 2.52 | 36.45 | 52.35 | |
| 調校，builtin（第 2 次） | 1,719 | 2.37 | 12.10 | 50.64 | 1,203 |
| 調校，builtin（第 3 次） | 1,736 | 2.33 | 11.93 | 50.85 | |
| 調校，io_uring | 1,587 | 2.49 | 28.11 | 52.45 | |
| 調校，shared_buffers 128MB | 1,730 | 2.35 | 11.09 | 50.02 | |
| 調校，wal_compression off | 1,716 | 2.29 | 11.93 | 50.94 | 1,213 |
| 調校，autovacuum 改回預設 | 1,023 | 2.57 | 47.03 | 69.54 | |

四組 lt-pg 都吃滿 1 vCPU（每 60 秒約 59.7 CPU 秒），所以 TPS 就是每 CPU 秒的效率。

消融結果：調校設定比預設多出的 55% 到 70% TPS，全部來自 `autovacuum_naptime = 10s` 與
`autovacuum_vacuum_cost_limit = 2000` 這一組。把這兩個改回預設，調校設定掉回 1,023 TPS，和預設一樣；
shared_buffers 128MB 或 512MB、wal_compression 開或關，都量不出差別。原因見 `results/pg_plan_check.txt`：
全新叢集上 decisions 表還沒被 ANALYZE，pgbench 以 `-M prepared` 第一次規劃 `SELECT body FROM decisions WHERE event_id = $1`
時表幾乎是空的，計畫選了 seq scan，之後一直沿用快取的 generic plan。autovacuum 預設每 60 秒才醒一次，30 秒的測試裡從未 ANALYZE：

| 30 秒 pgbench | TPS | decisions seq_scan | seq_tup_read | idx_scan | autoanalyze 次數 |
|---|---|---|---|---|---|
| 調校設定，autovacuum 預設 | 400 | 32,213 | 190,190,360 | 15,694 | 0 |
| 調校設定（naptime 10s、cost_limit 2000） | 1,438 | 19,384 | 61,583,974 | 153,161 | 2 |

所以這兩個參數的效果不是「vacuum 比較快」，而是「新表很快拿到統計，快取的計畫被失效後改走索引」。這在正式環境一樣會發生：
新部署、新的月分割區、大量匯入後的前幾分鐘。設定之外的對策是建立分割區或匯入後立即 ANALYZE。

其餘結論：
- io_uring 對 worker：單次 1,587 TPS，落在 builtin 三次（1,547 到 1,736）的範圍內，量不出差別，所以保留預設 `worker`。
  io_uring 在 Docker 預設 seccomp 下無法啟動（`FATAL: could not setup io_uring queue: Operation not permitted`，
  `results/iouring_default_seccomp.txt`）；`pg/seccomp-iouring.json` 只對 lt-pg 加上 io_uring 三個系統呼叫後可以啟動。
- 定序：滿載吞吐量 libc 1,605 對 builtin 1,547 到 1,736，在雜訊內；T1 的冪等查詢少 30%。保留 builtin，理由是熱查詢較快，
  而且逐位元組比較讓 event_id、txn_id 的唯一鍵不受 glibc 定序版本變動影響（正確性）。
- wal_compression：WAL 每筆交易 1,203 B（zstd）對 1,213 B（off），TPS 相同，因為這種負載的 WAL 裡 full-page image 很少。
  依「沒有數字支持就不留」的規則，從設定檔移除（原始設計要求 zstd，這裡是有意的偏離）。

### 每個參數的類型與 RDS 對應

| 參數 | 值 | 類型 | 依據 | RDS 參數群組可設定？ |
|---|---|---|---|---|
| max_connections | 60 | 尺寸 | 連線池合計 22 | 可以（RDS 預設是依記憶體的公式） |
| shared_buffers | 512MB | 尺寸 | 2 GB 的 25%；60 秒消融量不出差別 | 可以，但 RDS 預設是公式 `{DBInstanceClassMemory/32768}`（單位 8 KB 頁） |
| effective_cache_size | 1536MB | 尺寸 | 規劃器提示 | 可以（預設公式） |
| work_mem | 8MB | 尺寸 | 60 連線 x 8 MB 仍在上限內 | 可以 |
| maintenance_work_mem | 128MB | 尺寸 | autovacuum 每個 worker 一份 | 可以（預設公式） |
| checkpoint_timeout | 15min | 尺寸 | 60 秒的消融看不出，長跑才有意義 | 可以 |
| max_wal_size / min_wal_size | 4GB / 1GB | 尺寸 | 同上 | 可以 |
| checkpoint_completion_target | 0.9 | 尺寸 | PG 預設值，寫明 | 可以 |
| default_toast_compression | lz4 | 尺寸 | decision body 多半小於 TOAST 門檻，量不出 | 可以 |
| synchronous_commit | on | 正確性 | Decision、outbox、付款、資格不可在當機後遺失 | 可以；也可在交易內 SET LOCAL |
| random_page_cost | 1.1 | 尺寸 | SSD；本負載都是點查 | 可以 |
| effective_io_concurrency | 200 | 尺寸 | 本負載沒有 bitmap heap scan | 可以 |
| io_method / io_workers | worker / 3 | 量測 | io_uring 量不出差別 | 依引擎版本：PG18 才有；io_uring 需要主機核心與 seccomp 支援，RDS 是否開放需查該版本參數清單 |
| jit | off | 正確性（延遲） | OLTP 短查詢不該付編譯成本 | 可以 |
| autovacuum_naptime | 10s | 量測 | +55% 到 70% TPS（消融） | 可以 |
| autovacuum_vacuum_cost_limit | 2000 | 量測 | 同上 | 可以 |
| shared_preload_libraries | pg_stat_statements | 觀測 | 稽核與 A/B 數字 | 可以（RDS 預設已載入） |
| track_io_timing | on | 觀測 | pg_stat_io 時間欄位 | 可以 |
| log_* | 見設定檔 | 觀測 | autovacuum 次數、慢查詢 | 可以 |
| 定序 builtin C.UTF-8 | initdb | 正確性 + 量測 | 熱查詢 -30% | 建立資料庫時指定（`CREATE DATABASE ... LOCALE_PROVIDER builtin`），不是參數群組 |

### 依資料類別決定耐久性

- Decision、outbox、next-day job、付款紀錄、資格：`synchronous_commit = on`。這些是稽核與金流事實，當機後必須還在。
- 可重建的資料（User Context、最近事件；T6b 的 Postgres 版本）：寫入交易用 `SET LOCAL synchronous_commit = off`。
  當機最多遺失最後幾百毫秒的 Context 更新，可以從事件流重算。T6b 另外測了 UNLOGGED 表：當機時整張表會被清空，
  也不會複寫到 RDS Multi-AZ 的備援節點，只適合真正可丟的快取。

## 索引與 schema 稽核

### Postgres

已做的改動（`sql/schema.sql`）：

| 項目 | 做法 |
|---|---|
| 定序 | 叢集以 builtin C.UTF-8 初始化；event_id、user_id、txn_id 逐位元組比較。T1 冪等查詢 mean 0.031 到 0.034 ms 降到 0.023 ms（見 Postgres 設定一節） |
| 外鍵 | outbox.decision_id、next_day_jobs.decision_id 參照 decisions；outbox.job_id、entitlements.job_id 參照 next_day_jobs；entitlements.payment_id 參照 payments |
| CHECK | outbox `num_nonnulls(decision_id, job_id) = 1`；所有 status、arm、action、trigger、model_status、result、currency、channel 欄位限定允許值；entitlements `(redeemed_at IS NULL) = (payment_id IS NULL)` |
| 外鍵欄位索引 | outbox(decision_id)、outbox(job_id)、entitlements(job_id)、entitlements(payment_id) |
| 欄位順序 | 8 位元組型別在前，再來 4 位元組、bool、變長欄位，列內沒有對齊填充 |
| Decision body | bytea msgpack；冪等路徑讀出後轉成 JSON 回應 |
| fillfactor | 原地更新的表（next_day_jobs、outbox、payments、entitlements）設 70 |
| autovacuum | 熱表（decisions、next_day_jobs、outbox）`autovacuum_vacuum_scale_factor = 0.01`、`autovacuum_vacuum_insert_scale_factor = 0.05` |

外鍵的寫入成本（pgbench decide 熱路徑，8 client，60 秒，lt-pg 吃滿 1 vCPU；`scripts/pg_fk_check.sh`）：

| schema | TPS（各次） | p99 ms |
|---|---|---|
| 有外鍵（調校設定） | 1,719、1,736 | 50.6、50.9 |
| 拿掉五個外鍵 | 2,070、1,952 | 43.7、47.5 |

Postgres 滿載時外鍵約花掉 15% 的吞吐量：每次插入 outbox 與 next-day job 都要在 decisions 上做一次索引查找並加 `FOR KEY SHARE` 列鎖。
但在 T4（修正後）的尖峰，lt-pg 只用到 0.20 核（配額 20%），Postgres 不是膝點，這 15% 在 T4 量不出來，所以外鍵保留。
門檻：當 lt-pg 的 CPU 超過配額約 70%、而且 decide 的寫入延遲開始上升時，重新評估（例如只保留 outbox 的 CHECK、改用延遲檢查）。

HOT 更新（T1，10 分鐘，`results/t1_baseline/pgstats.txt`）：

| 表 | n_tup_upd | n_tup_hot_upd | HOT 比例 | 原因 |
|---|---|---|---|---|
| outbox | 6,713 | 0 | 0% | `status` 是部分索引 `outbox_pending` 的條件欄位 |
| next_day_jobs | 889 | 0 | 0% | `status` 是部分索引 `next_day_jobs_due` 的條件欄位 |
| payments | 886 | 0 | 0% | `emitted` 是部分索引 `payments_unemitted` 的條件欄位 |
| entitlements | 85 | 0 | 0% | 兌付更新 `payment_id`，而 `entitlements_payment`（外鍵索引）就在這個欄位上 |

四張表的更新量都很小（outbox 每秒 11 筆最多）。outbox 是唯一稱得上熱的：每筆 Action 一次 insert、一次 update，
非 HOT 更新讓 `outbox_pending` 與其他三個索引各多一筆。替代做法是「送出即刪除」的佇列表（列只 insert 與 delete），
或把已送出的列搬到 outbox_sent。門檻：當 outbox 的非 HOT 更新讓 autovacuum 跟不上（n_dead_tup 在兩次 autovacuum 之間持續上升）
或 outbox 的索引大小超過表本身時改做；T1 時 outbox 10 分鐘 autovacuum 56 次，n_dead_tup 停在 100，離門檻很遠。
payments 的 `emitted` 同理：每秒約 1.5 筆，不改成 pending-emit 佇列（而且 payments 在新的架構邊界裡屬於既有付款後端）。

保留期與分割：decisions、outbox、contact_log 會無限成長。以設計規模推算：DAU 2.5 萬，每人每天約 2 個 session、每 session
3.2 次 decide，decisions 每天約 16 萬列、每月約 480 萬列。T1 實測 decisions 每列（含索引）約 510 位元組（5,288 kB / 10,610 列），所以每月約 2.4 GB。
門檻：任何保留政策一確定（例如 Decision 保留 13 個月供實驗回溯），就按月分割，用 DROP PARTITION 取代 DELETE；
沒有保留政策時，表超過 5,000 萬列（約 10 個月）或熱索引超過 shared_buffers 的一半（256 MB）就分割。
分割時注意上面 Postgres 設定一節的發現：新分割區要在建立後立即 ANALYZE，否則快取的計畫會在空分割區上選 seq scan。

### 每個 SQL 的計畫（`results/index_audit.md`）

`scripts/index_audit.py` 在 60 分鐘 soak 之後、資料還在的 lt-pg 上，對 lt/*.py 的每一個 SQL 跑 `EXPLAIN (ANALYZE, BUFFERS)`
（寫入語句在 rollback 的交易內執行），完整的計畫節點、列數與 buffer 在 `results/index_audit.md`。平均時間取 soak 的 pg_stat_statements
（`results/t2_soak/pgstats.txt`，60 分鐘；index_audit.md 裡標成「not called」的，是 pg_stat_statements 把字面值換成 $n、字串對不上，數字在這張表補上）。

| 語句 | 計畫裡的存取路徑 | EXPLAIN ANALYZE ms | soak 呼叫次數 / 平均 ms |
|---|---|---|---|
| decision.SQL_GET（冪等查詢） | Index Scan using decisions_event_id_key（hit 4） | 0.065 | 63,560 / 0.024 |
| decision.SQL_WRITE（Decision + outbox + job） | 三個 INSERT；衝突判斷用唯一索引 decisions_event_id_key | 1.027 | 63,560 / 0.195 |
| decision.SQL_POLICIES | Seq Scan on policies（4 列，允許） | 0.071 | 746 / 0.043 |
| payments.SQL_LOCK | advisory lock，不讀表 | 0.037 | 8,751 / 0.008 |
| payments.SQL_PAY | Index Scan using entitlements_user_id_campaign_key、payments_user；衝突判斷用 payments_provider_txn_id_key | 0.292 | 5,252 / 0.133 |
| payments.SQL_EMITTED | Index Scan using payments_pkey | 0.178 | 5,252 / 0.087 |
| payments.SQL_UNEMITTED | Index Scan using payments_unemitted（部分索引） | 0.022 | T1：0.022 |
| scheduler.SQL_CLAIM | Index Scan using next_day_jobs_due（部分索引） | 0.029 | 1,831 / 0.039 |
| scheduler.SQL_FACTS | Index Only Scan using payments_user（兩次）、entitlements_user_id_campaign_key | 0.095 | 3,499 / 0.030 |
| scheduler.SQL_GRANT | 三個 INSERT；衝突判斷用 contact_log_user_id_day_key、entitlements_user_id_campaign_key | 0.670 | 2,270 / 0.243 |
| scheduler.SQL_DONE | Index Scan using next_day_jobs_pkey | 0.169 | 5,266 / 0.072 |
| scheduler.SQL_RELAY | Index Scan using outbox_pending（部分索引）、outbox_pkey | 0.054 | 1,825 / 0.358 |
| scheduler.SQL_BACKLOG | Index Only Scan using next_day_jobs_due、outbox_pending | 0.041 | 1,825 / 0.046 |

soak 期間的 seq_scan（`pg_stat_user_tables`）：policies 746（允許，4 列）、entitlements 391、decisions 133、next_day_jobs 123、outbox 15、
contact_log 0、payments 0。entitlements、decisions、next_day_jobs、outbox 的 seq scan 都發生在表還只有一兩個 page 的開頭幾十秒：
規劃器在那個大小選 seq scan 是對的，而且 decisions 在第一次 autoanalyze 前用的是快取的計畫（見 Postgres 設定一節）。
表長大之後，上表每個熱語句都走索引；同一段時間 decisions_event_id_key 有 127,055 次 idx_scan、entitlements_user_id_campaign_key 10,749 次。

idx_scan 為 0 的非約束索引：
- decisions_created_brin（24 kB）：熱路徑不用；留給按時間範圍的保留期刪除與報表查詢，BRIN 的寫入成本接近零，依審查意見保留。
- outbox_decision（1.6 MB）、outbox_job（464 kB）、entitlements_job、entitlements_payment：外鍵欄位的索引。熱路徑不用，但刪除或更新被參照的
  decisions、next_day_jobs、payments 時，外鍵檢查要靠它們找參照列（沒有它們就是整表掃描），離線的不變式查詢也用這些欄位 join。保留。
- 其餘 0 次的都是主鍵或唯一約束的索引（contact_log_pkey、entitlements_pkey、next_day_jobs_decision_id_key、policies_pkey）。

### Valkey

| 項目 | 設定或量測 |
|---|---|
| 記憶體政策 | `maxmemory 700mb`、`maxmemory-policy noeviction`：被驅逐的 stream 等於遺失事件，所以寧可寫入失敗（collector 回 503，App 重試）。Context 鍵帶 7 天 TTL |
| 持久化 | `appendonly yes`、`appendfsync everysec`；`lazyfree-lazy-user-del yes`；`io-threads 2` |
| Context 編碼 | 樣本 200 個使用者：`ctx:{uid}` hash 全部 listpack，平均 330 B（約 10 個欄位）；`ev:{uid}` 最近 50 筆 list 全部 listpack，平均 3,772 B；`dd:{uid}` 去重 zset（最多 128 個 event_id）全部 listpack，平均 1,720 B |
| 每位活躍使用者 | 約 5.8 KB。7 天內活躍 10 萬人約 580 MB，這是 1 GB 節點的主要用量；要縮小可以把去重上限從 128 降到 64 |
| stream | 每筆約 145 B（T1：93,838 筆 13.6 MB）。`XADD ... MAXLEN ~ 500000`：約 73 MB，在每秒 150 筆時保留 55 分鐘，等於 consumer 可以停 55 分鐘而不掉事件；10 倍尖峰時只剩 5.5 分鐘 |
| 最近事件 | `LPUSH` 後 `LTRIM 0 49`，固定 50 筆 |
| 腳本 | EVALSHA + NOSCRIPT 重新載入（Dragonfly 不支援 FUNCTION，所以不用 FUNCTION LOAD）。所有鍵都在 KEYS[] 宣告；同一使用者的鍵用 `{uid}` hash tag 落在同一個 slot |
| 跨重啟 | `docker restart lt-redis` 後：collector 第一次請求回 503（舊連線失效），重試成功；context 收到 NOSCRIPT，重新載入腳本後重讀 pending，4 筆事件全部套用（`results/script_restart/run.txt`） |

這個重啟測試抓到一個真實的 bug：最初的版本把 XACK 和 EVALSHA 放在同一個非交易 pipeline，NOSCRIPT 讓腳本沒執行，
但 XACK 照樣把事件確認掉，事件就遺失了（修正前的紀錄也在同一個檔案）。修正是把 XACK 放進套用腳本裡、放在每個 return 之前：
事件只會被「套用它的那次腳本」確認。

SLOWLOG 與 LATENCY DOCTOR（T1 後 `results/t1_baseline/valkey_audit.txt`、T4 後 `results/t4_stress/redis_stats.txt`）：
- T1 後：SLOWLOG 空（沒有超過 10 ms 的命令）。LATENCY DOCTOR 回報延遲監控未啟用，之後的設定加上 `--latency-monitor-threshold 5`。
- 第一次 T4 後：SLOWLOG 有 1 筆 EVALSHA 10.5 ms（collector 整批寫入腳本，發生在錯誤爆發的那幾秒）；LATENCY DOCTOR：command 尖峰 5 次（平均 10 ms、最差 28 ms）、aof-write 尖峰 2 次（平均 5 ms）。
- 修正後的 T4：SLOWLOG 空；LATENCY DOCTOR：command 6 次、fast-command 4 次、aof-write 6 次，平均都是 5 ms、最差 7 ms。在 2 個共享核心上的 AOF fsync 偶爾造成 5 到 7 ms 的停頓，沒有影響服務的 p99。

## T3 故障注入（基準負載，一次一種）

指令：`LT_SSH=<alias> scripts/t3.sh`（七種依序跑，每種結束都跑 `sql/invariants.sql` 與 `sql/facts.sql`）。
負載都是 T1 的 traffic（每秒 5.3 個 session）。每一輪的 16 條不變式全部 0 違反（`scripts/inv_check.sh results/t3_*/invariants.txt`）。

一個流程上的錯誤要先說：第一次跑 context 停機與 Redis 暫停時，注入指令用 ssh 在主機背景執行，但 ssh 會等背景程序結束才返回，
結果 k6 在注入結束後才開始（`k6_exit.txt` 的 start 晚於 `inject.log` 的 start），那兩輪等於沒有注入。修正 `scripts/run_test.sh`
（改成在本機把 ssh 放到背景）後重跑，下表是重跑的結果；其他五種不使用注入指令，不受影響。

| 情境 | 指令重點 | 客戶端 p95 / p99 ms（events；decide） | 錯誤 | 系統的反應 | 不變式 |
|---|---|---|---|---|---|
| 模型 100% 逾時 | `POST /admin/faults {"timeout_rate":1.0}`，3 分鐘 | 73.9 / 77.0；193.8 / 197.3 | 0 | 1,230 次模型呼叫全部在 151.8 ms 逾時；`/decide` 伺服器 p95 155 ms、最大 165 ms。模型組的儲值中斷改用政策預設（小額方案提示 102 次），leave_room 一律不行動（849 次） | 16 / 0 |
| lt-context 停 60 秒 | `INJECT="sleep 60; docker stop lt-context; sleep 60; docker start lt-context"`，4 分鐘 | 48.1 / 75.9；125.7 / 187.9 | 0（webhook 1 筆 EOF，見下） | 停機期間 567 次 decide 因 Context 不存在、4 次因 Context 過時而不行動；重啟後先讀自己的 pending，再追 60 秒的積壓：ingest 到套用的 p95 53.5 秒、最大 62.3 秒，結束時 lag 0。事件 37,109 + 伺服器端儲值 348 = stream 37,457 筆，全部套用。狀態逾時 Trigger 延後但照樣送出（132 次） | 16 / 0 |
| lt-redis 暫停 10 秒 | `INJECT="sleep 60; docker pause lt-redis; sleep 10; docker unpause lt-redis"`，3 分鐘 | 1,039 / 1,041；153.5 / 342.8 | events 請求 9.7%（collector 回 503 共 336 次） | App 依退避重試，3,163 筆事件重送，最終遺失 0；有 30 筆在第一次其實已寫入，被 context 去重。decide 讀不到 Context 75 次，其中 19 次因此不行動。付款 stub 有 14 筆伺服器端事件當下送不出，由 sweeper 補送。k6 因 p95 閾值回報失敗（預期中） | 16 / 0 |
| 每個事件送兩次（同時） | `-e DUP=1`，3 分鐘 | 75.3 / 78.7；123.1 / 182.1 | 0 | collector 接受 55,630 筆（= 2 x 27,815），context 套用 28,077（= 27,815 + 262 伺服器端），去重 27,815 筆，剛好一半。decide 同一 event 同時兩次：3,059 次由行程內 single-flight 回同一份結果，1 次由資料庫冪等鍵回傳 | 16 / 0 |
| webhook 每筆送兩次（同時）+ 1% 錯簽章 | `-e WEBHOOK_DUP=1 -e BAD_SIG=0.01`，3 分鐘 | webhook 83.7 / 85.3 | 0 | 274 筆付款各送兩次：付款紀錄 274 筆、重複回覆 274 次、兌付 22 次。錯簽章送 1 筆、拒收 1 筆、0 筆寫入（1% 的樣本只有 1 筆；整合測試另外覆蓋這個情境） | 16 / 0 |
| 兩個 scheduler 同時跑 | `docker compose --profile dual up -d lt-scheduler-2`，3 分鐘 | 73.9 / 75.9；126.6 / 190.2 | 0 | 這一輪 scheduler-1 處理 54 個 job、scheduler-2 處理 206 個，兩個都在搶；`attempts > 1` 的 job 0 個，同一 job 的推播列 0 筆重複，同一人同一天推播 0 次重複 | 16 / 0 |
| 偽造客戶端儲值 | `-e FORGE=0.2`，3 分鐘 + 隔天 | 73.5 / 75.8；127.7 / 189.5 | 0 | 190 次只送客戶端 recharge、沒有 webhook；這些使用者中 109 人已拿到隔天的加贈資格，兌付 0 次、付款紀錄 0 筆 | 16 / 0 |

說明：
- 「lt-context 停機」那一輪的 1 筆 webhook 錯誤是客戶端收到 EOF，付款 stub 沒有收到這個請求（伺服器端 348 筆全部 2xx）。
  這是 k6 重用一條剛被關閉的 keep-alive 連線，與 context 停機無關；金流商的重送會補上。
- Redis 暫停那一輪的高延遲來自 collector 的 1 秒 socket timeout 加上 App 的退避；decide 沒有錯誤，因為讀 Context 的逾時是 0.3 秒，
  逾時就照「Context 不可用」處理，只會少做。
- 每一種故障都只讓系統少做（不行動、晚一點才做），沒有一種讓它多發優惠或重複處理。

### 外部依賴的契約測試（付款 stub）

付款不在決策架構的範圍內（既有付款後端負責驗證收據、寫帳、加點）。`lt-payments` 是那個後端的 stub，模擬架構依賴的三個保證：
只有驗證過的事件、以金流交易 ID 去重、兌付在加點的同一個交易內。上表的 webhook 與偽造情境，以及
`tests/test_integration.py` 的錯簽章、重送與偽造測試，屬於對這個外部依賴的契約測試，不是架構元件本身的測試。
stub 送出的伺服器端事件型別是 `recharge`（`src=server`），代表一筆已完成的儲值（設計上的事件名稱是 `recharge_completed`）。

## T4 壓力測試（k6 在主機 core 2）

指令：`K6_MODE=host scripts/run_test.sh <名稱> -e PROFILE=stress -e PEAK_X=<倍數> -e RAMP=270s -e MAX_VUS=<VU 上限>`。
同一份 traffic，從基準（每秒 5.3 個 session）30 秒後線性拉高，共 5 分鐘；閾值為 `/events` 或 `/decide` p95 > 500 ms、或錯誤 > 1% 時中止；
紅線守衛另外在主機 load > 3.5 時停止 k6。每 10 秒一列的完整表：`uv run python scripts/windows.py results/<名稱>/remote/k6_points.csv.gz`。

### 容量結果（修正後，`t4_fixed`，目標 25 倍）

| 時間 s | decide/s | events 請求/s | decide p95 / p99 ms | events p95 / p99 ms | 錯誤 |
|---|---|---|---|---|---|
| 20 | 16.8 | 17.4 | 118.5 / 154.1 | 2.2 / 4.1 | 0 |
| 60 | 58.7 | 57.9 | 82.5 / 142.4 | 1.9 / 5.8 | 0 |
| 100 | 103.2 | 102.1 | 84.4 / 140.7 | 2.4 / 4.4 | 0 |
| 140 | 159.8 | 160.2 | 89.1 / 153.1 | 3.5 / 9.0 | 0 |
| 180 | 218.3 | 224.0 | 106.1 / 155.1 | 9.5 / 24.1 | 0 |
| 200 | 247.3 | 253.9 | 173.6 / 227.8 | 17.7 / 50.0 | 0 |

約第 217 秒主機 load 到 3.77，紅線守衛停止 k6（k6 被終止，沒有 end-of-test 摘要；上表由 k6 的逐筆 CSV 計算）。
停止前沒有任何錯誤：226,524 筆事件被接受，context 套用 228,483 筆（含 1,959 筆伺服器端儲值），lag 0。
`remote/k6_points.csv.gz` 因 k6 被終止而截斷（`gzip -t` 失敗），可讀到 02:40:33（第 216 秒，即紅線時刻），原定 300 秒；上表到第 200 秒，事件與套用筆數來自伺服器端 `stats_*.json`，都不受影響。

第 200 秒（約 14.5 倍基準：每秒 247 次 decide、約 2,300 events）時各容器的 CPU（上限）：
lt-decision 0.44（0.5）、lt-redis 0.27（1.0）、lt-context 0.20（0.5）、lt-pg 0.20（1.0）、lt-collector 0.14（0.5）、k6 0.31。

膝點與先飽和的元件：lt-decision。它在第 200 秒用到配額的 88%，`/decide` 的 p95 從 90 到 106 ms（模型 mock 決定的底）
升到 174 ms，這是排隊開始的訊號；其他服務都在配額的 55% 以下。依 p95 > 500 ms 的閾值，真正的膝點在每秒 247 次 decide 以上，
但這一輪在到達之前就被主機紅線停止，所以容量數字寫成：在這台主機的紅線內，至少 14.5 倍基準、0 錯誤；第一個飽和的是 lt-decision
（0.5 vCPU 約可處理每秒 250 到 280 次 decide）。快取層（Valkey）在同一時間只用 0.27 核，離它的上限很遠（見 T6）。

另一輪相同設定的診斷（`t4_diag`，修正前、15 倍目標）在第 253 秒被紅線停止（load 3.60），停止前也沒有錯誤。

### 第一次 T4 的錯誤（`t4_stress`，修正前）

整輪：客戶端 events p95 5.5 ms、decide p95 92.3 ms；伺服器端 collector p99 14.2 ms、`/decide` p95 96.8 ms。293,611 筆事件被接受，
context 套用 296,191 筆（含 2,550 筆伺服器端儲值），相等。錯誤累計 1.04%，全部出現在第 270 秒以後，在最後被 1% 閾值中止（k6 exit 99）：

| 時間 s | decide/s | decide 錯誤 % | events 錯誤 % |
|---|---|---|---|
| 260 | 204.9 | 0 | 0 |
| 270 | 214.5 | 0.33 | 0 |
| 280 | 224.2 | 2.14 | 0 |
| 290 | 172.1 | 6.33 | 24.83（collector 481 個 503） |

#### 根因（`results/t4_rootcause/`）

第一次 T4 在約第 270 秒起出現錯誤：k6 對 `/decide` 收到 status 0（code 1000 EOF、1220 connection reset），最後 10 秒 collector
回 481 個 503（伺服器端 p50 0.7 ms）。四個假說逐一檢驗：

| 假說 | 檢驗 | 結果 |
|---|---|---|
| 1. k6 測試結束的假象（ramp-down 或 graceful stop 中斷進行中的請求） | 這個 executor 是 ramping-arrival-rate，只有 `gracefulStop`（90 秒），從第 300 秒才開始；錯誤從第 270 秒就出現，落在升載階段內 | 不成立 |
| 2. keep-alive 競態（伺服器剛關的連線被客戶端重用） | `k6/keepalive_repro.js`：200 個 VU 在同一條連線上 POST 兩次，中間閒置 75 ± 0.3 秒，正好跨過伺服器 75 秒的 keep-alive。伺服器 keep-alive 75 秒：800 個請求 0 失敗；600 秒：800 個 0 失敗 | 不成立：k6（Go）在重用前就發現連線已關 |
| 3. collector 的 redis-py 連線池耗盡 | 在描述符耗盡的重現中，collector 日誌是 `event store error: MaxConnectionsError: Too many connections`，997 次，伺服器端 503 p50 0.038 ms | 成立，但它是放大器，不是起點（見下） |
| 4. 檔案描述符上限 | 容器內 `ulimit -n` = 1024。原始 T4 的 k6 CSV：k6 對 decision 累計開的連線在第 270 秒是 975、第 280 秒 1,026，對 collector 在第 290 秒 1,071，錯誤開始的時間與服務跨過約 1,000 條連線的時間吻合（`t4_original_connections.txt`）。重現：1,200 個 VU 各持有一條到 collector 的連線，nofile 1024 時 collector 的 `/proc/1/fd` 停在 1024，760 / 4,800 個請求失敗，錯誤碼與 T4 相同（EOF 1000 共 348、connection reset 1220 共 26）；nofile 65536 時描述符到 1,214，0 / 4,800 失敗 | 成立，這是根因 |

結論：每個 HTTP 服務是單一 process，每條客戶端 keep-alive 連線佔一個描述符；Docker 在這台主機的預設 soft limit 是 1024，
k6 開到約 1,000 個 VU 時，decision 先用完（它還有到模型、Postgres、Valkey 的連線），collector 隨後用完。用完之後新連線被拒或被重置（EOF、RST），
而 collector 在這種擁塞下同時有超過 64 個請求在等 Valkey 連線，redis-py 的非阻塞連線池立即丟 `MaxConnectionsError`，變成快速 503。

修正（已放進 `compose.yaml` 與 `lt/common.py`）：
- 所有應用容器 `ulimits: nofile 65536`。
- Valkey 連線池改成 `BlockingConnectionPool`：池滿時最多等 socket timeout（collector 1 秒）而不是立刻失敗。
- 同一個重現（1,200 個 VU、每次 POST 一筆真事件）在兩個修正都套用後：4,800 / 4,800 成功，0 個 503。

注意：這是壓測工具的連線模型造成的上限（每個 VU 一條長連線）。正式環境的 App 連線經過負載平衡器，服務看到的是少量長連線，
但原則一樣：每個 process 的描述符上限必須大於「同時連線數 + 下游連線池」。


#### 證據檔與指令

- `scripts/t4_rootcause.sh`（診斷 T4 與 keep-alive 重現）、`scripts/fd_repro.sh`（描述符重現，nofile 1024 對 65536）、
  `scripts/fd_repro_events.sh` 與 `scripts/fd_repro_events_fixed.sh`（帶真事件，修正前後）。
- `results/t4_rootcause/t4_original_connections.txt`：原始 T4 每 10 秒的累計新連線數與錯誤數。
- `results/t4_rootcause/fd_repro_*.txt`：k6 錯誤碼統計、collector 計數器與日誌、`/proc/1/fd` 的取樣。
- `results/t4_rootcause/evidence_*.txt`：Valkey `INFO clients`（maxclients 10000、connected_clients 最多 28）、`INFO stats`
  （rejected_connections 0）、容器內 `ulimit -n`（1024）、`/proc/net/sockstat`、`ListenOverflows`（0）。Valkey 端沒有拒絕連線，Sanic 的 listen backlog 沒有溢位。

## T5 尖峰（10 倍 60 秒，再回到基準）

指令：`K6_MODE=host scripts/run_test.sh t5_spike -e PROFILE=spike -e SPIKE_X=10 -e MAX_VUS=3000`。
基準 60 秒，5 秒內升到 10 倍、維持 60 秒、5 秒內降回、再基準 110 秒。

| 時間 s | decide/s | events 請求/s | decide p50 / p95 / p99 ms | events p95 / p99 ms |
|---|---|---|---|---|
| 50（基準） | 16.9 | 17.4 | 3.5 / 79.5 / 103.8 | 2.0 / 4.7 |
| 80（尖峰） | 129.2 | 129.7 | 4.8 / 89.6 / 144.5 | 3.2 / 8.4 |
| 110（尖峰） | 157.8 | 160.2 | 5.7 / 90.0 / 152.5 | 3.9 / 12.0 |
| 120（尖峰） | 151.7 | 163.6 | 6.9 / 97.9 / 154.7 | 4.4 / 14.4 |
| 140（回落後） | 20.2 | 22.5 | 3.8 / 92.6 / 153.7 | 2.1 / 4.3 |
| 160（基準） | 20.6 | | 3.5 / 77.9 / 150.5 | |

- 尖峰最高延遲：decide p99 155 ms（等於模型逾時上限），events p99 14.4 ms；錯誤 0。
- 恢復時間：尖峰結束後的第一個 10 秒視窗（第 140 秒）延遲就回到基準水準，恢復時間小於 10 秒；
  context 的 ingest 到套用 p99 12.1 ms、最大 46 ms，沒有積壓。
- 115,207 筆事件 + 1,060 筆伺服器端儲值 = 116,267 筆套用，相等；k6 有 427 個 session 因 VU 配置來不及而沒啟動（dropped_iterations），
  實際尖峰約為 9.5 倍。

## T6 快取層 A/B（Valkey 8.1.4、Redis 7.2、Dragonfly 1.38）

指令：`LT_SSH=<alias> scripts/t6.sh`。三個 store 一次一個，各自是 lt- 容器（cpuset 0,1、1 GB、127.0.0.1:17120），
三者都關閉持久化（Dragonfly 沒有 AOF，為了公平其他兩個也關）。Valkey 與 Redis 開 `io-threads 2`，Dragonfly `--proactor_threads=2`。
driver 是 `bench/cache_bench.py`（uvloop + redis-py + hiredis），容器 lt-bench 釘在 core 2。op mix 照服務實際送出的：
每 10 個事件一個 XADD pipeline、XREADGROUP COUNT 200、每事件一個 EVALSHA 套用腳本（腳本內 XACK）、每次 decide 一組 HGETALL + LRANGE。
每事件約 2.5 個命令，基準 150 events/s 約 378 ops/s（EVALSHA 150、XADD 150、XREADGROUP 45、HGETALL 17、LRANGE 17）。
基準跑 60 秒，ramp 每階 20 秒。

基準（150 events/s）每種操作的延遲，ms：

| 操作 | Valkey p50 / p95 / p99 | Redis 7.2 p50 / p95 / p99 | Dragonfly p50 / p95 / p99 |
|---|---|---|---|
| XADD pipeline（10 筆） | 2.07 / 2.62 / 3.45 | 2.07 / 2.62 / 3.59 | 1.84 / 2.52 / 3.19 |
| EVALSHA 套用 pipeline | 0.67 / 0.91 / 1.06 | 0.67 / 0.95 / 1.06 | 1.06 / 2.84 / 3.73 |
| 每事件攤提的套用 | 0.59 / 0.84 / 0.91 | 0.59 / 0.81 / 0.91 | 0.67 / 1.20 / 1.84 |
| 讀 Context（HGETALL + LRANGE） | 1.06 / 1.92 / 2.62 | 1.06 / 1.92 / 2.62 | 1.11 / 1.92 / 2.52 |

（XREADGROUP 的延遲包含 BLOCK 等待，不當作 store 延遲。）

ramp：store CPU 與每千事件的 store CPU 秒：

| events/s | Valkey store 核 | Valkey CPU s / 千事件 | Redis 7.2 store 核 | Redis 7.2 CPU s / 千事件 | Dragonfly store 核 | Dragonfly CPU s / 千事件 | driver 核 |
|---|---|---|---|---|---|---|---|
| 150 | 0.01 | 0.099 | 0.01 | 0.099 | 0.05 | 0.349 | 0.05 |
| 1,000 | 0.08 | 0.077 | 0.08 | 0.078 | 0.31 | 0.313 | 0.25 |
| 2,000 | 0.14 | 0.072 | 0.15 | 0.075 | 0.59 | 0.295 | 0.47 |
| 4,000 | 0.24 | 0.060 | 0.25 | 0.063 | 0.98 | 0.245 | 0.68 到 0.80 |
| 8,000 | 0.42 | 0.053 | 0.45 | 0.056 | 1.19（消費只到 5,591） | 0.149 | 0.74 到 0.99 |

| store | 所有操作 p99 ≤ 10 ms 的最高階 | 第一個失敗階 | 失敗時誰先滿 | 峰值記憶體 |
|---|---|---|---|---|
| Valkey 8.1.4 | 4,000 events/s（約 10,100 ops/s） | 8,000 | driver（0.99 核），store 只用 0.42 核 | 157 MB |
| Redis 7.2.16 | 4,000 events/s | 8,000（p99 15.9 / 8.2 / 17.2 ms） | driver（0.99 核），store 0.45 核 | 157 MB |
| Dragonfly 1.38.0 | 4,000 events/s（apply p99 9.2 ms） | 8,000 | store：1.19 核、消費跟不上（5,591 / 7,998） | 182 MB |

讀法：
- Valkey 與 Redis 7.2 在每一階都幾乎一樣（Valkey 從 Redis 7.2 分出來，這個 op mix 沒有用到新功能）。兩者的上限都沒有量到：
  單核的 Python driver 先滿。用 store CPU 外推：Valkey 每千事件約 0.053 到 0.060 CPU 秒，主執行緒 1 核約可處理 17,000 到 19,000 events/s。
- Dragonfly 在這個 op mix 下每事件的 CPU 是 Valkey 的 3.5 到 4 倍，並在 8,000 events/s 由 store 自己先滿。工作量以 Lua（EVALSHA）為主；
  Dragonfly 執行 Lua 的成本較高（推測和它多執行緒下的鍵鎖定有關，這裡沒有另外驗證）。
- 設計基準 150 events/s 時三者的 store CPU 都在 0.05 核以下；T4 尖峰（約 2,000 events/s）時 lt-redis 只用 0.21 到 0.27 核。
  快取層離膝點很遠，T4 先飽和的是 lt-decision。這支持「預設 Valkey；只有快取層成為膝點時才考慮 Dragonfly」，而且對這種 Lua 為主的負載，
  Dragonfly 反而更差。
- 相容性：三個 store 都跑完同一組命令，driver 沒有記錄到命令不相容。輸出裡唯一的 `incompatible` 項目是過載時 gauge 的
  `XINFO` 讀取逾時，是 driver 過載，不是不相容。Lua 腳本本來就在 KEYS[] 宣告所有鍵，所以 Dragonfly 不需要 undeclared-keys 旗標。

### 整個系統跑在 Dragonfly 上（T1，5 分鐘）

指令：`scripts/restack.sh REDIS_IMAGE=docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0 'REDIS_CMD=dragonfly --proactor_threads=2 --maxmemory=700mb --dbfilename=' 'REDIS_PING=redis-cli ping'` 後
`scripts/run_test.sh t1_dragonfly -e DURATION=5m`。服務程式完全沒改：Lua 腳本、stream、consumer group、XAUTOCLAIM、XINFO、glide 與 redis-py 都照常運作，
沒有遇到命令不相容。

| 指標 | Valkey（T1，10 分鐘） | Dragonfly（5 分鐘） |
|---|---|---|
| 客戶端 events p95 / p99 ms | 42.05 / 74.93 | 44.04 / 75.94 |
| 伺服器端 `/events` p50 / p99 ms | 0.691 / 2.073 | 0.748 / 2.837 |
| ingest 到 Context 套用 p50 / p95 / p99 ms | 2.073 / 3.068 / 4.037 | 4.037 / 8.179 / 12.107 |
| context 套用一批 p50 / p99 ms | 1.107 / 2.073 | 2.95 / 8.846 |
| decide 讀 Context p50 / p99 ms | 0.505 / 1.456 | 0.639 / 1.916 |
| store CPU 秒 / 千事件 | 0.171 | 0.441 |
| 事件接受 + 伺服器端儲值 = 套用 | 92,951 + 887 = 93,838 | 46,404 + 414 = 46,818 |
| 不變式 | 16 / 0 | 16 / 0 |

Dragonfly 在設計負載下可以直接替換，但這一輪每事件要 2.6 倍的 store CPU（0.441 / 0.171，整個系統的 T1），Context 更新延遲約為兩倍。

## T6b：只用 Postgres 取代 Valkey（Context 與事件流兩個角色）

指令：`LT_SSH=<alias> scripts/t6b.sh`；tuned 與 tuned_unlogged 用修正後的腳本重跑：`T6B_OUT=t6b_rerun DESIGNS="tuned tuned_unlogged" scripts/t6b.sh`。lt-pg（1 vCPU / 2 GB、cores 0,1、上面的調校設定），應用服務停止，driver
`bench/pg_bench.py` 在 core 2 的 lt-bench 容器，資料庫 `lt_t6b`（結束時刪除）。每種設計：基準 150 events/s 60 秒，再 ramp
150、300、600、1,200、2,400、4,800、9,600，每階 20 秒，整段最多 180 秒。Context 讀取預算 p95 20 ms。

三種設計：
- naive：每人一列 `context jsonb`，狀態加最近 50 筆事件，每個事件用 upsert 重寫整個 jsonb。
- tuned：每人一列固定寬度的熱狀態（`fillfactor = 70`、更新的欄位沒有索引，所以可以 HOT）；最近事件寫進只追加、按日分割的
  `recent_events`，索引 `(user_id, ts DESC)`，讀最近 50 筆用 LIMIT 50；保留期用 DROP PARTITION。
- tuned_unlogged：同 tuned，但狀態表與 recent_events 的分割區是 UNLOGGED（PG18 不允許 UNLOGGED 的分割父表，所以父表維持 logged）。
  UNLOGGED 表在當機時會被清空，也不會複寫到 RDS Multi-AZ 的備援節點。

事件流的角色（三種設計相同）：`events` 表（按日分割）帶 `txid xid8 DEFAULT pg_current_xact_id()`。兩個獨立的 consumer group 各有自己的
cursor（`stream_cursors(group, shard, last_txid, last_id, lease_owner, lease_until)`），consumer 用 `FOR UPDATE SKIP LOCKED` 搶一個
(group, shard) 的租約，租約到期（visibility timeout）別的 consumer 可以接手。沒有用 LISTEN/NOTIFY。
identity 值依插入順序配發、卻可能不依序 commit：reader 只讀 txid 小於 `pg_snapshot_xmin(pg_current_snapshot())` 的列，並以 (txid, id) 排序前進，
所以晚 commit 的較小 id 不會被跳過。實測：tuned 重跑的 ramp 中有 4,713 次 id 與 txid 順序相反，結束時兩個 group 的消費數都等於 cursor 以下的事件數（audit consistent）。
可重建的資料（Context、最近事件）的寫入交易用 `SET LOCAL synchronous_commit = off`。

| 設計 | events/s | ingest p50 / p95 / p99 ms | Context 讀取 p50 / p95 / p99 ms | 套用 p95 ms | lag A / B 秒 | WAL KB/s | 狀態表 HOT % | lt-pg 核 |
|---|---|---|---|---|---|---|---|---|
| naive | 150（基準） | 2.62 / 3.45 / 4.04 | 1.30 / 2.07 / 2.73 | 3.73 | 0 / 0 | 426 | 42 | |
| naive | 2,400 | 2.62 / 5.75 / 34.91 | 1.51 / 3.88 / 29.84 | 12.59 | 0.03 / 0 | 2,320 | 72 | |
| naive | 4,800 | 5.31 / 238.5 / 326.5 | 3.59 / 238.5 / 326.5 | 167.6 | 0.70 / 0.42 | 5,133 | 74 | |
| tuned（重跑） | 150（基準） | 2.62 / 3.32 / 3.73 | 1.35 / 2.16 / 2.73 | 2.24 | 0 / 0 | 267 | 100 | 0.09 |
| tuned（重跑） | 2,400 | 2.43 / 3.88 / 9.20 | 1.46 / 2.73 / 4.91 | 4.72 | 0.01 / 0.02 | 1,596 | 100 | 約 0.40 |
| tuned（重跑） | 4,800 | 2.73 / 6.46 / 40.84 | 1.84 / 5.31 / 37.76 | 7.86 | 0.01 / 0.01 | 3,012 | 100 | 約 0.60 |
| tuned（重跑） | 9,600（達到 8,309） | 2,063 / 2,925 / 2,925 | 2,063 / 2,927 / 2,927 | 17.2 | 0 / 2.97 | 5,191 | 100 | |
| tuned_unlogged（重跑） | 150（基準） | 2.62 / 3.45 / 4.20 | 1.35 / 2.16 / 2.95 | 2.07 | 0 / 0 | 74 | 100 | 0.09 |
| tuned_unlogged（重跑） | 4,800 | 2.73 / 5.98 / 34.91 | 1.84 / 4.91 / 32.28 | 8.18 | 0.03 / 0 | 1,361 | 100 | 約 0.60 |
| tuned_unlogged（重跑） | 9,600（達到 9,457） | 127 / 314 / 340 | 127 / 314 / 340 | 15.3 | 0.32 / 0.31 | 2,588 | 100 | |

違規後跑完、已作廢的第一輪 tuned 與 tuned_unlogged（`results/t6b/tuned*.jsonl`）結論相同：4,800 通過、9,600 失敗。

表的大小（tuned 重跑，ramp 結束時）：events 當日分割區 73.6 MB（約 37 萬筆，含索引 24.5 MB）、recent_events 當日分割區 56.5 MB（索引 15.5 MB）、
狀態表 6.5 MB（5 萬人，dead tuple 1,213）。分割區只追加，沒有 dead tuple；整段 ramp 每階約 5 到 6 次 autovacuum。

和 Valkey（T6，同樣 2 核、op mix 對應）並列：

| 項目 | Valkey 8.1.4 | Postgres naive | Postgres tuned | Postgres tuned + UNLOGGED |
|---|---|---|---|---|
| 150 events/s 時 Context 讀取 p95 | 1.92 ms | 2.07 ms | 2.16 ms | 2.33 ms |
| 150 events/s 時 store CPU | 0.01 核 | | 0.09 核 | 0.09 核 |
| 讀取 p95 ≤ 20 ms 且 lag 有界的最高實測階 | ≥ 8,000 events/s（driver 先滿） | 2,400 | 4,800 | 4,800 |
| 第一個失敗階 | 未達到 | 4,800（讀取 p95 239 ms） | 9,600（讀取 p95 2.9 秒） | 9,600（讀取 p95 314 ms） |
| 4,800 events/s 時 WAL | 無（AOF everysec，另計） | 5.0 MB/s | 2.9 MB/s | 1.3 MB/s |

結論（break point，不是「需不需要」）：
- naive 設計在 2,400 events/s 以內符合預算，4,800 events/s 失敗。這一階同時觸發了紅線（主機 load 3.69），ramp 在那裡停止。
- tuned 與 tuned + UNLOGGED 在 4,800 events/s 以內符合預算，9,600 events/s 失敗（tuned 只達到 8,309、讀取 p95 2.9 秒；UNLOGGED 達到 9,457、讀取 p95 314 ms）；
  這一階 driver 也用到 0.92 到 0.94 核，所以 4,800 到 9,600 之間的確切膝點沒有量到。UNLOGGED 把 WAL 減半以上，但沒有提高 break point。
- 設計尖峰 150 events/s 時，三種 Postgres 設計的讀取 p95 都在 2.4 ms 以內，tuned 的 break point 是設計尖峰的 32 倍以上。
  Postgres 可以承擔兩個角色；代價是 store CPU（基準時 0.09 核，是 Valkey 的約 9 倍）、每秒數百 KB 到數 MB 的 WAL，以及分割區管理。
- 紅線事件：naive 的 4,800 階觸發守衛（load1 3.69），守衛停止了 driver。第一版 `scripts/t6b.sh` 在兩個 design 之間不檢查 ABORT，
  tuned 與 tuned_unlogged 在違規後照樣跑完；這兩組作廢並用修正後的腳本重跑（重跑期間 load 最高 2.88，沒有違規）。
  另外第一版在主機上 `cd` 之後才用相對路徑建立結果目錄，守衛在 naive 基準的前約 2 分鐘沒有啟動（那段時間 load 約 1），之後手動補上。細節見「紅線事件」一節。

## T7 模型呼叫的資源成本

問題：Decision 服務經 HTTP 呼叫 System One（正式環境是 Jev API）要花多少資源？模型變慢時會不會安全地少做？
指令：`LT_SSH=<alias> scripts/t7.sh`（之後 `scripts/batch7.sh` 重跑不保持連線的 TLS 兩輪與關閉斷路器的慢模型一輪）。
每輪 3 分鐘。lt-decision 為了 T7 加了量測：進行中的模型請求（每秒最大值）、到模型端點的 TCP 連線數與每秒新連線數
（每秒讀一次 `/proc/net/tcp`），mock 則記錄自己的進行中請求、連線數與被客戶端斷線取消的次數；`/stats` 每 5 秒輪詢一次存到 `stats_poll.jsonl`。

### 1. HTTP client 佔多少 CPU（py-spy `--native --idle`，250 Hz，120 秒）

| 元件 | T1 負載：樣本 | 佔忙碌樣本 | 13 倍負載：樣本 | 佔忙碌樣本 |
|---|---|---|---|---|
| rnet HTTP client（Python 端可見的 frame） | 3 | 0.2% | 8 | 0.0% |
| msgspec JSON 編解碼（模型請求與回應） | 2 | 0.1% | 23 | 0.1% |
| 政策邏輯（lt/logic.py） | 6 | 0.4% | 8 | 0.0% |
| Postgres（psqlpy） | 141 | 9.0% | 967 | 3.7% |
| Valkey（redis-py） | 109 | 7.0% | 959 | 3.7% |
| Sanic + httptools（請求與回應） | 299 | 19.1% | 5,707 | 21.9% |
| 事件迴圈與 socket 系統呼叫 | 352 | 22.5% | 6,703 | 25.7% |
| T7 自己的 TCP 量測（/proc/net/tcp） | 293 | 18.7% | 2,351 | 9.0% |
| 其他服務程式碼 | 158 | 10.1% | 1,853 | 7.1% |
| 其他（libc、CPython、未符號化的 Rust 執行緒） | 202 | 12.9% | 7,502 | 28.8% |
| 忙碌樣本合計（Python 佔比） | 1,565（25.9%） | | 26,081（33.2%） | |

- 模型呼叫在 Python 這一側幾乎不花 CPU：Python 側可見的 rnet 與 JSON 編解碼 frame 合計不到忙碌樣本的 0.5%。rnet 的 Rust 執行緒沒有符號，
  落在「其他」裡，上限是那一列（T1 時 12.9%）。
- 13 倍那一輪的延遲不能當容量數字：py-spy 在容器內執行，和 lt-decision 共用 0.5 vCPU 配額，服務被壓滿（`/decide` p50 1.1 秒），
  斷路器也因為本地排隊造成的逾時而打開。那一輪只用來看 CPU 分布。
- `t7_prof_13x` 被守衛停止，`remote/k6_points.csv.gz` 截斷在 02:48:59（紅線時刻，原定 180 秒只到第 164 秒）；本節數字來自 py-spy 與伺服器端 `stats_decision.json`，不用這個檔。
- T7 加的 `/proc/net/tcp` 量測本身在 T1 時佔了 18.7% 的忙碌樣本（約 0.01 核）；T7 的每千筆 CPU 只在 T7 的各輪之間比較。

### 2. 連線池與併發

| 負載 | 模型呼叫 / 秒 | 平均延遲 | Little 定律的平均併發（rate x 平均延遲） | 觀察到的每秒最大併發 | 觀察到的最大連線數 | 設定的 idle 池 |
|---|---|---|---|---|---|---|
| T1（`t7_prof_1x`） | 6.7 | 51 ms | 0.34 | 4 | 4 | 64 |
| 5 倍（`t7_plain_ka_5x`） | 32 | 約 51 ms | 1.6 | 8 | 8 | 64 |
| 14.5 倍（`t4_fixed`） | 約 100 | 約 50 ms | 5 | 8 | 8 | 64 |

`t4_fixed` 這一列的平均延遲、併發與連線數來自伺服器端 `results/t4_fixed/stats_decision.json`（model_call_ms mean 51.2、inflight 與 conns 最大 8），不受截斷的 k6 CSV 影響。

連線池的 sizing heuristic（經驗估算，不是證明過的公式）：連線數 = 尖峰模型呼叫 / 秒 x p99 延遲 x 1.5。以 p99 0.155 秒計：設計尖峰（約 7 次 / 秒）需要 2 條，
14.5 倍（約 100 次 / 秒）需要 23 條；設定的 64 條 idle 上限足夠，而且量到的最大值只有 8。

### 3. 逾時時的取消

- 斷路器開著時，慢模型（p50 200 ms、p95 600 ms）一輪：斷路器 3 次打開，只送出 39 個模型請求，`/decide` p95 4.4 ms。
- 斷路器關掉、模型 100% 逾時（`t7_breaker_off`）：1,229 次呼叫全部在 155 ms 內結束（150 ms 預算 + 取消），mock 端
  `cancelled_by_client_disconnect` 也是 1,229：每一次逾時，`asyncio.timeout` 取消 rnet 的 future 都關掉了連線，mock 的 handler 隨即被取消。
  mock 的進行中請求最多 5、結束時 0，lt-decision 到模型的連線最多 4：請求沒有累積，連線沒有外洩，不需要額外修正。
  代價是每次逾時都丟掉一條連線（新連線 175 條）。rnet 的 client timeout 只接受整數秒，做不到 150 ms，所以 150 ms 預算只能靠
  `asyncio.timeout`；上面的數字證明這樣做會真的中止請求。
- 斷路器關掉、慢模型（`t7_slow_mock_nobreaker`）：1,199 次呼叫中 805 次逾時（67%），mock 端 796 次因客戶端斷線被取消（其餘 9 次在取消前剛好回完）；mock 的進行中請求整輪最多 6、每 30 秒的取樣都在 0 到 2 之間、結束時 0；lt-decision 到模型的連線最多 4，每秒新連線最多 3（每次逾時丟一條、下一次重開）。`/decide` 伺服器 p95 155 ms、p99 161 ms，16 條不變式 0。慢模型時請求不會累積。

### 4. TLS 成本（lt-model-tls：nginx TLS 1.3 終端，自簽憑證，127.0.0.1）

| 執行 | 負載 | 連線 | CPU 秒 / 千次 decide | CPU 秒 / 千次模型呼叫 | 模型呼叫 p50 / p95 / p99 ms |
|---|---|---|---|---|---|
| `t7_tls_ka_1x` | 1x | TLS，保持連線 | 2.618 | 6.816 | 40.8 / 127.4 / 151.5 |
| `t7_tls_noka2_1x` | 1x | TLS，每次新連線 | 3.000 | 7.770 | 44.2 / 132.5 / 151.2 |
| `t7_plain_ka_5x` | 5x | 明文，保持連線 | 1.936 | 5.077 | 42.5 / 122.5 / 154.6 |
| `t7_tls_ka_5x` | 5x | TLS，保持連線 | 1.975 | 5.174 | 42.5 / 122.5 / 155.0 |
| `t7_tls_noka2_5x` | 5x | TLS，每次新連線 | 2.333 | 6.129 | 44.2 / 122.5 / 155.0 |

- 保持連線時 TLS 幾乎不花錢：5 倍時比明文多 2%（每千次模型呼叫 +0.1 CPU 秒）。
- 每次都重新握手：每千次模型呼叫多約 1 CPU 秒（1x：6.82 對 7.77；5x：5.17 對 6.13，約每次呼叫 1 ms CPU；每次呼叫都設定成新連線，握手次數沒有單獨計數），模型呼叫 p50 多 1.7 到 3.4 ms，約佔 44 ms 的 4% 到 8%。5x 時 lt-decision 的 TIME_WAIT 約 1,300（`t7_tls_noka2_5x_sockstat.txt`）。
  第一次跑「每次新連線」時 rnet 的 `no_keepalive=True` 沒有讓連線停止重用（TIME_WAIT 維持在 0，`t7_tls_noka_*`，作廢），
  改成 `pool_max_idle_per_host=0` 才真的每次開新連線（lt-decision 的 TIME_WAIT 約 310，`t7_tls_noka2_1x_sockstat.txt`）。

### 5. 斷路器

實作在 `lt/decision.py` 的 `Breaker`：過去 10 秒內至少 10 次呼叫且逾時比例超過 20% 就打開 30 秒，期間不呼叫模型、改用政策預設；
30 秒後半開，只放一個探測請求，成功就關閉，失敗再開 30 秒。單元測試在 `tests/test_breaker.py`。

| 模型 100% 逾時 | `/decide` 伺服器 p95 / p99 ms | 客戶端 decide p95 / p99 ms | 模型呼叫次數 | lt-decision CPU 秒（3 分鐘） | 到模型的最大連線 | 不變式 |
|---|---|---|---|---|---|---|
| 斷路器關（`t7_breaker_off`） | 155.0 / 161.2 | 192.9 / 195.9 | 1,229 | 8.73 | 4 | 16 / 0 |
| 斷路器開（`t7_breaker_on`） | 4.4 / 10.0 | 76.0 / 79.6 | 18（1 次打開 + 6 次失敗的探測 + 開啟前的呼叫） | 7.71 | 3 | 16 / 0 |

斷路器打開後，`/decide` 的 p95 從約 155 ms 降到 4.4 ms，和預期一致；模型組在這段期間全部用政策預設，16 條不變式維持 0。

### 設計決策引用的數字

- 連線池大小的 sizing heuristic：尖峰模型呼叫 / 秒 x p99 延遲 x 1.5（Little 定律只描述平均值，到達分布與排隊沒有驗證）。
- 每千次模型呼叫的 Decision CPU：5 倍負載、保持連線約 5.1 到 5.2 CPU 秒，含 Decision 每次呼叫的其他工作（明文或 TLS 差 2%）；每次都新建 TLS 連線：每千次模型呼叫多約 1 CPU 秒（約每次呼叫 1 ms CPU）。
- 斷路器：10 秒視窗、至少 10 次、逾時比例 > 20% 打開 30 秒、半開一次探測。模型全逾時時 `/decide` p95 從 155 ms 降到 4.4 ms。
- 150 ms 的模型預算包含到 Jev 端點的網路往返。本報告的模型在同一台主機上（往返小於 1 ms）；Decision 服務和模型端點必須在同一個區域，
  本報告從本機經 SSH tunnel 到主機的往返約 38 ms：同樣的往返如果出現在 Decision 與模型之間，就吃掉 150 ms 預算的四分之一。

## T2 長時間（60 分鐘，基準負載）

指令：`LT_SSH=<alias> scripts/run_test.sh t2_soak -e DURATION=60m`（k6 在本機，經 SSH tunnel）。依優先順序最後執行，時長沒有縮短。

| 指標 | T1（10 分鐘） | T2（60 分鐘） |
|---|---|---|
| 事件 / decide / webhook | 92,951 / 10,603 / 887 | 557,222 / 63,560 / 5,253 |
| 錯誤 | 0 | events 0、decide 0、webhook 1 / 5,253（客戶端 EOF，伺服器端 5,252 筆全部 2xx） |
| 客戶端 events p95 / p99 ms | 42.05 / 74.93 | 41.63 / 45.96（max 1,796，SSH tunnel 的瞬間停頓；伺服器端 max 15.7） |
| 客戶端 decide p95 / p99 ms | 125.8 / 190.4 | 125.4 / 183.6 |
| 伺服器端 `/events` p50 / p99 ms | 0.691 / 2.073 | 0.719 / 2.156 |
| 伺服器端 `/decide` p95 / p99 ms | 89.5 / 155.0 | 89.5 / 149.0 |
| Decision 的 Postgres 寫入 p99 ms | 2.95 | 2.95 |
| ingest 到 Context 套用 p99 ms | 4.04 | 4.04 |
| collector / context CPU 秒 / 千事件 | 0.0885 / 0.1324 | 0.0916 / 0.1326 |
| 接受 + 伺服器端儲值 = 套用 | 92,951 + 887 = 93,838 | 557,222 + 5,252 = 562,474 |
| 不變式 | 16 / 0 | 16 / 0 |

記憶體（cgroup `memory.current`，每 5 秒取樣；`scripts/soak_summary.py`）：

| 容器 | 第 5 分鐘 MB | 第 30 分鐘 MB | 結束 MB | 斜率 MB / 小時 | 平均 CPU 核 |
|---|---|---|---|---|---|
| lt-collector | 35.9 | 35.9 | 34.8 | +0.1 | 0.014 |
| lt-context | 42.2 | 42.3 | 42.3 | +0.2 | 0.020 |
| lt-decision | 40.1 | 40.8 | 39.5 | +0.1 | 0.042 |
| lt-model | 31.2 | 31.4 | 31.5 | +0.3 | 0.010 |
| lt-payments | 37.5 | 37.3 | 37.0 | +0.1 | 0.004 |
| lt-scheduler | 34.4 | 34.4 | 34.4 | 0.0 | 0.002 |
| lt-redis | 19.1 | 92.7 | 168.6 | +167 | 0.027 |
| lt-pg | 157.1 | 237.1 | 296.1 | +140 | 0.040 |

- 六個 Python 服務沒有記憶體成長（斜率都在 ±0.3 MB / 小時內，process RSS 45 到 66 MB）。
- Valkey 的成長是預期中的資料：stream 在約第 55 分鐘到達 `MAXLEN ~ 500000`（結束時 500,025 筆、69 MB，每筆約 138 B），之後由修剪維持；
  Context 鍵 51,913 個，每位活躍使用者約 6.0 KB（ctx 332 B、ev 3,808 B、dd 1,864 B，全部 listpack）。used_memory 162 MB，遠低於 700 MB 的 maxmemory。
  AOF rewrite 的 fork 6 次，每次 6 到 9 ms（LATENCY DOCTOR）；SLOWLOG 空。
- Postgres 的成長是資料加 page cache：decisions 30 MB、outbox 20 MB；autovacuum 在 outbox 跑了 162 次、next_day_jobs 65 次、decisions 26 次，
  結束時 outbox 的 n_dead_tup 278、payments 2,740（emitted 更新）。
- 60 分鐘裡延遲沒有往上漂：伺服器端各項 p99 與 T1 相同。

## 不變式

`sql/invariants.sql` 的 16 條查詢，每一輪測試結束都跑（`scripts/inv_check.sh results/*/invariants.txt`）：

| 不變式 | 意義 |
|---|---|
| bonus_granted_to_prior_payer | 曾付費的人拿到加贈資格 |
| bonus_redeemed_on_non_first_payment | 不是首次儲值卻兌付 |
| holdout_action_now / holdout_action_next_day / holdout_entitlement | holdout 的人收到任何 Action 或資格 |
| entitlements_per_user_campaign_gt1 | 同一人同一活動超過一筆資格 |
| push_per_user_day_gt1 | 同一人同一天超過一則推播 |
| redemption_without_verified_webhook | 沒有驗證過的付款紀錄卻兌付 |
| duplicate_decision_per_event_id | 同一 event_id 超過一筆 Decision |
| arm_changed_for_user | 同一人同一實驗的分組改變 |
| forged_recharge_redeemed / forged_recharge_payment_row | 偽造客戶端儲值造成兌付或付款紀錄 |
| bad_signature_payment_row | 錯簽章的 webhook 被寫入 |
| job_processed_more_than_once / push_rows_per_job_gt1 | 隔天 job 被處理兩次 |
| payment_event_not_emitted_after_30s | 付款 30 秒後伺服器端事件仍未送出 |

結果：39 輪（T1、T2、T1-R、T3 七種、T4 三輪、T5、T7 十四輪、Postgres A/B、實作 A/B、Dragonfly）每輪 16 條全部 0。
唯一鍵本來就讓其中幾條不可能違反（例如 duplicate_decision_per_event_id）；對應的實際行為由計數器證明：T3 重送那一輪
3,059 次同時重複的 decide 由 single-flight 回同一份結果、27,815 筆重複事件被 context 去重。

## 紅線事件與執行中的錯誤

| 時間 | 事件 | 處理 |
|---|---|---|
| 第一次 T1 開始時 | 守衛在開始的瞬間停止 k6：前一步的 pgbench 把 1 分鐘 load 留在 5.12 | 加上開始前等 load 降到 2.0 以下，重跑；本報告的 T1 是重跑結果 |
| Python collector ramp | 約第 160 秒 load 3.57，守衛停止 | 報告停止前的數字 |
| T4 診斷（修正前） | 第 253 秒 load 3.60，守衛停止 | 停止前 0 錯誤 |
| T4（修正後） | 第 217 秒 load 3.77，守衛停止 | 容量數字寫成「紅線內至少 14.5 倍」 |
| T7 13 倍剖析 | load 3.53，守衛停止 | 那一輪只用來看 CPU 分布 |
| T6b naive 4,800 階（03:49:50） | load 3.69，守衛以 `docker stop lt-k6 lt-bench` 停止 driver（依名稱，不靠 PID），naive 的 ramp 沒有 9,600 階也沒有摘要 | 4,800 已失敗（讀取 p95 239 ms） |
| 同一次 T6b，違規之後 | `scripts/t6b.sh` 在兩個 design 之間不檢查 ABORT，tuned 與 tuned_unlogged 在違規後照樣跑完（守衛仍在，違規後 load 最高 3.39） | 兩者作廢，用修正後的腳本重跑（`results/t6b_rerun/`），本報告採用重跑 |
| T6b 開始的約 2 分鐘 | `scripts/t6b.sh` 在 `cd` 後才用相對路徑建目錄：守衛沒有啟動（load 約 1），sampler 的 PID 檔寫不進去 | 手動補上守衛 |
| `batch7.log` 的兩行 `kill: usage` | 不是守衛：是 t6b.sh 結束時用 `kill $(cat 不存在的 PID 檔)` 停 sampler 與守衛 | `scripts/remote/bg.sh`：啟動時確認 PID 非空且存活、停止時 PID 檔缺失或空白就報錯；`tests/test_bg.py` 重現空 PID；所有腳本改用絕對路徑並在步驟之間檢查 ABORT |
| Postgres 第一輪 A/B | 測試中修改正在執行的 `run_test.sh`，bash 邊讀邊執行而出錯 | 停掉重跑；之後改用「寫新檔再 rename」 |
| T3 第一次的 context 停機與 Valkey 暫停 | 注入的 ssh 等背景程序結束才返回，k6 在注入結束後才開始 | 修正並重跑（本報告採用重跑） |
| T7 第一次「每次新連線」 | rnet `no_keepalive` 沒有停止連線重用 | 改 `pool_max_idle_per_host=0` 重跑 |

沒有任何一次測試觸發可用記憶體低於 6 GB 的紅線（46 份守衛紀錄中，可用記憶體最低 17.8 GB，load 最高 3.77）。

## 這份壓測證明不了什麼

- AWS 託管服務的行為：ElastiCache、RDS、Fargate 的網路、CPU credit、儲存延遲與節流規則都和這台主機不同；這裡的
  0.5 vCPU 是 cgroup 配額，不是 Fargate 的 vCPU。
- RDS failover 與 Multi-AZ：沒有測主節點切換時的連線中斷、交易重試與 Decision 冪等在切換期間的行為。
- ElastiCache 的 failover、複寫延遲與 AOF/快照差異：lt-redis 用 AOF everysec；ElastiCache 的持久化模型不同。
- 真實模型延遲：System One 是 mock（lognormal，中位數 40 ms、p95 120 ms）。真模型的延遲分布、冷啟動、GPU 排隊都沒測，
  推論效能不在範圍內。
- 從台灣到正式區域的真實網路：本機（WSL）經 SSH tunnel 到主機的往返約 35 到 40 ms，這只是一條隧道，
  不是行動網路、不是 CDN 或負載平衡器後面的路徑。
- 真實的使用者行為分布：流量是合成的 session 序列，時鐘壓縮（「隔天」= 30 秒、狀態逾時 = 10 秒）。
- 長期資料量：測試從空表開始，最長 60 分鐘；數月資料量下的索引大小、vacuum 成本、分割區切換沒有量到。
- 單一主機上的鄰居干擾：主機同時跑其他服務（CPU 使用很低，但 load 與記憶體頻寬會互相影響）。
- 業務效果：實驗組別、優惠的轉換效果與統計檢定力都不在壓測範圍內；這裡只證明分組穩定、護欄不被違反。

## 測試後的修正（2026-10-03）

本報告的數字都來自下列修正之前的程式碼，修正後沒有重跑壓測。這些錯誤是發布前的 code review 找到的；
每一項都有一個在修正前失敗、修正後通過的測試（`tests/`、`rust-collector/src/batch.rs`）。

| 範圍 | 錯誤與修正 | 是否動到量測的熱路徑 |
|---|---|---|
| 斷路器 | 半開探測被取消（客戶端斷線）後，斷路器永遠不再探測。現在取消算一次失敗的探測。 | 否 |
| webhook 簽章 | 非 ASCII 的簽章回 500。現在比較 bytes，回 401。 | 否 |
| Context 套用腳本 | 去重標記先寫，腳本中途出錯時事件被當成重複而遺失。現在標記在所有狀態寫入之後；`minutes` 先轉成 0 到 1440 的整數。 | 是，未重測 |
| 逾時 Trigger | 紀錄用 `\x1f` 拼接，含這個字元的欄位會讓逾時迴圈停止；長 user_id 產生超長 event_id。現在用 JSON 陣列，壞紀錄單獨丟棄；event_id 用 user_id 的雜湊；4xx（429 除外）不再重試。 | 否（只有逾時路徑） |
| 頻率上限 | key 沒有 user_id，沒帶 session_id 的使用者共用一個上限；同一事件重試會被自己擋下。現在 key 含 user_id，`SET NX GET` 讓同一事件通過。 | 指令數不變 |
| 新鮮度護欄 | 用客戶端時鐘判斷過期。現在用伺服器端的 `updated_st`。 | 否 |
| 延遲 histogram | key 用原始路徑，未知 URL 讓記憶體一直增加。現在用 route 樣式，未知路徑記為 `unmatched`。 | 否 |
| Context 啟動 | 沒有接住 Glide 的連線錯誤，背景工作不會啟動。現在接住並重試。 | 否 |
| Decision 的 PG 連線 | 等模型與 Valkey 時一直持有連線。現在只在 SQL 執行時持有。 | 是，未重測 |
| 整數範圍 | Python 與 Rust collector 接受的整數範圍不同。現在兩邊都只接受 signed 64-bit 範圍內的數字。 | 是（多一個範圍檢查），未重測 |
| k6 計數 | 被隔離的事件也算成功。現在分開計數並設門檻。47 份 `results/*/stats_collector*.json` 的隔離計數都是 0，所以已發布的 k6 數字不變。 | 只動量測工具 |
| 容量判定 | 沒有成功樣本的階段被當成 0 延遲而通過。現在判失敗。用新規則重算 `results/t6`、`t6b`、`t6b_rerun` 的每一階，判定都沒有改變。 | 只動量測工具 |
| 腳本 | cleanup 只刪 `lt-` 開頭的容器與部署前不存在的映像，檢查 START，遵守 `LT_REMOTE_DIR`；restack 與 batch 不再吞掉錯誤；中斷時停止 guard 與 sampler；`pg_*` 腳本也啟動 guard。 | 否 |

動到熱路徑的三項（Context 套用腳本、Decision 的 PG 連線、整數範圍）沒有重新量測，T1 到 T7 的 CPU 與延遲都是修正前的值。

## 清理

指令：`ssh <alias> 'bash -s -- "2026-10-02 14:55" --apply' < scripts/remote/cleanup.sh`（不加 `--apply` 時只列出要移除的東西）。
它只移除本次測試建立的東西：`docker compose -p lt --profile dual --profile tls down -v`、所有 `lt-` 容器、volume 與 network、
本次拉取或建立的 11 個映像檔、重建 lt-app 時留下的無標籤映像層、開始時間之後建立的 build cache 項目（逐一以 `--filter id=` 刪除，
有相依關係的項目要多刪幾輪，共 10 輪），最後刪除主機上的 `~/lt-loadtest`。沒有執行任何全域的 prune。

清理後的證明（原始輸出在 `results/private/cleanup_proof.txt`）：

| 檢查 | 結果 |
|---|---|
| `docker ps -a --filter name=lt-` | 空 |
| `docker volume ls \| grep lt-` | 空 |
| `docker network ls \| grep lt-` | 空 |
| `ls ~/lt-loadtest` | No such file or directory |
| 本次拉取或建立的映像檔（lt-app、lt-collector-rs、postgres:18.6、postgres:17-alpine、valkey 8.1.4、grafana/k6 2.3.0、redis 7.2、dragonfly 1.38.0、python 3.13-slim、rust 1.97.1、distroless cc） | 0 個仍存在 |
| 開始時間之後建立的 build cache 項目 | 0 |
| 既有服務 | 11 個容器仍在執行；主機 load 1.03、可用記憶體 19 GB |

本機保留：所有原始碼與結果在本 repo；k6 2.3.0 靜態執行檔在本機的使用者 bin 目錄。
