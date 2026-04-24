# 多源卫星影像自动处理流水线（Sentinel-2 MSI + Sentinel-3 OLCI + Landsat 8/9）
全自动下载欧空局 & USGS 原始影像 → ACOLITE 大气校正 + 云掩膜 → 湖泊边界裁剪 → 输出各波段 GeoTIFF 的分布式处理系统。
基于 RabbitMQ 消息队列，可横向扩展多个 Worker，适合批量处理大量湖泊。

## 📚 功能
- **数据检索**：通过 CDSE API（Sentinel）和 USGS M2M API（Landsat）自动查找指定时间范围内覆盖目标湖泊的影像。
- **智能打包**：同一景影像可能覆盖多个湖泊，系统会自动合并任务，只下载一次，避免重复。
- **大气校正**：使用 ACOLITE 进行大气校正及云掩膜（`dsf` 算法），输出 `Rrs` 等参数。
- **掩膜裁剪**：利用湖泊 SHP 矢量文件进行精确裁剪，仅保留湖泊水体区域，并增加 Alpha 透明通道。
- **重试与容错**：所有 Worker 均内置重试机制，失败任务可自动重新排队。
## 📊 架构与队列
```
monitor_producer.py (定时监测)
         │
         ▼
[ download_acolite_queue ] (下载+解压)
         │
         ▼
[ acolite_run_queue ] (ACOLITE 大气校正)
         │
         ▼
[ mosaic_postprocess_queue ] (拼接、裁剪、清理)
```
- `worker_download.py`：负责从 CDSE / USGS 下载原始影像，解压并发送到 ACOLITE 队列。
- `worker_acolite.py`：运行 ACOLITE 命令行工具进行大气校正，将每个湖泊的结果送入后处理队列。
- `worker_postprocess.py`：收集 ACOLITE 输出的波段 TIF，拼接后按湖泊 SHP 裁剪，输出最终产品。
- **所有 Worker 可独立运行或同时运行**。
## 📂 目录结构

```
项目目录/
├── config.py              # 全局配置（环境变量）
├── shared_utils.py        # CDSE / USGS 认证、下载、断点续传
├── providers/             # 数据检索插件（CDSE、USGS）
├── monitor_producer.py    # 监测并派发下载任务
├── worker_download.py     # 下载 Worker
├── worker_acolite.py      # ACOLITE Worker
├── worker_postprocess.py  # 后处理 Worker
├── run_producer.ps1       # 单独启动监测生产者（可指定传感器）
├── start_pipeline.ps1     # 一键启动整套流程
├── lake_shp/              # 存放各湖泊的 .shp 文件（自动扫描）
└── README.md
```

## ⚙️ 环境与依赖

### 必需软件
- Python 3.9+
- ACOLITE（需配置独立 Python 环境）
- RabbitMQ 服务（默认 `localhost`）
- GDAL（通过 `pip install GDAL` 或自行编译）

### Python 依赖
```bash
pip install pika geopandas requests osgeo python-dotenv
```

### ACOLITE 环境
请提前下载并配置好 ACOLITE（[官网](https://odnature.naturalsciences.be/remsem/acolite/)），并确保：
- `ACOLITE_PYTHON` 指向 ACOLITE 专用 conda 环境的 `python.exe`
- `ACOLITE_CLI_PATH` 指向 `launch_acolite.py`
## 🔧 快速开始
### 1. 配置环境变量

**必须设置以下环境变量**

| 变量名 | 说明 | 示例 |
|--------|------|------|
| `CDSE_USERNAME` | 欧空局账号 | `your@email.com` |
| `CDSE_PASSWORD` | 欧空局密码 | `your_password` |
| `USGS_USERNAME` | USGS 账号 | `your_usgs_username` |
| `USGS_TOKEN` | USGS M2M Token | `your_usgs_token` |
| `RABBITMQ_HOST` | RabbitMQ 地址 | `localhost`（默认） |
| **存储路径相关**（按需修改） | | |
| `TEMP_DIR` | 原始影像存放目录 | `G:/Temp_L1_Data` |
| `ACOLITE_OUT_DIR` | ACOLITE 输出目录 | `G:/L2_Acolite` |
| `FINAL_OUTPUT_DIR` | 最终产品目录 | `G:/Final_Products` |
| `MASK_SHP_DIR` | 湖泊 SHP 文件夹 | `G:/lake_shp` |
| **传感器开关** | | |
| `ENABLED_SENSOR_KEYS` | 启用的传感器，逗号分隔 | `MSI,OLCI,LANDSAT` |

**Windows 设置方式**（可加入启动脚本）：  
```powershell
$env:CDSE_USERNAME = "your@email.com"
$env:CDSE_PASSWORD = "your_password"
# ... 其他变量
```
### 2. 准备湖泊 SHP 文件
将每个湖泊的矢量文件（`.shp`, `.shx`, `.dbf`, `.prj` 等）放入 `MASK_SHP_DIR` 指定的文件夹，文件名即为湖泊缩写（如 `Dianchi.shp`）。  
系统会自动扫描所有 `.shp` 文件并生成对应的任务。

### 3. 启动 RabbitMQ
确保 RabbitMQ 服务正在运行。

### 4. 启动处理流水线

#### 一键启动所有组件
```powershell
.\start_pipeline.ps1 -Sensors "MSI,OLCI,LANDSAT"
```
- 会同时启动 `worker_acolite.py` 和 `worker_postprocess.py` 两个后台窗口，并自动运行一次 `monitor_producer.py`。
- 若仅需运行消费者，可加 `-NoProducer` 跳过监测。

#### 单独启动监测（生产者）
```powershell
.\run_producer.ps1 -Sensors "MSI,LANDSAT"
```
只会搜索指定传感器的影像并派发任务。

#### 手动启动 Worker（适合调试）
```bash
python worker_acolite.py
python worker_postprocess.py
```
生产者在另一终端运行 `python monitor_producer.py`。

### 5. 查看结果
最终产品位于 `FINAL_OUTPUT_DIR/湖泊名/日期/` 下，文件命名规则：  
`<湖泊名>_<影像日期>_<参数名>.tif`  
例如 `Dianchi_20250101_Rrs_443.tif`。

## 📝 注意事项
- **坐标系统**：所有 SHP 文件将自动转换为 WGS84 坐标系。
- **ACOLITE 设置**：在 `worker_acolite.py` 的 `run_acolite` 函数中可修改校正参数，如 `dsf_min_tile_fraction`、`l1_mac_th` 等。
- **并行处理**：可以同时启动多个下载、ACOLITE 或后处理 Worker 以加速处理，只需在多个终端运行对应脚本即可。
- **错误处理**：所有失败任务会记录日志，并在退出前重试 3 次。可通过 RabbitMQ 管理界面观察队列状态。

## 🤝 致谢
- [ACOLITE](https://odnature.naturalsciences.be/remsem/acolite/) 大气校正软件
- Copernicus Open Access Hub 与 USGS 提供免费卫星影像
- 项目中的 CDSE/USGS API 交互代码参考了社区实践

## 📄 开源协议
本项目采用 MIT 协议开源。使用前请遵守各数据提供方的使用条款。

---

### 最后检查清单

- [ ] 已从 `config.py` 移除所有真实密码
- [ ] 已创建 `.gitignore` 并包含 `.env`、临时文件夹等
- [ ] 已确认所有环境变量在本地正确设置
- [ ] 测试启动脚本能正常运行

