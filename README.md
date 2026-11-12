# 多源卫星影像自动处理流水线（Sentinel-2 MSI + Sentinel-3 OLCI + Landsat 8/9）

通过 CDSE 和 USGS 检索、下载原始影像，使用 ACOLITE 进行大气校正，再按湖泊 SHP 边界裁剪并输出 GeoTIFF。RabbitMQ 将三个处理阶段连接起来，每个阶段可以独立增加 Worker。

## 处理流程

| 程序 | 职责 | 默认输入队列 |
| --- | --- | --- |
| monitor_producer.py | 检索启用的传感器，合并同一景覆盖的湖泊 | |
| worker_download.py | 下载或续传 ZIP、TAR，解压原始产品 | download_acolite_queue |
| worker_acolite.py | 按湖泊范围运行 ACOLITE，记录每景影像的校正产物 | acolite_run_queue |
| worker_postprocess.py | 拼接同一景内同参数的瓦片，裁剪并输出 GeoTIFF | mosaic_postprocess_queue |

CDSE 和 USGS 可以同时检索。USGS 在合并重复影像后批量查询 Landsat Level-1 产品包。重试或重复派发时，Worker 会优先复用有效的下载、校正或裁剪结果。

## 安装

需要 Python 3.9+、RabbitMQ、GDAL Python 绑定和一个可运行 ACOLITE 的 Python 环境。GDAL 与 Python 绑定需匹配，使用 Conda 的环境可通过 conda-forge 安装。

```powershell
conda install -c conda-forge gdal
python -m pip install requests pika geopandas python-dotenv filelock
```

在项目根目录的 .env 中填写所启用传感器需要的账户和本机路径。系统环境变量优先于 .env。

| 变量 | 含义 | 默认值或示例 |
| --- | --- | --- |
| CDSE_USERNAME、CDSE_PASSWORD | MSI 和 OLCI 下载账户 | CDSE 用户名和密码 |
| USGS_USERNAME、USGS_TOKEN | Landsat M2M 账户 | USGS 用户名和应用令牌 |
| RABBITMQ_HOST、RABBITMQ_PORT | RabbitMQ 地址和端口 | localhost、5672 |
| TEMP_DIR | 原始影像与解压目录 | data/raw |
| ACOLITE_OUT_DIR | ACOLITE 校正产物和日志 | data/acolite |
| FINAL_OUTPUT_DIR | 最终 GeoTIFF | data/products |
| MASK_SHP_DIR | 湖泊矢量目录 | lake_shp |
| ACOLITE_CLI_PATH | ACOLITE 启动脚本 | 项目目录下的 launch_acolite.py |
| ACOLITE_PYTHON | ACOLITE 环境的 Python | 当前 Python 解释器 |
| MONITOR_LOOKBACK_DAYS | 每次检索回溯天数 | 1 |

将每个湖泊的 .shp、.shx、.dbf、.prj 等文件放入 MASK_SHP_DIR。文件名用作湖泊 ID。生产者根据 SHP 的坐标系计算 WGS84 检索范围，后处理用原始矢量边界裁剪。

## 启动

启动 RabbitMQ 后，从已安装 Python 依赖的 PowerShell 运行。

```powershell
.\start_pipeline.ps1 -Sensors "MSI,OLCI,LANDSAT"
```

脚本会启动三个阶段的 Worker 并运行一次生产者。Worker 日志保存在项目的 logs 目录，启动时会打印各进程的 PID。可用 -NoProducer 只启动 Worker，或运行 .\run_producer.ps1 -Sensors "MSI,LANDSAT" 单独派发任务。也可以分别运行三个 worker_*.py 脚本。

新产品路径为 FINAL_OUTPUT_DIR/湖泊/日期/传感器/景ID/湖泊_日期_参数.tif。例如下面这景 Landsat 影像。

```text
data/products/Dianchi/20260416/LANDSAT/LC81210382026106LGN00/Dianchi_20260416_Rrs_443.tif
```

裁剪生成的 Alpha 波段标记湖泊范围以外的像元，并使用源 TIFF 的 NoData 定义。同一湖泊同一天的各景产物各自保存。已有的旧目录可以继续读取。升级时建议重启三个阶段的 Worker，使新任务统一采用上述路径。

## 调整吞吐量和磁盘占用

```powershell
.\start_pipeline.ps1 -DownloadWorkers 2 -AcoliteWorkers 2 -PostprocessWorkers 1
```

- 每个 Worker 同时处理一条消息。下载并发取决于账户配额和带宽，大气校正并发取决于单景峰值内存。分别观察日志中的下载速度和各阶段用时，再调整进程数。 [CDSE 配额与限速](https://documentation.dataspace.copernicus.eu/Quotas.html)
- 如果只需要 Rrs，设置 ACOLITE_EXPORT_L2R=0 可减少 L2R GeoTIFF 的写盘和后续裁剪。设置 ACOLITE_KEEP_NETCDF=0 可在导出后清理 NetCDF，并关闭 DSF 诊断参数输出。默认保留这两类产物。校正仍使用原有 DSF 参数和 l2w_mask=False，湖泊裁剪与 ACOLITE 的云掩膜设置独立。
- 原始影像默认在 ACOLITE 成功交接后清理。设置 KEEP_RAW_CACHE=1 可保留原始影像与解压目录，适合反复调整湖泊范围。设置 CLEAN_ACOLITE_TEMP=1 可在裁剪成功后清理该景的校正产物。
- GDAL_THREADS 默认为 1。处理大幅 GeoTIFF 时可按可用 CPU 和 Worker 数适度提高。多个 Worker 共享相同的存储目录时会协调同一景影像的读写。 [GDAL Warp 文档](https://gdal.org/en/stable/programs/gdalwarp.html)
- 每个阶段在下游确认收到消息后再确认当前消息。任务最多重试三次，随后进入对应的 .failed 队列。修复错误后可将该队列的消息重新发布到原队列，并清除 retry_count 消息头。长任务的单条消息确认时限由 RabbitMQ 的 consumer_timeout 控制。 [RabbitMQ 消费者文档](https://www.rabbitmq.com/docs/consumers)

## 测试

安装 pytest 后，可运行 python -m pytest -q 执行回归测试。测试使用临时数据与模拟消息，不需要连接卫星服务或 RabbitMQ。

## 致谢

感谢 [ACOLITE](https://odnature.naturalsciences.be/remsem/acolite/)、[Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu/) 和 [USGS](https://www.usgs.gov/landsat-missions)。

本项目采用 MIT 协议开源。使用前请遵守各数据提供方的使用条款。
