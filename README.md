# NiDAQ Python Package Unit

這是一個用於National Instruments DAQmx設備數據採集的Python套件，支援多種類比輸入類型，包括電壓、電流和加速度計通道。

## 專案概述

本專案提供了一個模組化的解決方案來初始化和管理NI DAQmx設備，透過INI配置檔案來設定通道參數和採樣設定。主要功能包括：

- 支援多種類比輸入類型（電壓、電流、加速度計）
- 透過INI檔案進行配置管理
- 連續採樣模式
- 多通道數據同步採集

## 專案結構

```
NiDAQ_Python_via_Package_Unit/
├── API/
│   └── NiDAQ.ini          # DAQmx設備配置檔案
├── src/
│   ├── main.py            # 主程式入口點
│   └── nidaq_module.py    # 核心DAQmx操作模組
├── requirements.txt       # Python依賴套件
└── README.md             # 專案說明文件
```

## 安裝需求

### 系統需求
- Python 3.7+
- National Instruments DAQmx驅動程式
- 相容的NI DAQmx硬體設備

### Python套件依賴

安裝所需的Python套件：

```bash
pip install -r requirements.txt
```

主要依賴套件：
- `nidaqmx` - NI DAQmx Python API
- `awsiotsdk` - AWS IoT SDK
- `pyserial` - 序列通訊
- `pymodbus` - Modbus通訊協定

## 配置說明

### INI檔案結構

`API/NiDAQ.ini` 檔案包含以下主要區段：

#### DAQmxChannel 區段
定義類比輸入通道的配置：
- `ChanType`: 通道類型（Analog Input）
- `AI.MeasType`: 測量類型（Voltage/Current/Accelerometer）
- `PhysicalChanName`: 實體通道名稱
- `AI.Min/Max`: 測量範圍
- `AI.Accel.Sensitivity`: 加速度計靈敏度（適用於加速度計通道）

#### DAQmxTask 區段
定義採樣任務的配置：
- `SampClk.Rate`: 採樣率（Hz）
- `SampQuant.SampPerChan`: 每次讀取的樣本數
- `SampQuant.SampMode`: 採樣模式（Continuous Samples）

#### 硬體配置區段
- `DAQmxCDAQChassis`: CompactDAQ機箱配置
- `DAQmxCDAQModule`: CompactDAQ模組配置

## 使用方法

### 基本使用

1. 確保硬體設備已正確連接並安裝驅動程式
2. 根據您的硬體配置修改 `API/NiDAQ.ini` 檔案
3. 執行程式：

```bash
cd src
python main.py
```

### 程式碼範例

```python
from nidaq_module import init_task, read_task_data

# 初始化DAQmx任務
ini_path = "./API/NiDAQ.ini"
task, samples_per_read, sample_rate, channel_names = init_task(ini_path)

# 連續讀取數據
is_running = True
while is_running:
    data = read_task_data(task, samples_per_read)
    # 處理數據...
```

## 核心功能

### `init_task(ini_path: str) -> tuple`
初始化DAQmx任務並配置所有通道和時序設定。

**參數：**
- `ini_path`: INI配置檔案路徑

**回傳：**
- `task`: DAQmx任務物件
- `samples_per_read`: 每次讀取的樣本數
- `sample_rate`: 採樣率
- `channel_names`: 通道名稱列表

### `read_task_data(task, samples_per_read) -> list`
從任務中讀取數據並以元組格式回傳。

**參數：**
- `task`: DAQmx任務物件
- `samples_per_read`: 每次讀取的樣本數

**回傳：**
- 數據列表，格式為 `[(x1, y1, z1), (x2, y2, z2), ...]`

### `config_filter(ini_path: str, sections: str) -> list`
從INI檔案中提取指定區段的配置數據。

**參數：**
- `ini_path`: INI檔案路徑
- `sections`: 區段名稱前綴

**回傳：**
- 包含配置數據的字典列表

## 支援的通道類型

### 電壓通道 (Voltage)
```ini
AI.MeasType = Voltage
AI.Min = -10
AI.Max = 10
```

### 電流通道 (Current)
```ini
AI.MeasType = Current
AI.Min = -20
AI.Max = 20
```

### 加速度計通道 (Accelerometer)
```ini
AI.MeasType = Accelerometer
AI.Min = -5
AI.Max = 5
AI.Accel.Sensitivity = 1000
AI.Excit.Val = 0.002
```

## 注意事項

1. **硬體相容性**: 確保您的NI DAQmx硬體設備與配置檔案中的設定相符
2. **驅動程式**: 必須安裝最新版本的NI DAQmx驅動程式
3. **權限**: 某些操作可能需要管理員權限
4. **模擬模式**: 配置檔案中的 `DevIsSimulated = 1` 表示使用模擬設備進行測試

## 故障排除

### 常見問題

1. **設備未找到**: 檢查硬體連接和驅動程式安裝
2. **權限錯誤**: 以管理員身份執行程式
3. **配置錯誤**: 驗證INI檔案中的通道名稱和參數設定

### 除錯建議

- 使用NI MAX (Measurement & Automation Explorer) 驗證硬體配置
- 檢查設備管理員中的設備狀態
- 查看NI DAQmx錯誤代碼對照表

## 授權

本專案僅供學習和研究使用。使用NI DAQmx相關功能需要遵守National Instruments的授權條款。

## 貢獻

歡迎提交問題報告和功能請求。如需貢獻程式碼，請遵循以下步驟：

1. Fork 本專案
2. 建立功能分支
3. 提交變更
4. 發起 Pull Request

## 聯絡資訊

如有任何問題或建議，請透過專案Issues頁面聯繫。 