# 核对已留存的日线来源

已有不可变市场日分区和 `source-capture/v1` 来源回执时，可通过现有的
`ashare-data-acquire` 入口做只读核对。此模式不访问网络、不需要供应商令牌，
也不会生成回执或恢复日索引。参数中的哈希、行数和日期应来自待核对的
分区及外部已保存清单，而不是根据检查结果反填。

```sh
ashare-data-acquire \
  --verify-source-receipt SOURCE_RECEIPT_ID \
  --source-root SOURCE_CAPTURE_DIRECTORY \
  --publication-root IMMUTABLE_DATASET_DIRECTORY \
  --dataset-id DATASET_ID \
  --manifest-sha256 EXPECTED_MANIFEST_SHA256 \
  --trade-date YYYY-MM-DD \
  --expected-rows ROW_COUNT \
  --as-of 'YYYY-MM-DDTHH:MM:SS+08:00'
```

成功时返回 `daily-source-verification/1.0.0` JSON；不匹配或缺少文件时失败。
输出分别保留该次来源接收时间和分区生成时间，`available_at` 仅指来源
回执接收时间。校验只证明回执所绑定的原始行按现有规则标准化后，与指定
分区内容相同；它不证明该次请求创建了该分区、指定哈希来自可信目录，
也不证明历史时点可得或供应商端点真实性。其他范围和未经核验的回执
不会被认定为通过。

# 供应方共享请求预算

HTTP 传输默认按供应方主机和端口共用持久预算，进程重启或切换工作目录
不会重置已用次数。默认最短间隔为 2 秒、滚动 24 小时最多 5000 次；
限流时保留至少 60 秒冷却并收紧后续间隔。更宽松的调用者不能放宽已有
策略，损坏的预算文件或时钟回退会阻止新请求。预算只记录时间和策略，
不保存令牌、证券或行情；`TUSHARE_BUDGET_DIR` 用于指定共享预算目录。
调用者必须使用与现用进程相同的目录，才能共享额度。

# 正常请求的来源回执

设置 `TUSHARE_SOURCE_CAPTURE_ROOT` 为绝对路径，且目录末尾必须是
`runtime/upstream-source-data`，正常 HTTP 客户端才会留存来源。客户端在
全部分页及字段检查完成后，仅对原响应存一份内容寻址表和回执，不额外
发起供应方请求；记录的请求开始和响应完成时间各自保留。记录内容必须
符合现用来源字段目录，不包含令牌。本阶段只迁入原目录中日线与交易日历
两项定义，使用独立的切片格式和原目录哈希标明范围；其他接口尚未迁入。
请求开始是分页逻辑调用的开始，可能早于预算等待结束后的首个网络请求。
可选归档失败不会伪造成功回执；后续恢复必须取得本次请求的有效回执和
另一份独立匹配证据。

# 按交易日入库与中断恢复

现有日更所调用的模块入口可按供应方交易日历逐日发布不可变分区：

```sh
python -m ashare_data_gateway.market_daily \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --generated-at 'YYYY-MM-DDTHH:MM:SS+00:00' \
  --publication-root IMMUTABLE_DATASET_DIRECTORY
```

`generated-at` 须为有时区的 UTC 时间。已有日索引时只读回并校验，不再
请求行情。如果分区已提交而日索引缺失，入口会校验现存分区及辅助文件，
并要求本次完整来源回执和另一份独立留存回执的原始行均与分区匹配，才
原子恢复原格式的日索引并留存 `market-day-recovery/v1` 证明。缺证、
损坏或不同的竞争索引会失败，不覆盖原数据。不同输入可能生成新的数据集；
并发冲突也可能留下不可变的孤立数据集。相同来源内容不能证明哪次响应
创建了原分区，也不能单凭它证明历史逐时可得性。此候选尚未接入主线的
个人日更调度或部署到现用 App。
