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
