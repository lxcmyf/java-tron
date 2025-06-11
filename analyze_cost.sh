#!/bin/bash

# 设定日志文件路径
LOG_FILE="arthas_cost.log"

# 提取所有耗时字段（单位为 ms），保留小数点精度
times=($(grep -oP '\d+\.\d+(?=ms)' "$LOG_FILE"))

# 判断是否有数据
if [ ${#times[@]} -eq 0 ]; then
  echo "未找到任何耗时记录。"
  exit 1
fi

# 初始化 max, min, sum
max=${times[0]}
min=${times[0]}
sum=0

for time in "${times[@]}"; do
  # 更新最大值
  awk -v a="$time" -v b="$max" 'BEGIN{ if (a > b) exit 0; else exit 1 }'
  if [ $? -eq 0 ]; then
    max=$time
  fi

  # 更新最小值
  awk -v a="$time" -v b="$min" 'BEGIN{ if (a < b) exit 0; else exit 1 }'
  if [ $? -eq 0 ]; then
    min=$time
  fi

  # 累加
  sum=$(awk -v a="$sum" -v b="$time" 'BEGIN{ printf "%.6f", a + b }')
done

# 计算平均值
count=${#times[@]}
avg=$(awk -v s="$sum" -v c="$count" 'BEGIN{ printf "%.6f", s / c }')

# 输出结果
echo "最大值：$max ms"
echo "最小值：$min ms"
echo "平均值：$avg ms"