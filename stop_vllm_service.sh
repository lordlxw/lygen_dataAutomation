#!/bin/bash

# 停止vLLM服务的脚本

echo "停止vLLM服务..."

# 停止并删除容器
container_name="vllm"
echo "停止容器 $container_name..."
docker stop $container_name 2>/dev/null || true
docker rm $container_name 2>/dev/null || true
echo "✅ 容器 $container_name 已停止并删除"

echo "vLLM服务已停止！" 