#!/bin/bash

# 启动vLLM服务的脚本
# 使用8张显卡

echo "开始启动vLLM服务..."

# 停止可能存在的旧服务
echo "停止旧服务..."
docker stop vllm 2>/dev/null || true
docker rm vllm 2>/dev/null || true

# 启动vLLM服务（使用所有8张显卡）
echo "启动vLLM服务（使用所有8张显卡）..."
docker run --gpus 'all' \
    -v /data/.cache/vllm:/root/.cache/vllm \
    -v /data/.cache/huggingface:/root/.cache/huggingface \
    -v /data/training/model:/root/model \
    -v /data/training/llama3.1_8b_checkpoint:/root/lora \
    -p 8201:8000 \
    --ipc=host \
    -d --name vllm \
    vllm/vllm-openai:latest \
    --enable-lora \
    --lora-modules llama3.1_8b=/root/lora/20250604/checkpoint-1005 \
    --model /root/model/Meta-Llama-3.1-8B-Instruct \
    --tensor-parallel-size 8

# 等待服务启动
echo "等待服务启动..."
sleep 160

# 检查服务状态
echo "检查服务状态..."
if curl -s http://localhost:8201/health > /dev/null; then
    echo "✅ 服务端口 8201 运行正常"
else
    echo "❌ 服务端口 8201 启动失败"
fi

echo "vLLM服务启动完成！"
echo "服务地址：http://localhost:8201" 