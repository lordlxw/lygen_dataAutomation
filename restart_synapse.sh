#!/bin/bash

# 1. 拉取最新代码
echo "==== 1. 拉取最新代码 ===="
git pull

# 2. 关闭现有 synapse_flow.web.flask_server 进程
echo "==== 2. 关闭现有 synapse_flow.web.flask_server 进程 ===="
ps aux | grep "python -m synapse_flow.web.flask_server" | grep -v grep | awk '{print $2}' | xargs -r kill -9

# 3. 关闭所有模型相关docker容器
echo "==== 3. 关闭所有模型相关docker容器 ===="
CONTAINERS=$(python3 -c "import sys; sys.path.insert(0, '.'); from model_config import MODEL_CONFIGS; print(' '.join([cfg['container_name'] for cfg in MODEL_CONFIGS.values() if 'container_name' in cfg]))")
for name in $CONTAINERS; do
  echo "Stopping and removing container: $name"
  docker stop $name 2>/dev/null
  docker rm $name 2>/dev/null
done

# 4. 后台启动 flask_server
echo "==== 4. 后台启动 flask_server ===="
nohup python -m synapse_flow.web.flask_server > flask_server.out 2>&1 &

echo "==== 启动完成 ===="
echo "日志输出：flask_server.out" 