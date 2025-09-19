#!/usr/bin/env python3.12
"""
获取报告节点列表的helper脚本
用于GitHub Actions动态读取配置
"""
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.config import config
    nodes = config.get_report_nodes()
    print(' '.join(nodes))
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)