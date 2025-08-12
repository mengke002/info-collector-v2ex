#!/usr/bin/env python3
"""
V2EX数据采集系统
主执行脚本
"""
import sys
import argparse
import json
import logging
from datetime import datetime, timezone, timedelta

from src.scheduler import scheduler


def setup_logging():
    """设置日志配置"""
    from src.config import config
    log_config = config.get_logging_config()
    
    logging.basicConfig(
        level=getattr(logging, log_config['log_level'].upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_config['log_file'], encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def get_beijing_time():
    """获取北京时间（UTC+8）"""
    utc_time = datetime.now(timezone.utc)
    beijing_time = utc_time + timedelta(hours=8)
    return beijing_time.replace(tzinfo=None)


def main():
    """主函数"""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='V2EX数据采集系统')
    parser.add_argument('--task', choices=['crawl', 'cleanup', 'stats', 'full'], 
                       default='crawl', help='要执行的任务类型')
    parser.add_argument('--retention-days', type=int, 
                       help='数据保留天数（仅用于cleanup任务）')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    parser.add_argument('--force-nodes', action='store_true',
                       help='强制更新节点信息（忽略时间限制）')
    
    args = parser.parse_args()
    
    print(f"V2EX数据采集系统")
    print(f"执行时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"执行任务: {args.task}")
    print("-" * 50)
    
    # 执行对应任务
    if args.task == 'crawl':
        result = scheduler.run_crawl_task(force_update_nodes=args.force_nodes)
    elif args.task == 'cleanup':
        result = scheduler.run_cleanup_task(args.retention_days)
    elif args.task == 'stats':
        result = scheduler.run_stats_task()
    elif args.task == 'full':
        result = scheduler.run_full_maintenance()
    else:
        print(f"未知任务类型: {args.task}")
        sys.exit(1)
    
    # 输出结果
    if args.output == 'json':
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, args.task)
    
    # 根据结果设置退出码
    if result.get('success', False):
        print("\n✅ 任务执行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 任务执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


def print_result(result: dict, task_type: str):
    """打印结果"""
    if not result.get('success', False):
        print(f"❌ 任务失败: {result.get('error', '未知错误')}")
        return
    
    if task_type == 'crawl':
        print(f"✅ 爬取任务完成")
        print(f"   节点保存: {result.get('nodes_saved', 0)} 个")
        print(f"   发现主题: {result.get('topics_found', 0)} 个")
        print(f"   成功爬取: {result.get('topics_crawled', 0)} 个")
        print(f"   用户保存: {result.get('users_saved', 0)} 个")
        if 'success_rate' in result:
            print(f"   成功率: {result['success_rate']}")
    
    elif task_type == 'cleanup':
        print(f"✅ 清理任务完成")
        print(f"   删除过期主题: {result.get('deleted_topics', 0)} 个")
        print(f"   保留天数: {result.get('retention_days', 0)} 天")
    
    elif task_type == 'stats':
        stats = result.get('stats', {})
        print(f"✅ 统计信息")
        print(f"   节点数量: {stats.get('nodes_count', 0)}")
        print(f"   用户数量: {stats.get('users_count', 0)}")
        print(f"   主题数量: {stats.get('topics_count', 0)}")
        print(f"   今日主题: {stats.get('today_topics', 0)}")
        if stats.get('latest_activity'):
            print(f"   最新活动: {stats['latest_activity']}")
        if stats.get('oldest_activity'):
            print(f"   最旧数据: {stats['oldest_activity']}")
    
    elif task_type == 'full':
        results = result.get('results', {})
        print(f"✅ 完整维护任务完成")
        
        # 爬取结果
        crawl_result = results.get('crawl', {})
        if crawl_result.get('success'):
            print(f"   爬取: 发现 {crawl_result.get('topics_found', 0)} 个主题，成功 {crawl_result.get('topics_crawled', 0)} 个")
        
        # 清理结果
        cleanup_result = results.get('cleanup', {})
        if cleanup_result.get('success'):
            print(f"   清理: 删除 {cleanup_result.get('deleted_topics', 0)} 个过期主题")
        
        # 统计结果
        stats_result = results.get('stats', {})
        if stats_result.get('success'):
            stats = stats_result.get('stats', {})
            print(f"   统计: 节点 {stats.get('nodes_count', 0)}, 用户 {stats.get('users_count', 0)}, 主题 {stats.get('topics_count', 0)}")


if __name__ == "__main__":
    main()