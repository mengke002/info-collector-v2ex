"""
V2EX数据采集调度器
负责协调各种任务的执行
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from .v2ex_crawler import crawler
from .database import db_manager
from .config import config


class Scheduler:
    """任务调度器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def get_beijing_time(self):
        """获取北京时间"""
        utc_time = datetime.now(timezone.utc)
        beijing_time = utc_time + timedelta(hours=8)
        return beijing_time.replace(tzinfo=None)
    
    def run_crawl_task(self) -> Dict[str, Any]:
        """执行爬取任务（增量更新）"""
        self.logger.info("开始执行增量爬取任务")
        
        try:
            # 初始化数据库
            db_manager.init_database()
            
            # 爬取目标节点的主题（网页解析 + 详情获取）
            nodes_topics_result = crawler.crawl_topics_by_nodes()
            
            # 汇总结果
            total_topics_found = nodes_topics_result.get('topics_found', 0)
            total_topics_crawled = nodes_topics_result.get('topics_crawled', 0)
            total_users_saved = nodes_topics_result.get('users_saved', 0)
            total_replies_saved = nodes_topics_result.get('replies_saved', 0)
            
            success_rate = f"{(total_topics_crawled / total_topics_found * 100):.1f}%" if total_topics_found > 0 else "0%"
            
            result = {
                'success': True,
                'topics_found': total_topics_found,
                'topics_crawled': total_topics_crawled,
                'users_saved': total_users_saved,
                'replies_saved': total_replies_saved,
                'success_rate': success_rate,
                'timestamp': self.get_beijing_time()
            }
            
            self.logger.info(f"爬取任务完成: 发现 {total_topics_found} 个主题，成功爬取 {total_topics_crawled} 个")
            return result
            
        except Exception as e:
            self.logger.error(f"爬取任务失败: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.get_beijing_time()
            }
    
    def run_cleanup_task(self, retention_days: int = None) -> Dict[str, Any]:
        """执行数据清理任务"""
        if retention_days is None:
            retention_days = config.get_data_retention_days()
        
        self.logger.info(f"开始执行数据清理任务，保留 {retention_days} 天的数据")
        
        try:
            deleted_count = db_manager.clean_old_data(retention_days)
            
            result = {
                'success': True,
                'deleted_topics': deleted_count,
                'retention_days': retention_days,
                'timestamp': self.get_beijing_time()
            }
            
            self.logger.info(f"数据清理完成: 删除了 {deleted_count} 个过期主题")
            return result
            
        except Exception as e:
            self.logger.error(f"数据清理任务失败: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.get_beijing_time()
            }
    
    def run_stats_task(self) -> Dict[str, Any]:
        """执行统计任务"""
        self.logger.info("开始执行统计任务")
        
        try:
            stats = db_manager.get_stats()
            
            result = {
                'success': True,
                'stats': stats,
                'timestamp': self.get_beijing_time()
            }
            
            self.logger.info(f"统计任务完成: 节点 {stats.get('nodes_count', 0)}, "
                           f"用户 {stats.get('users_count', 0)}, "
                           f"主题 {stats.get('topics_count', 0)}")
            return result
            
        except Exception as e:
            self.logger.error(f"统计任务失败: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.get_beijing_time()
            }
    
    def run_full_maintenance(self) -> Dict[str, Any]:
        """执行完整维护任务"""
        self.logger.info("开始执行完整维护任务")
        
        results = {}
        
        # 执行爬取任务
        crawl_result = self.run_crawl_task()
        results['crawl'] = crawl_result
        
        # 执行清理任务
        cleanup_result = self.run_cleanup_task()
        results['cleanup'] = cleanup_result
        
        # 执行统计任务
        stats_result = self.run_stats_task()
        results['stats'] = stats_result
        
        # 判断整体是否成功
        overall_success = all(result.get('success', False) for result in results.values())
        
        result = {
            'success': overall_success,
            'results': results,
            'timestamp': self.get_beijing_time()
        }
        
        if overall_success:
            self.logger.info("完整维护任务全部成功")
        else:
            failed_tasks = [task for task, result in results.items() if not result.get('success', False)]
            self.logger.warning(f"完整维护任务部分失败: {failed_tasks}")
        
        return result


# 全局调度器实例
scheduler = Scheduler()