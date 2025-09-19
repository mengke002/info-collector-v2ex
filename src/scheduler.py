"""
V2EX数据采集调度器
负责协调各种任务的执行
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from .v2ex_crawler import crawler
from .database import db_manager
from .config import config
from .analyzer import analyzer
from .report_generator import report_generator


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
            # 初始化数据库（确保表结构存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
            
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
            # 初始化数据库（确保表结构存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
            
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
    
    def run_analysis_task(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        执行数据分析任务
        
        Args:
            hours_back: 分析回溯时间（小时）
            
        Returns:
            分析结果
        """
        self.logger.info(f"开始执行数据分析任务，回溯 {hours_back} 小时")
        
        try:
            # 初始化数据库（确保表结构存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
            
            # 分析最近活跃的主题
            analysis_result = analyzer.analyze_recent_topics(hours_back)
            
            if analysis_result['success']:
                result = {
                    'success': True,
                    'analyzed_topics': analysis_result['analyzed_topics'],
                    'updated_thanks': analysis_result['updated_thanks'],
                    'updated_scores': analysis_result['updated_scores'],
                    'timestamp': self.get_beijing_time()
                }
                
                self.logger.info(f"数据分析完成: 分析 {analysis_result['analyzed_topics']} 个主题，"
                               f"更新感谢数 {analysis_result['updated_thanks']}，"
                               f"更新热度分数 {analysis_result['updated_scores']}")
            else:
                result = {
                    'success': False,
                    'error': analysis_result.get('error', '未知错误'),
                    'timestamp': self.get_beijing_time()
                }
                self.logger.error(f"数据分析失败: {result['error']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"数据分析任务失败: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.get_beijing_time()
            }
    
    def run_report_task(self, nodes: str = None, hours_back: int = 24,
                       report_type: str = 'hotspot', include_global: bool = True) -> Dict[str, Any]:
        """
        执行报告生成任务
        
        Args:
            nodes: 节点名称字符串（用逗号分隔），None或空表示只生成全站报告
            hours_back: 报告回溯时间（小时）
            report_type: 报告类型 ('hotspot', 'trend', 'summary')
            include_global: 是否额外生成全站报告

        Returns:
            报告生成结果
        """
        self.logger.info("开始执行报告生成任务")
        try:
            # 初始化数据库
            db_manager.init_database()
            self.logger.info("数据库初始化完成")

            all_reports = []
            node_list = [node.strip() for node in nodes.split(',')] if nodes else []
            
            # 如果指定了节点，则并行处理
            if node_list:
                self.logger.info(f"将为 {len(node_list)} 个指定节点生成报告: {node_list}")
                
                # 从配置获取最大并行数
                max_workers = config.get_llm_config().get('max_parallel_reports', 4)
                self.logger.info(f"使用最大并行数: {max_workers}")

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_node = {
                        executor.submit(
                            report_generator.generate_node_report,
                            node_name=node_name,
                            hours_back=hours_back,
                            report_type=report_type
                        ): node_name
                        for node_name in node_list if node_name
                    }
                    
                    self.logger.info(f"已提交 {len(future_to_node)} 个节点报告任务进行并行处理。")

                    for future in as_completed(future_to_node):
                        node_name = future_to_node[future]
                        try:
                            report_result = future.result()
                            all_reports.append(report_result)
                            if report_result.get('success'):
                                self.logger.info(f"--- 节点 '{node_name}' 报告生成成功 ---")
                            else:
                                self.logger.error(f"--- 节点 '{node_name}' 报告生成失败: {report_result.get('error')} ---")
                        except Exception as exc:
                            self.logger.error(f"--- 节点 '{node_name}' 报告生成时发生意外错误: {exc} ---", exc_info=True)
                            all_reports.append({'success': False, 'error': str(exc), 'node_name': node_name})
                
                if include_global:
                    self.logger.info("--- 开始生成附加的全站报告 ---")
                    global_report_result = report_generator.generate_global_report(
                        hours_back=hours_back,
                        report_type=report_type
                    )
                    all_reports.append(global_report_result)
                    if global_report_result.get('success'):
                        self.logger.info("--- 全站报告生成成功 ---")
                    else:
                        self.logger.error(f"--- 全站报告生成失败: {global_report_result.get('error')} ---")

            else:
                if include_global:
                    # 未指定节点，仅生成全站报告
                    self.logger.info("--- 未指定节点，仅生成全站报告 ---")
                    global_report_result = report_generator.generate_global_report(
                        hours_back=hours_back,
                        report_type=report_type
                    )
                    all_reports.append(global_report_result)
                else:
                    self.logger.info("未指定节点且已跳过全站报告，任务无可执行项")

            # 整理最终结果
            successful_reports = [r for r in all_reports if r.get('success')]
            
            # 区分硬失败和软失败（无内容）
            hard_failed_reports = []
            soft_failed_reports = []
            for r in all_reports:
                if not r.get('success'):
                    error_msg = r.get('error', '')
                    if '无热门内容' in error_msg or '无热门主题' in error_msg or '部分结果' in error_msg:
                        soft_failed_reports.append(r)
                    else:
                        hard_failed_reports.append(r)

            final_result = {
                # 只有在没有硬失败时，任务才被认为是成功的
                'success': len(hard_failed_reports) == 0,
                'total_reports': len(all_reports),
                'successful_reports': len(successful_reports),
                'hard_failed_reports': len(hard_failed_reports),
                'soft_failed_reports': len(soft_failed_reports),
                'reports': all_reports,
                'timestamp': self.get_beijing_time()
            }

            if final_result['success']:
                self.logger.info(f"报告生成任务完成。成功: {len(successful_reports)}, "
                               f"无内容跳过: {len(soft_failed_reports)}。")
            else:
                self.logger.warning(f"报告生成任务部分或全部失败。成功: {len(successful_reports)}, "
                               f"硬失败: {len(hard_failed_reports)}, "
                               f"无内容跳过: {len(soft_failed_reports)}。")

            # 为了兼容旧的单报告输出，如果只有一个报告，则返回该报告的详细信息
            if len(all_reports) == 1:
                return all_reports[0]

            return final_result

        except Exception as e:
            self.logger.error(f"报告生成任务遭遇意外失败: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.get_beijing_time()
            }
    
    def run_full_maintenance(self) -> Dict[str, Any]:
        """执行完整维护任务：爬取 -> 分析 -> 报告 -> 清理 -> 统计"""
        self.logger.info("开始执行完整维护任务")
        
        results = {}
        
        # 1. 执行爬取任务
        crawl_result = self.run_crawl_task()
        results['crawl'] = crawl_result
        
        # 2. 执行数据分析任务（仅在爬取成功时）
        if crawl_result.get('success', False):
            analysis_result = self.run_analysis_task(hours_back=24)
            results['analysis'] = analysis_result
            
            # 3. 生成全站热点报告（仅在分析成功时）
            if analysis_result.get('success', False):
                report_result = self.run_report_task(node_name=None, hours_back=24, report_type='hotspot')
                results['report'] = report_result
            else:
                results['report'] = {
                    'success': False,
                    'error': '跳过报告生成，因为数据分析失败',
                    'timestamp': self.get_beijing_time()
                }
        else:
            results['analysis'] = {
                'success': False,
                'error': '跳过数据分析，因为爬取失败',
                'timestamp': self.get_beijing_time()
            }
            results['report'] = {
                'success': False,
                'error': '跳过报告生成，因为爬取失败',
                'timestamp': self.get_beijing_time()
            }
        
        # 4. 执行清理任务
        cleanup_result = self.run_cleanup_task()
        results['cleanup'] = cleanup_result
        
        # 5. 执行统计任务
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
