"""
V2EX热度分析模块
负责计算主题热度分数和总感谢数
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from .database import db_manager


class V2EXAnalyzer:
    """V2EX热度分析器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        
        # 热度计算权重配置
        self.reply_weight = 5.0      # 回复数权重  
        self.thanks_weight = 3.0     # 感谢数权重
        self.time_decay_hours = 168  # 时间衰减周期：7天
        self.max_hotness_score = 999999.0  # 热度分数最大值限制
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)
    
    def update_total_thanks(self, topic_ids: List[int] = None) -> int:
        """
        更新主题的总感谢数
        
        Args:
            topic_ids: 指定要更新的主题ID列表，None表示更新所有主题
            
        Returns:
            更新的主题数量
        """
        try:
            updated_count = self.db.update_total_thanks(topic_ids)
            self.logger.info(f"成功更新 {updated_count} 个主题的总感谢数")
            return updated_count
        except Exception as e:
            self.logger.error(f"更新总感谢数失败: {e}")
            raise
    
    def update_hotness_scores(self, topic_ids: List[int] = None,
                            reply_weight: Optional[float] = None,
                            thanks_weight: Optional[float] = None,
                            time_decay_hours: Optional[int] = None) -> int:
        """
        更新主题的热度分数
        
        V2EX热度计算公式：
        hotness_score = (回复数 * reply_weight + 总感谢数 * thanks_weight) * 时间衰减因子
        
        时间衰减因子：基于最后活跃时间，越新的主题衰减越少
        
        Args:
            topic_ids: 指定要更新的主题ID列表，None表示更新所有主题
            reply_weight: 回复数权重，None使用默认值
            thanks_weight: 感谢数权重，None使用默认值
            time_decay_hours: 时间衰减周期（小时），None使用默认值
            
        Returns:
            更新的主题数量
        """
        try:
            # 使用提供的权重或默认权重
            rw = reply_weight if reply_weight is not None else self.reply_weight
            tw = thanks_weight if thanks_weight is not None else self.thanks_weight
            tdh = time_decay_hours if time_decay_hours is not None else self.time_decay_hours
            
            updated_count = self.db.update_hotness_scores(
                topic_ids=topic_ids,
                reply_weight=rw,
                thanks_weight=tw,
                time_decay_hours=tdh,
                max_score=self.max_hotness_score
            )
            
            self.logger.info(f"成功更新 {updated_count} 个主题的热度分数（最大值限制: {self.max_hotness_score}）")
            return updated_count
        except Exception as e:
            self.logger.error(f"更新热度分数失败: {e}")
            raise
    
    def analyze_recent_topics(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        分析最近活跃的主题
        
        Args:
            hours_back: 回溯的小时数
            
        Returns:
            分析结果字典
        """
        import time
        start_time = time.time()
        
        try:
            self.logger.info(f"开始分析最近 {hours_back} 小时的活跃主题")
            
            # 获取最近活跃的主题
            self.logger.info(f"查询最近 {hours_back} 小时的活跃主题...")
            query_start_time = time.time()
            
            # 使用限制数量的查询，避免数据量过大
            recent_topics = self.db.get_recent_active_topics(hours_back, limit=2000)  # 限制最多2000个主题
            
            query_time = time.time() - query_start_time
            self.logger.info(f"查询完成，耗时 {query_time:.2f} 秒，找到 {len(recent_topics) if recent_topics else 0} 个最近活跃的主题")
            
            if not recent_topics:
                self.logger.warning(f"未找到最近 {hours_back} 小时的活跃主题")
                return {
                    'success': True,
                    'analyzed_topics': 0,
                    'updated_thanks': 0,
                    'updated_scores': 0,
                    'query_time': query_time,
                    'total_time': time.time() - start_time
                }
            
            # 提取主题ID
            topic_ids = [topic['id'] for topic in recent_topics]
            self.logger.info(f"提取到 {len(topic_ids)} 个主题ID进行分析")
            
            # 更新总感谢数
            self.logger.info("开始更新总感谢数...")
            thanks_start_time = time.time()
            updated_thanks = self.update_total_thanks(topic_ids)
            thanks_time = time.time() - thanks_start_time
            self.logger.info(f"更新感谢数完成，影响 {updated_thanks} 个主题，耗时 {thanks_time:.2f} 秒")
            
            # 更新热度分数
            self.logger.info("开始更新热度分数...")
            scores_start_time = time.time()
            updated_scores = self.update_hotness_scores(topic_ids)
            scores_time = time.time() - scores_start_time
            self.logger.info(f"更新热度分数完成，影响 {updated_scores} 个主题，耗时 {scores_time:.2f} 秒")
            
            total_time = time.time() - start_time
            
            result = {
                'success': True,
                'analyzed_topics': len(recent_topics),
                'updated_thanks': updated_thanks,
                'updated_scores': updated_scores,
                'analysis_time': self.get_beijing_time(),
                'query_time': query_time,
                'thanks_update_time': thanks_time,
                'scores_update_time': scores_time,
                'total_time': total_time
            }
            
            self.logger.info(f"分析完成：{len(recent_topics)} 个主题，更新感谢数 {updated_thanks}，更新热度分数 {updated_scores}，总耗时 {total_time:.2f} 秒")
            return result
            
        except Exception as e:
            total_time = time.time() - start_time
            self.logger.error(f"分析最近主题失败: {e}，总耗时 {total_time:.2f} 秒")
            return {
                'success': False,
                'error': str(e),
                'analyzed_topics': 0,
                'updated_thanks': 0,
                'updated_scores': 0,
                'total_time': total_time
            }
    
    def analyze_all_topics(self) -> Dict[str, Any]:
        """
        分析所有主题的热度
        
        Returns:
            分析结果字典
        """
        try:
            self.logger.info("开始分析所有主题的热度")
            
            # 更新所有主题的总感谢数
            updated_thanks = self.update_total_thanks()
            
            # 更新所有主题的热度分数
            updated_scores = self.update_hotness_scores()
            
            result = {
                'success': True,
                'updated_thanks': updated_thanks,
                'updated_scores': updated_scores,
                'analysis_time': self.get_beijing_time()
            }
            
            self.logger.info(f"全量分析完成：更新感谢数 {updated_thanks}，更新热度分数 {updated_scores}")
            return result
            
        except Exception as e:
            self.logger.error(f"分析所有主题失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'updated_thanks': 0,
                'updated_scores': 0
            }
    
    def get_hotness_stats(self) -> Dict[str, Any]:
        """
        获取热度统计信息
        
        Returns:
            统计信息字典
        """
        try:
            with self.db.get_cursor() as (cursor, connection):
                # 获取基本统计
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_topics,
                        AVG(hotness_score) as avg_hotness,
                        MAX(hotness_score) as max_hotness,
                        MIN(hotness_score) as min_hotness,
                        AVG(total_thanks_count) as avg_thanks,
                        MAX(total_thanks_count) as max_thanks
                    FROM v2ex_topics 
                    WHERE hotness_score > 0
                """)
                
                stats = cursor.fetchone()
                
                if not stats:
                    return {
                        'success': False,
                        'error': '无法获取统计信息'
                    }
                
                # 获取节点统计
                cursor.execute("""
                    SELECT 
                        node_name,
                        COUNT(*) as topic_count,
                        AVG(hotness_score) as avg_hotness,
                        MAX(hotness_score) as max_hotness
                    FROM v2ex_topics 
                    WHERE hotness_score > 0 AND node_name IS NOT NULL
                    GROUP BY node_name
                    ORDER BY avg_hotness DESC
                    LIMIT 10
                """)
                
                node_stats = cursor.fetchall()
                
                return {
                    'success': True,
                    'total_topics': stats['total_topics'],
                    'avg_hotness': float(stats['avg_hotness']) if stats['avg_hotness'] else 0.0,
                    'max_hotness': float(stats['max_hotness']) if stats['max_hotness'] else 0.0,
                    'min_hotness': float(stats['min_hotness']) if stats['min_hotness'] else 0.0,
                    'avg_thanks': float(stats['avg_thanks']) if stats['avg_thanks'] else 0.0,
                    'max_thanks': stats['max_thanks'] if stats['max_thanks'] else 0,
                    'top_nodes': node_stats,
                    'analysis_time': self.get_beijing_time()
                }
                
        except Exception as e:
            self.logger.error(f"获取热度统计失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def analyze_node_topics(self, node_name: str, hours_back: int = 24) -> Dict[str, Any]:
        """
        分析指定节点的主题热度
        
        Args:
            node_name: 节点名称
            hours_back: 回溯的小时数
            
        Returns:
            分析结果字典
        """
        try:
            self.logger.info(f"开始分析节点 '{node_name}' 最近 {hours_back} 小时的主题")
            
            # 获取指定节点的热门主题
            hot_topics = self.db.get_hot_topics_by_node(node_name, limit=50, period_hours=hours_back)
            
            if not hot_topics:
                self.logger.warning(f"节点 '{node_name}' 最近 {hours_back} 小时无热门主题")
                return {
                    'success': True,
                    'node_name': node_name,
                    'hot_topics_count': 0,
                    'avg_hotness': 0.0,
                    'max_hotness': 0.0
                }
            
            # 提取主题ID进行分析
            topic_ids = [topic['id'] for topic in hot_topics]
            
            # 更新这些主题的感谢数和热度分数
            updated_thanks = self.update_total_thanks(topic_ids)
            updated_scores = self.update_hotness_scores(topic_ids)
            
            # 重新获取更新后的热门主题
            updated_hot_topics = self.db.get_hot_topics_by_node(node_name, limit=50, period_hours=hours_back)
            
            # 计算统计信息
            hotness_scores = [topic['hotness_score'] for topic in updated_hot_topics if topic['hotness_score'] > 0]
            avg_hotness = sum(hotness_scores) / len(hotness_scores) if hotness_scores else 0.0
            max_hotness = max(hotness_scores) if hotness_scores else 0.0
            
            result = {
                'success': True,
                'node_name': node_name,
                'hot_topics_count': len(updated_hot_topics),
                'updated_thanks': updated_thanks,
                'updated_scores': updated_scores,
                'avg_hotness': avg_hotness,
                'max_hotness': max_hotness,
                'analysis_time': self.get_beijing_time()
            }
            
            self.logger.info(f"节点 '{node_name}' 分析完成：{len(updated_hot_topics)} 个热门主题，平均热度 {avg_hotness:.2f}")
            return result
            
        except Exception as e:
            self.logger.error(f"分析节点 '{node_name}' 失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'node_name': node_name
            }


# 全局分析器实例
analyzer = V2EXAnalyzer()