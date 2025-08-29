"""
V2EX智能分析报告生成器
基于节点的热点内容分析和Markdown报告生成
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

from .database import db_manager
from .llm_client import llm_client


class V2EXReportGenerator:
    """V2EX智能分析报告生成器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        self.llm = llm_client
        
        # 报告配置
        self.top_topics_per_node = 30
        self.top_replies_per_topic = 10
        self.max_content_length = 50000
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)
    
    def _truncate_content_for_logging(self, content: str, max_length: int = 300) -> str:
        """
        为日志记录截取内容，避免暴露过多敏感信息
        
        Args:
            content: 要截取的内容
            max_length: 最大长度
            
        Returns:
            截取后的内容
        """
        if len(content) <= max_length:
            return content
        
        truncated = content[:max_length]
        # 找到最后一个完整的行或句子
        last_newline = truncated.rfind('\n')
        if last_newline > max_length // 2:  # 如果找到了合理位置的换行符
            truncated = truncated[:last_newline]
        
        return f"{truncated}... [内容被截断，总长度: {len(content)} 字符]"
    
    def _get_hotspot_prompt_template(self) -> str:
        """获取热点分析的“超级提示词”模板"""
        return """你是一位为顶级技术公司服务的资深行业分析师。你的任务是分析以下来自V2EX社区的、已编号的原始讨论材料，并为技术决策者撰写一份循序渐进、可追溯来源的情报简报。

**原始讨论材料:**
{content}

---

**你的分析任务:**
请严格按照以下两个阶段进行分析和内容生成。**至关重要的一点是：你的每一条分析、洞察和建议都必须在结尾处使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注其信息来源。**

**第一阶段：热门主题速览 (Top Topics Summary)**
首先，请通读所有材料，对每个热门主题进行简明扼要的总结。

**第二阶段：深度情报洞察 (In-depth Intelligence Report)**
在完成速览后，请转换视角，基于第一阶段你总结的所有信息，进行更高层级的趋势分析和洞察提炼。

---

**请严格遵照以下Markdown格式输出完整报告:**

# 📈 V2EX社区热点与情报洞察报告

## 🔥 本时段热门主题速览

[在此处罗列最重要的5-10个热门主题的速览]

### **1. [主题A的标题]**
*   **核心内容**: [对该主题的核心内容、讨论焦点和主要结论进行3-5句话的摘要。] [Source: T_n]

### **2. [主题B的标题]**
*   **核心内容**: [对该主题的核心内容、讨论焦点和主要结论进行3-5句话的摘要。] [Source: T_m]

...(以此类推)

---

## 💡 核心洞察 (Executive Summary)

*   **[洞察一]**: [用一句话高度概括你发现的最重要的趋势或洞察。例如：对云服务成本和替代方案的讨论激增，反映出开发者对成本控制的普遍焦虑。] [Sources: T1, T5, T8]
*   **[洞察二]**: [第二个重要洞察。例如：AI Agent的实现和应用成为新的技术焦点，多个热门项目围绕此展开。] [Sources: T2, T9]
*   **[洞察三]**: [第三个重要洞察。] [Source: T4]

## 🔍 趋势与信号分析 (Trends & Signals Analysis)

### 🚀 新兴技术与工具风向
*   **[技术/工具A]**: [描述它是什么，为什么它现在很热门，以及在讨论中是如何体现的。] [Source: T3]
*   **[技术/工具B]**: [同上。] [Source: T7]

### 🔗 讨论热点的内在关联
*   **[关联性分析]**: [详细阐述你发现的不同热点之间的联系。例如：对“XX云服务高昂费用”的抱怨（主题A）与“YY开源替代方案”的出现（主题B）形成了呼应，共同指向了开发者对基础设施成本优化的探索。] [Sources: T1, T6]

### ⚠️ 普遍痛点与潜在需求
*   **[痛点一]**: [描述社区开发者普遍遇到的一个问题或挑战。] [Source: T5]
*   **[痛点二]**: [同上。] [Source: T10]

##  actionable 建议 (Actionable Recommendations)

*   **对于开发者**: [基于以上分析，给个人开发者提出1-2条具体建议。例如：建议关注XX技术，尝试将YY工具集成到当前工作流中以提高效率。] [Sources: T3, T7]
*   **对于技术团队**: [给技术团队或决策者提出1-2条建议。例如：建议评估引入XX解决方案的可行性，以解决团队在YY方面遇到的普遍问题。] [Source: T5]
"""    

    def _format_topics_for_analysis(self, hot_topics_data: List[Dict[str, Any]]) -> str:
        """将所有热门主题合并为一个文档用于LLM统一分析"""
        content_parts = [
            f"""=== V2EX热门主题综合分析文档 ===""",
            f"总计 {len(hot_topics_data)} 个热门主题",
            "",
        ]
        
        for i, topic_data in enumerate(hot_topics_data, 1):
            topic = topic_data['topic']
            replies = topic_data['replies']
            
            content_parts.extend([
                f"\n### [Source: T{i}] {topic['title']}",
                f"- 节点: {topic['node_name']}",
                f"- 作者: {topic['member_username']}",
                f"- 回复数: {topic['replies']}",
                f"- 感谢数: {topic['total_thanks_count']}",
                f"- 热度: {topic['hotness_score']:.2f}",
                f"- URL: {topic['url']}",
                ""
            ])
            
            if topic.get('content'):
                content = topic['content'].strip()
                if len(content) > 800: content = content[:800] + "..."
                content_parts.extend(["**主贴内容:**", content, ""])
            
            if replies:
                content_parts.append("**热门回复:**")
                for j, reply in enumerate(replies[:5], 1): # Limit to 5 replies
                    if reply.get('content'):
                        reply_content = reply['content'].strip()
                        if len(reply_content) > 200: reply_content = reply_content[:200] + "..."
                        thanks_info = f"(感谢: {reply['thanks_count']})" if reply['thanks_count'] > 0 else ""
                        content_parts.append(f"{j}. {reply['member_username']} {thanks_info}: {reply_content}")
                content_parts.append("")

            content_parts.append("---\n")
        
        full_content = "\n".join(content_parts)
        self.logger.info(f"格式化后的主题内容总长度: {len(full_content)} 字符")
        return self._truncate_unified_content(full_content) # Still need truncation
    
    def _truncate_unified_content(self, content: str) -> str:
        """截断统一分析内容到合适长度"""
        if len(content) <= self.max_content_length:
            return content
        
        # 智能截断：尝试在主题分隔符处截断
        truncated = content[:self.max_content_length]
        
        # 尝试在主题分隔符"---"处截断
        last_separator = truncated.rfind('---')
        if last_separator > self.max_content_length * 0.7:
            truncated = truncated[:last_separator]
        else:
            # 尝试在段落分隔符处截断
            for delimiter in ['\n\n', '\n', '。', '.']:
                last_pos = truncated.rfind(delimiter)
                if last_pos > self.max_content_length * 0.8:
                    truncated = truncated[:last_pos + len(delimiter)]
                    break
        
        self.logger.info(f"统一分析内容被截断: {len(content)} -> {len(truncated)} 字符")
        return truncated + "\n\n...[内容过长已被截断]"
    
    def generate_node_report(self, node_name: str, hours_back: int = 24, 
                           report_type: str = 'hotspot') -> Dict[str, Any]:
        """
        为指定节点生成分析报告
        """
        try:
            self.logger.info(f"开始为节点 '{node_name}' 生成 {report_type} 报告")
            
            # 时间范围
            end_time = self.get_beijing_time()
            start_time = end_time - timedelta(hours=hours_back)
            
            # 获取热门主题
            hot_topics = self.db.get_hot_topics_by_node(
                node_name=node_name, 
                limit=self.top_topics_per_node,
                period_hours=hours_back
            )
            
            if not hot_topics:
                self.logger.warning(f"节点 '{node_name}' 在过去 {hours_back} 小时内无热门主题")
                return {
                    'success': False,
                    'error': f"节点 '{node_name}' 无热门内容",
                    'node_name': node_name
                }
            
            # 批量获取主题详细信息和回复，避免N+1查询
            self.logger.info("开始批量获取主题详情...")
            import time
            start_time_db = time.time()
            topic_ids = [topic['id'] for topic in hot_topics]
            hot_topics_data = self.db.get_topics_with_replies_batch(
                topic_ids,
                reply_limit=self.top_replies_per_topic
            )
            db_duration = time.time() - start_time_db
            self.logger.info(f"批量获取主题详情完成，耗时 {db_duration:.2f} 秒")

            if not hot_topics_data:
                return {
                    'success': False,
                    'error': f"无法获取节点 '{node_name}' 的主题详情",
                    'node_name': node_name
                }
            
            # 直接调用统一报告生成方法
            return self._generate_unified_report(
                node_name=node_name,
                hot_topics_data=hot_topics_data,
                start_time=start_time,
                end_time=end_time,
                report_type=report_type
            )
            
        except Exception as e:
            error_msg = f"生成节点 '{node_name}' 报告失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'error': error_msg,
                'node_name': node_name
            }
    
    def generate_global_report(self, hours_back: int = 24, 
                             report_type: str = 'hotspot') -> Dict[str, Any]:
        """
        生成全站热点报告
        
        Args:
            hours_back: 回溯时间（小时）
            report_type: 报告类型
            
        Returns:
            报告生成结果
        """
        try:
            self.logger.info(f"开始生成全站 {report_type} 报告")
            
            # 时间范围
            end_time = self.get_beijing_time()
            start_time = end_time - timedelta(hours=hours_back)
            
            # 获取全站热门主题
            hot_topics = self.db.get_hot_topics_by_node(
                node_name=None,  # None表示全站
                limit=self.top_topics_per_node,
                period_hours=hours_back
            )
            
            if not hot_topics:
                return {
                    'success': False,
                    'error': f"全站在过去 {hours_back} 小时内无热门主题"
                }
            
            # 批量获取主题详细信息
            self.logger.info("开始批量获取主题详情...")
            import time
            start_time_db = time.time()
            topic_ids = [topic['id'] for topic in hot_topics]
            hot_topics_data = self.db.get_topics_with_replies_batch(
                topic_ids,
                reply_limit=self.top_replies_per_topic
            )
            db_duration = time.time() - start_time_db
            self.logger.info(f"批量获取主题详情完成，耗时 {db_duration:.2f} 秒")
            
            # 使用与节点报告相同的逻辑
            return self._generate_unified_report(
                node_name="全站",
                hot_topics_data=hot_topics_data,
                start_time=start_time,
                end_time=end_time,
                report_type=report_type
            )
            
        except Exception as e:
            error_msg = f"生成全站报告失败: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg
            }
    
    def _generate_unified_report(self, node_name: str, hot_topics_data: List[Dict[str, Any]],
                               start_time: datetime, end_time: datetime, 
                               report_type: str) -> Dict[str, Any]:
        """生成统一报告的内部方法（已升级）"""
        
        # 使用新的方法格式化所有主题内容
        unified_content = self._format_topics_for_analysis(hot_topics_data)

        # LLM分析
        if not self.llm:
            return {'success': False, 'error': 'LLM客户端未初始化'}

        # 获取报告类型的对应prompt
        if report_type == 'hotspot':
            prompt_template = self._get_hotspot_prompt_template()
        else:
            # 为其他报告类型可以定义不同的prompt，此处使用一个简单的默认值
            self.logger.warning(f"报告类型 '{report_type}' 没有专门的Prompt模板，将使用默认分析。")
            prompt_template = "请总结以下内容的主要看点、争议和结论：\n\n{content}"

        llm_result = self.llm.analyze_content(unified_content, prompt_template)
        
        if not llm_result.get('success'):
            return {
                'success': False,
                'error': f"LLM分析失败: {llm_result.get('error', '未知错误')}"
            }
        
        # 生成报告标题
        if node_name == "全站":
            report_title = f"🌟 V2EX全站热点洞察报告"
        else:
            report_title = f"📈 [{node_name}]节点热点洞察报告"
        
        # 生成Markdown报告
        markdown_report = self._generate_markdown_report(
            node_name=node_name,
            analysis_result=llm_result,
            hot_topics_data=hot_topics_data,
            start_time=start_time,
            end_time=end_time,
            report_title=report_title,
            report_type=report_type
        )
        
        # 保存报告
        report_data = {
            'node_name': node_name,
            'report_type': report_type,
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'topics_analyzed': len(hot_topics_data),
            'report_title': report_title,
            'report_content': markdown_report,
            'generated_at': self.get_beijing_time()
        }
        
        report_id = self.db.insert_report(report_data)
        
        final_result = {
            'success': True,
            'report_id': report_id,
            'node_name': node_name,
            'report_type': report_type,
            'topics_analyzed': len(hot_topics_data),
            'report_title': report_title,
            'report_content': markdown_report,
            'report_content_preview': self._truncate_content_for_logging(markdown_report, 300),
            'analysis_provider': llm_result.get('provider'),
            'analysis_model': llm_result.get('model'),
            'generated_at': report_data['generated_at']
        }

        if llm_result.get('partial'):
            final_result['success'] = False
            final_result['error'] = "部分结果：LLM连接中断"
        
        return final_result
    
    def _generate_markdown_report(self, node_name: str, analysis_result: Dict[str, Any],
                                hot_topics_data: List[Dict[str, Any]], start_time: datetime,
                                end_time: datetime, report_title: str, 
                                report_type: str) -> str:
        """生成Markdown格式的报告"""
        
        # 时间信息
        start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        generate_time = self.get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建报告内容
        report_lines = [
            f"# {report_title}",
            "",
            f"*报告生成时间: {generate_time}*",
            f"*数据范围: {start_str} - {end_str}*",
            "",
            "---",
            "",
            analysis_result.get('analysis', '分析内容生成失败。'),
            "",
            "---",
            "",
            "## 📚 来源清单 (Source List)",
            ""
        ]
        
        # 生成来源清单
        for i, topic_data in enumerate(hot_topics_data, 1):
            topic_info = topic_data['topic']
            report_lines.append(
                f"- **[T{i}]**: [{topic_info['title']}]({topic_info['url']}) "
                f"({topic_info['node_name']} | {topic_info['replies']}回复 | {topic_info['total_thanks_count']}感谢)"
            )
        
        report_lines.extend(["", "---", ""])
        
        # 技术信息
        if analysis_result.get('provider'):
            report_lines.append(
                f"*分析引擎: {analysis_result['provider']} ({analysis_result.get('model', 'unknown')})*"
            )
        
        report_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(hot_topics_data)} 个热门主题",
            ""
        ])

        if analysis_result.get('partial'):
            report_lines.append("")
            report_lines.append("*注意：由于与分析引擎的连接意外中断，此报告可能不完整。*")

        report_lines.extend([
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        
        return "\n".join(report_lines)


# 全局报告生成器实例
report_generator = V2EXReportGenerator()