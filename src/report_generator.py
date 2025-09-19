"""
V2EX智能分析报告生成器
基于节点的热点内容分析和Markdown报告生成
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

from .database import db_manager
from .llm_client import llm_client
from .config import config


class V2EXReportGenerator:
    """V2EX智能分析报告生成器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        self.llm = llm_client

        # 报告配置
        report_config = config.get_report_config()
        self.top_topics_per_node = report_config.get('top_topics_per_node', 50)
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
        """获取热点分析的“超级提示词”模板（情境感知强化版）"""
        return """你是一位顶级的多领域分析师，擅长从不同社群的讨论中挖掘出针对特定人群的深刻洞察。你的任务是分析以下来自V2EX社区特定节点（板块）的原始讨论材料，并撰写一份结构清晰、由浅入深的洞察报告。

**分析原则:**
1.  **情境感知 (Context-Aware)**: 首先，注意这些主题主要来自哪些节点（例如 '酷工作', '职场话题', '分享发现'）。节点的属性决定了分析的视角和价值取向。
2.  **忠于原文**: 所有的分析和洞察都必须基于下文提供的原始材料。
3.  **可追溯性**: 你的每一条结论、洞察和建议，都必须在句末使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注信息来源。这是硬性要求。
4.  **由浅入深**: 报告结构必须从表层信息总结，逐步过渡到深层趋势和战略建议。

---
**原始讨论材料 (已编号，包含节点信息):**
{content}
---

**你的报告生成任务:**
请严格按照以下四个层次的分析框架，生成一份完整的Markdown报告。

**第一层次：热门主题概览 (Top Topics Overview)**
*   任务：通读所有材料，为每个热门主题撰写一个简明扼要的摘要。
*   要求：清晰总结每个主题的核心议题、主要讨论方向和最终的普遍共识或结论。

**第二层次：核心洞察提炼 (Key Insights Extraction)**
*   任务：基于第一层次的总结，并结合你对节点属性的理解，从全局视角提炼出最关键的、超越单个主题的洞察。
*   要求：洞察应反映该节点用户的核心关切。例如，如果是'酷工作'节点，洞察可能关于招聘趋势；如果是'职场话题'，可能关于行业焦虑。至少提炼5个独立的、有价值的洞察点。

**第三层次：趋势与信号分析 (Trends & Signals Analysis)**
*   任务：在所有讨论中寻找重复出现的模式、新兴的概念和普遍存在的问题。分析的侧重点应与节点属性保持一致。
*   要求：
    *   **热议趋势/新风向**: 识别并列出被热议的新鲜事物、观点或趋势。例如，技术节点中的“新工具”，生活节点中的“新消费方式”，职场节点中的“新求职策略”。
    *   **普遍痛点/共同需求**: 总结该社群共同面临的挑战或未被满足的需求。
    *   **讨论的内在关联**: 分析不同热点话题之间是否存在因果、呼应或矛盾的关系。

**第四层次： actionable 建议 (Actionable Recommendations)**
*   任务：基于以上所有分析，为该节点的核心用户群体提供具体、可行的建议。
*   要求：建议必须有高度的针对性。例如，不要总是给“开发者”提建议，如果节点是'Apple'，就给Apple用户提建议；如果节点是'创业'，就给创业者提建议。

---

**请严格遵照以下Markdown格式输出你的完整报告:**

# 📈 V2EX社区热点与情报洞察报告

## 一、热门主题概览 (Top Topics Overview)

### 1. [主题A的标题]
*   **核心内容**: [对该主题的核心议题、讨论焦点和主要结论进行摘要。] [Source: T_n]

### 2. [主题B的标题]
*   **核心内容**: [同上。] [Source: T_m]

...(罗列最重要的5-10个主题)

---

## 二、核心洞察 (Executive Summary)

*   **洞察一**: [用一句话高度概括你发现的最重要的趋势或洞察。例如：'酷工作'节点的招聘需求显示，远程工作岗位在减少，对复合型人才的需求在增加。] [Sources: T1, T5]
*   **洞察二**: [例如：'分享发现'节点中，关于“平替”消费的讨论激增，反映出用户在消费决策上更趋于理性。] [Sources: T2, T9]
*   **洞察三**: [第三个重要洞察。] [Source: T4]
*   ...(至少5条)

---

## 三、趋势与信号深度分析 (In-depth Analysis of Trends & Signals)

### 🚀 热议趋势与新风向
*   **[趋势/风向A]**: [它是什么，为什么它现在很热门，以及在讨论中是如何体现的？] [Source: T3]
*   **[趋势/风向B]**: [同上。] [Source: T7]
*   ...(至少3条)

### ⚠️ 普遍痛点与共同需求
*   **[痛点/需求A]**: [描述该节点用户普遍遇到的一个问题或挑战，并分析其背后的原因。] [Source: T5]
*   **[痛点/需求B]**: [同上。] [Source: T10]
*   ...(至少3条)

### 🔗 热点间的内在关联
*   **[关联性分析]**: [详细阐述你发现的不同热点之间的联系。例如：'职场话题'中对“35岁危机”的焦虑（主题A）与'酷工作'中“招聘要求年龄上限”的讨论（主题B）形成了相互印证。] [Sources: T1, T6]

---

## 四、 actionable 建议 (Actionable Recommendations)

### [针对该节点核心用户的建议，例如：给求职者的建议 / 给Apple产品用户的建议 / 给创业者的建议]
*   [基于以上分析，提出2-3条具体、可操作的建议。例如，对于求职者：“鉴于市场对复合型人才的需求增加，建议在深化专业技能的同时，补充项目管理或产品设计知识。” [Source: T1]]
*   [第二条建议。例如，对于创业者：“社区对‘出海’话题的反馈普遍积极，但普遍提到本地化支付是关键难点，建议优先解决支付渠道问题。” [Source: T8]]
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

            # 首先检查该节点在指定时间范围内是否有任何主题
            cutoff_timestamp = int((datetime.now() - timedelta(hours=hours_back)).timestamp())
            with self.db.get_cursor() as (cursor, connection):
                cursor.execute(
                    "SELECT COUNT(*) as total_count FROM v2ex_topics WHERE node_name = %s AND created_timestamp >= %s",
                    (node_name, cutoff_timestamp)
                )
                total_topics = cursor.fetchone()['total_count']

                # 同时检查有多少主题有互动（回复或感谢）
                cursor.execute(
                    "SELECT COUNT(*) as active_count FROM v2ex_topics WHERE node_name = %s AND created_timestamp >= %s AND (replies > 0 OR total_thanks_count > 0)",
                    (node_name, cutoff_timestamp)
                )
                active_topics = cursor.fetchone()['active_count']

                # 检查热度分数大于0的主题数量
                cursor.execute(
                    "SELECT COUNT(*) as hot_count FROM v2ex_topics WHERE node_name = %s AND created_timestamp >= %s AND hotness_score > 0",
                    (node_name, cutoff_timestamp)
                )
                hot_topics_count = cursor.fetchone()['hot_count']

            self.logger.info(f"节点 '{node_name}' 在过去 {hours_back} 小时内统计：总主题数={total_topics}, 有互动主题数={active_topics}, 热门主题数={hot_topics_count}")

            # 获取热门主题（现在使用改进的多级策略）
            hot_topics = self.db.get_hot_topics_by_node(
                node_name=node_name,
                limit=self.top_topics_per_node,
                period_hours=hours_back
            )

            if not hot_topics:
                # 如果仍然没有找到主题，扩大时间范围再试一次
                extended_hours = hours_back * 2  # 扩大到48小时
                self.logger.warning(f"在 {hours_back} 小时内未找到主题，尝试扩大到 {extended_hours} 小时")

                hot_topics = self.db.get_hot_topics_by_node(
                    node_name=node_name,
                    limit=self.top_topics_per_node,
                    period_hours=extended_hours
                )

                if hot_topics:
                    self.logger.info(f"扩大时间范围后找到 {len(hot_topics)} 个主题")
                    # 更新实际使用的时间范围
                    start_time = end_time - timedelta(hours=extended_hours)
                else:
                    return {
                        'success': False,
                        'error': f"节点 '{node_name}' 在过去 {extended_hours} 小时内仍无可分析内容",
                        'node_name': node_name,
                        'debug_info': {
                            'total_topics_24h': total_topics,
                            'active_topics_24h': active_topics,
                            'hot_topics_24h': hot_topics_count
                        }
                    }

            # 批量获取主题详细信息和回复，避免N+1查询
            self.logger.info(f"开始批量获取 {len(hot_topics)} 个主题的详情...")
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
                    'node_name': node_name,
                    'debug_info': {
                        'topics_found': len(hot_topics),
                        'topic_ids': topic_ids[:5]  # 只显示前5个ID用于调试
                    }
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
    
    def generate_global_report(self, hours_back: int = 48, 
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

        # 尝试推送到Notion
        try:
            from .notion_client import v2ex_notion_client

            # 格式化Notion标题
            beijing_time = self.get_beijing_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] {node_name}节点热点报告 ({len(hot_topics_data)}个主题)"

            self.logger.info(f"开始推送报告到Notion: {notion_title}")

            notion_result = v2ex_notion_client.create_report_page(
                report_title=notion_title,
                report_content=markdown_report,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"报告成功推送到Notion: {notion_result.get('page_url')}")
                final_result['notion_push'] = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                self.logger.warning(f"推送到Notion失败: {notion_result.get('error')}")
                final_result['notion_push'] = {
                    'success': False,
                    'error': notion_result.get('error')
                }

        except Exception as e:
            self.logger.warning(f"推送到Notion时出错: {e}")
            final_result['notion_push'] = {
                'success': False,
                'error': str(e)
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
            f"*报告生成时间: {generate_time}*  ",
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
            # 将标题中的方括号替换为中文方括号，避免干扰Markdown链接解析
            clean_title = topic_info['title'].replace('[', '【').replace(']', '】')
            report_lines.append(
                f"- **[T{i}]**: [{clean_title}]({topic_info['url']}) "
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
