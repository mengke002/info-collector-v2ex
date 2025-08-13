"""
V2EX网页解析模块
通过解析HTML页面获取更多主题数据
"""
import requests
import logging
import time
import random
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
from datetime import datetime

from .config import config


class V2EXWebParser:
    """V2EX网页解析器"""
    
    def __init__(self):
        self.crawler_config = config.get_crawler_config()
        self.logger = logging.getLogger(__name__)
        self.ua = UserAgent()
        
        # 请求会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        })
    
    def _get_random_headers(self) -> Dict[str, str]:
        """获取随机请求头"""
        return {
            'User-Agent': self.ua.random,
            'Referer': 'https://www.v2ex.com/',
        }
    
    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """发起HTTP请求并返回BeautifulSoup对象"""
        max_retries = self.crawler_config['max_retries']
        timeout = self.crawler_config['timeout_seconds']
        
        for attempt in range(max_retries + 1):
            try:
                headers = self._get_random_headers()
                self.logger.debug(f"请求URL: {url} (尝试 {attempt + 1}/{max_retries + 1})")
                
                response = self.session.get(url, headers=headers, timeout=timeout)
                
                if response.status_code == 200:
                    response.encoding = 'utf-8'
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return soup
                elif response.status_code == 429:
                    # 被限流，等待更长时间
                    wait_time = (2 ** attempt) * 3 + random.uniform(2, 5)
                    self.logger.warning(f"被限流，等待 {wait_time:.2f} 秒后重试")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.warning(f"请求失败 (尝试 {attempt + 1}): {url} - 状态码: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"请求异常 (尝试 {attempt + 1}): {url} - {e}")
            
            if attempt < max_retries:
                retry_delay = (2 ** attempt) + random.uniform(1, 3)
                self.logger.info(f"将在 {retry_delay:.2f} 秒后重试...")
                time.sleep(retry_delay)
        
        self.logger.error(f"请求最终失败: {url}")
        return None
    
    def _delay_between_requests(self):
        """请求间随机延迟"""
        delay = self.crawler_config['delay_seconds']
        actual_delay = delay + random.uniform(1, 3)  # 网页解析需要更长延迟
        time.sleep(actual_delay)
    
    def _extract_topic_id_from_url(self, url: str) -> Optional[int]:
        """从URL中提取主题ID"""
        if not url:
            return None
        
        # V2EX主题URL格式: /t/123456 或 /t/123456#reply1
        match = re.search(r'/t/(\d+)', url)
        if match:
            return int(match.group(1))
        return None
    
    def _parse_relative_time(self, time_text: str) -> Optional[int]:
        """解析相对时间为时间戳"""
        if not time_text:
            return None
        
        try:
            # V2EX时间格式示例: "3 小时 21 分钟前", "1 天前", "2023-12-25 14:30:15"
            now = int(datetime.now().timestamp())
            
            if '分钟前' in time_text:
                minutes = re.search(r'(\d+)\s*分钟前', time_text)
                if minutes:
                    return now - int(minutes.group(1)) * 60
            elif '小时前' in time_text:
                hours = re.search(r'(\d+)\s*小时前', time_text)
                if hours:
                    return now - int(hours.group(1)) * 3600
            elif '天前' in time_text:
                days = re.search(r'(\d+)\s*天前', time_text)
                if days:
                    return now - int(days.group(1)) * 86400
            elif re.match(r'\d{4}-\d{2}-\d{2}', time_text):
                # 绝对时间格式
                dt = datetime.strptime(time_text[:19], '%Y-%m-%d %H:%M:%S')
                return int(dt.timestamp())
            
            return now
        except Exception as e:
            self.logger.warning(f"解析时间失败: {time_text} - {e}")
            return int(datetime.now().timestamp())
    
    def parse_node_page(self, node_name: str, page: int = 1) -> List[Dict[str, Any]]:
        """解析节点页面获取主题列表"""
        url = f"https://www.v2ex.com/go/{node_name}"
        if page > 1:
            url += f"?p={page}"
        
        self.logger.info(f"解析节点页面: {node_name} (第{page}页)")
        
        soup = self._make_request(url)
        if not soup:
            return []
        
        topics = []
        
        # 更健壮的主题容器查找逻辑
        topic_box = None
        wrapper = soup.select_one('div#Wrapper')
        if not wrapper:
            self.logger.warning("未找到页面主容器 (div#Wrapper)")
            return []
        
        boxes = wrapper.select('div.box')
        self.logger.debug(f"找到 {len(boxes)} 个div.box容器")
        
        # 查找包含主题的box（通常是第2个，但要验证）
        for i, box in enumerate(boxes):
            box_classes = box.get('class', [])
            topic_links = box.select('a.topic-link')
            
            self.logger.debug(f"Box {i+1}: classes={box_classes}, {len(topic_links)} topic-links")
            
            # 跳过节点头部box
            if 'node-header' in box_classes:
                continue
                
            # 找到包含主题链接的box
            if len(topic_links) > 0:
                topic_box = box
                self.logger.debug(f"选择Box {i+1}作为主题容器")
                break
        
        if not topic_box:
            self.logger.info("未找到包含主题的容器")
            return []

        topic_cells = topic_box.select('div.cell')
        self.logger.debug(f"找到 {len(topic_cells)} 个.cell元素")
        
        valid_topic_cells = []
        for cell in topic_cells:
            if cell.select_one('a.topic-link'):
                valid_topic_cells.append(cell)
        
        self.logger.info(f"找到 {len(valid_topic_cells)} 个有效主题容器")
        
        for cell in valid_topic_cells:
            try:
                topic_data = self._parse_topic_cell(cell, node_name)
                if topic_data:
                    topics.append(topic_data)
                else:
                    self.logger.debug("主题解析返回None，跳过")
            except Exception as e:
                self.logger.warning(f"解析主题元素失败: {e}")
                continue
        
        self.logger.info(f"成功解析节点 '{node_name}' 第{page}页的 {len(topics)} 个主题")
        return topics
    
    def _parse_topic_cell(self, cell, node_name: str) -> Optional[Dict[str, Any]]:
        """解析单个主题元素 (适配移动版布局)"""
        try:
            title_link = cell.select_one('a.topic-link')
            if not title_link:
                self.logger.debug("未找到a.topic-link元素")
                return None
            
            # 提取基本信息
            title = title_link.get_text(strip=True)
            topic_url = title_link.get('href', '')
            topic_id = self._extract_topic_id_from_url(topic_url)
            
            if not topic_id:
                self.logger.debug(f"无法从URL提取主题ID: {topic_url}")
                return None
            
            if not title:
                self.logger.debug(f"主题标题为空: ID={topic_id}")
                return None
            
            # 查找作者信息 - V2EX作者链接格式是 /member/用户名
            author_username = None
            author_link = cell.select_one('a[href^="/member/"]')
            if author_link:
                href = author_link.get('href', '')
                # 从 /member/username 中提取用户名
                if '/member/' in href:
                    author_username = href.split('/member/')[-1]
            
            if not author_username:
                self.logger.debug(f"未找到作者信息: ID={topic_id}")
            
            # 查找回复数 - 根据分析结果，在 .count_livid 中
            reply_count = 0
            reply_element = cell.select_one('a.count_livid')
            if reply_element:
                reply_text = reply_element.get_text(strip=True)
                if reply_text.isdigit():
                    reply_count = int(reply_text)

            # Mobile view does not have a reliable timestamp. Default to now.
            created_timestamp = int(datetime.now().timestamp())
            topic_data = {
                'id': topic_id,
                'title': title,
                'url': f"https://www.v2ex.com{topic_url}" if topic_url.startswith('/') else topic_url,
                'node': {'name': node_name, 'title': node_name},
                'node_id': None,
                'node_name': node_name,
                'member': {'username': author_username} if author_username else None,
                'member_id': None,
                'member_username': author_username,
                'replies': reply_count,
                'last_reply_by': None, # Not available in mobile view
                'created_timestamp': created_timestamp,
                'last_touched_timestamp': created_timestamp,
                'last_modified_timestamp': None,
                'is_deleted': 0,
                'content': '',
                # 保持向后兼容的字段
                'created': created_timestamp,
                'last_touched': created_timestamp,
                'last_modified': None,
                'deleted': 0
            }

            self.logger.debug(f"成功解析主题: ID={topic_id}, 标题={title[:30]}...")
            return topic_data

        except Exception as e:
            self.logger.warning(f"解析主题元素失败: {e}", exc_info=True)
            return None
    

    
    def crawl_node_with_pagination(self, node_name: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """爬取节点的多页数据（直接按页码顺序爬取）"""
        self.logger.info(f"开始爬取节点 '{node_name}' 的多页数据（最多{max_pages}页）")
        
        all_topics = []
        
        for page in range(1, max_pages + 1):
            try:
                topics = self.parse_node_page(node_name, page)
                
                # 如果这一页没有主题，说明已经到了最后一页
                if not topics:
                    self.logger.info(f"节点 '{node_name}' 第{page}页无内容，停止爬取")
                    break
                
                all_topics.extend(topics)
                self.logger.info(f"节点 '{node_name}' 第{page}页获取 {len(topics)} 个主题")
                
                # 页面间延迟
                if page < max_pages:
                    self._delay_between_requests()
                    
            except Exception as e:
                self.logger.error(f"爬取节点 '{node_name}' 第{page}页失败: {e}")
                # 如果连续失败，可能是网络问题或已经到了最后一页
                if page > 1:  # 第一页失败才是真的失败
                    break
                continue
        
        self.logger.info(f"节点 '{node_name}' 总共获取 {len(all_topics)} 个主题")
        return all_topics


# 全局网页解析器实例
web_parser = V2EXWebParser()
