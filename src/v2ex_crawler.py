"""
V2EX爬虫模块
统一的爬虫实现，支持串行和并行模式
"""
import asyncio
import aiohttp
import requests
import logging
import time
import random
from typing import List, Dict, Any, Optional
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import re
from datetime import datetime
import html2text
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .config import config
from .database import db_manager
from .web_parser import web_parser


class V2EXCrawler:
    """V2EX爬虫类"""
    
    def __init__(self):
        self.crawler_config = config.get_crawler_config()
        self.target_nodes = config.get_target_nodes()
        self.logger = logging.getLogger(__name__)
        self.ua = UserAgent()
        
        # API基础URL
        self.base_api_url = "https://www.v2ex.com/api"
        
        # 并发控制
        self.max_concurrent_nodes = self.crawler_config.get('max_concurrent_nodes', 1)
        self.max_concurrent_replies = self.crawler_config.get('max_concurrent_replies', 1)
        
        # 请求会话
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        })
        
        # 限流控制
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_delay = 1.0
        
        # HTML到Markdown转换器
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = False
        self.html_converter.body_width = 0  # 不限制行宽
    
    def _get_random_headers(self) -> Dict[str, str]:
        """获取随机请求头"""
        return {
            'User-Agent': self.ua.random,
            'Referer': 'https://www.v2ex.com/',
        }
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """发起HTTP请求，包含重试机制"""
        max_retries = self.crawler_config['max_retries']
        timeout = self.crawler_config['timeout_seconds']
        
        for attempt in range(max_retries + 1):
            try:
                headers = self._get_random_headers()
                self.logger.debug(f"请求URL: {url} (尝试 {attempt + 1}/{max_retries + 1})")
                
                response = self.session.get(
                    url, 
                    params=params,
                    headers=headers,
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # 被限流，等待更长时间
                    wait_time = (2 ** attempt) * 2 + random.uniform(1, 3)
                    self.logger.warning(f"被限流，等待 {wait_time:.2f} 秒后重试")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.warning(f"请求失败 (尝试 {attempt + 1}): {url} - 状态码: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"请求异常 (尝试 {attempt + 1}): {url} - {e}")
            
            if attempt < max_retries:
                retry_delay = (2 ** attempt) + random.uniform(0.5, 1.5)
                self.logger.info(f"将在 {retry_delay:.2f} 秒后重试...")
                time.sleep(retry_delay)
        
        self.logger.error(f"请求最终失败: {url}")
        return None
    
    def _delay_between_requests(self):
        """请求间随机延迟"""
        delay = self.crawler_config['delay_seconds']
        actual_delay = delay + random.uniform(0, delay * 0.5)
        time.sleep(actual_delay)
    
    def get_topic_detail(self, topic_id: int) -> Optional[Dict[str, Any]]:
        """获取主题详情"""
        self.logger.debug(f"获取主题详情: {topic_id}")
        
        url = f"{self.base_api_url}/topics/show.json"
        params = {'id': topic_id}
        
        topics_data = self._make_request(url, params)
        
        if topics_data and len(topics_data) > 0:
            return topics_data[0]
        else:
            self.logger.warning(f"获取主题 {topic_id} 详情失败")
            return None
    
    def get_topic_content_and_replies_from_html(self, topic_id: int) -> Dict[str, Any]:
        """通过HTML页面解析获取主题内容和回复"""
        url = f"https://www.v2ex.com/t/{topic_id}"
        
        try:
            headers = self._get_random_headers()
            response = self.session.get(url, headers=headers, timeout=self.crawler_config['timeout_seconds'])
            
            if response.status_code != 200:
                self.logger.warning(f"获取主题页面失败: {topic_id} - 状态码: {response.status_code}")
                return {'content': '', 'replies': []}
            
            # 显式设置编码为utf-8，避免乱码问题
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 解析主题内容
            content = ''
            
            # V2EX主题内容通常在 .topic_content 或 .cell 中
            content_div = soup.select_one('.topic_content')
            if not content_div:
                # 备选方案：查找包含主题内容的cell
                content_div = soup.select_one('div.cell:not([id])')
            
            if content_div:
                # 转换HTML为Markdown
                content = self._html_to_markdown(str(content_div))
            
            # 解析回复
            replies = []
            reply_cells = soup.select('div.cell[id^="r_"]')
            
            for i, cell in enumerate(reply_cells):
                try:
                    reply_data = self._parse_reply_cell(cell, topic_id, i + 1)
                    if reply_data:
                        replies.append(reply_data)
                except Exception as e:
                    self.logger.warning(f"解析回复失败: {e}")
                    continue
            
            self.logger.debug(f"主题 {topic_id} 解析到内容 {len(content)} 字符, {len(replies)} 个回复")
            
            return {
                'content': content,
                'replies': replies
            }
            
        except Exception as e:
            self.logger.error(f"获取主题 {topic_id} 内容和回复失败: {e}")
            return {'content': '', 'replies': []}
    
    def get_topic_replies_from_html(self, topic_id: int) -> List[Dict[str, Any]]:
        """通过HTML页面解析获取主题回复（保持向后兼容）"""
        result = self.get_topic_content_and_replies_from_html(topic_id)
        return result['replies']
    
    def _html_to_markdown(self, html_content: str) -> str:
        """将HTML内容转换为Markdown格式"""
        try:
            if not html_content or not html_content.strip():
                return ''
            
            # 使用html2text转换
            markdown_content = self.html_converter.handle(html_content)
            
            # 清理多余的空行
            lines = markdown_content.split('\n')
            cleaned_lines = []
            prev_empty = False
            
            for line in lines:
                line = line.strip()
                if line:
                    cleaned_lines.append(line)
                    prev_empty = False
                elif not prev_empty:
                    cleaned_lines.append('')
                    prev_empty = True
            
            # 移除开头和结尾的空行
            while cleaned_lines and not cleaned_lines[0]:
                cleaned_lines.pop(0)
            while cleaned_lines and not cleaned_lines[-1]:
                cleaned_lines.pop()
            
            return '\n'.join(cleaned_lines)
            
        except Exception as e:
            self.logger.warning(f"HTML转Markdown失败: {e}")
            # 如果转换失败，返回纯文本
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup.get_text(strip=True)
    
    def _parse_reply_cell(self, cell, topic_id: int, floor: int) -> Optional[Dict[str, Any]]:
        """解析单个回复元素"""
        try:
            # 提取回复ID
            reply_id_attr = cell.get('id', '')
            reply_id = None
            if reply_id_attr.startswith('r_'):
                try:
                    reply_id = int(reply_id_attr[2:])
                except ValueError:
                    reply_id = topic_id * 1000 + floor  # 生成一个唯一ID
            else:
                reply_id = topic_id * 1000 + floor
            
            # 提取用户信息
            username = None
            user_link = cell.select_one('a[href*="/member/"]')
            if user_link:
                href = user_link.get('href', '')
                if '/member/' in href:
                    username = href.split('/member/')[-1]
            
            # 提取回复内容并转换为Markdown
            content = ""
            content_div = cell.select_one('.reply_content')
            if content_div:
                content = self._html_to_markdown(str(content_div))
            
            # 提取时间信息
            created_timestamp = None
            time_element = cell.select_one('.ago')
            if time_element:
                time_text = time_element.get_text(strip=True)
                created_timestamp = self._parse_relative_time(time_text)
            
            if not created_timestamp:
                created_timestamp = int(datetime.now().timestamp())
            
            # 提取感谢数
            thanks_count = 0
            thanks_element = cell.select_one('.small.fade')
            if thanks_element:
                thanks_text = thanks_element.get_text(strip=True)
                if '♥' in thanks_text:
                    try:
                        thanks_count = int(re.search(r'(\d+)', thanks_text).group(1))
                    except (AttributeError, ValueError):
                        thanks_count = 0
            
            return {
                'id': reply_id,
                'topic_id': topic_id,
                'member_id': None,  # HTML解析无法获取用户ID
                'member_username': username,
                'content': content,
                'reply_floor': floor,
                'created_timestamp': created_timestamp,
                'last_modified_timestamp': created_timestamp,
                'thanks_count': thanks_count,
                # 保持向后兼容的字段
                'created': created_timestamp,
                'last_modified': created_timestamp,
                'thanks': thanks_count
            }
            
        except Exception as e:
            self.logger.warning(f"解析回复元素失败: {e}")
            return None
    
    def _parse_relative_time(self, time_text: str) -> Optional[int]:
        """解析相对时间为时间戳"""
        if not time_text:
            return None
        
        try:
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
                dt = datetime.strptime(time_text[:19], '%Y-%m-%d %H:%M:%S')
                return int(dt.timestamp())
            
            return now
        except Exception as e:
            self.logger.warning(f"解析时间失败: {time_text} - {e}")
            return int(datetime.now().timestamp())
    

    
    def crawl_topics_by_nodes(self) -> Dict[str, Any]:
        """按节点爬取主题（统一框架，支持串行和并发）"""
        concurrent_nodes = self.max_concurrent_nodes
        mode = "并发" if concurrent_nodes > 1 else "串行"
        self.logger.info(f"开始爬取主题（{mode}模式，并发数: {concurrent_nodes}）")
        
        try:
            if concurrent_nodes > 1:
                # 并发模式
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(self._crawl_all_nodes_async())
                finally:
                    loop.close()
            else:
                # 串行模式
                result = self._crawl_all_nodes_sync()
            
            all_topics = result['topics_to_update']
            all_users = result['all_users']
            all_replies = result['all_replies']
            
            self.logger.info(f"{mode}爬取完成: {len(all_topics)} 个主题, {len(all_replies)} 个回复")
            
        except Exception as e:
            self.logger.error(f"爬取失败: {e}")
            return {
                'topics_found': 0,
                'topics_crawled': 0,
                'users_saved': 0,
                'replies_saved': 0,
                'success': False,
                'error': str(e)
            }
        
        # 批量保存数据
        return self._save_crawled_data(all_topics, all_users, all_replies)
    
    def _crawl_all_nodes_sync(self) -> Dict[str, Any]:
        """串行爬取所有节点"""
        all_topics = []
        all_users = []
        all_replies = []
        
        for node_name, node_title in self.target_nodes.items():
            try:
                result = self._crawl_single_node_sync(node_name, node_title)
                all_topics.extend(result['topics_to_update'])
                all_users.extend(result['all_users'])
                all_replies.extend(result['all_replies'])
                
                # 节点间延迟
                self._delay_between_requests()
                
            except Exception as e:
                self.logger.error(f"串行爬取节点 '{node_name}' 失败: {e}")
                continue
        
        return {
            'topics_to_update': all_topics,
            'all_users': all_users,
            'all_replies': all_replies
        }
    
    async def _crawl_all_nodes_async(self) -> Dict[str, Any]:
        """并发爬取所有节点"""
        connector = aiohttp.TCPConnector(
            limit=5,           # 总连接数限制
            limit_per_host=2,  # 每个主机连接数限制
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(
            total=120,      # 总超时时间2分钟
            connect=15,     # 连接超时15秒
            sock_read=60,   # 读取超时60秒
            sock_connect=15 # socket连接超时15秒
        )
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            semaphore = asyncio.Semaphore(self.max_concurrent_nodes)
            
            async def crawl_node_with_semaphore(node_name, node_title):
                async with semaphore:
                    return await self._crawl_single_node_async(session, node_name, node_title)
            
            tasks = [
                crawl_node_with_semaphore(node_name, node_title)
                for node_name, node_title in self.target_nodes.items()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 合并结果
        all_topics = []
        all_users = []
        all_replies = []
        
        for result in results:
            if isinstance(result, dict) and 'error' not in result:
                all_topics.extend(result['topics_to_update'])
                all_users.extend(result['all_users'])
                all_replies.extend(result['all_replies'])
            elif isinstance(result, Exception):
                self.logger.error(f"节点爬取任务失败: {result}")
        
        return {
            'topics_to_update': all_topics,
            'all_users': all_users,
            'all_replies': all_replies
        }
    
    def _crawl_single_node_sync(self, node_name: str, node_title: str) -> Dict[str, Any]:
        """串行爬取单个节点"""
        self.logger.info(f"开始串行爬取节点: {node_name} ({node_title})")
        
        try:
            # 1. 网页解析获取主题列表
            max_pages_per_node = self.crawler_config.get('max_pages_per_node', 5)
            node_topics = web_parser.crawl_node_with_pagination(node_name, max_pages_per_node)
            
            if not node_topics:
                return {
                    'topics_to_update': [],
                    'all_users': [],
                    'all_replies': []
                }
            
            # 2. 筛选需要更新的主题
            topics_to_update = self._filter_topics_to_update(node_topics)
            self.logger.info(f"节点 '{node_name}' 需要更新 {len(topics_to_update)}/{len(node_topics)} 个主题")
            
            # 3. 获取主题详情和回复（生产者消费者模式，支持分批入库）
            all_replies = []
            all_users = []
            
            fetch_replies = self.crawler_config.get('fetch_replies', True)
            if fetch_replies:
                # 对所有主题获取详情内容，包括没有回复的主题
                self.logger.info(f"节点 '{node_name}' 开始生产者消费者模式爬取 {len(topics_to_update)} 个主题（并发数: {self.max_concurrent_replies}，分批入库）")
                
                # 使用线程池模式获取主题内容和回复
                try:
                    updated_topics, all_replies, all_users = self._get_topic_content_and_replies_batch_threaded(
                        topics_to_update, node_name
                    )
                    topics_to_update = updated_topics
                    
                    self.logger.info(f"节点 '{node_name}' 线程池模式完成，总共获取 {len(all_replies)} 个回复，{len(all_users)} 个用户")
                    
                except Exception as e:
                    self.logger.error(f"节点 '{node_name}' 线程池模式失败，回退到串行模式: {e}")
                    # 回退到原来的串行方式
                    for i, topic in enumerate(topics_to_update, 1):
                        topic_id = topic['id']
                        reply_count = topic.get('replies', 0)
                        
                        self.logger.info(f"串行模式获取主题 {topic_id} ({i}/{len(topics_to_update)}, 预期 {reply_count} 个回复)")
                        
                        # 获取主题内容和回复
                        result = self.get_topic_content_and_replies_from_html(topic_id)
                        
                        # 更新主题的内容字段（已转换为Markdown格式）
                        topic['content'] = result.get('content', '')
                        
                        # 如果有回复，则添加到回复列表
                        if result.get('replies'):
                            all_replies.extend(result['replies'])
                            
                            # 提取回复中的用户信息
                            for reply in result['replies']:
                                if reply.get('member_username'):
                                    all_users.append({'username': reply['member_username']})
                        
                        self.logger.info(f"主题 {topic_id} 获取内容 {len(result.get('content', ''))} 字符, 回复 {len(result.get('replies', []))} 个")
                        
                        # 减少延迟
                        time.sleep(0.3)  # 从1秒减少到0.3秒
                    
                    self.logger.info(f"节点 '{node_name}' 串行模式总共获取 {len(all_replies)} 个回复")
            
            # 提取主题中的用户信息，并清理嵌套dict
            for topic in topics_to_update:
                if topic.get('member'):
                    all_users.append(topic['member'])
                    # 移除嵌套dict，避免数据库写入问题
                    del topic['member']
                if topic.get('node'):
                    # 移除嵌套dict，避免数据库写入问题
                    del topic['node']
            
            return {
                'topics_to_update': topics_to_update,
                'all_users': all_users,
                'all_replies': all_replies
            }
            
        except Exception as e:
            self.logger.error(f"串行爬取节点 '{node_name}' 失败: {e}")
            return {
                'topics_to_update': [],
                'all_users': [],
                'all_replies': []
            }
    
    async def _crawl_single_node_async(self, session: aiohttp.ClientSession, node_name: str, node_title: str) -> Dict[str, Any]:
        """异步爬取单个节点"""
        self.logger.info(f"开始异步爬取节点: {node_name} ({node_title})")
        
        try:
            # 1. 网页解析获取主题列表（这部分仍然是同步的）
            max_pages_per_node = self.crawler_config.get('max_pages_per_node', 5)
            node_topics = web_parser.crawl_node_with_pagination(node_name, max_pages_per_node)
            
            if not node_topics:
                return {
                    'topics_to_update': [],
                    'all_users': [],
                    'all_replies': []
                }
            
            # 2. 筛选需要更新的主题
            topics_to_update = self._filter_topics_to_update(node_topics)
            self.logger.info(f"节点 '{node_name}' 需要更新 {len(topics_to_update)}/{len(node_topics)} 个主题")
            
            # 3. 并发获取回复
            all_replies = []
            all_users = []
            
            fetch_replies = self.crawler_config.get('fetch_replies', True)
            if fetch_replies:
                topics_with_replies = [t for t in topics_to_update if t.get('replies', 0) > 0]
                if topics_with_replies:
                    self.logger.info(f"节点 '{node_name}' 开始并发获取 {len(topics_with_replies)} 个主题的回复")
                    all_replies = await self._get_replies_batch_async(session, topics_with_replies)
                    self.logger.info(f"节点 '{node_name}' 并发获取到 {len(all_replies)} 个回复")
                    
                    # 提取回复中的用户信息（回复数据已经没有嵌套dict了）
                    for reply in all_replies:
                        if reply.get('member'):
                            all_users.append(reply['member'])
            
            # 提取主题中的用户信息，并清理嵌套dict
            for topic in topics_to_update:
                if topic.get('member'):
                    all_users.append(topic['member'])
                    # 移除嵌套dict，避免数据库写入问题
                    del topic['member']
                if topic.get('node'):
                    # 移除嵌套dict，避免数据库写入问题
                    del topic['node']
            
            return {
                'topics_to_update': topics_to_update,
                'all_users': all_users,
                'all_replies': all_replies
            }
            
        except Exception as e:
            self.logger.error(f"异步爬取节点 '{node_name}' 失败: {e}")
            return {
                'topics_to_update': [],
                'all_users': [],
                'all_replies': []
            }
    
    async def _get_replies_batch_async(self, session: aiohttp.ClientSession, topics: List[Dict]) -> List[Dict]:
        """批量异步获取回复"""
        semaphore = asyncio.Semaphore(self.max_concurrent_replies)
        
        async def get_replies_for_topic(topic):
            async with semaphore:
                topic_id = topic.get('id')
                try:
                    replies = await self._get_topic_replies_async(session, topic_id)
                    return replies
                except Exception as e:
                    self.logger.error(f"获取主题 {topic_id} 回复失败: {type(e).__name__}: {e}")
                    return []
        
        tasks = [get_replies_for_topic(topic) for topic in topics]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_replies = []
        failed_count = 0
        for i, result in enumerate(results):
            if isinstance(result, list):
                all_replies.extend(result)
            elif isinstance(result, Exception):
                failed_count += 1
                topic_id = topics[i].get('id', 'unknown') if i < len(topics) else 'unknown'
                self.logger.error(f"主题 {topic_id} 回复获取任务异常: {type(result).__name__}: {result}")
        
        if failed_count > 0:
            self.logger.warning(f"批量获取回复完成，{failed_count}/{len(topics)} 个主题失败")
        
        return all_replies
    
    def _get_topic_content_and_replies_batch_threaded(self, topics: List[Dict], node_name: str) -> tuple:
        """使用线程池批量获取主题内容和回复，支持分批入库"""
        total_topics = len(topics)
        processed_count = 0
        failed_count = 0
        all_replies = []
        all_users = []
        
        # 分批入库配置
        batch_size = 10
        current_batch_topics = []
        current_batch_replies = []
        current_batch_users = []
        
        # 线程安全锁
        lock = threading.Lock()
        
        def process_single_topic(topic):
            """处理单个主题的线程函数"""
            nonlocal processed_count, failed_count
            
            topic_id = topic.get('id')
            try:
                # 使用同步方法获取内容和回复
                result = self.get_topic_content_and_replies_from_html(topic_id)
                
                # 更新主题的内容字段
                topic['content'] = result.get('content', '')
                
                # 提取回复中的用户信息
                topic_users = []
                for reply in result.get('replies', []):
                    if reply.get('member_username'):
                        topic_users.append({'username': reply['member_username']})
                
                with lock:
                    processed_count += 1
                    
                    # 收集数据
                    current_batch_topics.append(topic)
                    current_batch_replies.extend(result.get('replies', []))
                    current_batch_users.extend(topic_users)
                    all_replies.extend(result.get('replies', []))
                    all_users.extend(topic_users)
                    
                    # 显示进度
                    if processed_count % 5 == 0 or processed_count == total_topics:
                        progress = (processed_count / total_topics) * 100
                        self.logger.info(f"节点 '{node_name}' 爬取进度: {processed_count}/{total_topics} ({progress:.1f}%)")
                    
                    # 分批入库
                    if len(current_batch_topics) >= batch_size or processed_count == total_topics:
                        try:
                            # 保存当前批次的数据
                            if current_batch_topics:
                                db_manager.batch_insert_or_update_topics(current_batch_topics.copy())
                                self.logger.info(f"节点 '{node_name}' 批量保存 {len(current_batch_topics)} 个主题")
                            
                            if current_batch_replies:
                                db_manager.batch_insert_or_update_replies(current_batch_replies.copy())
                                self.logger.info(f"节点 '{node_name}' 批量保存 {len(current_batch_replies)} 个回复")
                            
                            if current_batch_users:
                                # 提取用户名列表
                                usernames = [user.get('username') for user in current_batch_users
                                           if user.get('username')]
                                
                                if usernames:
                                    self.logger.info(f"节点 '{node_name}' 开始批量保存 {len(usernames)} 个用户")
                                    try:
                                        saved_count = db_manager.batch_insert_users_by_username(usernames)
                                        self.logger.info(f"节点 '{node_name}' 批量保存用户完成: {saved_count} 个")
                                    except Exception as e:
                                        self.logger.error(f"节点 '{node_name}' 批量保存用户失败: {e}")
                            
                            # 清空当前批次
                            current_batch_topics.clear()
                            current_batch_replies.clear()
                            current_batch_users.clear()
                            
                        except Exception as e:
                            self.logger.error(f"节点 '{node_name}' 分批入库失败: {e}")
                
                return True
                
            except Exception as e:
                with lock:
                    failed_count += 1
                    processed_count += 1
                self.logger.error(f"获取主题 {topic_id} 失败: {type(e).__name__}: {e}")
                
                # 即使失败也要更新主题内容为空
                topic['content'] = ''
                with lock:
                    current_batch_topics.append(topic)
                
                return False
        
        # 使用线程池处理
        max_workers = min(self.max_concurrent_replies, 3)  # 限制最大线程数
        self.logger.info(f"节点 '{node_name}' 开始线程池模式爬取 {total_topics} 个主题（线程数: {max_workers}，分批入库）")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_topic = {executor.submit(process_single_topic, topic): topic for topic in topics}
            
            # 等待所有任务完成
            for future in as_completed(future_to_topic):
                topic = future_to_topic[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"线程处理主题 {topic.get('id')} 异常: {e}")
        
        self.logger.info(f"节点 '{node_name}' 线程池爬取完成: 成功 {processed_count - failed_count}/{total_topics}, 失败 {failed_count}, 总回复 {len(all_replies)}")
        
        return topics, all_replies, all_users

    async def _get_topic_content_and_replies_batch_async(self, topics: List[Dict], node_name: str) -> tuple:
        """生产者消费者模式批量异步获取主题内容和回复，支持分批入库"""
        # 使用更保守的连接配置，避免被V2EX限制
        connector = aiohttp.TCPConnector(
            limit=5,           # 总连接数限制
            limit_per_host=2,  # 每个主机连接数限制
            ttl_dns_cache=300, # DNS缓存时间
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        # 设置更宽松的超时配置
        timeout = aiohttp.ClientTimeout(
            total=120,      # 总超时时间2分钟
            connect=15,     # 连接超时15秒
            sock_read=60,   # 读取超时60秒
            sock_connect=15 # socket连接超时15秒
        )
        
        # 创建队列用于生产者消费者模式
        topic_queue = asyncio.Queue(maxsize=self.max_concurrent_replies * 2)  # 队列大小为并发数的2倍
        result_queue = asyncio.Queue()
        
        # 统计信息
        total_topics = len(topics)
        processed_count = 0
        failed_count = 0
        all_replies = []
        all_users = []
        
        # 分批入库配置
        batch_size = 10  # 每10个主题入库一次
        current_batch_topics = []
        current_batch_replies = []
        current_batch_users = []
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # 生产者：将主题放入队列
            async def producer():
                for topic in topics:
                    await topic_queue.put(topic)
                # 添加结束标记
                for _ in range(self.max_concurrent_replies):
                    await topic_queue.put(None)
            
            # 消费者：处理主题并获取内容和回复
            async def consumer(consumer_id: int):
                nonlocal processed_count, failed_count
                consumer_processed = 0
                
                while True:
                    topic = await topic_queue.get()
                    if topic is None:  # 结束标记
                        break
                    
                    topic_id = topic.get('id')
                    try:
                        # 增加延迟，避免被V2EX限制
                        if consumer_processed > 0:
                            await asyncio.sleep(0.5)  # 每个请求后都延迟0.5秒
                        
                        result = await self._get_topic_content_and_replies_async(session, topic_id)
                        
                        # 更新主题的内容字段
                        topic['content'] = result.get('content', '')
                        
                        # 提取回复中的用户信息
                        topic_users = []
                        for reply in result.get('replies', []):
                            if reply.get('member_username'):
                                topic_users.append({'username': reply['member_username']})
                        
                        # 将结果放入结果队列
                        await result_queue.put({
                            'topic': topic,
                            'replies': result.get('replies', []),
                            'users': topic_users,
                            'success': True
                        })
                        
                        consumer_processed += 1
                        processed_count += 1
                        
                        # 显示进度
                        if processed_count % 5 == 0 or processed_count == total_topics:
                            progress = (processed_count / total_topics) * 100
                            self.logger.info(f"节点 '{node_name}' 爬取进度: {processed_count}/{total_topics} ({progress:.1f}%) - 消费者{consumer_id}已处理{consumer_processed}个")
                        
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"消费者{consumer_id} 获取主题 {topic_id} 失败: {type(e).__name__}: {e}")
                        
                        # 即使失败也要放入结果队列，保持计数正确
                        topic['content'] = ''
                        await result_queue.put({
                            'topic': topic,
                            'replies': [],
                            'users': [],
                            'success': False
                        })
                        processed_count += 1
                    
                    finally:
                        topic_queue.task_done()
            
            # 结果处理器：分批入库
            async def result_processor():
                nonlocal current_batch_topics, current_batch_replies, current_batch_users
                results_processed = 0
                
                while results_processed < total_topics:
                    try:
                        result = await asyncio.wait_for(result_queue.get(), timeout=10.0)  # 增加等待结果的超时时间
                        
                        # 收集数据
                        current_batch_topics.append(result['topic'])
                        current_batch_replies.extend(result['replies'])
                        current_batch_users.extend(result['users'])
                        all_replies.extend(result['replies'])
                        all_users.extend(result['users'])
                        
                        results_processed += 1
                        
                        # 分批入库
                        if len(current_batch_topics) >= batch_size or results_processed == total_topics:
                            try:
                                # 保存当前批次的数据
                                if current_batch_topics:
                                    db_manager.batch_insert_or_update_topics(current_batch_topics)
                                    self.logger.info(f"节点 '{node_name}' 批量保存 {len(current_batch_topics)} 个主题")
                                
                                if current_batch_replies:
                                    db_manager.batch_insert_or_update_replies(current_batch_replies)
                                    self.logger.info(f"节点 '{node_name}' 批量保存 {len(current_batch_replies)} 个回复")
                                
                                if current_batch_users:
                                    # 去重用户
                                    unique_users = {user.get('username', f"user_{i}"): user
                                                  for i, user in enumerate(current_batch_users)
                                                  if user.get('username')}
                                    for user in unique_users.values():
                                        try:
                                            db_manager.insert_or_update_user(user)
                                        except Exception as e:
                                            self.logger.error(f"保存用户 {user.get('username', 'unknown')} 失败: {e}")
                                    self.logger.info(f"节点 '{node_name}' 批量保存 {len(unique_users)} 个用户")
                                
                                # 清空当前批次
                                current_batch_topics = []
                                current_batch_replies = []
                                current_batch_users = []
                                
                            except Exception as e:
                                self.logger.error(f"节点 '{node_name}' 分批入库失败: {e}")
                        
                        result_queue.task_done()
                        
                    except asyncio.TimeoutError:
                        self.logger.warning(f"节点 '{node_name}' 等待结果超时，已处理 {results_processed}/{total_topics}")
                        break
            
            # 启动所有任务
            producer_task = asyncio.create_task(producer())
            consumer_tasks = [asyncio.create_task(consumer(i)) for i in range(self.max_concurrent_replies)]
            processor_task = asyncio.create_task(result_processor())
            
            # 等待生产者完成
            await producer_task
            
            # 等待所有消费者完成
            await asyncio.gather(*consumer_tasks)
            
            # 等待结果处理器完成
            await processor_task
            
            self.logger.info(f"节点 '{node_name}' 并发爬取完成: 成功 {processed_count - failed_count}/{total_topics}, 失败 {failed_count}, 总回复 {len(all_replies)}")
            
            return topics, all_replies, all_users
    
    async def _get_topic_content_and_replies_async(self, session: aiohttp.ClientSession, topic_id: int) -> Dict[str, Any]:
        """异步获取主题内容和回复，带重试机制"""
        url = f"https://www.v2ex.com/t/{topic_id}"
        max_retries = 2  # 最多重试2次
        
        for attempt in range(max_retries + 1):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Referer': 'https://www.v2ex.com/',
                    'Cache-Control': 'no-cache',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin'
                }
                
                timeout = aiohttp.ClientTimeout(
                    total=120,      # 总超时时间2分钟
                    connect=15,     # 连接超时15秒
                    sock_read=60,   # 读取超时60秒
                    sock_connect=15 # socket连接超时15秒
                )
                
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status == 429:  # 被限流
                        wait_time = (2 ** attempt) * 2 + 1
                        self.logger.warning(f"主题 {topic_id} 被限流，等待 {wait_time} 秒后重试 (尝试 {attempt + 1}/{max_retries + 1})")
                        await asyncio.sleep(wait_time)
                        continue
                    elif response.status != 200:
                        self.logger.warning(f"获取主题页面失败: {topic_id} - 状态码: {response.status} (尝试 {attempt + 1}/{max_retries + 1})")
                        if attempt < max_retries:
                            await asyncio.sleep(1)
                            continue
                        return {'content': '', 'replies': []}
                    
                    content = await response.text()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # 解析主题内容
                    topic_content = ''
                    content_div = soup.select_one('.topic_content')
                    if not content_div:
                        content_div = soup.select_one('div.cell:not([id])')
                    
                    if content_div:
                        topic_content = self._html_to_markdown(str(content_div))
                    
                    # 解析回复
                    replies = []
                    reply_cells = soup.select('div.cell[id^="r_"]')
                    
                    for i, cell in enumerate(reply_cells):
                        try:
                            reply_data = self._parse_reply_cell(cell, topic_id, i + 1)
                            if reply_data:
                                replies.append(reply_data)
                        except Exception as e:
                            self.logger.warning(f"解析主题 {topic_id} 第 {i+1} 个回复失败: {e}")
                            continue
                    
                    self.logger.debug(f"主题 {topic_id} 解析到内容 {len(topic_content)} 字符, {len(replies)} 个回复")
                    
                    return {
                        'content': topic_content,
                        'replies': replies
                    }
                    
            except asyncio.TimeoutError:
                self.logger.warning(f"异步获取主题 {topic_id} 内容和回复超时 (尝试 {attempt + 1}/{max_retries + 1})")
                if attempt < max_retries:
                    await asyncio.sleep(2)  # 超时后等待2秒再重试
                    continue
                self.logger.error(f"异步获取主题 {topic_id} 内容和回复超时")
                return {'content': '', 'replies': []}
            except aiohttp.ClientError as e:
                self.logger.warning(f"异步获取主题 {topic_id} 网络错误: {e} (尝试 {attempt + 1}/{max_retries + 1})")
                if attempt < max_retries:
                    await asyncio.sleep(1)
                    continue
                self.logger.error(f"异步获取主题 {topic_id} 内容和回复网络错误: {e}")
                return {'content': '', 'replies': []}
            except Exception as e:
                self.logger.warning(f"异步获取主题 {topic_id} 失败: {type(e).__name__}: {e} (尝试 {attempt + 1}/{max_retries + 1})")
                if attempt < max_retries:
                    await asyncio.sleep(1)
                    continue
                self.logger.error(f"异步获取主题 {topic_id} 内容和回复失败: {type(e).__name__}: {e}")
                return {'content': '', 'replies': []}
        
        return {'content': '', 'replies': []}
    
    async def _get_topic_replies_async(self, session: aiohttp.ClientSession, topic_id: int) -> List[Dict[str, Any]]:
        """异步获取主题回复（通过HTML解析）"""
        url = f"https://www.v2ex.com/t/{topic_id}"
        
        try:
            await self._rate_limit_async()
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://www.v2ex.com/',
                'Cache-Control': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin'
            }
            
            timeout = aiohttp.ClientTimeout(
                total=120,      # 总超时时间2分钟
                connect=15,     # 连接超时15秒
                sock_read=60,   # 读取超时60秒
                sock_connect=15 # socket连接超时15秒
            )
            
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status != 200:
                    self.logger.warning(f"获取主题页面失败: {topic_id} - 状态码: {response.status}")
                    return []
                
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                replies = []
                
                reply_cells = soup.select('div.cell[id^="r_"]')
                
                for i, cell in enumerate(reply_cells):
                    try:
                        reply_data = self._parse_reply_cell(cell, topic_id, i + 1)
                        if reply_data:
                            replies.append(reply_data)
                    except Exception as e:
                        self.logger.warning(f"解析主题 {topic_id} 第 {i+1} 个回复失败: {e}")
                        continue
                
                self.logger.debug(f"主题 {topic_id} 解析到 {len(replies)} 个回复")
                return replies
                
        except asyncio.TimeoutError:
            self.logger.error(f"异步获取主题 {topic_id} 回复超时")
            return []
        except aiohttp.ClientError as e:
            self.logger.error(f"异步获取主题 {topic_id} 回复网络错误: {e}")
            return []
        except Exception as e:
            self.logger.error(f"异步获取主题 {topic_id} 回复失败: {type(e).__name__}: {e}")
            return []
    
    async def _rate_limit_async(self):
        """异步限流控制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _filter_topics_to_update(self, topics: List[Dict]) -> List[Dict]:
        """筛选需要更新的主题"""
        if not topics:
            return []
        
        topic_ids = [topic.get('id') for topic in topics if topic.get('id')]
        db_last_touched_map = db_manager.get_topics_last_touched_batch(topic_ids)
        
        topics_to_update = []
        for topic in topics:
            topic_id = topic.get('id')
            if not topic_id:
                continue
            
            db_last_touched = db_last_touched_map.get(topic_id)
            current_last_touched = topic.get('last_touched')
            
            should_update = (
                db_last_touched is None or
                (current_last_touched and current_last_touched > db_last_touched)
            )
            
            if should_update:
                topics_to_update.append(topic)
        
        return topics_to_update
    
    def _save_crawled_data(self, all_topics: List[Dict], all_users: List[Dict], all_replies: List[Dict]) -> Dict[str, Any]:
        """保存爬取的数据（适配生产者消费者模式，大部分数据已在过程中保存）"""
        result = {
            'topics_found': len(all_topics),
            'topics_crawled': len(all_topics),  # 在生产者消费者模式中已经保存
            'users_saved': 0,
            'replies_saved': len(all_replies),  # 在生产者消费者模式中已经保存
            'success': False
        }
        
        try:
            # 处理剩余的用户信息（如果有的话）
            if all_users:
                unique_users = {user.get('username', f"user_{i}"): user for i, user in enumerate(all_users) if user.get('username')}
                for user in unique_users.values():
                    try:
                        db_manager.insert_or_update_user(user)
                        result['users_saved'] += 1
                    except Exception as e:
                        self.logger.error(f"保存用户 {user.get('username', 'unknown')} 失败: {e}")
            
            # 在生产者消费者模式中，主题和回复已经分批保存，这里只需要处理剩余数据
            # 如果有未保存的主题（回退模式），则批量保存
            unsaved_topics = [topic for topic in all_topics if not hasattr(topic, '_saved')]
            if unsaved_topics:
                db_manager.batch_insert_or_update_topics(unsaved_topics)
                self.logger.info(f"补充保存 {len(unsaved_topics)} 个未保存的主题")
            
            # 如果有未保存的回复（回退模式），则批量保存
            unsaved_replies = [reply for reply in all_replies if not hasattr(reply, '_saved')]
            if unsaved_replies:
                db_manager.batch_insert_or_update_replies(unsaved_replies)
                self.logger.info(f"补充保存 {len(unsaved_replies)} 个未保存的回复")
            
            result['success'] = True
            self.logger.info(f"数据保存完成: 主题 {result['topics_crawled']}, 用户 {result['users_saved']}, 回复 {result['replies_saved']}")
            
        except Exception as e:
            self.logger.error(f"批量保存数据失败: {e}")
            result['error'] = str(e)
        
        return result
    
    def crawl_hot_and_latest(self) -> Dict[str, Any]:
        """爬取热门和最新主题"""
        self.logger.info("开始爬取热门和最新主题")
        
        all_topics = []
        all_users = []
        
        try:
            # 获取热门主题
            hot_topics = self.get_hot_topics()
            all_topics.extend(hot_topics)
            
            self._delay_between_requests()
            
            # 获取最新主题
            latest_topics = self.get_latest_topics()
            all_topics.extend(latest_topics)
            
            # 去重
            unique_topics = {topic['id']: topic for topic in all_topics if topic.get('id')}
            all_topics = list(unique_topics.values())
            
            # 提取用户信息
            for topic in all_topics:
                if 'member' in topic and topic['member']:
                    all_users.append(topic['member'])
            
            # 保存数据
            result = {
                'topics_found': len(all_topics),
                'topics_crawled': 0,
                'users_saved': 0,
                'success': False
            }
            
            # 保存用户
            unique_users = {user['id']: user for user in all_users if user.get('id')}
            for user in unique_users.values():
                try:
                    db_manager.insert_or_update_user(user)
                    result['users_saved'] += 1
                except Exception as e:
                    self.logger.error(f"保存用户失败: {e}")
            
            # 保存主题
            if all_topics:
                db_manager.batch_insert_or_update_topics(all_topics)
                result['topics_crawled'] = len(all_topics)
            
            result['success'] = True
            self.logger.info(f"热门和最新主题爬取完成: {result['topics_crawled']} 个主题")
            
        except Exception as e:
            self.logger.error(f"爬取热门和最新主题失败: {e}")
            result = {
                'topics_found': 0,
                'topics_crawled': 0,
                'users_saved': 0,
                'success': False,
                'error': str(e)
            }
        
        return result


# 全局爬虫实例
crawler = V2EXCrawler()
