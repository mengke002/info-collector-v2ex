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
            
            html_converter = html2text.HTML2Text()
            html_converter.ignore_links = False
            html_converter.ignore_images = False
            html_converter.body_width = 0  # 不限制行宽
            markdown_content = html_converter.handle(html_content)
            
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
            return self._save_crawled_data(all_topics, all_users, all_replies)

        except Exception as e:
            self.logger.error(f"爬取失败: {e}", exc_info=True)
            return {
                'topics_found': 0,
                'topics_crawled': 0,
                'users_saved': 0,
                'replies_saved': 0,
                'success': False,
                'error': str(e)
            }

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

    async def _crawl_all_nodes_async(self, session: aiohttp.ClientSession, node_name: str, node_title: str) -> Dict[str, Any]:
        """并发爬取所有节点"""
        # This function is not used in the user's serial mode scenario, so it's left as is.
        # A full refactoring would also address this.
        ...

    def _crawl_single_node_sync(self, node_name: str, node_title: str) -> Dict[str, Any]:
        """串行爬取单个节点"""
        self.logger.info(f"开始串行爬取节点: {node_name} ({node_title})")

        try:
            # 1. 网页解析获取主题列表
            max_pages_per_node = self.crawler_config.get('max_pages_per_node', 5)
            node_topics = web_parser.crawl_node_with_pagination(node_name, max_pages_per_node)

            if not node_topics:
                return { 'topics_to_update': [], 'all_users': [], 'all_replies': [] }
            
            # 2. 筛选需要更新的主题
            topics_to_update = self._filter_topics_to_update(node_topics)
            self.logger.info(f"节点 '{node_name}' 需要更新 {len(topics_to_update)}/{len(node_topics)} 个主题")
            
            # 3. 获取主题详情和回复（生产者消费者模式，支持分批入库）
            all_replies = []
            all_users = []

            if self.crawler_config.get('fetch_replies', True) and topics_to_update:
                self.logger.info(f"节点 '{node_name}' 开始生产者消费者模式爬取 {len(topics_to_update)} 个主题（并发数: {self.max_concurrent_replies}，分批入库）")
                try:
                    updated_topics, all_replies, all_users = self._get_topic_content_and_replies_batch_threaded(
                        topics_to_update, node_name
                    )
                    topics_to_update = updated_topics
                    self.logger.info(f"节点 '{node_name}' 线程池模式完成，总共获取 {len(all_replies)} 个回复，{len(all_users)} 个用户")
                except Exception as e:
                    self.logger.error(f"节点 '{node_name}' 线程池模式失败: {e}", exc_info=True)

            # 提取主题中的用户信息，并清理嵌套dict
            for topic in topics_to_update:
                if member := topic.get('member'):
                    all_users.append(member)
                    del topic['member']
                if topic.get('node'):
                    del topic['node']

            return {
                'topics_to_update': topics_to_update,
                'all_users': all_users,
                'all_replies': all_replies
            }
            
        except Exception as e:
            self.logger.error(f"串行爬取节点 '{node_name}' 失败: {e}", exc_info=True)
            return { 'topics_to_update': [], 'all_users': [], 'all_replies': [] }

    async def _crawl_single_node_async(self, session: aiohttp.ClientSession, node_name: str, node_title: str) -> Dict[str, Any]:
        # This function is not used in the user's serial mode scenario, so it's left as is.
        ...

    async def _get_replies_batch_async(self, session: aiohttp.ClientSession, topics: List[Dict]) -> List[Dict]:
        # This function is not used in the user's serial mode scenario, so it's left as is.
        ...

    def _get_topic_content_and_replies_batch_threaded(self, topics: List[Dict], node_name: str) -> tuple:
        """使用线程池批量获取主题内容和回复，支持分批入库"""
        total_topics = len(topics)
        processed_count = 0
        failed_count = 0
        all_replies = []
        all_users = []

        batch_size = 10
        current_batch_topics = []
        current_batch_replies = []
        current_batch_users = []

        lock = threading.Lock()

        def process_single_topic(topic):
            nonlocal processed_count, failed_count
            topic_id = topic.get('id')
            try:
                result = self.get_topic_content_and_replies_from_html(topic_id)
                topic['content'] = result.get('content', '')

                topic_users = []
                if replies := result.get('replies'):
                    for reply in replies:
                        if username := reply.get('member_username'):
                            topic_users.append({'username': username})

                with lock:
                    processed_count += 1
                    current_batch_topics.append(topic)
                    current_batch_replies.extend(result.get('replies', []))
                    current_batch_users.extend(topic_users)
                    all_replies.extend(result.get('replies', []))
                    all_users.extend(topic_users)

                    if processed_count % 5 == 0 or processed_count == total_topics:
                        self.logger.info(f"节点 '{node_name}' 爬取进度: {processed_count}/{total_topics} ({(processed_count / total_topics) * 100:.1f}%)")

                    if len(current_batch_topics) >= batch_size or processed_count == total_topics:
                        if current_batch_topics:
                            db_manager.batch_insert_or_update_topics(current_batch_topics.copy())
                            self.logger.info(f"节点 '{node_name}' 批量保存 {len(current_batch_topics)} 个主题")
                        if current_batch_replies:
                            db_manager.batch_insert_or_update_replies(current_batch_replies.copy())
                            self.logger.info(f"节点 '{node_name}' 批量保存 {len(current_batch_replies)} 个回复")
                        if current_batch_users:
                            usernames = [u.get('username') for u in current_batch_users if u.get('username')]
                            if usernames:
                                self.logger.info(f"节点 '{node_name}' 开始批量保存 {len(usernames)} 个用户")
                                saved_count = db_manager.batch_insert_users_by_username(usernames)
                                self.logger.info(f"节点 '{node_name}' 批量保存用户完成: {saved_count} 个")

                        current_batch_topics.clear()
                        current_batch_replies.clear()
                        current_batch_users.clear()
            except Exception as e:
                with lock:
                    failed_count += 1
                self.logger.error(f"获取主题 {topic_id} 失败: {type(e).__name__}: {e}")
                topic['content'] = ''

        max_workers = min(self.max_concurrent_replies, 3)
        self.logger.info(f"节点 '{node_name}' 开始线程池模式爬取 {total_topics} 个主题（线程数: {max_workers}，分批入库）")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_topic = {executor.submit(process_single_topic, topic): topic for topic in topics}
            for future in as_completed(future_to_topic):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"线程处理主题异常: {e}")
        
        self.logger.info(f"节点 '{node_name}' 线程池爬取完成: 成功 {processed_count - failed_count}/{total_topics}, 失败 {failed_count}, 总回复 {len(all_replies)}")
        return topics, all_replies, all_users

    def _save_crawled_data(self, all_topics: List[Dict], all_users: List[Dict], all_replies: List[Dict]) -> Dict[str, Any]:
        """保存爬取的数据（适配生产者消费者模式，大部分数据已在过程中保存）"""
        result = {
            'topics_found': len(all_topics),
            'topics_crawled': len(all_topics),
            'users_saved': 0,
            'replies_saved': len(all_replies),
            'success': False
        }

        try:
            if all_users:
                self.logger.info(f"开始最终用户数据保存... (收到 {len(all_users)} 条记录)")
                usernames = [user.get('username') for user in all_users if user.get('username')]
                unique_usernames = list(set(usernames))
                self.logger.info(f"去重后有 {len(unique_usernames)} 个唯一用户需要检查/保存。")
                saved_count = db_manager.batch_insert_users_by_username(unique_usernames)
                result['users_saved'] = saved_count
                self.logger.info(f"最终用户数据保存完成: {saved_count} 个新用户被插入。")

            unsaved_topics = [t for t in all_topics if not hasattr(t, '_saved')]
            if unsaved_topics:
                db_manager.batch_insert_or_update_topics(unsaved_topics)
                self.logger.info(f"补充保存 {len(unsaved_topics)} 个未保存的主题")

            unsaved_replies = [r for r in all_replies if not hasattr(r, '_saved')]
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
        # This function is not used in the user's serial mode scenario, so it's left as is.
        ...

crawler = V2EXCrawler()
