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
        
        self.base_api_url = "https://www.v2ex.com/api"
        
        self.max_concurrent_nodes = self.crawler_config.get('max_concurrent_nodes', 1)
        self.max_concurrent_replies = self.crawler_config.get('max_concurrent_replies', 1)
        
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br', # Removed to avoid compression/decoding issues
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        })
        
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_delay = 1.0
    
    def _get_random_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.ua.random,
            'Referer': 'https://www.v2ex.com/',
        }
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        max_retries = self.crawler_config['max_retries']
        timeout = self.crawler_config['timeout_seconds']
        
        for attempt in range(max_retries + 1):
            try:
                headers = self._get_random_headers()
                self.logger.debug(f"请求URL: {url} (尝试 {attempt + 1}/{max_retries + 1})")
                
                response = self.session.get(url, params=params, headers=headers, timeout=timeout)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
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
        delay = self.crawler_config['delay_seconds']
        actual_delay = delay + random.uniform(0, delay * 0.5)
        time.sleep(actual_delay)
    
    def get_topic_detail(self, topic_id: int) -> Optional[Dict[str, Any]]:
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
        url = f"https://www.v2ex.com/t/{topic_id}"
        try:
            headers = self._get_random_headers()
            response = self.session.get(url, headers=headers, timeout=self.crawler_config['timeout_seconds'])
            if response.status_code != 200:
                self.logger.warning(f"获取主题页面失败: {topic_id} - 状态码: {response.status_code}")
                return {'content': '', 'replies': []}
            
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            content_div = soup.select_one('.topic_content')
            if not content_div:
                content_div = soup.select_one('div.cell:not([id])')
            
            content = self._html_to_markdown(str(content_div)) if content_div else ''
            
            replies = []
            reply_cells = soup.select('div.cell[id^="r_"]')
            for i, cell in enumerate(reply_cells):
                try:
                    if reply_data := self._parse_reply_cell(cell, topic_id, i + 1):
                        replies.append(reply_data)
                except Exception as e:
                    self.logger.warning(f"解析回复失败: {e}")
            
            self.logger.debug(f"主题 {topic_id} 解析到内容 {len(content)} 字符, {len(replies)} 个回复")
            return {'content': content, 'replies': replies}
        except Exception as e:
            self.logger.error(f"获取主题 {topic_id} 内容和回复失败: {e}")
            return {'content': '', 'replies': []}

    def _html_to_markdown(self, html_content: str) -> str:
        try:
            if not html_content or not html_content.strip():
                return ''
            
            html_converter = html2text.HTML2Text()
            html_converter.ignore_links = False
            html_converter.ignore_images = False
            html_converter.body_width = 0
            markdown_content = html_converter.handle(html_content)
            
            lines = markdown_content.split('\n')
            cleaned_lines = [line.strip() for line in lines if line.strip()]
            return '\n'.join(cleaned_lines)
        except Exception as e:
            self.logger.warning(f"HTML转Markdown失败: {e}")
            return BeautifulSoup(html_content, 'html.parser').get_text(strip=True)

    def _parse_reply_cell(self, cell, topic_id: int, floor: int) -> Optional[Dict[str, Any]]:
        try:
            reply_id_attr = cell.get('id', '')
            reply_id = int(reply_id_attr[2:]) if reply_id_attr.startswith('r_') else topic_id * 1000 + floor
            
            username = None
            if user_link := cell.select_one('a[href*="/member/"]'):
                if '/member/' in (href := user_link.get('href', '')):
                    username = href.split('/member/')[-1]
            
            content = ""
            if content_div := cell.select_one('.reply_content'):
                content = self._html_to_markdown(str(content_div))

            created_timestamp = int(datetime.now().timestamp())
            if time_element := cell.select_one('.ago'):
                created_timestamp = self._parse_relative_time(time_element.get_text(strip=True)) or created_timestamp

            thanks_count = 0
            if thanks_element := cell.select_one('.small.fade'):
                if '♥' in (thanks_text := thanks_element.get_text(strip=True)):
                    if match := re.search(r'(\d+)', thanks_text):
                        thanks_count = int(match.group(1))
            
            return {
                'id': reply_id, 'topic_id': topic_id, 'member_id': None,
                'member_username': username, 'content': content, 'reply_floor': floor,
                'created_timestamp': created_timestamp, 'last_modified_timestamp': created_timestamp,
                'thanks_count': thanks_count, 'created': created_timestamp,
                'last_modified': created_timestamp, 'thanks': thanks_count
            }
        except Exception as e:
            self.logger.warning(f"解析回复元素失败: {e}")
            return None

    def _parse_relative_time(self, time_text: str) -> Optional[int]:
        if not time_text: return None
        now = int(datetime.now().timestamp())
        try:
            if '分钟前' in time_text:
                return now - int(re.search(r'(\d+)', time_text).group(1)) * 60
            if '小时前' in time_text:
                return now - int(re.search(r'(\d+)', time_text).group(1)) * 3600
            if '天前' in time_text:
                return now - int(re.search(r'(\d+)', time_text).group(1)) * 86400
            if re.match(r'\d{4}-\d{2}-\d{2}', time_text):
                return int(datetime.strptime(time_text[:19], '%Y-%m-%d %H:%M:%S').timestamp())
            return now
        except Exception as e:
            self.logger.warning(f"解析时间失败: {time_text} - {e}")
            return now

    def crawl_topics_by_nodes(self) -> Dict[str, Any]:
        concurrent_nodes = self.crawler_config.get('max_concurrent_nodes', 1)
        mode = "并发" if concurrent_nodes > 1 else "串行"
        self.logger.info(f"开始爬取主题（{mode}模式，并发数: {concurrent_nodes}）")
        try:
            if concurrent_nodes > 1:
                # The async implementation is not the focus of this fix and is left as is.
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(self._crawl_all_nodes_async())
                finally:
                    loop.close()
            else:
                result = self._crawl_all_nodes_sync()
            
            self.logger.info(f"{mode}爬取完成: 找到 {result.get('topics_found', 0)} 个主题, "
                           f"更新 {result.get('topics_crawled', 0)} 个, "
                           f"保存 {result.get('replies_saved', 0)} 条回复, "
                           f"{result.get('users_saved', 0)} 个用户。")
            return result
        except Exception as e:
            self.logger.error(f"爬取任务主流程失败: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _crawl_all_nodes_sync(self) -> Dict[str, Any]:
        stats = {'topics_found': 0, 'topics_crawled': 0, 'replies_saved': 0, 'users_saved': 0}
        for node_name, node_title in self.target_nodes.items():
            try:
                node_stats = self._crawl_single_node_sync(node_name, node_title)
                if node_stats.get('success'):
                    for key in stats:
                        stats[key] += node_stats.get(key, 0)
                self._delay_between_requests()
            except Exception as e:
                self.logger.error(f"串行爬取节点 '{node_name}' 时发生意外错误: {e}", exc_info=True)
        stats['success'] = True
        return stats

    def _crawl_single_node_sync(self, node_name: str, node_title: str) -> Dict[str, Any]:
        self.logger.info(f"开始串行爬取节点: {node_name} ({node_title})")
        try:
            node_topics = web_parser.crawl_node_with_pagination(node_name, self.crawler_config.get('max_pages_per_node', 5))
            topics_found = len(node_topics)
            if not node_topics:
                return {'success': True, 'topics_found': 0, 'topics_crawled': 0, 'replies_saved': 0, 'users_saved': 0}
            
            topics_to_update = self._filter_topics_to_update(node_topics)
            self.logger.info(f"节点 '{node_name}' 需要更新 {len(topics_to_update)}/{topics_found} 个主题")
            if not topics_to_update:
                return {'success': True, 'topics_found': topics_found, 'topics_crawled': 0, 'replies_saved': 0, 'users_saved': 0}

            stats = self._get_and_save_details_threaded(topics_to_update, node_name)
            
            topic_authors = [t.get('member') for t in topics_to_update if t.get('member')]
            if topic_authors:
                usernames = list(set(user.get('username') for user in topic_authors if user.get('username')))
                self.logger.info(f"节点 '{node_name}' 开始批量保存 {len(usernames)} 个主题作者")
                saved_authors = db_manager.batch_insert_users_by_username(usernames)
                self.logger.info(f"节点 '{node_name}' 批量保存主题作者完成: {saved_authors} 个")
                stats['users_saved'] += saved_authors
            
            return {'success': True, 'topics_found': topics_found, **stats}
        except Exception as e:
            self.logger.error(f"串行爬取节点 '{node_name}' 失败: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _get_and_save_details_threaded(self, topics: List[Dict], node_name: str) -> Dict[str, int]:
        total_topics = len(topics)
        stats = {'topics_crawled': 0, 'replies_saved': 0, 'users_saved': 0}
        lock = threading.Lock()

        def process_batch(batch_topics: List[Dict]):
            batch_replies, batch_users_from_replies = [], []
            for topic in batch_topics:
                try:
                    details = self.get_topic_content_and_replies_from_html(topic['id'])
                    topic.update(details)
                    if replies := details.get('replies'):
                        batch_replies.extend(replies)
                        batch_users_from_replies.extend(r.get('member_username') for r in replies if r.get('member_username'))
                except Exception as e:
                    self.logger.error(f"获取主题 {topic.get('id')} 详情失败: {e}")

            for topic in batch_topics:
                topic.pop('member', None)
                topic.pop('node', None)

            db_manager.batch_insert_or_update_topics(batch_topics)
            if batch_replies:
                db_manager.batch_insert_or_update_replies(batch_replies)

            saved_users = 0
            if batch_users_from_replies:
                saved_users = db_manager.batch_insert_users_by_username(list(set(batch_users_from_replies)))

            with lock:
                stats['topics_crawled'] += len(batch_topics)
                stats['replies_saved'] += len(batch_replies)
                stats['users_saved'] += saved_users
                self.logger.info(f"节点 '{node_name}' 批量保存完成: "
                               f"{len(batch_topics)} 主题, {len(batch_replies)} 回复, {saved_users} 用户。 "
                               f"总进度: {stats['topics_crawled']}/{total_topics}")

        max_workers = min(self.crawler_config.get('max_concurrent_replies', 5), 10)
        self.logger.info(f"节点 '{node_name}' 开始线程池模式爬取和保存 {total_topics} 个主题（线程数: {max_workers}）")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            batch_size = 10
            futures = [executor.submit(process_batch, topics[i:i + batch_size]) for i in range(0, total_topics, batch_size)]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"线程池批处理任务异常: {e}", exc_info=True)
        
        self.logger.info(f"节点 '{node_name}' 线程池处理完成。")
        return stats

    def _filter_topics_to_update(self, topics: List[Dict]) -> List[Dict]:
        if not topics: return []
        topic_ids = [t.get('id') for t in topics if t.get('id')]
        db_last_touched_map = db_manager.get_topics_last_touched_batch(topic_ids)
        return [t for t in topics if t.get('id') and t.get('last_touched', 0) > db_last_touched_map.get(t.get('id'), 0)]
    
    # The original async methods are left untouched as requested by the user's workflow (serial mode)
    async def _crawl_all_nodes_async(self, *args, **kwargs):
        self.logger.warning("异步节点爬取功能尚未适配新的重构逻辑。")
        return {}
    
    def crawl_hot_and_latest(self, *args, **kwargs):
        self.logger.info("开始爬取热门和最新主题 (当前未实现)")
        return {}

crawler = V2EXCrawler()
