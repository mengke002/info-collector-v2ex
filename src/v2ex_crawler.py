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
            'Accept-Encoding': 'gzip, deflate, br',
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
    
    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """获取所有节点信息"""
        self.logger.info("开始获取所有节点信息")
        
        url = f"{self.base_api_url}/nodes/all.json"
        nodes_data = self._make_request(url)
        
        if nodes_data:
            self.logger.info(f"成功获取 {len(nodes_data)} 个节点信息")
            return nodes_data
        else:
            self.logger.error("获取节点信息失败")
            return []
    

    
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
                return {'content': '', 'content_rendered': '', 'replies': []}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 解析主题内容
            content = ''
            content_rendered = ''
            
            # V2EX主题内容通常在 .topic_content 或 .cell 中
            content_div = soup.select_one('.topic_content')
            if not content_div:
                # 备选方案：查找包含主题内容的cell
                content_div = soup.select_one('div.cell:not([id])')
            
            if content_div:
                content = content_div.get_text(strip=True)
                content_rendered = str(content_div)
            
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
                'content_rendered': content_rendered,
                'replies': replies
            }
            
        except Exception as e:
            self.logger.error(f"获取主题 {topic_id} 内容和回复失败: {e}")
            return {'content': '', 'content_rendered': '', 'replies': []}
    
    def get_topic_replies_from_html(self, topic_id: int) -> List[Dict[str, Any]]:
        """通过HTML页面解析获取主题回复（保持向后兼容）"""
        result = self.get_topic_content_and_replies_from_html(topic_id)
        return result['replies']
    
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
            
            # 提取回复内容
            content = ""
            content_div = cell.select_one('.reply_content')
            if content_div:
                content = content_div.get_text(strip=True)
            
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
                'content_rendered': content,
                'reply_floor': floor,
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
    

    
    def crawl_nodes(self, force_update: bool = False) -> int:
        """爬取并保存所有节点信息"""
        # 检查是否需要更新节点信息（默认7天更新一次）
        if not force_update and db_manager.should_skip_nodes_update():
            self.logger.info("节点信息最近已更新，跳过节点爬取")
            return 0
        
        self.logger.info("开始爬取节点信息")
        
        nodes = self.get_all_nodes()
        if not nodes:
            return 0
        
        # 批量保存节点，每批100个
        batch_size = 100
        success_count = 0
        
        for i in range(0, len(nodes), batch_size):
            batch = nodes[i:i + batch_size]
            try:
                db_manager.batch_insert_or_update_nodes(batch)
                success_count += len(batch)
                self.logger.info(f"批量保存节点进度: {success_count}/{len(nodes)}")
            except Exception as e:
                self.logger.error(f"批量保存节点失败 (批次 {i//batch_size + 1}): {e}")
                # 如果批量失败，尝试逐个保存
                for node in batch:
                    try:
                        db_manager.insert_or_update_node(node)
                    except Exception as node_e:
                        self.logger.error(f"保存节点 {node.get('name', 'unknown')} 失败: {node_e}")
        
        # 记录节点更新时间
        db_manager.update_nodes_last_crawled()
        
        self.logger.info(f"成功保存 {success_count}/{len(nodes)} 个节点")
        return success_count
    
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
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=60)
        
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
            
            # 3. 获取主题详情和回复（串行）
            all_replies = []
            all_users = []
            
            fetch_replies = self.crawler_config.get('fetch_replies', True)
            if fetch_replies:
                # 对所有主题获取详情内容，包括没有回复的主题
                self.logger.info(f"节点 '{node_name}' 开始获取 {len(topics_to_update)} 个主题的详情内容")
                
                for i, topic in enumerate(topics_to_update, 1):
                    topic_id = topic['id']
                    reply_count = topic.get('replies', 0)
                    
                    self.logger.debug(f"获取主题 {topic_id} 的详情内容 ({i}/{len(topics_to_update)}, 预期 {reply_count} 个回复)")
                    
                    # 获取主题内容和回复
                    result = self.get_topic_content_and_replies_from_html(topic_id)
                    
                    # 更新主题的内容字段
                    topic['content'] = result.get('content', '')
                    topic['content_rendered'] = result.get('content_rendered', '')
                    
                    # 如果有回复，则添加到回复列表
                    if result.get('replies'):
                        all_replies.extend(result['replies'])
                        
                        # 提取回复中的用户信息
                        for reply in result['replies']:
                            if reply.get('member_username'):
                                all_users.append({'username': reply['member_username']})
                    
                    self.logger.debug(f"主题 {topic_id} 获取内容 {len(result.get('content', ''))} 字符, 回复 {len(result.get('replies', []))} 个")
                    
                    # 主题间延迟
                    self._delay_between_requests()
                
                self.logger.info(f"节点 '{node_name}' 总共获取 {len(all_replies)} 个回复")
            
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
                    self.logger.warning(f"获取主题 {topic_id} 回复失败: {e}")
                    return []
        
        tasks = [get_replies_for_topic(topic) for topic in topics]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_replies = []
        for result in results:
            if isinstance(result, list):
                all_replies.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"回复获取任务失败: {result}")
        
        return all_replies
    
    async def _get_topic_replies_async(self, session: aiohttp.ClientSession, topic_id: int) -> List[Dict[str, Any]]:
        """异步获取主题回复（通过HTML解析）"""
        url = f"https://www.v2ex.com/t/{topic_id}"
        
        try:
            await self._rate_limit_async()
            
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': 'https://www.v2ex.com/',
            }
            
            timeout = aiohttp.ClientTimeout(total=self.crawler_config.get('timeout_seconds', 30))
            
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
                        self.logger.warning(f"解析回复失败: {e}")
                        continue
                
                self.logger.debug(f"主题 {topic_id} 解析到 {len(replies)} 个回复")
                return replies
                
        except Exception as e:
            self.logger.error(f"异步获取主题 {topic_id} 回复失败: {e}")
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
        """保存爬取的数据"""
        result = {
            'topics_found': len(all_topics),
            'topics_crawled': 0,
            'users_saved': 0,
            'replies_saved': 0,
            'success': False
        }
        
        try:
            # 保存用户信息
            unique_users = {user.get('username', f"user_{i}"): user for i, user in enumerate(all_users) if user.get('username')}
            for user in unique_users.values():
                try:
                    db_manager.insert_or_update_user(user)
                    result['users_saved'] += 1
                except Exception as e:
                    self.logger.error(f"保存用户 {user.get('username', 'unknown')} 失败: {e}")
            
            # 批量保存主题
            if all_topics:
                db_manager.batch_insert_or_update_topics(all_topics)
                result['topics_crawled'] = len(all_topics)
            
            # 批量保存回复
            if all_replies:
                db_manager.batch_insert_or_update_replies(all_replies)
                result['replies_saved'] = len(all_replies)
            
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
        all_nodes = []
        
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
            
            # 提取用户和节点信息
            for topic in all_topics:
                if 'member' in topic and topic['member']:
                    all_users.append(topic['member'])
                if 'node' in topic and topic['node']:
                    all_nodes.append(topic['node'])
            
            # 保存数据
            result = {
                'topics_found': len(all_topics),
                'topics_crawled': 0,
                'users_saved': 0,
                'nodes_saved': 0,
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
            
            # 保存节点
            unique_nodes = {node['id']: node for node in all_nodes if node.get('id')}
            for node in unique_nodes.values():
                try:
                    db_manager.insert_or_update_node(node)
                    result['nodes_saved'] += 1
                except Exception as e:
                    self.logger.error(f"保存节点失败: {e}")
            
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
                'nodes_saved': 0,
                'success': False,
                'error': str(e)
            }
        
        return result


# 全局爬虫实例
crawler = V2EXCrawler()
