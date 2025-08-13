"""
V2EX数据库模块
负责MySQL数据库连接、表创建和数据持久化操作
"""
import pymysql
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

from .config import config


class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self):
        self.db_config = config.get_database_config()
        self.logger = logging.getLogger(__name__)
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)
    
    def _sanitize_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证用户数据"""
        sanitized = user_data.copy()
        
        if 'username' in sanitized and sanitized['username']:
            original = str(sanitized['username'])
            sanitized['username'] = original[:50]
            if len(original) > 50:
                self.logger.warning(f"用户名被截断: {original[:20]}...")
        
        if 'avatar_url' in sanitized and sanitized['avatar_url']:
            original = str(sanitized['avatar_url'])
            sanitized['avatar_url'] = original[:500]
            if len(original) > 500:
                self.logger.warning(f"头像URL被截断")
        
        return sanitized
    
    def _sanitize_topic_data(self, topic_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证主题数据"""
        sanitized = topic_data.copy()
        
        if 'title' in sanitized and sanitized['title']:
            original = str(sanitized['title'])
            sanitized['title'] = original[:500]
            if len(original) > 500:
                self.logger.warning(f"标题被截断")
        
        if 'url' in sanitized and sanitized['url']:
            original = str(sanitized['url'])
            sanitized['url'] = original[:500]
            if len(original) > 500:
                self.logger.warning(f"URL被截断")
        
        if 'content' in sanitized and sanitized['content']:
            original = str(sanitized['content'])
            # 主题内容限制为10000字符
            max_chars = 10000
            if len(original) > max_chars:
                sanitized['content'] = original[:max_chars] + "...[内容被截断]"
                self.logger.warning(f"主题内容被截断: {len(original)} -> {max_chars} 字符")
        
        # 限制数值字段范围
        if 'replies' in sanitized:
            original_count = int(sanitized['replies'] or 0)
            sanitized['replies'] = min(max(original_count, 0), 65535)
        
        return sanitized
    
    def get_connection(self):
        """获取数据库连接"""
        try:
            ssl_enabled = self.db_config.get('ssl_mode', 'disabled').lower() != 'disabled'
            
            connection = pymysql.connect(
                host=self.db_config.get('host'),
                port=int(self.db_config.get('port', 3306)),
                user=self.db_config.get('user'),
                password=self.db_config.get('password'),
                database=self.db_config.get('database'),
                charset='utf8mb4',
                ssl={} if ssl_enabled else None,
                autocommit=False,
                connect_timeout=10,  # 连接超时10秒
                read_timeout=30,     # 读取超时30秒
                write_timeout=30     # 写入超时30秒
            )
            return connection
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise
    
    @contextmanager
    def get_cursor(self):
        """获取数据库游标的上下文管理器"""
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            yield cursor, connection
        except Exception as e:
            if connection:
                connection.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def init_database(self):
        """初始化数据库表结构"""
        create_tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS v2ex_users (
                id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键ID',
                v2ex_id INT UNIQUE COMMENT 'V2EX用户ID',
                username VARCHAR(50) UNIQUE NOT NULL COMMENT '用户名',
                url VARCHAR(500) COMMENT '用户主页URL',
                website VARCHAR(500) COMMENT '个人网站',
                github VARCHAR(100) COMMENT 'GitHub用户名',
                twitter VARCHAR(100) COMMENT 'Twitter用户名',
                location VARCHAR(200) COMMENT '地理位置',
                tagline VARCHAR(500) COMMENT '个人标语',
                bio TEXT COMMENT '个人简介',
                avatar_mini VARCHAR(500) COMMENT '小头像URL',
                avatar_normal VARCHAR(500) COMMENT '普通头像URL',
                avatar_large VARCHAR(500) COMMENT '大头像URL',
                created_timestamp INT UNSIGNED COMMENT '账号创建时间戳',
                last_modified_timestamp INT UNSIGNED COMMENT '最后修改时间戳',
                is_pro TINYINT DEFAULT 0 COMMENT '是否为Pro用户',
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '首次采集时间',
                
                INDEX idx_v2ex_id (v2ex_id),
                INDEX idx_username (username),
                INDEX idx_created (created_timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """,
            """
            CREATE TABLE IF NOT EXISTS v2ex_topics (
                id INT PRIMARY KEY COMMENT '主题ID',
                title VARCHAR(500) NOT NULL COMMENT '主题标题',
                url VARCHAR(500) UNIQUE NOT NULL COMMENT '主题URL',
                content TEXT COMMENT '主题内容(Markdown格式)',
                node_id INT COMMENT '所属节点ID',
                node_name VARCHAR(50) COMMENT '所属节点名称',
                member_id INT COMMENT '作者用户ID',
                member_username VARCHAR(50) COMMENT '作者用户名',
                replies SMALLINT UNSIGNED DEFAULT 0 COMMENT '回复数',
                last_reply_by VARCHAR(50) COMMENT '最后回复者',
                created_timestamp INT UNSIGNED NOT NULL COMMENT '创建时间戳',
                last_touched_timestamp INT UNSIGNED COMMENT '最后活跃时间戳',
                last_modified_timestamp INT UNSIGNED COMMENT '最后修改时间戳',
                is_deleted TINYINT DEFAULT 0 COMMENT '是否已删除',
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
                
                INDEX idx_node (node_name),
                INDEX idx_member (member_username),
                INDEX idx_created (created_timestamp),
                INDEX idx_last_touched (last_touched_timestamp),
                INDEX idx_replies (replies),
                FOREIGN KEY (member_id) REFERENCES v2ex_users(v2ex_id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """,
            """
            CREATE TABLE IF NOT EXISTS v2ex_replies (
                id INT PRIMARY KEY COMMENT '回复ID',
                topic_id INT NOT NULL COMMENT '所属主题ID',
                member_id INT COMMENT '回复用户ID',
                member_username VARCHAR(50) COMMENT '回复用户名',
                content VARCHAR(3000) COMMENT '回复内容(Markdown格式，最大3000字符)',
                reply_floor SMALLINT UNSIGNED COMMENT '楼层号',
                created_timestamp INT UNSIGNED NOT NULL COMMENT '回复创建时间戳',
                last_modified_timestamp INT UNSIGNED COMMENT '最后修改时间戳',
                thanks_count SMALLINT UNSIGNED DEFAULT 0 COMMENT '感谢数',
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
                
                INDEX idx_topic (topic_id),
                INDEX idx_member (member_username),
                INDEX idx_created (created_timestamp),
                INDEX idx_floor (reply_floor),
                FOREIGN KEY (topic_id) REFERENCES v2ex_topics(id) ON DELETE CASCADE,
                FOREIGN KEY (member_id) REFERENCES v2ex_users(v2ex_id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """
        ]
        
        with self.get_cursor() as (cursor, connection):
            for sql in create_tables_sql:
                cursor.execute(sql)
            connection.commit()
            self.logger.info("数据库表结构初始化完成")
    
    def insert_or_update_user(self, user_data: Dict[str, Any]):
        """插入或更新用户数据"""
        sanitized_data = self._sanitize_user_data(user_data)
        
        # 如果没有v2ex_id字段，只有username，则使用仅username的插入方式
        if 'id' not in sanitized_data and 'username' in sanitized_data:
            # 对于只有username的用户，使用简化的插入方式
            sql = """
            INSERT IGNORE INTO v2ex_users (username, first_seen_at)
            VALUES (%(username)s, %(first_seen_at)s)
            """
            
            if 'first_seen_at' not in sanitized_data:
                sanitized_data['first_seen_at'] = self.get_beijing_time()
            
            # 只保留必要的字段
            minimal_data = {
                'username': sanitized_data['username'],
                'first_seen_at': sanitized_data['first_seen_at']
            }
            
            try:
                with self.get_cursor() as (cursor, connection):
                    cursor.execute(sql, minimal_data)
                    connection.commit()
                    self.logger.debug(f"插入用户: {minimal_data['username']}")
            except Exception as e:
                self.logger.warning(f"插入用户 {minimal_data['username']} 失败: {e}")
            return
        
        # 原有的完整用户数据插入逻辑（将id字段映射为v2ex_id）
        sql = """
        INSERT INTO v2ex_users (
            v2ex_id, username, url, website, github, twitter, location, tagline, bio,
            avatar_mini, avatar_normal, avatar_large, created_timestamp,
            last_modified_timestamp, is_pro, first_seen_at
        ) VALUES (
            %(id)s, %(username)s, %(url)s, %(website)s, %(github)s, %(twitter)s,
            %(location)s, %(tagline)s, %(bio)s, %(avatar_mini)s, %(avatar_normal)s,
            %(avatar_large)s, %(created)s, %(last_modified)s, %(pro)s, %(first_seen_at)s
        ) ON DUPLICATE KEY UPDATE
            url = VALUES(url),
            website = VALUES(website),
            github = VALUES(github),
            twitter = VALUES(twitter),
            location = VALUES(location),
            tagline = VALUES(tagline),
            bio = VALUES(bio),
            avatar_mini = VALUES(avatar_mini),
            avatar_normal = VALUES(avatar_normal),
            avatar_large = VALUES(avatar_large),
            last_modified_timestamp = VALUES(last_modified_timestamp),
            is_pro = VALUES(is_pro)
        """
        
        if 'first_seen_at' not in sanitized_data:
            sanitized_data['first_seen_at'] = self.get_beijing_time()
        
        # 修复is_pro字段范围问题
        if 'pro' in sanitized_data:
            pro_value = sanitized_data['pro']
            if pro_value is None:
                sanitized_data['pro'] = 0
            else:
                # 确保值在TINYINT范围内 (0-255)
                sanitized_data['pro'] = min(max(int(pro_value), 0), 1)
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, sanitized_data)
            connection.commit()
    
    def batch_insert_users_by_username(self, usernames: List[str]):
        """批量插入只有用户名的用户数据"""
        if not usernames:
            return
        
        # 去重
        unique_usernames = list(set(usernames))
        beijing_time = self.get_beijing_time()
        
        # 准备批量数据
        user_data = []
        for username in unique_usernames:
            if username and len(username.strip()) > 0:
                user_data.append({
                    'username': username.strip()[:50],  # 限制长度
                    'first_seen_at': beijing_time
                })
        
        if not user_data:
            return
        
        sql = """
        INSERT IGNORE INTO v2ex_users (username, first_seen_at)
        VALUES (%(username)s, %(first_seen_at)s)
        """
        
        try:
            with self.get_cursor() as (cursor, connection):
                cursor.executemany(sql, user_data)
                affected_rows = cursor.rowcount
                connection.commit()
                self.logger.info(f"批量插入用户: {affected_rows}/{len(user_data)} 个用户")
                return affected_rows
        except Exception as e:
            self.logger.error(f"批量插入用户失败: {e}")
            # 如果批量失败，尝试逐个插入
            success_count = 0
            for user in user_data:
                try:
                    self.insert_or_update_user(user)
                    success_count += 1
                except Exception as single_e:
                    self.logger.warning(f"单个插入用户 {user['username']} 失败: {single_e}")
            self.logger.info(f"逐个插入用户完成: {success_count}/{len(user_data)}")
            return success_count
    
    def insert_or_update_topic(self, topic_data: Dict[str, Any]):
        """插入或更新主题数据"""
        sanitized_data = self._sanitize_topic_data(topic_data)
        
        sql = """
        INSERT INTO v2ex_topics (
            id, title, url, content, node_id, node_name,
            member_id, member_username, replies, last_reply_by, created_timestamp,
            last_touched_timestamp, last_modified_timestamp, is_deleted, crawled_at
        ) VALUES (
            %(id)s, %(title)s, %(url)s, %(content)s,
            %(node_id)s, %(node_name)s, %(member_id)s, %(member_username)s,
            %(replies)s, %(last_reply_by)s, %(created)s, %(last_touched)s,
            %(last_modified)s, %(deleted)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            content = VALUES(content),
            replies = VALUES(replies),
            last_reply_by = VALUES(last_reply_by),
            last_touched_timestamp = VALUES(last_touched_timestamp),
            last_modified_timestamp = VALUES(last_modified_timestamp),
            is_deleted = VALUES(is_deleted),
            crawled_at = VALUES(crawled_at)
        """
        
        if 'crawled_at' not in sanitized_data:
            sanitized_data['crawled_at'] = self.get_beijing_time()
        
        # 提取节点和用户信息，并移除嵌套dict
        if 'node' in sanitized_data and isinstance(sanitized_data['node'], dict):
            if 'node_id' not in sanitized_data:
                sanitized_data['node_id'] = sanitized_data['node'].get('id')
            if 'node_name' not in sanitized_data:
                sanitized_data['node_name'] = sanitized_data['node'].get('name')
            # 移除嵌套dict，避免数据库写入错误
            del sanitized_data['node']
        
        if 'member' in sanitized_data and isinstance(sanitized_data['member'], dict):
            if 'member_id' not in sanitized_data:
                sanitized_data['member_id'] = sanitized_data['member'].get('id')
            if 'member_username' not in sanitized_data:
                sanitized_data['member_username'] = sanitized_data['member'].get('username')
            # 移除嵌套dict，避免数据库写入错误
            del sanitized_data['member']
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, sanitized_data)
            connection.commit()
    
    def batch_insert_or_update_topics(self, topics_data: List[Dict[str, Any]]):
        """批量插入或更新主题数据"""
        if not topics_data:
            return
        
        beijing_time = self.get_beijing_time()
        sanitized_topics = []
        
        for topic_data in topics_data:
            sanitized = self._sanitize_topic_data(topic_data)
            if 'crawled_at' not in sanitized:
                sanitized['crawled_at'] = beijing_time
            
            # 提取嵌套的节点和用户信息，并移除嵌套dict
            if 'node' in sanitized and isinstance(sanitized['node'], dict):
                if 'node_id' not in sanitized:
                    sanitized['node_id'] = sanitized['node'].get('id')
                if 'node_name' not in sanitized:
                    sanitized['node_name'] = sanitized['node'].get('name')
                # 移除嵌套dict，避免数据库写入错误
                del sanitized['node']
            
            if 'member' in sanitized and isinstance(sanitized['member'], dict):
                if 'member_id' not in sanitized:
                    sanitized['member_id'] = sanitized['member'].get('id')
                if 'member_username' not in sanitized:
                    sanitized['member_username'] = sanitized['member'].get('username')
                # 移除嵌套dict，避免数据库写入错误
                del sanitized['member']
            
            sanitized_topics.append(sanitized)
        
        sql = """
        INSERT INTO v2ex_topics (
            id, title, url, content, node_id, node_name,
            member_id, member_username, replies, last_reply_by, created_timestamp,
            last_touched_timestamp, last_modified_timestamp, is_deleted, crawled_at
        ) VALUES (
            %(id)s, %(title)s, %(url)s, %(content)s,
            %(node_id)s, %(node_name)s, %(member_id)s, %(member_username)s,
            %(replies)s, %(last_reply_by)s, %(created)s, %(last_touched)s,
            %(last_modified)s, %(deleted)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            content = VALUES(content),
            replies = VALUES(replies),
            last_reply_by = VALUES(last_reply_by),
            last_touched_timestamp = VALUES(last_touched_timestamp),
            last_modified_timestamp = VALUES(last_modified_timestamp),
            is_deleted = VALUES(is_deleted),
            crawled_at = VALUES(crawled_at)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.executemany(sql, sanitized_topics)
            connection.commit()
            self.logger.info(f"批量插入/更新 {len(sanitized_topics)} 个主题")
    
    def get_topic_last_touched(self, topic_id: int) -> Optional[int]:
        """获取主题的最后活跃时间戳"""
        sql = "SELECT last_touched_timestamp FROM v2ex_topics WHERE id = %s"
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, (topic_id,))
            result = cursor.fetchone()
            return result['last_touched_timestamp'] if result else None
    
    def get_topics_last_touched_batch(self, topic_ids: List[int]) -> Dict[int, int]:
        """批量获取多个主题的最后活跃时间戳"""
        if not topic_ids:
            return {}
        
        # 构建IN查询
        placeholders = ','.join(['%s'] * len(topic_ids))
        sql = f"SELECT id, last_touched_timestamp FROM v2ex_topics WHERE id IN ({placeholders})"
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, topic_ids)
            results = cursor.fetchall()
            
            # 转换为字典格式
            return {row['id']: row['last_touched_timestamp'] for row in results if row['last_touched_timestamp'] is not None}
    
    def clean_old_data(self, retention_days: int) -> int:
        """清理过期数据"""
        # 计算保留的最早时间戳
        cutoff_timestamp = int((datetime.now() - timedelta(days=retention_days)).timestamp())
        
        sql = """
        DELETE FROM v2ex_topics 
        WHERE last_touched_timestamp < %s OR (last_touched_timestamp IS NULL AND created_timestamp < %s)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, (cutoff_timestamp, cutoff_timestamp))
            deleted_count = cursor.rowcount
            connection.commit()
            self.logger.info(f"清理了 {deleted_count} 个过期主题")
            return deleted_count
    
    def get_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        stats_sql = [
            ("users_count", "SELECT COUNT(*) as count FROM v2ex_users"),
            ("topics_count", "SELECT COUNT(*) as count FROM v2ex_topics"),
            ("today_topics", """
                SELECT COUNT(*) as count FROM v2ex_topics 
                WHERE DATE(crawled_at) = CURDATE()
            """),
            ("latest_activity", """
                SELECT FROM_UNIXTIME(MAX(last_touched_timestamp)) as latest 
                FROM v2ex_topics WHERE last_touched_timestamp IS NOT NULL
            """),
            ("oldest_activity", """
                SELECT FROM_UNIXTIME(MIN(created_timestamp)) as oldest 
                FROM v2ex_topics WHERE created_timestamp IS NOT NULL
            """)
        ]
        
        stats = {}
        with self.get_cursor() as (cursor, connection):
            for stat_name, sql in stats_sql:
                cursor.execute(sql)
                result = cursor.fetchone()
                if stat_name in ['latest_activity', 'oldest_activity']:
                    stats[stat_name] = result.get('latest' if 'latest' in result else 'oldest')
                else:
                    stats[stat_name] = result.get('count', 0)
        
        return stats
    
    def get_topics_need_update(self, node_names: List[str], hours_threshold: int = 1) -> List[int]:
        """获取需要更新的主题ID列表（基于最后活跃时间）"""
        if not node_names:
            return []
        
        placeholders = ','.join(['%s'] * len(node_names))
        sql = f"""
        SELECT id, last_touched_timestamp, replies 
        FROM v2ex_topics 
        WHERE node_name IN ({placeholders})
        AND (
            crawled_at < DATE_SUB(NOW(), INTERVAL %s HOUR)
            OR last_touched_timestamp > UNIX_TIMESTAMP(crawled_at)
        )
        ORDER BY last_touched_timestamp DESC
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, node_names + [hours_threshold])
            results = cursor.fetchall()
            return [row['id'] for row in results]
    
    def _sanitize_reply_data(self, reply_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证回复数据"""
        sanitized = reply_data.copy()
        
        if 'content' in sanitized and sanitized['content']:
            original = str(sanitized['content'])
            # 回复内容限制为1000字符
            max_chars = 1000
            if len(original) > max_chars:
                sanitized['content'] = original[:max_chars] + "...[回复被截断]"
                self.logger.warning(f"回复内容被截断: {len(original)} -> {max_chars} 字符")
        
        if 'member_username' in sanitized and sanitized['member_username']:
            original = str(sanitized['member_username'])
            sanitized['member_username'] = original[:50]
            if len(original) > 50:
                self.logger.warning(f"回复用户名被截断")
        
        return sanitized
    
    def insert_or_update_reply(self, reply_data: Dict[str, Any]):
        """插入或更新回复数据"""
        sanitized_data = self._sanitize_reply_data(reply_data)
        
        sql = """
        INSERT INTO v2ex_replies (
            id, topic_id, member_id, member_username, content,
            reply_floor, created_timestamp, last_modified_timestamp, thanks_count, crawled_at
        ) VALUES (
            %(id)s, %(topic_id)s, %(member_id)s, %(member_username)s, %(content)s,
            %(reply_floor)s, %(created)s, %(last_modified)s, %(thanks)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            content = VALUES(content),
            thanks_count = VALUES(thanks_count),
            crawled_at = VALUES(crawled_at)
        """
        
        if 'crawled_at' not in sanitized_data:
            sanitized_data['crawled_at'] = self.get_beijing_time()
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, sanitized_data)
            connection.commit()
    
    def batch_insert_or_update_replies(self, replies_data: List[Dict[str, Any]]):
        """批量插入或更新回复数据"""
        if not replies_data:
            return
        
        beijing_time = self.get_beijing_time()
        sanitized_replies = []
        
        for reply in replies_data:
            # 清理和验证回复数据
            sanitized = self._sanitize_reply_data(reply)
            
            if 'crawled_at' not in sanitized:
                sanitized['crawled_at'] = beijing_time
            
            # 处理嵌套的member信息，避免数据库写入错误
            if 'member' in sanitized and isinstance(sanitized['member'], dict):
                if 'member_id' not in sanitized:
                    sanitized['member_id'] = sanitized['member'].get('id')
                if 'member_username' not in sanitized:
                    sanitized['member_username'] = sanitized['member'].get('username')
                # 移除嵌套dict
                del sanitized['member']
            
            sanitized_replies.append(sanitized)
        
        sql = """
        INSERT INTO v2ex_replies (
            id, topic_id, member_id, member_username, content,
            reply_floor, created_timestamp, last_modified_timestamp, thanks_count, crawled_at
        ) VALUES (
            %(id)s, %(topic_id)s, %(member_id)s, %(member_username)s, %(content)s,
            %(reply_floor)s, %(created)s, %(last_modified)s, %(thanks)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            content = VALUES(content),
            thanks_count = VALUES(thanks_count),
            crawled_at = VALUES(crawled_at)
        """
        
        # 分批处理，每批500个回复
        batch_size = 500
        success_count = 0
        
        for i in range(0, len(sanitized_replies), batch_size):
            batch = sanitized_replies[i:i + batch_size]
            try:
                with self.get_cursor() as (cursor, connection):
                    cursor.executemany(sql, batch)
                    connection.commit()
                    success_count += len(batch)
                    self.logger.debug(f"批量保存回复进度: {success_count}/{len(sanitized_replies)}")
            except Exception as e:
                self.logger.error(f"批量保存回复失败 (批次 {i//batch_size + 1}): {e}")
                # 如果批量失败，尝试逐个保存
                for reply in batch:
                    try:
                        self.insert_or_update_reply(reply)
                        success_count += 1
                    except Exception as reply_e:
                        self.logger.error(f"保存回复 {reply.get('id', 'unknown')} 失败: {reply_e}")
        
        self.logger.info(f"批量插入/更新 {success_count}/{len(sanitized_replies)} 个回复")


# 全局数据库管理实例
db_manager = DatabaseManager()