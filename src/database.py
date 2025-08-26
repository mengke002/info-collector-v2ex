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
            # Per user request, limit topic content to a safe length for TEXT fields.
            max_chars = 16000
            suffix = "...[内容被截断]"
            if len(original) > max_chars:
                sanitized['content'] = original[:max_chars - len(suffix)] + suffix
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
                connect_timeout=30,  # 连接超时30秒
                read_timeout=120,    # 读取超时120秒（增加到2分钟）
                write_timeout=60     # 写入超时60秒
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
                username VARCHAR(50) UNIQUE NOT NULL COMMENT '用户名',
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '首次采集时间',
                
                INDEX idx_username (username)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """,
            """
            CREATE TABLE IF NOT EXISTS v2ex_topics (
                id INT PRIMARY KEY COMMENT '主题ID',
                title VARCHAR(500) NOT NULL COMMENT '主题标题',
                url VARCHAR(500) UNIQUE NOT NULL COMMENT '主题URL',
                content TEXT COMMENT '主题内容(Markdown格式)',
                node_name VARCHAR(50) COMMENT '所属节点名称',
                member_username VARCHAR(50) COMMENT '作者用户名',
                replies SMALLINT UNSIGNED DEFAULT 0 COMMENT '回复数',
                created_timestamp INT UNSIGNED NOT NULL COMMENT '创建时间戳',
                last_touched_timestamp INT UNSIGNED COMMENT '最后活跃时间戳',
                last_modified_timestamp INT UNSIGNED COMMENT '最后修改时间戳',
                is_deleted TINYINT DEFAULT 0 COMMENT '是否已删除',
                total_thanks_count INT UNSIGNED DEFAULT 0 COMMENT '主题下所有回复的总感谢数',
                hotness_score DECIMAL(10, 4) DEFAULT 0.0 COMMENT '热度分数，基于回复数、感谢数和时间衰减',
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
                
                INDEX idx_node (node_name),
                INDEX idx_member (member_username),
                INDEX idx_created (created_timestamp),
                INDEX idx_last_touched (last_touched_timestamp),
                INDEX idx_replies (replies),
                INDEX idx_hotness_score (hotness_score)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """,
            """
            CREATE TABLE IF NOT EXISTS v2ex_replies (
                id INT PRIMARY KEY COMMENT '回复ID',
                topic_id INT NOT NULL COMMENT '所属主题ID',
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
                FOREIGN KEY (topic_id) REFERENCES v2ex_topics(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """,
            """
            CREATE TABLE IF NOT EXISTS v2ex_reports (
                id MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '报告唯一ID',
                node_name VARCHAR(50) NOT NULL COMMENT '分析的节点名称',
                report_type ENUM('hotspot', 'trend', 'summary') DEFAULT 'hotspot' COMMENT '报告类型',
                analysis_period_start TIMESTAMP NOT NULL COMMENT '分析数据的起始时间',
                analysis_period_end TIMESTAMP NOT NULL COMMENT '分析数据的结束时间',
                topics_analyzed SMALLINT UNSIGNED DEFAULT 0 COMMENT '分析的主题数量',
                report_title VARCHAR(200) NOT NULL COMMENT '报告标题',
                report_content MEDIUMTEXT NOT NULL COMMENT '报告内容(Markdown格式)',
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '报告生成时间',
                
                INDEX idx_node_name (node_name),
                INDEX idx_generated_at (generated_at),
                INDEX idx_analysis_period (analysis_period_start, analysis_period_end)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """
        ]
        
        with self.get_cursor() as (cursor, connection):
            for sql in create_tables_sql:
                cursor.execute(sql)
                
            # 升级现有表结构：为v2ex_topics表添加热度相关字段
            try:
                cursor.execute("SHOW COLUMNS FROM v2ex_topics LIKE 'total_thanks_count'")
                if cursor.rowcount == 0:
                    cursor.execute("ALTER TABLE v2ex_topics ADD COLUMN total_thanks_count INT UNSIGNED DEFAULT 0 COMMENT '主题下所有回复的总感谢数'")
                    self.logger.info("已为v2ex_topics表添加total_thanks_count字段")
                    
                cursor.execute("SHOW COLUMNS FROM v2ex_topics LIKE 'hotness_score'")
                if cursor.rowcount == 0:
                    cursor.execute("ALTER TABLE v2ex_topics ADD COLUMN hotness_score DECIMAL(10, 4) DEFAULT 0.0 COMMENT '热度分数，基于回复数、感谢数和时间衰减'")
                    cursor.execute("ALTER TABLE v2ex_topics ADD INDEX idx_hotness_score (hotness_score)")
                    self.logger.info("已为v2ex_topics表添加hotness_score字段和索引")
            except Exception as e:
                self.logger.warning(f"升级表结构时出错: {e}")
            
            connection.commit()
            self.logger.info("数据库表结构初始化完成")
    
    def insert_or_update_user(self, user_data: Dict[str, Any]):
        """插入或更新用户数据（简化版，只保留有效字段）"""
        sanitized_data = self._sanitize_user_data(user_data)
        
        # 简化的用户插入方式，只保留username和first_seen_at
        sql = """
        INSERT IGNORE INTO v2ex_users (username, first_seen_at)
        VALUES (%(username)s, %(first_seen_at)s)
        """
        
        if 'first_seen_at' not in sanitized_data:
            sanitized_data['first_seen_at'] = self.get_beijing_time()
        
        # 只保留必要的字段
        minimal_data = {
            'username': sanitized_data.get('username', ''),
            'first_seen_at': sanitized_data['first_seen_at']
        }
        
        if not minimal_data['username']:
            self.logger.warning("用户名为空，跳过插入")
            return
        
        try:
            with self.get_cursor() as (cursor, connection):
                cursor.execute(sql, minimal_data)
                connection.commit()
                self.logger.debug(f"插入用户: {minimal_data['username']}")
        except Exception as e:
            self.logger.warning(f"插入用户 {minimal_data['username']} 失败: {e}")
    
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
        """插入或更新主题数据（简化版，删除无效字段）"""
        sanitized_data = self._sanitize_topic_data(topic_data)
        
        sql = """
        INSERT INTO v2ex_topics (
            id, title, url, content, node_name, member_username, replies,
            created_timestamp, last_touched_timestamp, last_modified_timestamp,
            is_deleted, crawled_at
        ) VALUES (
            %(id)s, %(title)s, %(url)s, %(content)s, %(node_name)s, %(member_username)s,
            %(replies)s, %(created)s, %(last_touched)s, %(last_modified)s,
            %(deleted)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            content = VALUES(content),
            replies = VALUES(replies),
            last_touched_timestamp = VALUES(last_touched_timestamp),
            last_modified_timestamp = VALUES(last_modified_timestamp),
            is_deleted = VALUES(is_deleted),
            crawled_at = VALUES(crawled_at)
        """
        
        if 'crawled_at' not in sanitized_data:
            sanitized_data['crawled_at'] = self.get_beijing_time()
        
        # 提取节点和用户信息，并移除嵌套dict
        if 'node' in sanitized_data and isinstance(sanitized_data['node'], dict):
            if 'node_name' not in sanitized_data:
                sanitized_data['node_name'] = sanitized_data['node'].get('name')
            # 移除嵌套dict，避免数据库写入错误
            del sanitized_data['node']
        
        if 'member' in sanitized_data and isinstance(sanitized_data['member'], dict):
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
                if 'node_name' not in sanitized:
                    sanitized['node_name'] = sanitized['node'].get('name')
                # 移除嵌套dict，避免数据库写入错误
                del sanitized['node']
            
            if 'member' in sanitized and isinstance(sanitized['member'], dict):
                if 'member_username' not in sanitized:
                    sanitized['member_username'] = sanitized['member'].get('username')
                # 移除嵌套dict，避免数据库写入错误
                del sanitized['member']
            
            sanitized_topics.append(sanitized)
        
        sql = """
        INSERT INTO v2ex_topics (
            id, title, url, content, node_name, member_username, replies,
            created_timestamp, last_touched_timestamp, last_modified_timestamp,
            is_deleted, crawled_at
        ) VALUES (
            %(id)s, %(title)s, %(url)s, %(content)s, %(node_name)s, %(member_username)s,
            %(replies)s, %(created)s, %(last_touched)s, %(last_modified)s,
            %(deleted)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            content = VALUES(content),
            replies = VALUES(replies),
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
            # 回复内容限制为3000字符
            max_chars = 3000
            suffix = "...[回复被截断]"
            if len(original) > max_chars:
                sanitized['content'] = original[:max_chars - len(suffix)] + suffix
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
            id, topic_id, member_username, content, reply_floor,
            created_timestamp, last_modified_timestamp, thanks_count, crawled_at
        ) VALUES (
            %(id)s, %(topic_id)s, %(member_username)s, %(content)s, %(reply_floor)s,
            %(created)s, %(last_modified)s, %(thanks)s, %(crawled_at)s
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
                if 'member_username' not in sanitized:
                    sanitized['member_username'] = sanitized['member'].get('username')
                # 移除嵌套dict
                del sanitized['member']
            
            sanitized_replies.append(sanitized)
        
        sql = """
        INSERT INTO v2ex_replies (
            id, topic_id, member_username, content, reply_floor,
            created_timestamp, last_modified_timestamp, thanks_count, crawled_at
        ) VALUES (
            %(id)s, %(topic_id)s, %(member_username)s, %(content)s, %(reply_floor)s,
            %(created)s, %(last_modified)s, %(thanks)s, %(crawled_at)s
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
    
    def update_total_thanks(self, topic_ids: List[int] = None) -> int:
        """
        更新主题的总感谢数
        
        Args:
            topic_ids: 指定要更新的主题ID列表，None表示更新所有主题
            
        Returns:
            更新的主题数量
        """
        if topic_ids:
            # 更新指定主题
            placeholders = ','.join(['%s'] * len(topic_ids))
            sql = f"""
            UPDATE v2ex_topics 
            SET total_thanks_count = (
                SELECT COALESCE(SUM(thanks_count), 0) 
                FROM v2ex_replies 
                WHERE topic_id = v2ex_topics.id
            )
            WHERE id IN ({placeholders})
            """
            params = topic_ids
        else:
            # 更新所有主题
            sql = """
            UPDATE v2ex_topics 
            SET total_thanks_count = (
                SELECT COALESCE(SUM(thanks_count), 0) 
                FROM v2ex_replies 
                WHERE topic_id = v2ex_topics.id
            )
            """
            params = []
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, params)
            updated_count = cursor.rowcount
            connection.commit()
            return updated_count
    
    def update_hotness_scores(self, topic_ids: List[int] = None,
                            reply_weight: float = 5.0,
                            thanks_weight: float = 3.0,
                            time_decay_hours: int = 168,
                            max_score: float = 999999.0) -> int:
        """
        更新主题的热度分数
        
        Args:
            topic_ids: 指定要更新的主题ID列表，None表示更新所有主题
            reply_weight: 回复数权重
            thanks_weight: 感谢数权重
            time_decay_hours: 时间衰减周期（小时）
            max_score: 热度分数最大值限制
            
        Returns:
            更新的主题数量
        """
        # 计算时间衰减因子
        current_timestamp = int(datetime.now().timestamp())
        
        if topic_ids:
            placeholders = ','.join(['%s'] * len(topic_ids))
            where_clause = f"WHERE id IN ({placeholders})"
            params = [reply_weight, thanks_weight, current_timestamp, time_decay_hours, max_score] + topic_ids
        else:
            where_clause = ""
            params = [reply_weight, thanks_weight, current_timestamp, time_decay_hours, max_score]
        
        sql = f"""
        UPDATE v2ex_topics 
        SET hotness_score = LEAST(
            (
                (replies * %s + total_thanks_count * %s) * 
                GREATEST(0.1, 1.0 - ((%s - last_touched_timestamp) / (%s * 3600)))
            ),
            %s
        )
        {where_clause}
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, params)
            updated_count = cursor.rowcount
            connection.commit()
            return updated_count
    
    def get_recent_active_topics(self, hours_back: int = 24, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        获取最近活跃的主题
        
        Args:
            hours_back: 回溯的小时数
            limit: 返回的最大主题数量，防止数据量过大导致性能问题
            
        Returns:
            主题列表
        """
        cutoff_timestamp = int((datetime.now() - timedelta(hours=hours_back)).timestamp())
        
        # 优化查询：添加LIMIT限制，使用更精确的字段选择，利用索引优化
        sql = """
        SELECT id, title, url, node_name, member_username, replies, 
               created_timestamp, last_touched_timestamp, total_thanks_count, hotness_score
        FROM v2ex_topics 
        WHERE last_touched_timestamp >= %s 
        ORDER BY last_touched_timestamp DESC
        LIMIT %s
        """
        
        try:
            with self.get_cursor() as (cursor, connection):
                self.logger.debug(f"查询最近 {hours_back} 小时活跃主题，时间戳阈值: {cutoff_timestamp}，限制: {limit}")
                cursor.execute(sql, (cutoff_timestamp, limit))
                results = cursor.fetchall()
                self.logger.debug(f"查询完成，找到 {len(results)} 个活跃主题")
                return results
        except Exception as e:
            self.logger.error(f"查询最近活跃主题失败: {e}")
            # 如果查询失败，尝试一个更保守的查询
            fallback_sql = """
            SELECT id, title, node_name, replies, last_touched_timestamp
            FROM v2ex_topics 
            WHERE last_touched_timestamp >= %s 
            ORDER BY last_touched_timestamp DESC
            LIMIT %s
            """
            try:
                with self.get_cursor() as (cursor, connection):
                    cursor.execute(fallback_sql, (cutoff_timestamp, min(limit, 500)))
                    results = cursor.fetchall()
                    self.logger.warning(f"使用备用查询，找到 {len(results)} 个活跃主题")
                    return results
            except Exception as fallback_e:
                self.logger.error(f"备用查询也失败: {fallback_e}")
                return []
    
    def get_hot_topics_by_node(self, node_name: str = None, limit: int = 30, 
                              period_hours: int = 24) -> List[Dict[str, Any]]:
        """
        获取指定节点或全站的热门主题
        
        Args:
            node_name: 节点名称，None表示全站
            limit: 返回主题数量限制
            period_hours: 时间范围（小时）
            
        Returns:
            热门主题列表，按热度分数降序排列
        """
        cutoff_timestamp = int((datetime.now() - timedelta(hours=period_hours)).timestamp())
        
        if node_name:
            sql = """
            SELECT * FROM v2ex_topics 
            WHERE node_name = %s 
            AND last_touched_timestamp >= %s
            AND hotness_score > 0
            ORDER BY hotness_score DESC, last_touched_timestamp DESC
            LIMIT %s
            """
            params = (node_name, cutoff_timestamp, limit)
        else:
            sql = """
            SELECT * FROM v2ex_topics 
            WHERE last_touched_timestamp >= %s
            AND hotness_score > 0
            ORDER BY hotness_score DESC, last_touched_timestamp DESC
            LIMIT %s
            """
            params = (cutoff_timestamp, limit)
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, params)
            return cursor.fetchall()
    
    def get_topic_with_replies(self, topic_id: int, reply_limit: int = 10) -> Optional[Dict[str, Any]]:
        """
        获取主题及其高感谢回复
        
        Args:
            topic_id: 主题ID
            reply_limit: 回复数量限制
            
        Returns:
            包含主题和回复的字典
        """
        # 获取主题信息
        topic_sql = "SELECT * FROM v2ex_topics WHERE id = %s"
        
        # 获取高感谢回复
        replies_sql = """
        SELECT * FROM v2ex_replies 
        WHERE topic_id = %s 
        ORDER BY thanks_count DESC, created_timestamp ASC
        LIMIT %s
        """
        
        with self.get_cursor() as (cursor, connection):
            # 获取主题
            cursor.execute(topic_sql, (topic_id,))
            topic = cursor.fetchone()
            if not topic:
                return None
            
            # 获取回复
            cursor.execute(replies_sql, (topic_id, reply_limit))
            replies = cursor.fetchall()
            
            return {
                'topic': topic,
                'replies': replies
            }

    def get_topics_with_replies_batch(self, topic_ids: List[int], reply_limit: int = 10) -> List[Dict[str, Any]]:
        """
        批量获取多个主题及其高感谢回复，解决N+1问题
        
        Args:
            topic_ids: 主题ID列表
            reply_limit: 每个主题的回复数量限制
            
        Returns:
            包含主题和回复的字典列表
        """
        if not topic_ids:
            return []

        placeholders = ','.join(['%s'] * len(topic_ids))
        
        # 批量获取主题信息
        topics_sql = f"SELECT * FROM v2ex_topics WHERE id IN ({placeholders})"
        
        # 批量获取高感谢回复
        # 注意：这里的LIMIT不是每个topic限制，而是总数限制。我们需要在Python中处理
        # 使用窗口函数是更优的方案，但需要MySQL 8.0+。这里采用兼容性更好的方法。
        replies_sql = f"""
        SELECT * FROM v2ex_replies 
        WHERE topic_id IN ({placeholders})
        ORDER BY topic_id, thanks_count DESC, created_timestamp ASC
        """

        with self.get_cursor() as (cursor, connection):
            # 获取所有相关主题
            cursor.execute(topics_sql, topic_ids)
            topics = cursor.fetchall()
            topics_map = {topic['id']: topic for topic in topics}

            # 获取所有相关回复
            cursor.execute(replies_sql, topic_ids)
            replies = cursor.fetchall()
            
            # 在Python中分组回复并限制数量
            replies_map = {}
            for reply in replies:
                topic_id = reply['topic_id']
                if topic_id not in replies_map:
                    replies_map[topic_id] = []
                if len(replies_map[topic_id]) < reply_limit:
                    replies_map[topic_id].append(reply)

            # 组装最终结果，并保持原始topic_ids的顺序
            results = []
            for topic_id in topic_ids:
                if topic_id in topics_map:
                    results.append({
                        'topic': topics_map[topic_id],
                        'replies': replies_map.get(topic_id, [])
                    })
            
            return results
    
    def insert_report(self, report_data: Dict[str, Any]):
        """
        插入分析报告
        
        Args:
            report_data: 报告数据
        """
        sql = """
        INSERT INTO v2ex_reports (
            node_name, report_type, analysis_period_start, analysis_period_end,
            topics_analyzed, report_title, report_content, generated_at
        ) VALUES (
            %(node_name)s, %(report_type)s, %(analysis_period_start)s, %(analysis_period_end)s,
            %(topics_analyzed)s, %(report_title)s, %(report_content)s, %(generated_at)s
        )
        """
        
        if 'generated_at' not in report_data:
            report_data['generated_at'] = self.get_beijing_time()
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, report_data)
            connection.commit()
            return cursor.lastrowid


    def get_table_diagnostic_info(self) -> Dict[str, Any]:
        """
        获取数据库表的诊断信息，包括表大小、索引信息等
        用于诊断性能问题
        
        Returns:
            诊断信息字典
        """
        try:
            with self.get_cursor() as (cursor, connection):
                diagnostic_info = {}
                
                # 获取表记录数
                cursor.execute("SELECT COUNT(*) as total_topics FROM v2ex_topics")
                diagnostic_info['total_topics'] = cursor.fetchone()['total_topics']
                
                cursor.execute("SELECT COUNT(*) as total_replies FROM v2ex_replies")
                diagnostic_info['total_replies'] = cursor.fetchone()['total_replies']
                
                cursor.execute("SELECT COUNT(*) as total_users FROM v2ex_users")
                diagnostic_info['total_users'] = cursor.fetchone()['total_users']
                
                # 获取有last_touched_timestamp的主题数
                cursor.execute("SELECT COUNT(*) as topics_with_timestamp FROM v2ex_topics WHERE last_touched_timestamp IS NOT NULL")
                diagnostic_info['topics_with_timestamp'] = cursor.fetchone()['topics_with_timestamp']
                
                # 获取最新和最旧的主题时间
                cursor.execute("""
                    SELECT 
                        MIN(last_touched_timestamp) as oldest_timestamp,
                        MAX(last_touched_timestamp) as newest_timestamp
                    FROM v2ex_topics 
                    WHERE last_touched_timestamp IS NOT NULL
                """)
                timestamp_result = cursor.fetchone()
                if timestamp_result:
                    diagnostic_info['oldest_timestamp'] = timestamp_result['oldest_timestamp']
                    diagnostic_info['newest_timestamp'] = timestamp_result['newest_timestamp']
                
                # 获取最近24小时的主题数（用于验证查询逻辑）
                cutoff_24h = int((datetime.now() - timedelta(hours=24)).timestamp())
                cursor.execute(
                    "SELECT COUNT(*) as recent_24h_topics FROM v2ex_topics WHERE last_touched_timestamp >= %s", 
                    (cutoff_24h,)
                )
                diagnostic_info['recent_24h_topics'] = cursor.fetchone()['recent_24h_topics']
                
                # 获取表大小信息
                cursor.execute("SHOW TABLE STATUS LIKE 'v2ex_topics'")
                table_status = cursor.fetchone()
                if table_status:
                    diagnostic_info['table_rows'] = table_status.get('Rows', 0)
                    diagnostic_info['table_data_length'] = table_status.get('Data_length', 0)
                    diagnostic_info['table_index_length'] = table_status.get('Index_length', 0)
                
                # 检查索引状况
                cursor.execute("SHOW INDEX FROM v2ex_topics")
                indexes = cursor.fetchall()
                diagnostic_info['indexes'] = [idx['Key_name'] for idx in indexes]
                
                return diagnostic_info
                
        except Exception as e:
            self.logger.error(f"获取诊断信息失败: {e}")
            return {'error': str(e)}
    
    def optimize_table_performance(self):
        """
        优化表性能，添加缺失的索引
        """
        try:
            with self.get_cursor() as (cursor, connection):
                # 检查并添加缺失的索引
                optimizations = [
                    ("idx_last_touched_timestamp", "ALTER TABLE v2ex_topics ADD INDEX idx_last_touched_timestamp (last_touched_timestamp)"),
                    ("idx_created_timestamp", "ALTER TABLE v2ex_topics ADD INDEX idx_created_timestamp (created_timestamp)"),
                    ("idx_combined_time", "ALTER TABLE v2ex_topics ADD INDEX idx_combined_time (last_touched_timestamp, created_timestamp)")
                ]
                
                # 检查已存在的索引
                cursor.execute("SHOW INDEX FROM v2ex_topics")
                existing_indexes = {idx['Key_name'] for idx in cursor.fetchall()}
                
                for index_name, sql in optimizations:
                    if index_name not in existing_indexes:
                        try:
                            cursor.execute(sql)
                            connection.commit()
                            self.logger.info(f"添加索引 {index_name} 成功")
                        except Exception as idx_e:
                            if "Duplicate key name" not in str(idx_e):
                                self.logger.warning(f"添加索引 {index_name} 失败: {idx_e}")
                
                # 分析表以更新统计信息
                cursor.execute("ANALYZE TABLE v2ex_topics")
                self.logger.info("表分析完成")
                
        except Exception as e:
            self.logger.error(f"优化表性能失败: {e}")


# 全局数据库管理实例
db_manager = DatabaseManager()