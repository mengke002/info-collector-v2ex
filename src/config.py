import os
import configparser
from typing import Dict, Any
from dotenv import load_dotenv

class Config:
    """
    配置加载器，负责从环境变量、config.ini文件和默认值中读取配置。
    优先级：环境变量 > config.ini配置 > 默认值
    """
    def __init__(self):
        # 在本地开发环境中，可以加载.env文件
        load_dotenv()
        
        # 读取config.ini文件，从src目录向上一级到v2ex根目录
        self.config_parser = configparser.ConfigParser()
        self.config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
        
        # 如果config.ini文件存在，则读取
        if os.path.exists(self.config_file):
            try:
                self.config_parser.read(self.config_file, encoding='utf-8')
            except (configparser.Error, UnicodeDecodeError):
                pass
    
    def _get_config_value(self, section: str, key: str, env_var: str, default_value: Any, value_type=str) -> Any:
        """
        按优先级获取配置值：环境变量 > config.ini > 默认值
        """
        # 1. 优先检查环境变量
        env_value = os.getenv(env_var)
        if env_value:
            try:
                return value_type(env_value)
            except (ValueError, TypeError):
                return default_value
        
        # 2. 检查config.ini文件
        try:
            if self.config_parser.has_section(section) and self.config_parser.has_option(section, key):
                config_value = self.config_parser.get(section, key)
                try:
                    return value_type(config_value)
                except (ValueError, TypeError):
                    return default_value
        except (configparser.Error, UnicodeDecodeError):
            pass
        
        # 3. 返回默认值
        return default_value

    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        config = {
            'host': self._get_config_value('database', 'host', 'DB_HOST', None),
            'user': self._get_config_value('database', 'user', 'DB_USER', None),
            'password': self._get_config_value('database', 'password', 'DB_PASSWORD', None),
            'database': self._get_config_value('database', 'database', 'DB_NAME', None),
            'port': self._get_config_value('database', 'port', 'DB_PORT', 3306, int),
            'ssl_mode': self._get_config_value('database', 'ssl_mode', 'DB_SSL_MODE', 'disabled')
        }
        if not all([config['host'], config['user'], config['password'], config['database']]):
            raise ValueError("数据库核心配置 (host, user, password, database) 必须在环境变量或config.ini中设置。")
        return config

    def get_crawler_config(self) -> Dict[str, Any]:
        """获取爬虫配置"""
        return {
            'delay_seconds': self._get_config_value('crawler', 'delay_seconds', 'CRAWLER_DELAY_SECONDS', 1.0, float),
            'max_retries': self._get_config_value('crawler', 'max_retries', 'CRAWLER_MAX_RETRIES', 3, int),
            'timeout_seconds': self._get_config_value('crawler', 'timeout_seconds', 'CRAWLER_TIMEOUT_SECONDS', 30, int),
            'max_pages_per_node': self._get_config_value('crawler', 'max_pages_per_node', 'CRAWLER_MAX_PAGES_PER_NODE', 5, int),
            'fetch_replies': self._get_config_value('crawler', 'fetch_replies', 'CRAWLER_FETCH_REPLIES', True, lambda x: str(x).lower() == 'true'),
            'max_concurrent_nodes': self._get_config_value('crawler', 'max_concurrent_nodes', 'CRAWLER_MAX_CONCURRENT_NODES', 1, int),
            'max_concurrent_replies': self._get_config_value('crawler', 'max_concurrent_replies', 'CRAWLER_MAX_CONCURRENT_REPLIES', 10, int)
        }

    def get_data_retention_days(self) -> int:
        """获取数据保留天数"""
        return self._get_config_value('data_retention', 'days', 'DATA_RETENTION_DAYS', 90, int)

    def get_logging_config(self) -> Dict[str, str]:
        """获取日志配置"""
        return {
            'log_level': self._get_config_value('logging', 'log_level', 'LOGGING_LOG_LEVEL', 'INFO'),
            'log_file': self._get_config_value('logging', 'log_file', 'LOGGING_LOG_FILE', 'v2ex_crawler.log')
        }

    def get_target_nodes(self) -> Dict[str, str]:
        """获取目标节点配置"""
        # 1. 优先从环境变量读取
        env_targets_str = os.getenv('TARGETS')
        if env_targets_str:
            targets = self._parse_targets_string(env_targets_str, "TARGETS环境变量")
            if targets:
                return targets
        
        # 2. 从config.ini文件读取
        targets = self._parse_targets_from_config()
        if targets:
            return targets
        
        # 3. 使用默认值
        default_targets = {
            #'jobs': '酷工作',
            'create': '分享创造',
            #'openai': 'OpenAI',
            'ideas': '奇思妙想',
            #'business': '商业模式',
            'qna': '问与答',
            'programmer': '程序员'
        }
        return default_targets
    
    def _parse_targets_string(self, targets_str: str, source_name: str) -> Dict[str, str]:
        """解析目标节点字符串"""
        if not targets_str:
            return {}
        
        targets = {}
        try:
            targets_str = targets_str.strip('\'"')
            pairs = targets_str.split(';')
            for pair in pairs:
                if '=' in pair:
                    node, title = pair.split('=', 1)
                    if node.strip() and title.strip():
                        targets[node.strip()] = title.strip()
        except Exception as e:
            raise ValueError(f"解析{source_name}时出错: {e}")
        
        return targets
    
    def _parse_targets_from_config(self) -> Dict[str, str]:
        """从config.ini文件解析目标节点"""
        targets = {}
        try:
            if self.config_parser.has_section('targets'):
                for key, value in self.config_parser.items('targets'):
                    if key.strip() and value.strip():
                        targets[key.strip()] = value.strip()
        except (configparser.Error, UnicodeDecodeError):
            pass
        
        return targets
    
    def get_llm_config(self) -> Dict[str, Any]:
        """获取LLM配置, 并映射到通用键名"""
        return {
            'api_key': self._get_config_value('llm', 'openai_api_key', 'OPENAI_API_KEY', None),
            'model': self._get_config_value('llm', 'openai_model', 'OPENAI_MODEL', 'gpt-3.5-turbo'),
            'base_url': self._get_config_value('llm', 'openai_base_url', 'OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'max_content_length': self._get_config_value('llm', 'max_content_length', 'LLM_MAX_CONTENT_LENGTH', 50000, int),
            'max_parallel_reports': self._get_config_value('llm', 'max_parallel_reports', 'LLM_MAX_PARALLEL_REPORTS', 4, int)
        }
    
    def get_analysis_config(self) -> Dict[str, Any]:
        """获取分析配置"""
        return {
            'reply_weight': self._get_config_value('analysis', 'reply_weight', 'ANALYSIS_REPLY_WEIGHT', 5.0, float),
            'thanks_weight': self._get_config_value('analysis', 'thanks_weight', 'ANALYSIS_THANKS_WEIGHT', 3.0, float),
            'time_decay_hours': self._get_config_value('analysis', 'time_decay_hours', 'ANALYSIS_TIME_DECAY_HOURS', 168, int),
            'max_hotness_score': self._get_config_value('analysis', 'max_hotness_score', 'ANALYSIS_MAX_HOTNESS_SCORE', 999999.0, float)
        }

    def get_report_config(self) -> Dict[str, Any]:
        """获取报告配置"""
        return {
            'hours_back': self._get_config_value('report', 'hours_back', 'HOURS_BACK', 48, int),
            'top_topics_per_node': self._get_config_value(
                'report',
                'top_topics_per_node',
                'REPORT_TOP_TOPICS_PER_NODE',
                50,
                int
            )
        }

    def get_notion_config(self) -> Dict[str, Any]:
        """获取Notion配置"""
        return {
            'integration_token': self._get_config_value('notion', 'integration_token', 'NOTION_INTEGRATION_TOKEN', None),
            'parent_page_id': self._get_config_value('notion', 'parent_page_id', 'NOTION_PARENT_PAGE_ID', None)
        }

    def get_report_nodes(self) -> list:
        """
        获取报告生成的节点列表（不包括被注释的节点）
        返回节点名称列表，用于GitHub Actions等自动化场景
        """
        target_nodes = self.get_target_nodes()
        return list(target_nodes.keys())


# 创建一个全局配置实例
config = Config()
