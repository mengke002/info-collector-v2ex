"""
日志配置模块
负责配置和管理系统日志
"""
import logging
import logging.handlers
import os
from datetime import datetime, timezone, timedelta

from .config import config


def get_beijing_time():
    """获取北京时间（UTC+8）"""
    utc_time = datetime.now(timezone.utc)
    beijing_time = utc_time + timedelta(hours=8)
    return beijing_time.replace(tzinfo=None)


def setup_logging():
    """设置日志配置"""
    log_config = config.get_logging_config()
    log_level_str = log_config.get('log_level', 'INFO').upper().strip()
    
    # 验证日志级别是否有效
    valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if not log_level_str or log_level_str not in valid_log_levels:
        print(f"警告: 无效的日志级别 '{log_config.get('log_level', '')}'，使用默认级别 'INFO'")
        log_level_str = 'INFO'
    
    log_level = getattr(logging, log_level_str)
    log_file = log_config.get('log_file', 'crawler.log').strip()
    
    # 验证日志文件名是否有效
    if not log_file or log_file == '.' or log_file == '/':
        print(f"警告: 无效的日志文件名 '{log_config.get('log_file', '')}'，使用默认文件名 'crawler.log'")
        log_file = 'crawler.log'
    
    # 创建日志目录
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.path.join(log_dir, log_file)
    
    # 配置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 文件处理器（带轮转）
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 记录启动信息
    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    logger.info("Linux.do论坛自动化数据运维系统启动")
    logger.info(f"日志级别: {log_config.get('log_level', 'INFO')}")
    logger.info(f"日志文件: {log_file_path}")
    logger.info("=" * 50)


def log_task_start(task_name: str):
    """记录任务开始"""
    logger = logging.getLogger('task')
    logger.info(f"任务开始: {task_name}")
    return get_beijing_time()


def log_task_end(task_name: str, start_time: datetime, **kwargs):
    """记录任务结束"""
    logger = logging.getLogger('task')
    end_time = get_beijing_time()
    duration = (end_time - start_time).total_seconds()
    
    log_msg = f"任务完成: {task_name}, 耗时: {duration:.2f}秒"
    
    # 添加额外信息
    if kwargs:
        extra_info = ", ".join([f"{k}: {v}" for k, v in kwargs.items()])
        log_msg += f", {extra_info}"
    
    logger.info(log_msg)
    return end_time, duration


def log_error(task_name: str, error: Exception, **kwargs):
    """记录错误"""
    logger = logging.getLogger('error')
    log_msg = f"任务失败: {task_name}, 错误: {str(error)}"
    
    if kwargs:
        extra_info = ", ".join([f"{k}: {v}" for k, v in kwargs.items()])
        log_msg += f", {extra_info}"
    
    logger.error(log_msg, exc_info=True)