"""
LLM客户端模块
支持OpenAI compatible接口的streaming实现
"""
import logging
from typing import Dict, Any
import httpx
import json

from .config import config

class LLMClient:
    """通用的LLM客户端，支持OpenAI compatible接口"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('api_key')
        self.base_url = llm_config.get('base_url')
        self.model = llm_config.get('model')
        
        if not all([self.api_key, self.base_url, self.model]):
            raise ValueError("LLM配置不完整 (api_key, base_url, model), 请检查config.ini")

        self.http_client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=360.0 
        )
        self.logger.info(f"LLM客户端初始化成功 - Model: {self.model}, Base URL: {self.base_url}")

    def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """
        使用streaming方式分析内容
        
        Args:
            content: 要分析的原始内容
            prompt_template: 包含 {content} 占位符的提示词模板
        
        Returns:
            分析结果字典
        """
        full_response_content = ""
        chunk_count = 0
        try:
            final_prompt = prompt_template.format(content=content)
            
            self.logger.info("开始LLM内容分析...")
            self.logger.info(f"内容长度: {len(content)} 字符")
            self.logger.debug(f"最终提示词长度: {len(final_prompt)} 字符")

            request_data = {
                "model": self.model,
                "messages": [
                    {"role": "user", "content": final_prompt}
                ],
                "stream": True,
                "temperature": 0.3,
            }
            
            self.logger.info("开始streaming响应处理...")
            with self.http_client.stream("POST", "/chat/completions", json=request_data) as response:
                response.raise_for_status()
                for line in response.iter_lines():
                    if line.startswith('data: '):
                        line_data = line[len('data: '):]
                        if line_data.strip() == '[DONE]':
                            break
                        try:
                            chunk = json.loads(line_data)
                            if 'choices' in chunk and chunk['choices']:
                                delta = chunk['choices'][0].get('delta', {})
                                content_part = delta.get('content')
                                if content_part:
                                    full_response_content += content_part
                                    chunk_count += 1
                        except json.JSONDecodeError:
                            self.logger.warning(f"无法解析的JSON chunk: {line_data}")

            self.logger.info("LLM分析完成")
            self.logger.info(f"处理了 {chunk_count} 个 chunks")
            self.logger.info(f"最终内容长度: {len(full_response_content)} 字符")
            
            return {
                'success': True,
                'analysis': full_response_content,
                'provider': 'custom_llm',
                'model': self.model
            }
        except httpx.RemoteProtocolError as e:
            self.logger.warning(f"LLM连接被提前关闭，但已接收部分数据: {e}")
            if full_response_content:
                self.logger.info(f"将使用已接收的部分内容({len(full_response_content)}字符)作为结果。")
                return {
                    'success': True,
                    'partial': True,
                    'analysis': full_response_content,
                    'error': f"Incomplete response from LLM: {e}",
                    'provider': 'custom_llm',
                    'model': self.model
                }
            else:
                error_msg = f"LLM客户端连接错误，未收到任何数据: {e}"
                self.logger.error(error_msg, exc_info=True)
                return {'success': False, 'error': error_msg}
        except httpx.HTTPStatusError as e:
            error_body = e.response.text
            error_msg = f"LLM API请求失败: {e.response.status_code} - {error_body}"
            self.logger.error(error_msg)
            return {'success': False, 'error': error_msg}
        except Exception as e:
            error_msg = f"LLM客户端未知错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {'success': False, 'error': error_msg}

# 全局LLM客户端实例
try:
    llm_client = LLMClient()
except Exception as e:
    logging.getLogger(__name__).warning(f"LLM客户端初始化失败: {e}")
    llm_client = None
