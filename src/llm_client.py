"""
LLM客户端模块
支持OpenAI compatible接口的streaming实现，包含重试机制
"""
import logging
import time
from typing import Dict, Any, List, Optional
import httpx
import json

from .config import config

class LLMClient:
    """通用的LLM客户端，支持OpenAI compatible接口和重试机制"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('api_key')
        self.base_url = llm_config.get('base_url')
        self.model = llm_config.get('model')
        models = llm_config.get('models') or []
        self.models = [m for m in models if isinstance(m, str) and m.strip()]
        if not self.models:
            if self.model:
                self.models = [self.model]
            else:
                self.models = []
        elif not self.model:
            self.model = self.models[0]

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
        if len(self.models) > 1:
            self.logger.info(
                "LLM客户端初始化成功 - Models: %s, Base URL: %s",
                ", ".join(self.models),
                self.base_url
            )
        else:
            self.logger.info(
                "LLM客户端初始化成功 - Model: %s, Base URL: %s",
                self.model,
                self.base_url
            )

    def analyze_content(
        self,
        content: str,
        prompt_template: str,
        max_retries: int = 3,
        model_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        使用streaming方式分析内容，支持重试机制

        Args:
            content: 要分析的原始内容
            prompt_template: 包含 {content} 占位符的提示词模板
            max_retries: 最大重试次数
            model_override: 指定使用的模型名称

        Returns:
            分析结果字典
        """
        final_prompt = prompt_template.format(content=content)
        models_to_try: List[str]
        if model_override:
            models_to_try = [model_override]
        else:
            models_to_try = self.models if self.models else ([self.model] if self.model else [])

        if not models_to_try:
            return {'success': False, 'error': '未配置可用的LLM模型'}

        last_response: Dict[str, Any] = {'success': False, 'error': '所有模型均尝试失败'}

        for model_name in models_to_try:
            result = self._call_model_with_retries(model_name, final_prompt, len(content), max_retries)
            if result.get('success'):
                return result
            last_response = result
            self.logger.warning(f"模型 {model_name} 调用失败，将尝试下一个模型。错误: {result.get('error')}")

        return last_response

    def _call_model_with_retries(
        self,
        model_name: str,
        prompt: str,
        content_length: int,
        max_retries: int
    ) -> Dict[str, Any]:
        """封装带重试的模型调用逻辑"""
        last_error: Dict[str, Any] = {'success': False, 'error': '未知错误'}

        for attempt in range(max_retries):
            try:
                self.logger.info(f"调用LLM: {model_name} (尝试 {attempt + 1}/{max_retries})")
                self.logger.info(f"内容长度: {content_length} 字符")
                self.logger.info(f"提示词长度: {len(prompt)} 字符")

                full_response_content = ""
                chunk_count = 0

                request_data = {
                    "model": model_name,
                    "messages": [
                        {"role": "user", "content": prompt}
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
                                self.logger.debug(f"无法解析的JSON chunk: {line_data}")

                self.logger.info(f"LLM调用完成 - 处理了 {chunk_count} 个chunks")
                self.logger.info(f"响应内容长度: {len(full_response_content)} 字符")

                if not full_response_content.strip():
                    raise ValueError("LLM返回空响应")

                return {
                    'success': True,
                    'analysis': full_response_content,
                    'provider': 'custom_llm',
                    'model': model_name,
                    'attempt': attempt + 1
                }

            except httpx.RemoteProtocolError as e:
                self.logger.warning(f"LLM连接被提前关闭 (尝试 {attempt + 1}/{max_retries}): {e}")
                if full_response_content:
                    self.logger.info(f"将使用已接收的部分内容({len(full_response_content)}字符)作为结果。")
                    return {
                        'success': True,
                        'partial': True,
                        'analysis': full_response_content,
                        'error': f"Incomplete response from LLM: {e}",
                        'provider': 'custom_llm',
                        'model': model_name,
                        'attempt': attempt + 1
                    }
                elif attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"LLM客户端连接错误，未收到任何数据: {e}"
                    self.logger.error(error_msg, exc_info=True)
                    last_error = {'success': False, 'error': error_msg, 'model': model_name}

            except httpx.HTTPStatusError as e:
                error_body = e.response.text
                error_msg = f"LLM API请求失败 (尝试 {attempt + 1}/{max_retries}): {e.response.status_code} - {error_body}"
                self.logger.error(error_msg)
                last_error = {'success': False, 'error': error_msg, 'model': model_name}

                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)

            except Exception as e:
                error_msg = f"LLM客户端未知错误 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                last_error = {'success': False, 'error': error_msg, 'model': model_name}

                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)

        return last_error
# 全局LLM客户端实例
try:
    llm_client = LLMClient()
except Exception as e:
    logging.getLogger(__name__).warning(f"LLM客户端初始化失败: {e}")
    llm_client = None
