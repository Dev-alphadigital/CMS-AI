"""
LLM Client - Wrapper for OpenAI and Anthropic APIs
"""

import logging
from typing import Dict, Any, List, Optional
from app.core.config import settings
import httpx

logger = logging.getLogger(__name__)


class LLMClient:
    """
    Unified client for LLM APIs (OpenAI GPT-4, Anthropic Claude)
    """
    
    def __init__(self, provider: str = "openai"):
        """
        Initialize LLM client
        
        Args:
            provider: 'openai' or 'anthropic'
        """
        self.provider = provider.lower()
        
        if self.provider == "openai":
            self.api_key = settings.OPENAI_API_KEY
            self.base_url = "https://api.openai.com/v1"
            self.model = "gpt-4-turbo-preview"
        elif self.provider == "anthropic":
            self.api_key = settings.ANTHROPIC_API_KEY
            self.base_url = "https://api.anthropic.com/v1"
            self.model = "claude-3-sonnet-20240229"
        else:
            raise ValueError(f"Unsupported provider: {provider}")
        
        self.client = httpx.AsyncClient(timeout=60.0)
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()
    
    async def generate_completion(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000
    ) -> str:
        """
        Generate text completion
        
        Args:
            prompt: User prompt
            system_prompt: System instructions
            temperature: Creativity (0-1)
            max_tokens: Maximum response length
            
        Returns:
            Generated text
        """
        try:
            if self.provider == "openai":
                return await self._openai_completion(
                    prompt, system_prompt, temperature, max_tokens
                )
            elif self.provider == "anthropic":
                return await self._anthropic_completion(
                    prompt, system_prompt, temperature, max_tokens
                )
        except Exception as e:
            logger.error(f"LLM completion failed: {str(e)}")
            raise
    
    async def _openai_completion(
        self,
        prompt: str,
        system_prompt: Optional[str],
        temperature: float,
        max_tokens: int
    ) -> str:
        """OpenAI API completion"""
        url = f"{self.base_url}/chat/completions"
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        json_data = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        response = await self.client.post(url, headers=headers, json=json_data)
        response.raise_for_status()
        
        data = response.json()
        return data["choices"][0]["message"]["content"]
    
    async def _anthropic_completion(
        self,
        prompt: str,
        system_prompt: Optional[str],
        temperature: float,
        max_tokens: int
    ) -> str:
        """Anthropic API completion"""
        url = f"{self.base_url}/messages"
        
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json"
        }
        
        json_data = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }
        
        if system_prompt:
            json_data["system"] = system_prompt
        
        response = await self.client.post(url, headers=headers, json=json_data)
        response.raise_for_status()
        
        data = response.json()
        return data["content"][0]["text"]
    
    async def generate_structured_output(
        self,
        prompt: str,
        output_schema: Dict[str, Any],
        system_prompt: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate structured JSON output
        
        Args:
            prompt: User prompt
            output_schema: Expected JSON schema
            system_prompt: System instructions
            
        Returns:
            Parsed JSON response
        """
        import json
        
        # Enhance prompt to request JSON
        json_prompt = f"""{prompt}

Please respond with ONLY valid JSON matching this schema:
{json.dumps(output_schema, indent=2)}

Do not include any text outside the JSON object."""
        
        response = await self.generate_completion(
            prompt=json_prompt,
            system_prompt=system_prompt,
            temperature=0.3  # Lower temperature for structured output
        )
        
        # Extract JSON from response
        try:
            # Try to parse directly
            return json.loads(response)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            import re
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            
            # Try to find JSON object
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))
            
            logger.error(f"Failed to parse JSON from response: {response}")
            raise ValueError("Could not parse JSON from LLM response")
    
    async def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of text
        
        Returns:
            {
                "sentiment": "positive" | "negative" | "neutral",
                "score": -1.0 to 1.0,
                "confidence": 0.0 to 1.0
            }
        """
        prompt = f"""Analyze the sentiment of the following text and respond with JSON:

Text: "{text}"

Respond with ONLY this JSON format:
{{
    "sentiment": "positive" | "negative" | "neutral",
    "score": <number from -1.0 to 1.0>,
    "confidence": <number from 0.0 to 1.0>
}}"""
        
        schema = {
            "sentiment": "string",
            "score": "number",
            "confidence": "number"
        }
        
        return await self.generate_structured_output(prompt, schema)