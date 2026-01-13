#!/usr/bin/env python3
"""
CAPTCHA Solver Integration Module

Supports multiple CAPTCHA solving services for bypassing anti-bot challenges:
- 2Captcha (https://2captcha.com)
- CapSolver (https://capsolver.com)

Handles PerimeterX "Press and Hold" challenges and other CAPTCHA types.
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Any

import requests

logger = logging.getLogger(__name__)


class CaptchaType(Enum):
    """Supported CAPTCHA types."""
    PERIMETERX = "perimeterx"
    RECAPTCHA_V2 = "recaptcha_v2"
    RECAPTCHA_V3 = "recaptcha_v3"
    HCAPTCHA = "hcaptcha"
    FUNCAPTCHA = "funcaptcha"
    IMAGE = "image"


@dataclass
class CaptchaSolution:
    """Result from a CAPTCHA solving attempt."""
    success: bool
    token: Optional[str] = None
    solution: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    task_id: Optional[str] = None
    cost: Optional[float] = None
    solve_time: Optional[float] = None


class CaptchaSolverBase(ABC):
    """Abstract base class for CAPTCHA solvers."""

    def __init__(self, api_key: str, timeout: int = 120):
        """
        Initialize solver.

        Args:
            api_key: API key for the service
            timeout: Maximum time to wait for solution (seconds)
        """
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

    @abstractmethod
    def solve_perimeterx(self, site_url: str, data_blob: Optional[str] = None,
                         user_agent: Optional[str] = None) -> CaptchaSolution:
        """Solve PerimeterX Press and Hold challenge."""
        pass

    @abstractmethod
    def solve_recaptcha_v2(self, site_url: str, site_key: str,
                           invisible: bool = False) -> CaptchaSolution:
        """Solve reCAPTCHA v2."""
        pass

    @abstractmethod
    def get_balance(self) -> float:
        """Get account balance."""
        pass


class TwoCaptchaSolver(CaptchaSolverBase):
    """
    2Captcha CAPTCHA solving service integration.

    Docs: https://2captcha.com/2captcha-api
    """

    BASE_URL = "https://2captcha.com"

    def __init__(self, api_key: str, timeout: int = 120):
        super().__init__(api_key, timeout)
        self.poll_interval = 5  # seconds between status checks

    def _create_task(self, task_data: Dict) -> Optional[str]:
        """Create a CAPTCHA solving task."""
        payload = {
            "key": self.api_key,
            "json": 1,
            **task_data
        }

        try:
            response = self.session.post(
                f"{self.BASE_URL}/in.php",
                data=payload,
                timeout=30
            )
            result = response.json()

            if result.get("status") == 1:
                task_id = result.get("request")
                logger.info(f"2Captcha task created: {task_id}")
                return task_id
            else:
                logger.error(f"2Captcha task creation failed: {result.get('request')}")
                return None

        except Exception as e:
            logger.error(f"2Captcha API error: {e}")
            return None

    def _get_result(self, task_id: str) -> CaptchaSolution:
        """Poll for task result."""
        start_time = time.time()

        while time.time() - start_time < self.timeout:
            try:
                response = self.session.get(
                    f"{self.BASE_URL}/res.php",
                    params={
                        "key": self.api_key,
                        "action": "get",
                        "id": task_id,
                        "json": 1
                    },
                    timeout=30
                )
                result = response.json()

                if result.get("status") == 1:
                    solve_time = time.time() - start_time
                    logger.info(f"2Captcha solved in {solve_time:.1f}s")
                    return CaptchaSolution(
                        success=True,
                        token=result.get("request"),
                        task_id=task_id,
                        solve_time=solve_time
                    )
                elif result.get("request") == "CAPCHA_NOT_READY":
                    time.sleep(self.poll_interval)
                else:
                    return CaptchaSolution(
                        success=False,
                        error=result.get("request"),
                        task_id=task_id
                    )

            except Exception as e:
                logger.error(f"2Captcha polling error: {e}")
                time.sleep(self.poll_interval)

        return CaptchaSolution(
            success=False,
            error="Timeout waiting for solution",
            task_id=task_id
        )

    def solve_perimeterx(self, site_url: str, data_blob: Optional[str] = None,
                         user_agent: Optional[str] = None) -> CaptchaSolution:
        """
        Solve PerimeterX "Press and Hold" challenge.

        Note: 2Captcha uses the 'funcaptcha' method for PerimeterX challenges
        or the custom 'perimeterx' method if available.

        Args:
            site_url: The URL where CAPTCHA appeared
            data_blob: PerimeterX data blob (if available)
            user_agent: User agent string to use

        Returns:
            CaptchaSolution with token or error
        """
        logger.info(f"Solving PerimeterX challenge for {site_url}")

        # 2Captcha treats PerimeterX as a custom task
        task_data = {
            "method": "funcaptcha",
            "publickey": "px",  # PerimeterX identifier
            "pageurl": site_url,
        }

        if data_blob:
            task_data["data[blob]"] = data_blob

        if user_agent:
            task_data["userAgent"] = user_agent

        task_id = self._create_task(task_data)
        if not task_id:
            return CaptchaSolution(success=False, error="Failed to create task")

        return self._get_result(task_id)

    def solve_recaptcha_v2(self, site_url: str, site_key: str,
                           invisible: bool = False) -> CaptchaSolution:
        """
        Solve reCAPTCHA v2.

        Args:
            site_url: Page URL with CAPTCHA
            site_key: reCAPTCHA site key
            invisible: Whether it's invisible reCAPTCHA

        Returns:
            CaptchaSolution with token
        """
        logger.info(f"Solving reCAPTCHA v2 for {site_url}")

        task_data = {
            "method": "userrecaptcha",
            "googlekey": site_key,
            "pageurl": site_url,
        }

        if invisible:
            task_data["invisible"] = 1

        task_id = self._create_task(task_data)
        if not task_id:
            return CaptchaSolution(success=False, error="Failed to create task")

        return self._get_result(task_id)

    def solve_hcaptcha(self, site_url: str, site_key: str) -> CaptchaSolution:
        """Solve hCaptcha."""
        logger.info(f"Solving hCaptcha for {site_url}")

        task_data = {
            "method": "hcaptcha",
            "sitekey": site_key,
            "pageurl": site_url,
        }

        task_id = self._create_task(task_data)
        if not task_id:
            return CaptchaSolution(success=False, error="Failed to create task")

        return self._get_result(task_id)

    def get_balance(self) -> float:
        """Get account balance in USD."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/res.php",
                params={
                    "key": self.api_key,
                    "action": "getbalance",
                    "json": 1
                },
                timeout=30
            )
            result = response.json()

            if result.get("status") == 1:
                return float(result.get("request", 0))

        except Exception as e:
            logger.error(f"Failed to get 2Captcha balance: {e}")

        return 0.0


class CapSolver(CaptchaSolverBase):
    """
    CapSolver CAPTCHA solving service integration.

    Docs: https://docs.capsolver.com/
    """

    BASE_URL = "https://api.capsolver.com"

    def __init__(self, api_key: str, timeout: int = 120):
        super().__init__(api_key, timeout)
        self.poll_interval = 3  # CapSolver is typically faster

    def _create_task(self, task: Dict) -> Optional[str]:
        """Create a CAPTCHA solving task."""
        payload = {
            "clientKey": self.api_key,
            "task": task
        }

        try:
            response = self.session.post(
                f"{self.BASE_URL}/createTask",
                json=payload,
                timeout=30
            )
            result = response.json()

            if result.get("errorId") == 0:
                task_id = result.get("taskId")
                logger.info(f"CapSolver task created: {task_id}")
                return task_id
            else:
                logger.error(f"CapSolver task creation failed: {result.get('errorDescription')}")
                return None

        except Exception as e:
            logger.error(f"CapSolver API error: {e}")
            return None

    def _get_result(self, task_id: str) -> CaptchaSolution:
        """Poll for task result."""
        start_time = time.time()

        while time.time() - start_time < self.timeout:
            try:
                response = self.session.post(
                    f"{self.BASE_URL}/getTaskResult",
                    json={
                        "clientKey": self.api_key,
                        "taskId": task_id
                    },
                    timeout=30
                )
                result = response.json()

                if result.get("errorId") != 0:
                    return CaptchaSolution(
                        success=False,
                        error=result.get("errorDescription"),
                        task_id=task_id
                    )

                status = result.get("status")

                if status == "ready":
                    solution = result.get("solution", {})
                    solve_time = time.time() - start_time
                    logger.info(f"CapSolver solved in {solve_time:.1f}s")

                    # Extract token based on CAPTCHA type
                    token = (
                        solution.get("token") or
                        solution.get("gRecaptchaResponse") or
                        solution.get("captcha_response")
                    )

                    return CaptchaSolution(
                        success=True,
                        token=token,
                        solution=solution,
                        task_id=task_id,
                        solve_time=solve_time
                    )
                elif status == "processing":
                    time.sleep(self.poll_interval)
                else:
                    return CaptchaSolution(
                        success=False,
                        error=f"Unknown status: {status}",
                        task_id=task_id
                    )

            except Exception as e:
                logger.error(f"CapSolver polling error: {e}")
                time.sleep(self.poll_interval)

        return CaptchaSolution(
            success=False,
            error="Timeout waiting for solution",
            task_id=task_id
        )

    def solve_perimeterx(self, site_url: str, data_blob: Optional[str] = None,
                         user_agent: Optional[str] = None) -> CaptchaSolution:
        """
        Solve PerimeterX "Press and Hold" challenge using CapSolver.

        CapSolver has native PerimeterX support via AntiPerimeterXTask.

        Args:
            site_url: The URL where CAPTCHA appeared
            data_blob: PerimeterX data blob from _px3 cookie or page
            user_agent: User agent string

        Returns:
            CaptchaSolution with token
        """
        logger.info(f"Solving PerimeterX challenge for {site_url}")

        task = {
            "type": "AntiPerimeterXTask",
            "websiteURL": site_url,
        }

        if data_blob:
            task["captchaScript"] = data_blob

        if user_agent:
            task["userAgent"] = user_agent

        task_id = self._create_task(task)
        if not task_id:
            return CaptchaSolution(success=False, error="Failed to create task")

        return self._get_result(task_id)

    def solve_recaptcha_v2(self, site_url: str, site_key: str,
                           invisible: bool = False) -> CaptchaSolution:
        """Solve reCAPTCHA v2."""
        logger.info(f"Solving reCAPTCHA v2 for {site_url}")

        task_type = "ReCaptchaV2TaskProxyLess"
        if invisible:
            task_type = "ReCaptchaV2TaskProxyLess"  # Same type, different handling

        task = {
            "type": task_type,
            "websiteURL": site_url,
            "websiteKey": site_key,
            "isInvisible": invisible
        }

        task_id = self._create_task(task)
        if not task_id:
            return CaptchaSolution(success=False, error="Failed to create task")

        return self._get_result(task_id)

    def solve_hcaptcha(self, site_url: str, site_key: str) -> CaptchaSolution:
        """Solve hCaptcha."""
        logger.info(f"Solving hCaptcha for {site_url}")

        task = {
            "type": "HCaptchaTaskProxyLess",
            "websiteURL": site_url,
            "websiteKey": site_key
        }

        task_id = self._create_task(task)
        if not task_id:
            return CaptchaSolution(success=False, error="Failed to create task")

        return self._get_result(task_id)

    def get_balance(self) -> float:
        """Get account balance in USD."""
        try:
            response = self.session.post(
                f"{self.BASE_URL}/getBalance",
                json={"clientKey": self.api_key},
                timeout=30
            )
            result = response.json()

            if result.get("errorId") == 0:
                return float(result.get("balance", 0))

        except Exception as e:
            logger.error(f"Failed to get CapSolver balance: {e}")

        return 0.0


class CaptchaSolverFactory:
    """Factory for creating CAPTCHA solver instances."""

    PROVIDERS = {
        "2captcha": TwoCaptchaSolver,
        "capsolver": CapSolver,
    }

    @classmethod
    def create(cls, provider: str, api_key: str, **kwargs) -> CaptchaSolverBase:
        """
        Create a CAPTCHA solver instance.

        Args:
            provider: Provider name ("2captcha" or "capsolver")
            api_key: API key for the provider
            **kwargs: Additional arguments passed to solver

        Returns:
            CaptchaSolver instance

        Raises:
            ValueError: If provider is not supported
        """
        provider = provider.lower()

        if provider not in cls.PROVIDERS:
            raise ValueError(
                f"Unknown CAPTCHA provider: {provider}. "
                f"Supported: {list(cls.PROVIDERS.keys())}"
            )

        return cls.PROVIDERS[provider](api_key, **kwargs)

    @classmethod
    def available_providers(cls) -> list:
        """Get list of available provider names."""
        return list(cls.PROVIDERS.keys())


class CaptchaSolverManager:
    """
    High-level manager for CAPTCHA solving with fallback support.

    Manages multiple solver providers and handles automatic fallback.
    """

    def __init__(self, config: Dict):
        """
        Initialize with configuration.

        Config format:
        {
            "enabled": true,
            "primary_provider": "capsolver",
            "fallback_provider": "2captcha",
            "providers": {
                "capsolver": {"api_key": "..."},
                "2captcha": {"api_key": "..."}
            },
            "timeout": 120,
            "max_retries": 2
        }
        """
        self.enabled = config.get("enabled", False)
        self.primary_provider = config.get("primary_provider", "capsolver")
        self.fallback_provider = config.get("fallback_provider")
        self.timeout = config.get("timeout", 120)
        self.max_retries = config.get("max_retries", 2)

        self.solvers: Dict[str, CaptchaSolverBase] = {}

        if self.enabled:
            self._init_solvers(config.get("providers", {}))

    def _init_solvers(self, providers_config: Dict):
        """Initialize configured solvers."""
        for provider_name, provider_config in providers_config.items():
            api_key = provider_config.get("api_key")
            if api_key:
                try:
                    self.solvers[provider_name] = CaptchaSolverFactory.create(
                        provider_name,
                        api_key,
                        timeout=self.timeout
                    )
                    logger.info(f"Initialized CAPTCHA solver: {provider_name}")
                except ValueError as e:
                    logger.warning(f"Failed to initialize {provider_name}: {e}")

    def is_available(self) -> bool:
        """Check if any solver is available."""
        return self.enabled and len(self.solvers) > 0

    def solve_perimeterx(self, site_url: str, data_blob: Optional[str] = None,
                         user_agent: Optional[str] = None) -> CaptchaSolution:
        """
        Solve PerimeterX challenge with automatic fallback.

        Args:
            site_url: URL where challenge appeared
            data_blob: PerimeterX data blob
            user_agent: Browser user agent

        Returns:
            CaptchaSolution
        """
        if not self.is_available():
            return CaptchaSolution(
                success=False,
                error="CAPTCHA solving is not configured or enabled"
            )

        # Try primary provider first
        providers_to_try = [self.primary_provider]
        if self.fallback_provider and self.fallback_provider != self.primary_provider:
            providers_to_try.append(self.fallback_provider)

        last_error = None

        for provider_name in providers_to_try:
            solver = self.solvers.get(provider_name)
            if not solver:
                continue

            for attempt in range(self.max_retries):
                logger.info(f"Attempting PerimeterX solve with {provider_name} (attempt {attempt + 1})")

                try:
                    result = solver.solve_perimeterx(site_url, data_blob, user_agent)

                    if result.success:
                        return result

                    last_error = result.error
                    logger.warning(f"{provider_name} failed: {result.error}")

                except Exception as e:
                    last_error = str(e)
                    logger.error(f"{provider_name} error: {e}")

        return CaptchaSolution(
            success=False,
            error=f"All CAPTCHA solvers failed. Last error: {last_error}"
        )

    def get_balances(self) -> Dict[str, float]:
        """Get balance from all configured providers."""
        balances = {}
        for name, solver in self.solvers.items():
            try:
                balances[name] = solver.get_balance()
            except Exception as e:
                logger.error(f"Failed to get balance for {name}: {e}")
                balances[name] = -1.0
        return balances
