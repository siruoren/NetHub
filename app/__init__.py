from __future__ import annotations
"""应用工厂 - 创建 FastAPI 实例并整合各模块"""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from app.checker import ProxyChecker, DEFAULT_CHECK_URLS
from app.config import AppConfig, load_config
from app.database import ProxyDatabase
from app.scheduler import TaskScheduler

logger = logging.getLogger(__name__)

# 全局单例
_config: AppConfig | None = None
_db: ProxyDatabase | None = None
_checker: ProxyChecker | None = None
_scheduler: TaskScheduler | None = None
_templates: Jinja2Templates | None = None


def get_config() -> AppConfig:
    return _config


def get_db() -> ProxyDatabase:
    return _db


def get_checker() -> ProxyChecker:
    return _checker


def get_scheduler() -> TaskScheduler:
    return _scheduler


def get_templates() -> Jinja2Templates:
    return _templates


def create_app(config_path: str = "config.yaml") -> FastAPI:
    """创建 FastAPI 应用实例"""
    global _config, _db, _checker, _scheduler, _templates

    # 加载配置
    _config = load_config(config_path)

    # 配置日志（控制台 + 按天归档文件，保留7天）
    log_level = logging.DEBUG if _config.server.debug else logging.INFO
    log_fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 控制台
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_fmt)
    root_logger.addHandler(console_handler)

    # 文件：只保留当前日志，不归档
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(
        filename=str(log_dir / "proxy_pool.log"),
        encoding="utf-8",
    )
    file_handler.setFormatter(log_fmt)
    root_logger.addHandler(file_handler)

    # 初始化组件
    _db = ProxyDatabase(_config.database.path)

    # 初始化模板
    template_dir = Path(__file__).parent / "templates"
    _templates = Jinja2Templates(directory=str(template_dir))
    _templates.env.cache_size = 128  # 启用模板编译缓存

    # 创建 FastAPI 实例
    app = FastAPI(
        title="NetHub",
        version="1.0.0",
        description="节点池管理系统",
    )

    # 注册生命周期事件
    @app.on_event("startup")
    async def startup():
        global _checker, _scheduler

        # 初始化数据库
        await _db.init()
        logger.info("数据库初始化完成: %s", _config.database.path)

        # 初始化检测 URL：仅当数据库为空时插入默认值
        await _db.init_check_urls(DEFAULT_CHECK_URLS)

        # 从数据库加载检测 URL
        db_check_urls = await _db.get_check_urls()
        check_urls = [u["url"] for u in db_check_urls] or DEFAULT_CHECK_URLS
        logger.info("检测 URL: %s", check_urls)

        # 初始化检测器
        _checker = ProxyChecker(
            check_urls=check_urls,
            timeout=_config.check.timeout,
            max_concurrent=_config.check.max_concurrent,
            socks_port=_config.check.socks_port,
            http_port=_config.check.http_port,
            check_mode=_config.check.check_mode,
            kernel_path=_config.check.kernel_path,
            check_retries=_config.check.check_retries,
        )

        # 初始化调度器
        _scheduler = TaskScheduler(_config, _db, _checker)
        _scheduler.start()

        # 启动后立即执行一次拉取
        logger.info("启动首次拉取任务...")
        import asyncio
        asyncio.create_task(_scheduler.fetch_and_check())

    @app.on_event("shutdown")
    async def shutdown():
        if _checker:
            await _checker.close_session()
        if _scheduler:
            _scheduler.shutdown()
        if _db:
            await _db.close()
        logger.info("应用已关闭")

    # 注册路由
    from app.routers.api import router as api_router
    from app.routers.web import router as web_router

    app.include_router(api_router)
    app.include_router(web_router)

    return app
