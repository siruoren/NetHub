"""NetHub 启动入口"""

import uvicorn

from app import create_app

app = create_app()

if __name__ == "__main__":
    from app import get_config

    config = get_config()
    uvicorn.run(
        "app.main:app",
        host=config.server.host,
        port=config.server.port,
        reload=config.server.debug,
    )
