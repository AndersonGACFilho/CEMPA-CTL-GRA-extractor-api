from contextlib import asynccontextmanager
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.database.base import Base
from app.etl.extractor import DownloadCEMPAData
from app.database.session import engine

# Initialize scheduler and logger
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Schedule the daily download at 03:30
    trigger = CronTrigger(hour=3, minute=30)
    scheduler.add_job(
        DownloadCEMPAData,
        trigger,
        id="DailyCEMPADataDownload",
    )
    scheduler.start()

    # Yield control back to FastAPI so it can start serving
    yield

    # --- Shutdown ---
    scheduler.shutdown(wait=False)
    await engine.dispose()

def create_app() -> FastAPI:
    app = FastAPI(
        title="CEMPA Data Extraction API",
        version="1.0.0",
        description="API for CodeSearch",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app
