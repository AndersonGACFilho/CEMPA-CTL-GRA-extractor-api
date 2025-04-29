import os
import logging
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from starlette import status
from starlette.exceptions import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# Try to get a pre-defined DATABASE_URL or construct it from individual variables
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    logger.debug("DATABASE_URL not found in environment variables, constructing it")
    DB_USER = os.getenv("DATABASE_USER")
    DB_PASSWORD = os.getenv("DATABASE_PASSWORD")
    DB_HOST = os.getenv("DATABASE_HOST")
    DB_PORT = os.getenv("DATABASE_PORT")
    DB_NAME = os.getenv("DATABASE_NAME")

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        raise ValueError("Missing database connection parameters in environment variables")

    DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create an asynchronous SQLAlchemy engine
logger.debug("Creating asynchronous database engine")
engine = create_async_engine(
    DATABASE_URL,
    echo=True,  # Enable SQL query logging for debugging
    future=True
)

# Configure the asynchronous session maker
logger.debug("Creating asynchronous session maker")
async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_session() -> AsyncSession:
    """
    Dependency function for FastAPI to provide a database session.

    Yields:
        AsyncSession: An instance of an asynchronous database session.
    """
    logger.debug("Creating new database session")
    async with async_session() as session:
        logger.debug("Yielding database session")
        yield session

@asynccontextmanager
async def get_session_for_queue() -> AsyncSession:
    """
    Dependency function for FastAPI to provide a database session.

    Yields:
        AsyncSession: An instance of an asynchronous database session.
    """
    logger.debug("Creating new database session")
    async with async_session() as session:
        logger.debug("Yielding database session")
        yield session

async def commit_and_refresh(db: AsyncSession, entity, entity_id: int):
    """
    Commit the transaction and refresh the given entity from the database.
    Logs the updated entity as a string after refreshing.

    Args:
        db (AsyncSession): The active asynchronous database session.
        entity: The entity to refresh after committing.
        entity_id (int): The ID of the entity (used for logging purposes).

    Returns:
        The refreshed entity.

    Raises:
        HTTPException: If committing or refreshing the entity fails.
    """
    try:
        logger.debug("Committing updated entity (id=%s) to the database", entity_id)
        await db.commit()
        await db.refresh(entity)
        # Log the updated entity as a string.
        logger.debug("Updated entity: %s", str(entity))
        return entity
    except Exception as exc:
        logger.error("Error updating entity (id=%s): %s", entity_id, exc)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Error updating entity"
        )