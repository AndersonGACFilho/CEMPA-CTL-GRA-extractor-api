from dotenv import load_dotenv
import asyncio
import logging
import os

load_dotenv()

logger = logging.getLogger(__name__)

ROOT_DIR = os.getenv('ROOT_DIR')


async def DownloadCEMPAData():
    # Delete all files in the directory "/tmp"
    delete_dir = os.path.join(ROOT_DIR, "cempa")

    # Create the directory if it doesn't exist
    if not os.path.exists(delete_dir):
        os.makedirs(delete_dir)
        logger.debug(f"Created directory: {delete_dir}")

    logger.debug(f"Deleting all files in directory: {delete_dir}")
    try:
        for filename in os.listdir(delete_dir):
            file_path = os.path.join(delete_dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.debug(f"Deleted file: {file_path}")
            else:
                logger.warning(f"Skipping non-file: {file_path}")
    except Exception as e:
        logger.error(f"Error deleting files: {e}")

    # Download the CEMPA data
    # This is a placeholder for the actual download logic


# Run the DownloadCEMPAData function
async def run_download():
    await DownloadCEMPAData()

if __name__ == "__main__":
    asyncio.run(run_download())
