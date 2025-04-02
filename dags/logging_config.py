import logging

# Configure logging with INFO level and a consistent format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Create a logger for this module
logger = logging.getLogger(__name__)