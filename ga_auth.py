import os
import logging
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

def get_ga_credentials():
    """
    Load and return Google Analytics service account credentials.

    Returns:
        google.oauth2.service_account.Credentials: The loaded credentials

    Raises:
        FileNotFoundError: If credentials file doesn't exist
        Exception: If credentials loading fails
    """
    ga_credentials_path = os.getenv("GA_CREDENTIALS_PATH")

    if not ga_credentials_path:
        raise ValueError("GA_CREDENTIALS_PATH environment variable is not set")

    if not os.path.exists(ga_credentials_path):
        raise FileNotFoundError(f"GA Service Account credentials file not found at: {ga_credentials_path}")

    try:
        credentials = service_account.Credentials.from_service_account_file(ga_credentials_path)
        logger.info("Google Analytics credentials loaded successfully")
        return credentials
    except Exception as e:
        logger.error(f"Failed to load Google Analytics credentials: {e}")
        raise
