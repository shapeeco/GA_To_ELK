import os
import logging
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


class GAConfig:
    """Centralized configuration for Google Analytics integration."""

    def __init__(self):
        """Load all environment variables for GA configuration."""
        # Credentials
        self.credentials_path = os.getenv("GA_CREDENTIALS_PATH")

        # Google Analytics settings
        self.account_id = os.getenv("GA_ACCOUNT_ID")
        self.property_id = os.getenv("GA_PROPERTY_ID")  # For backwards compatibility
        self.days_to_pull = int(os.getenv("GA_DAYS_TO_PULL", "30"))
        self.report_limit = int(os.getenv("GA_REPORT_LIMIT", "100000"))

        # Elasticsearch settings
        self.elasticsearch_host = os.getenv("ELASTICSEARCH_HOST")
        self.elasticsearch_api_key = os.getenv("ELASTICSEARCH_API_KEY")

        # Logging
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        # Cache for credentials
        self._credentials = None

    def validate_ga_config(self):
        """Validate required Google Analytics configuration."""
        # Check for account_id first, fall back to property_id for backwards compatibility
        if not self.account_id and not self.property_id:
            raise ValueError("GA_ACCOUNT_ID or GA_PROPERTY_ID environment variable must be set")
        if not self.credentials_path:
            raise ValueError("GA_CREDENTIALS_PATH environment variable is not set")
        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"GA Service Account credentials file not found at: {self.credentials_path}")

    def validate_elasticsearch_config(self):
        """Validate required Elasticsearch configuration."""
        if not self.elasticsearch_host:
            raise ValueError("ELASTICSEARCH_HOST environment variable is not set")

    def get_credentials(self):
        """
        Load and return Google Analytics service account credentials.
        Credentials are cached after first load.

        Returns:
            google.oauth2.service_account.Credentials: The loaded credentials

        Raises:
            FileNotFoundError: If credentials file doesn't exist
            Exception: If credentials loading fails
        """
        if self._credentials:
            return self._credentials

        if not self.credentials_path:
            raise ValueError("GA_CREDENTIALS_PATH environment variable is not set")

        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"GA Service Account credentials file not found at: {self.credentials_path}")

        try:
            self._credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
            logger.info("Google Analytics credentials loaded successfully")
            return self._credentials
        except Exception as e:
            logger.error(f"Failed to load Google Analytics credentials: {e}")
            raise



