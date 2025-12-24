import logging
from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from ga_auth import GAConfig

# Initialize configuration
config = GAConfig()

# Logging Setup
logging.basicConfig(
    level=config.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def validate_config():
    """Validate required configuration."""
    try:
        config.validate_ga_config()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration error: {e}")
        exit(1)

def main():
    """Main function to list Google Ads links for a GA4 property."""
    validate_config()

    # Authenticate Google Analytics Admin API client
    try:
        credentials = config.get_credentials()
        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics Admin API.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics Admin API: {e}")
        exit(1)

    # List Google Ads links
    try:
        property_name = f"properties/{config.property_id}"
        links = admin_client.list_google_ads_links(parent=property_name)

        logger.info(f"Fetching Google Ads links for property: {config.property_id}")

        print("\n" + "="*60)
        print("Google Ads Links:")
        print("="*60)

        link_count = 0
        for link in links:
            link_count += 1
            print(f"\nLink #{link_count}:")
            print(f"  Customer ID: {link.customer_id}")
            print(f"  Link Name: {link.name}")
            print(f"  Ads Personalization Enabled: {link.ads_personalization_enabled}")
            print(f"  Link State: {link.state}")

        if link_count == 0:
            print("  No Google Ads links found for this property.")
        else:
            print(f"\nTotal links found: {link_count}")

        print("="*60 + "\n")
        logger.info(f"Successfully retrieved {link_count} Google Ads link(s).")

    except Exception as e:
        logger.error(f"Failed to fetch Google Ads links: {e}")
        exit(1)

if __name__ == "__main__":
    main()