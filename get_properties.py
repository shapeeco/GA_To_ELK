import logging
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.admin import AnalyticsAdminServiceClient
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
    """Main function to list all properties and their available dimensions."""
    validate_config()

    # Authenticate Google Analytics Admin client
    try:
        credentials = config.get_credentials()
        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        data_client = BetaAnalyticsDataClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics: {e}")
        exit(1)

    # List all properties for the account
    try:
        # Use list_account_summaries which gives us all properties without complex parameters
        logger.info("Fetching all accessible properties via account summaries")

        account_summaries = admin_client.list_account_summaries()

        print("\n" + "="*100)
        print("All Accessible Properties")
        print("="*100)

        property_count = 0

        # Iterate through account summaries and their property summaries
        for summary in account_summaries:
            account_name = summary.account
            account_display_name = summary.display_name

            print(f"\nAccount: {account_display_name} ({account_name})")

            for prop_summary in summary.property_summaries:
                property_count += 1
                property_resource_name = prop_summary.property
                property_id = property_resource_name.split('/')[-1]

                # Get full property details
                try:
                    property = admin_client.get_property(name=property_resource_name)

                    print(f"\n{'='*100}")
                    print(f"Property #{property_count}")
                    print(f"{'='*100}")
                    print(f"  Property Name: {property.display_name}")
                    print(f"  Property ID: {property_id}")
                    print(f"  Resource Name: {property.name}")
                    print(f"  Time Zone: {property.time_zone}")
                    print(f"  Currency Code: {property.currency_code}")
                    print(f"  Industry Category: {property.industry_category}")

                    # Fetch available dimensions for this property
                    try:
                        logger.info(f"Fetching dimensions for property: {property_id}")

                        # Get metadata (dimensions and metrics)
                        metadata = data_client.get_metadata(name=f"properties/{property_id}/metadata")

                        print(f"\n  Available Dimensions ({len(metadata.dimensions)}):")
                        print(f"  {'-'*96}")

                        for dimension in metadata.dimensions:
                            print(f"    - {dimension.api_name:40} | Category: {dimension.category:20} | UI Name: {dimension.ui_name}")

                        print(f"\n  Available Metrics ({len(metadata.metrics)}):")
                        print(f"  {'-'*96}")

                        for metric in metadata.metrics:
                            print(f"    - {metric.api_name:40} | Category: {metric.category:20} | UI Name: {metric.ui_name}")

                    except Exception as e:
                        logger.error(f"Failed to fetch dimensions for property {property_id}: {e}")
                        print(f"  Error fetching dimensions: {e}")

                except Exception as e:
                    logger.error(f"Failed to get property details for {property_resource_name}: {e}")
                    print(f"  Error: {e}")

        print(f"\n{'='*100}")
        print(f"Total Properties Found: {property_count}")
        print(f"{'='*100}\n")

        if property_count == 0:
            print("\nNo properties found.")

    except Exception as e:
        logger.error(f"Failed to list properties: {e}")
        logger.exception("Full error details:")
        exit(1)

if __name__ == "__main__":
    main()
