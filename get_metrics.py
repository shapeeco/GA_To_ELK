import logging
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
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
    """Main function to fetch Google Ads metrics from GA4."""
    validate_config()

    # Authenticate Google Analytics client
    try:
        credentials = config.get_credentials()
        client = BetaAnalyticsDataClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics: {e}")
        exit(1)

    # Query Google Ads data in GA4
    try:
        logger.info(f"Fetching Google Ads metrics for property: {config.property_id}")

        request = RunReportRequest(
            property=f"properties/{config.property_id}",
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="sessionCampaignName"),
                Dimension(name="date"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="conversions"),
                Metric(name="totalRevenue"),
                Metric(name="advertiserAdClicks"),
                Metric(name="advertiserAdCost"),
                Metric(name="advertiserAdImpressions"),
            ],
            date_ranges=[DateRange(start_date=f"{config.days_to_pull}daysAgo", end_date="today")],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="sessionSource",
                    string_filter=Filter.StringFilter(value="google")
                )
            )
        )

        response = client.run_report(request)
        logger.info(f"Successfully retrieved {len(response.rows)} rows of Google Ads metrics.")

        # Display results
        print("\n" + "="*80)
        print("Google Ads Metrics Report")
        print(f"Period: Last {config.days_to_pull} days")
        print("="*80)

        if len(response.rows) == 0:
            print("\nNo Google Ads data found for this property.")
        else:
            for idx, row in enumerate(response.rows, 1):
                print(f"\n--- Record #{idx} ---")
                print(f"  Source: {row.dimension_values[0].value}")
                print(f"  Medium: {row.dimension_values[1].value}")
                print(f"  Campaign: {row.dimension_values[2].value}")
                print(f"  Date: {row.dimension_values[3].value}")
                print(f"  Sessions: {row.metric_values[0].value}")
                print(f"  Total Users: {row.metric_values[1].value}")
                print(f"  Conversions: {row.metric_values[2].value}")
                print(f"  Revenue: ${float(row.metric_values[3].value or 0):,.2f}")
                print(f"  Ad Clicks: {row.metric_values[4].value}")
                print(f"  Ad Cost: ${float(row.metric_values[5].value or 0):,.2f}")
                print(f"  Ad Impressions: {row.metric_values[6].value}")

            print(f"\n{'='*80}")
            print(f"Total records: {len(response.rows)}")
            print(f"{'='*80}\n")

    except Exception as e:
        logger.error(f"Failed to fetch Google Ads metrics: {e}")
        exit(1)

if __name__ == "__main__":
    main()