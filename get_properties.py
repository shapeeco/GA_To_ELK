import logging
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
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
    """Main function to fetch website engagement metrics from GA4."""
    validate_config()

    # Authenticate Google Analytics client
    try:
        credentials = config.get_credentials()
        client = BetaAnalyticsDataClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics: {e}")
        exit(1)

    # Query website engagement data from GA4
    try:
        logger.info(f"Fetching website engagement metrics for property: {config.property_id}")

        request = RunReportRequest(
            property=f"properties/{config.property_id}",
            dimensions=[
                Dimension(name="pageTitle"),
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="country"),
                Dimension(name="city"),
                Dimension(name="date"),
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="scrolledUsers"),
                Metric(name="userEngagementDuration"),
                Metric(name="eventCount"),
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="engagedSessions"),
            ],
            date_ranges=[DateRange(start_date=f"{config.days_to_pull}daysAgo", end_date="today")],
        )

        response = client.run_report(request)
        logger.info(f"Successfully retrieved {len(response.rows)} rows of website engagement metrics.")

        # Display results
        print("\n" + "="*100)
        print("Website Engagement Metrics Report")
        print(f"Property ID: {config.property_id}")
        print(f"Period: Last {config.days_to_pull} days")
        print("="*100)

        if len(response.rows) == 0:
            print("\nNo engagement data found for this property.")
        else:
            for idx, row in enumerate(response.rows, 1):
                print(f"\n--- Record #{idx} ---")
                print(f"  Page Title: {row.dimension_values[0].value}")
                print(f"  Page Path: {row.dimension_values[1].value}")
                print(f"  Source: {row.dimension_values[2].value}")
                print(f"  Medium: {row.dimension_values[3].value}")
                print(f"  Country: {row.dimension_values[4].value}")
                print(f"  City: {row.dimension_values[5].value}")
                print(f"  Date: {row.dimension_values[6].value}")
                print(f"  Page Views: {row.metric_values[0].value}")
                print(f"  Scrolled Users: {row.metric_values[1].value}")
                print(f"  Engagement Duration (sec): {float(row.metric_values[2].value or 0):,.2f}")
                print(f"  Total Events: {row.metric_values[3].value}")
                print(f"  Sessions: {row.metric_values[4].value}")
                print(f"  Total Users: {row.metric_values[5].value}")
                print(f"  Engaged Sessions: {row.metric_values[6].value}")

            print(f"\n{'='*100}")
            print(f"Total records: {len(response.rows)}")
            print(f"{'='*100}\n")

        # Query specific event types (outbound clicks, file downloads, video engagement)
        logger.info("Fetching specific event metrics...")

        event_request = RunReportRequest(
            property=f"properties/{config.property_id}",
            dimensions=[
                Dimension(name="eventName"),
                Dimension(name="pageTitle"),
                Dimension(name="date"),
            ],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="totalUsers"),
            ],
            date_ranges=[DateRange(start_date=f"{config.days_to_pull}daysAgo", end_date="today")],
        )

        event_response = client.run_report(event_request)
        logger.info(f"Successfully retrieved {len(event_response.rows)} rows of event metrics.")

        # Display event results
        print("\n" + "="*100)
        print("Event Metrics Report (Clicks, Downloads, Video Engagement)")
        print("="*100)

        if len(event_response.rows) == 0:
            print("\nNo event data found for this property.")
        else:
            # Filter for relevant events
            relevant_events = ['click', 'file_download', 'video_start', 'video_progress',
                             'video_complete', 'scroll', 'outbound']

            for idx, row in enumerate(event_response.rows, 1):
                event_name = row.dimension_values[0].value.lower()
                # Display all events or filter for relevant ones
                if any(relevant in event_name for relevant in relevant_events):
                    print(f"\n--- Event #{idx} ---")
                    print(f"  Event Name: {row.dimension_values[0].value}")
                    print(f"  Page Title: {row.dimension_values[1].value}")
                    print(f"  Date: {row.dimension_values[2].value}")
                    print(f"  Event Count: {row.metric_values[0].value}")
                    print(f"  Users: {row.metric_values[1].value}")

            print(f"\n{'='*100}\n")

    except Exception as e:
        logger.error(f"Failed to fetch website engagement metrics: {e}")
        logger.exception("Full error details:")
        exit(1)

if __name__ == "__main__":
    main()
