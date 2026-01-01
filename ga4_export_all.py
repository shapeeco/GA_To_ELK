import logging
import datetime
import warnings
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.analytics.admin import AnalyticsAdminServiceClient

from ga_auth import GAConfig

# Suppress SSL warnings when verify_certs=False
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
warnings.filterwarnings('ignore', message='Connecting to .* using TLS with verify_certs=False is insecure')

# Initialize configuration
config = GAConfig()

# Logging Setup
logging.basicConfig(
    level=config.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def validate_config():
    """Validate required configuration for GA4 export."""
    try:
        config.validate_ga_config()
        config.validate_elasticsearch_config()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration error: {e}")
        exit(1)

def create_index_if_not_exists(es, index_name):
    """Create Elasticsearch index if it doesn't exist."""
    try:
        if not es.indices.exists(index=index_name):
            # Try to delete the conflicting template first
            try:
                es.indices.delete_index_template(name="default_replicas_0_for_single_node", ignore=[404])
                logger.info("Deleted conflicting index template")
            except Exception:
                pass  # Template might not exist or we don't have permission

            # Create index with explicit settings to avoid data stream template conflicts
            es.indices.create(
                index=index_name,
                settings={
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            )
            logger.info(f"Created new Elasticsearch index: {index_name}")
        else:
            logger.info(f"Using existing Elasticsearch index: {index_name}")
    except Exception as e:
        # If index creation fails, log warning but continue - we'll try to insert anyway
        logger.warning(f"Could not create index {index_name}: {e}. Will attempt to insert documents anyway.")
        pass

def export_property_data(ga_client, es, property_id, property_name):
    """Export data for a single property to Elasticsearch."""
    try:
        # Create index name from property name (sanitize for Elasticsearch)
        # Use 'analytics-' prefix to avoid conflicts with data stream templates
        sanitized_name = property_name.lower().replace(' ', '-').replace('_', '-')
        index_name = f"analytics-ga4-{sanitized_name}-{property_id}"
        logger.info(f"Exporting data for property: {property_name} ({property_id}) to index: {index_name}")

        # Create index if it doesn't exist
        create_index_if_not_exists(es, index_name)

        # Prepare GA4 report request with comprehensive dimensions and metrics
        request = RunReportRequest(
            property=f"properties/{property_id}",
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
            limit=config.report_limit
        )

        # Fetch report data
        response = ga_client.run_report(request)
        logger.info(f"Pulled {len(response.rows)} rows for property {property_id}")

        if len(response.rows) == 0:
            logger.warning(f"No data found for property {property_id}")
            return 0

        # Send data to Elasticsearch
        doc_count = 0
        for row in response.rows:
            # Build document with dimensions
            doc = {
                "property_id": property_id,
                "property_name": property_name,
                "pageTitle": row.dimension_values[0].value,
                "pagePath": row.dimension_values[1].value,
                "sessionSource": row.dimension_values[2].value,
                "sessionMedium": row.dimension_values[3].value,
                "country": row.dimension_values[4].value,
                "city": row.dimension_values[5].value,
                "date": row.dimension_values[6].value,

                # Add metrics
                "screenPageViews": int(row.metric_values[0].value or 0),
                "scrolledUsers": int(row.metric_values[1].value or 0),
                "userEngagementDuration": float(row.metric_values[2].value or 0),
                "eventCount": int(row.metric_values[3].value or 0),
                "sessions": int(row.metric_values[4].value or 0),
                "totalUsers": int(row.metric_values[5].value or 0),
                "engagedSessions": int(row.metric_values[6].value or 0),

                # Add timestamp
                "@timestamp": datetime.utcnow().isoformat()
            }

            es.index(index=index_name, document=doc)
            doc_count += 1

        logger.info(f"Successfully sent {doc_count} documents to Elasticsearch index: {index_name}")
        return doc_count

    except Exception as e:
        logger.error(f"Failed to export data for property {property_id}: {e}")
        logger.exception("Full error details:")
        return 0

def main():
    """Main function to export GA4 data from all properties to Elasticsearch."""
    validate_config()

    # Authenticate Google Analytics clients
    try:
        credentials = config.get_credentials()
        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        ga_client = BetaAnalyticsDataClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics: {e}")
        exit(1)

    # Connect to Elasticsearch
    try:
        es = Elasticsearch(
            config.elasticsearch_host,
            api_key=config.elasticsearch_api_key,
            verify_certs=False  # Use only if self hosting ssl certificates
        )
        if not es.ping():
            raise Exception("Elasticsearch server not responding")
        logger.info("Connected to Elasticsearch.")
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {e}")
        exit(1)

    # Get all properties using account summaries
    try:
        logger.info("Fetching all accessible properties via account summaries")
        account_summaries = admin_client.list_account_summaries()

        total_properties = 0
        total_documents = 0

        # Iterate through account summaries and their property summaries
        for summary in account_summaries:
            account_name = summary.account
            account_display_name = summary.display_name

            logger.info(f"Processing account: {account_display_name} ({account_name})")

            for prop_summary in summary.property_summaries:
                total_properties += 1
                property_resource_name = prop_summary.property
                property_id = property_resource_name.split('/')[-1]
                property_display_name = prop_summary.display_name

                logger.info(f"Processing property #{total_properties}: {property_display_name} (ID: {property_id})")

                # Export data for this property
                doc_count = export_property_data(ga_client, es, property_id, property_display_name)
                total_documents += doc_count

        # Summary
        print("\n" + "="*100)
        print("Export Summary")
        print("="*100)
        print(f"Total Properties Processed: {total_properties}")
        print(f"Total Documents Exported: {total_documents}")
        print(f"Date Range: Last {config.days_to_pull} days")
        print("="*100 + "\n")

        logger.info(f"Export complete. Processed {total_properties} properties, exported {total_documents} documents.")

    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        logger.exception("Full error details:")
        exit(1)

if __name__ == "__main__":
    main()
