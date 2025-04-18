import singer

from .subscriptions import SubscriptionsStream

from dateutil.parser import parse
from datetime import datetime, timedelta
from tap_framework.config import get_config_start_date
from tap_chargebee.state import get_last_record_value_for_table, incorporate, \
    save_state
from tap_chargebee.streams.base import BaseChargebeeStream


LOGGER = singer.get_logger()

class UsagesStream(BaseChargebeeStream):
    TABLE = 'usages'
    ENTITY = 'usage'
    KEY_PROPERTIES = ['id']
    SELECTED_BY_DEFAULT = True
    REPLICATION_METHOD = "FULL"
    BOOKMARK_PROPERTIES = ['updated_at']
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    _already_checked_subscription = []
    sync_data_for_child_stream = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.PARENT_STREAM_INSTANCE = SubscriptionsStream(*args, **kwargs)

    def get_url(self):
        return 'https://{}/api/v2/usages'.format(self.config.get('full_site'))

    def sync_data(self):
        table = self.TABLE
        api_method = self.API_METHOD

        # Attempt to get the bookmark date from the state file
        LOGGER.info('Attempting to get the most recent bookmark_date for entity {}.'.format(self.ENTITY))
        bookmark_date = get_last_record_value_for_table(self.state, table, 'bookmark_date')

        # If there is no bookmark date, fall back to using the start date from the config
        if bookmark_date is None:
            LOGGER.info('Could not locate bookmark_date from STATE file. Falling back to start_date from config.json instead.')
            bookmark_date = get_config_start_date(self.config)
        else:
            bookmark_date = parse(bookmark_date)

        # Convert bookmarked start date to datetime
        current_window_start_dt = datetime.fromtimestamp(int(bookmark_date.timestamp()))
        
        batching_requests = True
        batch_size_in_months = self.config.get("batch_size_in_months")
        if batch_size_in_months:
            batch_size_in_months = min(batch_size_in_months, 12)
        else:
            batching_requests = False

        while current_window_start_dt < datetime.now():
            if batching_requests:
                # Calculate end of current month
                current_window_end_dt = (current_window_start_dt + timedelta(days=batch_size_in_months * 31)).replace(day=1)

                # Ensure we don't go beyond START_TIMESTAP
                current_window_end_dt = min(current_window_end_dt, 
                                        datetime.fromtimestamp(self.START_TIMESTAP))
            else:
                current_window_end_dt = datetime.fromtimestamp(self.START_TIMESTAP)
            
            # For the last window, extend end date by 1 minute into the future
            if current_window_end_dt >= datetime.now():
                current_window_end_dt = datetime.now() + timedelta(seconds=5)
            
            # Convert to timestamps for the API
            current_window_start = int(current_window_start_dt.timestamp())
            current_window_end = int(current_window_end_dt.timestamp())
            
            LOGGER.info(f"Syncing {table} from {current_window_start_dt.strftime('%Y-%m-%d')} "
                       f"to {current_window_end_dt.strftime('%Y-%m-%d')}")

            for subscription in self.PARENT_STREAM_INSTANCE.sync_parent_data():
                subscription_id = subscription["subscription"]["id"]
                if subscription_id in self._already_checked_subscription:
                    continue

                params = {
                    'subscription_id[is]': subscription_id,
                    'updated_at[after]': current_window_start,
                    'updated_at[before]': current_window_end
                }
                self._already_checked_subscription.append(subscription_id)

                try:
                    response = self.client.make_request(self.get_url(), api_method, params=params)
                except Exception as e:
                    LOGGER.error(f"Error fetching usages for subscription {subscription_id}: {str(e)}")
                    continue

                # Transform dates from timestamp to isoformat
                for obj in response.get('list', []):
                    record = obj.get("usage")
                    for key in ["created_at", "usage_date", "updated_at"]:
                        if key in record:
                            record[key] = datetime.fromtimestamp(record[key]).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                    singer.write_records(table, [record])

                if len(response.get('list', [])) > 0:
                    with singer.metrics.record_counter(endpoint=table) as ctr:
                        ctr.increment(amount=len(response.get('list', [])))

            # Move to next window
            current_window_start_dt = current_window_end_dt
            
            # Save state after each window
            # If this was the last window, subtract the 5 seconds we added earlier
            if current_window_end_dt >= datetime.now():
                current_window_end_dt = current_window_end_dt - timedelta(seconds=5)
            self.state = incorporate(self.state, table, 'bookmark_date',
                                   current_window_end_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
            save_state(self.state)
            
            # Reset checked subscriptions for next window
            self._already_checked_subscription = []