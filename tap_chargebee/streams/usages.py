import singer

from .subscriptions import SubscriptionsStream

from dateutil.parser import parse
from datetime import datetime, timedelta
from tap_framework.config import get_config_start_date
from tap_chargebee.state import get_last_record_value_for_table, incorporate, \
    save_state
from tap_chargebee.streams.base import BaseChargebeeStream


LOGGER = singer.get_logger()

def ensure_naive_datetime(dt):
    """Convert a datetime to timezone-naive if it has timezone info."""
    if dt and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

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

        # Determine if batching is enabled and set batch size
        batching_requests = True
        batch_size_in_months = self.config.get("batch_size_in_months")
        if batch_size_in_months:
            batch_size_in_months = min(batch_size_in_months, 12)
        else:
            batching_requests = False

        # Determine the starting point for data synchronization
        last_sync = get_last_record_value_for_table(self.state, table, 'bookmark_date')
        if last_sync:
            start_dt = ensure_naive_datetime(parse(last_sync))
        else:
            start_dt = ensure_naive_datetime(get_config_start_date(self.config))

        page_size = self.config.get('page_size', 100)
        max_updated = start_dt
        now = datetime.utcnow()

        if batching_requests:
            # Calculate the end date for the current batch
            while start_dt < now:
                end_dt = min(start_dt + timedelta(days=30 * batch_size_in_months), now)
                LOGGER.info(f"Syncing batch from {start_dt} to {end_dt}")

                for subscription in self.PARENT_STREAM_INSTANCE.sync_parent_data():
                    subscription_id = subscription['subscription']['id']
                    LOGGER.info(f"Syncing subscription {subscription_id}")
                    offset = None
                    while True:
                        params = {
                            'subscription_id[is]': subscription_id,
                            'updated_at[after]': int(start_dt.timestamp()),
                            'updated_at[before]': int(end_dt.timestamp()),
                            'limit': page_size
                        }
                        if offset:
                            params['offset'] = offset

                        resp = self.client.make_request(self.get_url(), self.API_METHOD, params=params)
                        usage_list = resp.get('list', [])
                        if not usage_list:
                            break

                        records = []
                        for obj in usage_list:
                            rec = obj['usage']
                            for key in ('created_at', 'usage_date', 'updated_at'):
                                if key in rec:
                                    dt = datetime.fromtimestamp(rec[key])
                                    rec[key] = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                            records.append(rec)
                            
                            # Parse and normalize the updated_at datetime
                            updated_str = rec.get('updated_at')
                            if updated_str:
                                updated = ensure_naive_datetime(parse(updated_str))
                                if updated > max_updated:
                                    max_updated = updated

                        singer.write_records(table, records)
                        singer.metrics.record_counter(endpoint=table).increment(len(records))

                        offset = resp.get('next_offset')
                        if not offset:
                            break

                start_dt = end_dt
        else:
            # If batching is not enabled, fetch all data since the last sync
            for subscription in self.PARENT_STREAM_INSTANCE.sync_parent_data():
                subscription_id = subscription['subscription']['id']
                offset = None
                while True:
                    params = {
                        'subscription_id[is]': subscription_id,
                        'updated_at[after]': int(start_dt.timestamp()),
                        'limit': page_size
                    }
                    if offset:
                        params['offset'] = offset

                    resp = self.client.make_request(self.get_url(), self.API_METHOD, params=params)
                    usage_list = resp.get('list', [])
                    if not usage_list:
                        break

                    records = []
                    for obj in usage_list:
                        rec = obj['usage']
                        for key in ('created_at', 'usage_date', 'updated_at'):
                            if key in rec:
                                dt = datetime.fromtimestamp(rec[key])
                                rec[key] = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        records.append(rec)
                        
                        # Parse and normalize the updated_at datetime
                        updated_str = rec.get('updated_at')
                        if updated_str:
                            updated = ensure_naive_datetime(parse(updated_str))
                            if updated > max_updated:
                                max_updated = updated

                    singer.write_records(table, records)
                    singer.metrics.record_counter(endpoint=table).increment(len(records))

                    offset = resp.get('next_offset')
                    if not offset:
                        break

        # Update the state with the latest synchronization timestamp
        new_bookmark = max_updated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.state = incorporate(self.state, table, 'bookmark_date', new_bookmark)
        save_state(self.state)
        LOGGER.info(f"Completed sync for {table} up to {new_bookmark}")

