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
        # figure out where we left off
        last_sync = get_last_record_value_for_table(self.state, table, 'bookmark_date')
        if last_sync:
            start_dt = parse(last_sync)
        else:
            start_dt = get_config_start_date(self.config)

        page_size = self.config.get('page_size', 100)

        max_updated = start_dt

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

                # write them and track the max updated_at seen
                records = []
                for obj in usage_list:
                    rec = obj['usage']
                    for key in ('created_at', 'usage_date', 'updated_at'):
                        if key in rec:
                            rec[key] = datetime.fromtimestamp(rec[key]).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    records.append(rec)
                    updated = parse(rec['updated_at'])
                    if updated > max_updated:
                        max_updated = updated

                singer.write_records(table, records)
                singer.metrics.record_counter(endpoint=table).increment(len(records))

                # prepare next page
                offset = resp.get('next_offset')
                if not offset:
                    break

        # once all subscriptions are done, persist the farthest‐out updated_at        new_bookmark = max_updated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.state = incorporate(self.state, table, 'bookmark_date', new_bookmark)
        save_state(self.state)
        LOGGER.info(f"Completed sync for {table} up to {new_bookmark}")

