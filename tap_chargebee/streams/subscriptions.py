from datetime import datetime, timedelta, timezone

import singer
from dateutil.parser import parse
from tap_framework.config import get_config_start_date

from tap_chargebee.state import get_last_record_value_for_table, incorporate, \
    save_state
from tap_chargebee.streams.base import BaseChargebeeStream


class SubscriptionsStream(BaseChargebeeStream):
    TABLE = 'subscriptions'
    ENTITY = 'subscription'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SORT_BY = 'updated_at'
    sync_for_grant_blocks = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._grant_blocks_subscription_ids = None

    def get_url(self):
        return 'https://{}/api/v2/subscriptions'.format(self.config.get('full_site'))

    def sync_data(self):
        # Keep arbitrary API filters on the existing server-side filtering path.
        if (not self.sync_for_grant_blocks
                or (self.config.get('filters') or {}).get(self.ENTITY)):
            return super().sync_data()

        bookmark_date = get_last_record_value_for_table(
            self.state, self.TABLE, 'bookmark_date')
        bookmark_date = (parse(bookmark_date) if bookmark_date is not None
                         else get_config_start_date(self.config))
        start = datetime.fromtimestamp(int(bookmark_date.timestamp()))
        end = datetime.fromtimestamp(self.END_TIMESTAMP)
        batch_size = self.config.get('batch_size_in_months')
        windows = []
        while start < end:
            window_end = end
            if batch_size:
                window_end = min(
                    (start + timedelta(days=min(batch_size, 12) * 31)).replace(day=1),
                    end)
            windows.append((int(start.timestamp()), int(window_end.timestamp()),
                            start <= datetime(2016, 9, 30)))
            start = window_end

        subscription_ids = []
        ids = set()
        max_timestamp = bookmark_date.timestamp()
        for page in self._get_full_subscription_pages():
            records = []
            for item in page:
                subscription = item[self.ENTITY]
                subscription_ids.append(subscription['id'])
                updated = subscription.get('updated_at')
                created = subscription.get('created_at')
                updated_in_window = updated is not None and any(
                    start < updated < end for start, end, _ in windows)
                created_in_window = created is not None and any(
                    legacy and start < created < end
                    for start, end, legacy in windows)
                if not (updated_in_window or created_in_window):
                    continue

                record = self.transform_record(subscription)
                key = self.get_deduplication_key(record)
                if key not in ids:
                    records.append(record)
                    ids.add(key)
                if updated_in_window:
                    max_timestamp = max(max_timestamp, updated)

            with singer.metrics.record_counter(endpoint=self.TABLE) as counter:
                singer.write_records(self.TABLE, records)
                counter.increment(amount=len(records))

        self._grant_blocks_subscription_ids = subscription_ids
        # Checkpoint only after the full scan succeeds: it is not date-sorted.
        if windows:
            self.state = incorporate(
                self.state, self.TABLE, 'bookmark_date',
                datetime.fromtimestamp(max_timestamp, tz=timezone.utc))
            save_state(self.state)

    def sync_parent_data(self, full_refresh=False):
        if not full_refresh:
            yield from super().sync_parent_data()
            return

        if self._grant_blocks_subscription_ids is not None:
            for subscription_id in self._grant_blocks_subscription_ids:
                yield {self.ENTITY: {'id': subscription_id}}
            return

        for page in self._get_full_subscription_pages():
            yield from page

    def _get_full_subscription_pages(self):
        # Grant blocks can change without the subscription changing. Fetch all
        # parents without applying subscription bookmarks or date filters.
        params = {}
        if self.config.get('include_deprecated') is True:
            params['include_deprecated'] = 'true'

        while True:
            response = self.client.make_request(
                self.get_url(), self.API_METHOD, params=params)
            yield response.get('list', [])

            if not response.get('next_offset'):
                break
            params['offset'] = response['next_offset']
