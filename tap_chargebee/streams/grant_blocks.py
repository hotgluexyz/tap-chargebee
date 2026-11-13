import json

import singer

from tap_chargebee.streams.base import BaseChargebeeStream
from tap_chargebee.streams.subscriptions import SubscriptionsStream


class GrantBlocksStream(BaseChargebeeStream):
    TABLE = 'grant_blocks'
    ENTITY = 'grant_block'
    REPLICATION_METHOD = 'FULL'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = []
    VALID_REPLICATION_KEYS = []
    SELECTED_BY_DEFAULT = True
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SUBSCRIPTION_BATCH_SIZE = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.PARENT_STREAM_INSTANCE = SubscriptionsStream(*args, **kwargs)
        self.synced_with_subscriptions = False

    def get_url(self):
        base_url = (self.config.get('grant_blocks_base_url')
                    or 'https://{}'.format(self.config.get('full_site')))
        if base_url.startswith('http://'):
            raise ValueError('grant_blocks_base_url must use https://')
        if not base_url.startswith('https://'):
            base_url = 'https://{}'.format(base_url)
        return '{}/api/v2/grant_blocks/grant_block_list_internal'.format(
            base_url.rstrip('/'))

    def get_subscription_batches(self):
        if 'grant_blocks_subscription_ids' in self.config:
            subscription_ids = self.config['grant_blocks_subscription_ids']
            if (not isinstance(subscription_ids, list) or not subscription_ids
                    or any(not isinstance(sid, str) or not sid.strip()
                           for sid in subscription_ids)):
                raise ValueError(
                    'grant_blocks_subscription_ids must be a non-empty list '
                    'of subscription IDs')
            subscription_ids = list(dict.fromkeys(
                sid.strip() for sid in subscription_ids))
            for start in range(
                    0, len(subscription_ids), self.SUBSCRIPTION_BATCH_SIZE):
                yield subscription_ids[
                    start:start + self.SUBSCRIPTION_BATCH_SIZE]
            return

        subscription_ids = []
        for item in self.PARENT_STREAM_INSTANCE.sync_parent_data(
                full_refresh=True):
            subscription_ids.append(item['subscription']['id'])
            if len(subscription_ids) == self.SUBSCRIPTION_BATCH_SIZE:
                yield subscription_ids
                subscription_ids = []

        if subscription_ids:
            yield subscription_ids

    def sync_data(self):
        if self.synced_with_subscriptions:
            return

        for subscription_ids in self.get_subscription_batches():
            self.sync_subscription_batch(subscription_ids)

    def sync_subscription_batch(self, subscription_ids):
        params = {'subscription_id[in]': json.dumps(subscription_ids)}
        while True:
            response = self.client.make_request(
                self.get_url(), self.API_METHOD, params=params)
            records = self.get_stream_data(response.get('list', []))

            with singer.metrics.record_counter(
                    endpoint=self.TABLE) as counter:
                singer.write_records(self.TABLE, records)
                counter.increment(amount=len(records))

            next_offset = response.get('next_offset')
            if not next_offset:
                break
            if next_offset == params.get('offset'):
                raise RuntimeError('Grant block pagination did not advance')
            params['offset'] = next_offset
