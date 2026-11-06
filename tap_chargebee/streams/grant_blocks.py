import json

import singer

from tap_chargebee.streams.base import BaseChargebeeStream


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

    def get_url(self):
        return (
            'https://{}/api/v2/grant_blocks/grant_block_list_internal'
        ).format(self.config.get('full_site'))

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

        url = 'https://{}/api/v2/subscriptions'.format(
            self.config.get('full_site'))
        # Grant blocks can change without the subscription changing.
        # Always scan all subscriptions, independently of their bookmark
        # and date filters.
        params = {}
        if self.config.get('include_deprecated') is True:
            params['include_deprecated'] = 'true'

        while True:
            response = self.client.make_request(
                url, self.API_METHOD, params=params)
            subscription_ids = [
                item['subscription']['id']
                for item in response.get('list', [])]
            for start in range(
                    0, len(subscription_ids), self.SUBSCRIPTION_BATCH_SIZE):
                yield subscription_ids[
                    start:start + self.SUBSCRIPTION_BATCH_SIZE]

            if not response.get('next_offset'):
                break
            params['offset'] = response['next_offset']

    def sync_data(self):
        for subscription_ids in self.get_subscription_batches():
            params = {'subscription_id[in]': json.dumps(subscription_ids)}
            while True:
                response = self.client.make_request(
                    self.get_url(), self.API_METHOD, params=params)
                records = self.get_stream_data(response.get('list', []))

                with singer.metrics.record_counter(
                        endpoint=self.TABLE) as counter:
                    singer.write_records(self.TABLE, records)
                    counter.increment(amount=len(records))

                if not response.get('next_offset'):
                    break
                params['offset'] = response['next_offset']
