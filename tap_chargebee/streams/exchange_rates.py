from tap_chargebee.streams.base import BaseChargebeeStream
import singer
from tap_framework.config import get_config_start_date
import dateutil.tz as dtz
from tap_chargebee.state import incorporate, save_state
from datetime import datetime, timedelta

LOGGER = singer.get_logger()

class ExchangeRatesStream(BaseChargebeeStream):
    TABLE = 'exchange_rates'
    ENTITY = 'exchange_rate'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'date'
    KEY_PROPERTIES = ['rates']
    BOOKMARK_PROPERTIES = ['date']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['date']
    INCLUSION = 'available'
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://{}/api/v2/currencies/get_auto_exchange_rates'.format(self.config.get('full_site'))

    def _get_bookmark_start_date(self):
        """Return the first calendar day to request on this sync run."""
        tz = self.TIMEZONE
        last_value = self.state.get('bookmarks', {}).get(self.TABLE, {}).get('bookmark_date')
        if last_value is None:
            LOGGER.info(
                'Could not locate bookmark_date from STATE file. '
                'Falling back to start_date from config.json instead.'
            )
            start = get_config_start_date(self.config)
            if start.tzinfo is None:
                start = start.replace(tzinfo=dtz.UTC)
            return start.astimezone(tz).date()

        last_synced = datetime.strptime(last_value, '%Y-%m-%d').date()
        return last_synced + timedelta(days=1)

    def get_stream_data(self, data):
        # response is a dictionary with rates per date, map the fields to match other forex taps
        data = data['exchange_rates']
        return [
            {
                "date": data['exchange_rate_date'],
                "base": data['exchange_rate_base_currency_code'],
                "rates": data['rates']
            }
        ]

    def sync_data(self):
        table = self.TABLE
        api_method = self.API_METHOD
        bookmark_key = "date"

        LOGGER.info('Attempting to get the most recent bookmark_date for entity {}.'.format(self.ENTITY))
        tz = self.TIMEZONE
        current_date = self._get_bookmark_start_date()
        end_date = datetime.fromtimestamp(self.END_TIMESTAMP).date()

        while current_date <= end_date:
            start_date = current_date.strftime('%Y-%m-%d')
            LOGGER.info(f"Syncing {table} for {start_date}")
            params = {"exchange_rate_date": start_date}

            try:
                response = self.client.make_request(
                    url=self.get_url(),
                    method=api_method,
                    params=params)
            except:
                response = {}

            if response:
                to_write = self.get_stream_data(response)

                with singer.metrics.record_counter(endpoint=table) as ctr:
                    singer.write_records(table, to_write)
                    ctr.increment(amount=len(to_write))

                # update the state with the max date after each iteration
                # only update the state if the max date has changed
                if to_write and bookmark_key is not None:
                    self.state = incorporate(self.state, table, 'bookmark_date', current_date.strftime('%Y-%m-%d'))
                    save_state(self.state)

            current_date = current_date + timedelta(days=1)