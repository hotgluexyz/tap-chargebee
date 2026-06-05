from tap_chargebee.streams.base import BaseChargebeeStream
import singer
from tap_framework.config import get_config_start_date
from dateutil.parser import parse
from tap_chargebee.state import get_last_record_value_for_table, incorporate, save_state
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
        bookmark_date = get_last_record_value_for_table(self.state, table, 'bookmark_date')

        # If there is no bookmark date, fall back to using the start date from the config
        if bookmark_date is None:
            LOGGER.info('Could not locate bookmark_date from STATE file. Falling back to start_date from config.json instead.')
            bookmark_date = get_config_start_date(self.config)
        else:
            bookmark_date = parse(bookmark_date)

        # Convert bookmarked start date to datetime
        current_window_start_dt = int(bookmark_date.timestamp())
        
        while current_window_start_dt < self.START_TIMESTAP:
            start_date = datetime.fromtimestamp(current_window_start_dt).strftime('%Y-%m-%d')
            LOGGER.info(f"Syncing {table} for {start_date}")
            params = {"exchange_rate_date": start_date}
            response = self.client.make_request(
                url=self.get_url(),
                method=api_method,
                params=params)

            to_write = self.get_stream_data(response)

            with singer.metrics.record_counter(endpoint=table) as ctr:
                singer.write_records(table, to_write)
                ctr.increment(amount=len(to_write))

            max_date = datetime.fromtimestamp(current_window_start_dt)

            # update the state with the max date after each iteration
            # only update the state if the max date has changed
            if bookmark_key is not None:
                self.state = incorporate(
                    self.state, table, 'bookmark_date', max_date)
                save_state(self.state)

            # Move to next day
            current_window_start_dt = int((max_date + timedelta(days=1)).timestamp())