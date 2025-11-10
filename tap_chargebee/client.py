import backoff
import time
import requests
import singer
import json

from simplejson.scanner import JSONDecodeError
from singer import utils
from tap_framework.client import BaseClient


LOGGER = singer.get_logger()

REQUEST_CONNECT_TIMEOUT = 10
REQUEST_READ_TIMEOUT = 300

class Server4xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class ChargebeeClient(BaseClient):

    def __init__(self, config, api_result_limit=100):
        super().__init__(config)

        self.api_result_limit = api_result_limit
        self.include_deleted = self.config.get('include_deleted', True)
        self.user_agent = self.config.get('user_agent')

    def get_headers(self):
        headers = {}

        if self.config.get('user_agent'):
            headers['User-Agent'] = self.config.get('user_agent')
        
        if self.config.get('customHeaders'):
            if not isinstance(self.config.get('customHeaders'), list):
                raise TypeError("Expected 'customHeaders' in config to be a list if provided")
            
            for header in self.config.get('customHeaders'):
                if "name" not in header or "value" not in header:
                    raise TypeError("Expected 'name' and 'value' in 'customHeaders' in config")
                headers[header['name']] = header['value']

        return headers

    def get_params(self, params):

        if params is None:
            params = {}

        params['limit'] = self.api_result_limit
        params['include_deleted'] = self.include_deleted

        return params

    @backoff.on_exception(backoff.expo,
                          (Server4xxError, Server429Error, JSONDecodeError, requests.exceptions.RequestException),
                          max_tries=6,
                          factor=2)
    @utils.ratelimit(100, 60)
    def make_request(self, url, method, params=None, body=None):

        if params is None:
            params = {}

        LOGGER.info("Making {} request to {} with the following params {}".format(method, url, params))

        response = requests.request(
            method,
            url,
            auth=(self.config.get("api_key"), ''),
            headers=self.get_headers(),
            params=self.get_params(params),
            json=body,
            timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT)
        )

        LOGGER.info(f"Processing response {response.status_code}")

        if response.status_code == 429:
            if not "Retry-After" in  response.headers:
                LOGGER.info("Status code 429 was raised without Retry-After")
            sleep_time = response.headers.get("Retry-After", 60)
            LOGGER.info(f"Status code 429. Sleeping for {sleep_time} seconds.")
            time.sleep(int(sleep_time))
            raise Server429Error()

        if response.status_code >= 400:
            if "configuration_incompatible" in response.text and "/price_variants" in url:
                LOGGER.warning(f"{response.request.url}: {response.text}")
            else:
                try:
                    error_json = response.json()
                    error_msg = error_json.get("error_msg") or error_json.get("message") or "Unknown error"
                except JSONDecodeError as e:
                    error_msg = response.text
                    LOGGER.warning(f"HTTP {response.status_code} error at {url}: Could not parse JSON error. Raw response: {error_msg}. Exception: {e}")
                else:
                    LOGGER.warning(f"HTTP {response.status_code} error at {url}: {error_msg}")

                raise Server4xxError(error_msg)

        response_json = response.json()

        return response_json
