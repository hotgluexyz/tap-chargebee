import singer
import tap_framework
import tap_chargebee.client
import tap_chargebee.streams

LOGGER = singer.get_logger()


class ChargebeeRunner(tap_framework.Runner):
    def get_streams_to_replicate(self):
        streams = super().get_streams_to_replicate()
        subscriptions = next(
            (stream for stream in streams if stream.TABLE == 'subscriptions'),
            None)
        grant_blocks = next(
            (stream for stream in streams if stream.TABLE == 'grant_blocks'),
            None)

        if (subscriptions is not None and grant_blocks is not None
                and 'grant_blocks_subscription_ids' not in self.config):
            grant_blocks.PARENT_STREAM_INSTANCE = subscriptions
            subscriptions.grant_blocks_stream = grant_blocks
            # Sync grant blocks during the subscription scan, in either order.
            if streams.index(subscriptions) > streams.index(grant_blocks):
                streams.remove(grant_blocks)
                streams.insert(streams.index(subscriptions) + 1, grant_blocks)

        return streams


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(
        required_config_keys=['api_key', 'start_date'])

    full_site = args.config.get('full_site', None)
    args.config['include_deprecated'] = args.config.get('include_deprecated', False)
    
    product_catalog = args.config.get('product_catalog', '1.0')

    site = args.config.get('site', None)
    if full_site is None and site is None:
        raise Exception("Config is missing required key: atleast one key 'site' or 'full_site' is required")

    if full_site is None:
        args.config['full_site'] = f"{site}.chargebee.com"

    client = tap_chargebee.client.ChargebeeClient(args.config)

    available_streams = None
    if args.discover:
        available_streams = tap_chargebee.streams.AVAILABLE_STREAMS_ALL
    else:
        available_streams = tap_chargebee.streams.AVAILABLE_STREAMS_2_0 if product_catalog == "2.0" else tap_chargebee.streams.AVAILABLE_STREAMS_1_0


    runner = ChargebeeRunner(
        args, client, available_streams
        )

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == '__main__':
    main()
