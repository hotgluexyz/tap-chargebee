from urllib.parse import urlsplit, urlunsplit

from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class ChargebeeTestRunner(VCRTapTestRunner):
    PRESERVE_KEYS = {
        # Incremental bookmarks and deduplication dates must keep their boundaries.
        "updated_at", "created_at", "date", "generated_at", "deleted",
        # Opaque numeric pagination cursors are reused verbatim in the next request.
        "next_offset",
        # The tap branches on configuration-incompatible API responses.
        "api_error_code", "error_code",
    }

    def module(self) -> str:
        return "tap_chargebee"

    def launch(self):
        from tap_chargebee import main

        main()

    def scrub_uri(self, uri: str) -> str:
        # A Chargebee site hostname identifies the customer account.
        parts = urlsplit(uri)
        return urlunsplit(parts._replace(netloc="fake-site.chargebee.com"))


if __name__ == "__main__":
    ChargebeeTestRunner.main()
