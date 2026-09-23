import unittest
from unittest.mock import Mock

from hotglue_etl_exceptions import InvalidPayloadError
from hotglue_singer_sdk.exceptions import RetriableAPIError

from target_airbase.clients import AirbaseSink


class ValidateResponseTest(unittest.TestCase):
    def test_logs_http_status_for_client_errors(self):
        sink = object.__new__(AirbaseSink)
        sink.logger = Mock()
        response = Mock(status_code=400, text="Entity not found", reason="Bad Request")

        with self.assertRaises(InvalidPayloadError):
            sink.validate_response(response)

        sink.logger.warning.assert_called_once_with(
            "Airbase request returned HTTP status %s", 400
        )

    def test_logs_http_status_for_server_errors(self):
        sink = object.__new__(AirbaseSink)
        sink.logger = Mock()
        response = Mock(
            status_code=500,
            text="Server error",
            reason="Server Error",
            url="https://api.airbase.io/v1/accounting/vendors/",
        )

        with self.assertRaises(RetriableAPIError):
            sink.validate_response(response)

        sink.logger.warning.assert_called_once_with(
            "Airbase request returned HTTP status %s", 500
        )
