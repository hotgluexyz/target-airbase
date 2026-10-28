import unittest
from unittest.mock import Mock

from hotglue_etl_exceptions import InvalidPayloadError
from hotglue_singer_sdk.exceptions import RetriableAPIError

from target_airbase.clients import AirbaseBatchSink, AirbaseSink


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

    def test_classifies_not_found_as_invalid_payload(self):
        sink = object.__new__(AirbaseSink)
        sink.logger = Mock()
        response = Mock(status_code=404, text="Entity not found", reason="Not Found")

        with self.assertRaises(InvalidPayloadError):
            sink.validate_response(response)

        sink.logger.warning.assert_called_once_with(
            "Airbase request returned HTTP status %s", 404
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


class BulkSuccessStatesTest(unittest.TestCase):
    def _sink(self) -> AirbaseBatchSink:
        sink = object.__new__(AirbaseBatchSink)
        sink.logger = Mock()
        sink.stream_name = "Accounts"
        return sink

    def test_updated_sets_is_updated(self):
        sink = self._sink()
        staging = [{
            "payload": {
                "airbase_id": "id-1",
                "erp_reference_id": "1000",
                "type": "expense",
            },
            "hash": "h1",
            "external_id": "ext-1",
        }]
        body = {
            "updated": [{"airbase_id": "id-1", "erp_reference_id": "1000"}],
            "created": [],
        }

        states = sink._success_states(staging, body)

        self.assertEqual(len(states), 1)
        self.assertTrue(states[0]["success"])
        self.assertTrue(states[0]["is_updated"])
        self.assertEqual(states[0]["id"], "id-1")
        self.assertNotIn("previous_id", states[0])
        sink.logger.info.assert_called()
        self.assertIn("updated", sink.logger.info.call_args[0][0])

    def test_id_rewrite_sets_previous_id_and_warns(self):
        sink = self._sink()
        staging = [{
            "payload": {
                "airbase_id": "old-id",
                "erp_reference_id": "1000",
                "type": "others",
            },
            "hash": "h1",
            "external_id": "ext-1",
        }]
        body = {
            "updated": [],
            "created": [{"airbase_id": "new-id", "erp_reference_id": "1000"}],
        }

        states = sink._success_states(staging, body)

        self.assertEqual(len(states), 1)
        self.assertTrue(states[0]["success"])
        self.assertNotIn("is_updated", states[0])
        self.assertEqual(states[0]["id"], "new-id")
        self.assertEqual(states[0]["previous_id"], "old-id")
        sink.logger.warning.assert_called()
        self.assertIn("created new id", sink.logger.warning.call_args[0][0])


class AccountsFlushRoutingTest(unittest.TestCase):
    def test_flush_staging_patches_when_airbase_id_present(self):
        from unittest.mock import patch

        from target_airbase.sinks import AccountsSink

        sink = object.__new__(AccountsSink)
        sink.logger = Mock()
        sink.stream_name = "Accounts"
        sink.update_endpoint = "/accounts/"
        sink.endpoint = "/accounts/bulk_upsert/"

        patched = []
        bulk_called = []

        def fake_patch(entry):
            patched.append(entry)
            return {
                "success": True,
                "hash": entry["hash"],
                "id": entry["payload"]["airbase_id"],
                "is_updated": True,
            }

        def fake_bulk(self, staging):
            bulk_called.append(staging)
            return [{"success": True, "hash": e["hash"], "id": "created-1"} for e in staging]

        sink._patch_account_state = fake_patch
        staging = [
            {"payload": {"airbase_id": "upd-1", "type": "others"}, "hash": "h-upd", "external_id": None},
            {"payload": {"type": "expense", "erp_reference_id": "9"}, "hash": "h-new", "external_id": None},
        ]
        with patch.object(AirbaseBatchSink, "_flush_staging", fake_bulk):
            states = AccountsSink._flush_staging(sink, staging)

        self.assertEqual(len(patched), 1)
        self.assertEqual(patched[0]["hash"], "h-upd")
        self.assertEqual(len(bulk_called), 1)
        self.assertEqual(bulk_called[0][0]["hash"], "h-new")
        self.assertEqual(len(states), 2)
