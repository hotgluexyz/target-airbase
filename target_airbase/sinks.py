from copy import deepcopy

from hotglue_etl_exceptions import InvalidPayloadError
from target_airbase.clients import AirbaseBatchSink, AirbaseSink
from hotglue_models_accounting.accounting import Account, Vendor, Subsidiary


class AccountsSink(AirbaseBatchSink):
    name = "Accounts"
    endpoint = "/accounts/bulk_upsert/"
    # bulk_upsert creates a new airbase_id when type changes; PATCH updates type in place.
    update_endpoint = "/accounts/"
    unified_schema = Account

    def process_batch_record(self, record: dict, index: int) -> dict:
        payload: dict = {
            "id": record.get("id"),
            "name": record.get("name"),
            "erp_reference_id": record.get("accountNumber"),
            "type": record.get("type"),
            "category": record.get("category") or "",
            "account_number": record.get("accountNumber"),
        }
        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiaryRef"))
        payload["erp_currency_reference_id"] = self.get_currency(record.get("currency"))
        return super().process_batch_record(payload, index)

    def _patch_account_state(self, entry: dict) -> dict:
        payload = deepcopy(entry["payload"])
        record_id = payload.pop("airbase_id", None)
        payload.pop("erp_reference_id", None)
        try:
            response = self.request_api(
                "PATCH",
                f"{self.update_endpoint}{record_id}/",
                request_data=payload,
            )
            returned_id = response.json().get("airbase_id") or record_id
            success = response.ok and bool(returned_id)
            extra = {"id": returned_id} if returned_id else {}
            if success:
                extra["is_updated"] = True
            else:
                extra["error"] = response.text or response.reason
            self._log_bulk_result(
                is_updated=success,
                success=success,
                record_id=returned_id,
                sent_id=record_id,
                payload=entry["payload"],
            )
            return self._build_state(entry, success, **extra)
        except Exception as e:
            self.logger.exception("Accounts PATCH error %s", e)
            metadata = self._get_error_classification_metadata(e)
            return self._build_state(entry, False, error=str(e), **metadata)

    def _flush_staging(self, staging: list[dict]) -> list[dict]:
        """
        Send records to Airbase and return states.

        Creates new accounts and patches updates to existing accounts.
        """
        creates = [entry for entry in staging if not entry["payload"].get("airbase_id")]
        updates = [entry for entry in staging if entry["payload"].get("airbase_id")]

        states: list[dict] = []
        if creates:
            states.extend(super()._flush_staging(creates))
        # Patch updates to existing accounts
        for entry in updates:
            states.append(self._patch_account_state(entry))
        return states


class SuppliersSink(AirbaseBatchSink):
    name = "Vendors"
    endpoint = "/vendors/bulk_upsert/"
    unified_schema = Vendor

    def process_batch_record(self, record: dict, index: int) -> dict:
        payload: dict = {
            "name": record.get("vendorName"),
            "erp_reference_id": record.get("vendorNumber"),
            "id": record.get("id"),
        }

        if not payload["id"]:
            vendor = next(
                (
                    v
                    for v in self.vendors
                    if v.get("erp_reference_id") == record.get("vendorNumber")
                    and v.get("name") == record.get("vendorName")
                ),
                None,
            )
            if vendor:
                payload["id"] = vendor.get("airbase_id")

        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiaryRef"))
        return super().process_batch_record(payload, index)


class SubsidiariesSink(AirbaseSink):
    name = "Subsidiaries"
    endpoint = "/subsidiaries/"
    unified_schema = Subsidiary

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload: dict = {
            "erp_reference_id": record.get("subsidiaryNumber"),
            "name": record.get("name"),
            "id": record.get("id"),
        }
        payload["erp_currency_reference_id"] = self.get_currency(record.get("currency"))
        country = record.get("addresses")[0].get("country") if record.get("addresses") else None

        if not country:
            raise InvalidPayloadError(f"Country is required for subsidiary '{record.get('subsidiaryNumber')}'")

        payload["iso_code"] = country

        return payload


class CurrenciesSink(AirbaseSink):
    name = "Currencies"
    endpoint = "/currencies/"


class LedgerEntriesSink(AirbaseSink):
    name = "LedgerEntries"
    endpoint = "/ledger_entries/"  # used to update bills (no POST endpoint available)

    def upsert_record(self, record: dict, context: dict):
        state_updates = {}

        # check if the bill is already marked as sync_complete
        record_id = record.pop("id", None)

        if not record_id:
            raise InvalidPayloadError("Record ID is required to update a bill")

        if record.get("status") == "sync_complete":
            record["error_message"] = None

        response = self.request_api("PATCH", f"{self.endpoint}{record_id}/", request_data=record)
        return record_id, response.ok, state_updates


class TagsSink(AirbaseBatchSink):
    name = "Tags"
    endpoint = "/tags/bulk_upsert/"

    def process_batch_record(self, record: dict, index: int) -> dict:
        payload = dict(record)
        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiary_reference_ids"))
        return super().process_batch_record(payload, index)
