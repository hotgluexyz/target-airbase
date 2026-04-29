from target_airbase.clients import AirbaseSink
from hotglue_models_accounting.accounting import Account, Vendor, Subsidiary


class AccountsSink(AirbaseSink):
    name = "Accounts"
    endpoint = "/accounts/"
    unified_schema = Account

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload: dict = {
            "id": record.get("id"),
            "name": record.get("name"),
            "erp_reference_id": record.get("accountNumber"),
            "type": record.get("type"),
            "category": record.get("category") or "",
            "account_number": record.get("accountNumber")
        }
        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiaryRef"))
        payload["erp_currency_reference_id"] = self.get_currency(record.get("currency"))

        return payload


class SuppliersSink(AirbaseSink):
    name = "Vendors"
    endpoint = "/vendors/"
    unified_schema = Vendor

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload: dict = {
            "name": record.get("vendorName"),
            "erp_reference_id": record.get("vendorNumber"),
            "id": record.get("id"),
        }

        # lookup by erp_reference_id and name
        if not payload["id"]:
            vendor = next((v for v in self.vendors if v.get("erp_reference_id") == record.get("vendorNumber") and v.get("name") == record.get("vendorName")), None)
            if vendor:
                payload["id"] = vendor.get("airbase_id")

        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiaryRef"))
        return payload


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
            raise ValueError(f"Country is required for subsidiary '{record.get('subsidiaryNumber')}'")

        payload["iso_code"] = country

        return payload


class CurrenciesSink(AirbaseSink):
    name = "Currencies"
    endpoint = "/currencies/"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record


class LedgerEntriesSink(AirbaseSink):
    name = "LedgerEntries"
    endpoint = "/ledger_entries/"  # used to update bills (no POST endpoint available)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record
    

    def upsert_record(self, record: dict, context: dict):
        state_updates = {}

        # check if the bill is already marked as sync_complete
        record_id = record.pop("id", None)

        if not record_id:
            raise ValueError("Record ID is required to update a bill")
        
        response = self.request_api("GET", f"{self.endpoint}{record_id}/")
        res_json = response.json()
        if res_json.get("status") == "sync_complete":
            return record_id, True, {"existing": True}
        
        response = self.request_api("PATCH", f"{self.endpoint}{record_id}/", request_data=record)
        return record_id, response.ok, state_updates