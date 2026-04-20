from target_airbase.clients import AirbaseSink
from hotglue_models_accounting.accounting import Account, Vendor, Subsidiary


class AccountsSink(AirbaseSink):
    name = "Accounts"
    endpoint = "/accounts/"
    unified_schema = Account

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload: dict = {
            "name": record.get("name"),
            "erp_reference_id": record.get("accountNumber"),
            "type": record.get("type"),
            "category": record.get("category") or "",
            "erp_currency_reference_id": record.get("currency"),  # iso code symbol
            "account_number": record.get("accountNumber")
        }
        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiaryNumber"))
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
        }

        payload["subsidiary_reference_ids"] = self.get_subsidiary(record.get("subsidiaryNumber"))
        return payload


class SubsidiariesSink(AirbaseSink):
    name = "Subsidiaries"
    endpoint = "/subsidiaries/"
    unified_schema = Subsidiary

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload: dict = {
            "erp_reference_id": record.get("subsidiaryNumber"),
            "name": record.get("name"),
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