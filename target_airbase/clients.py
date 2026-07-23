from hotglue_singer_sdk.target_sdk.client import HotglueSink


class AirbaseSink(HotglueSink):
    
    base_url = "https://api-stage.airbase.io/v1/accounting"
    
    @property
    def name(self) -> str:
        return self.stream_name
    
    @property
    def http_headers(self) -> dict:
        return {
            "Authorization": f"Token {self.config.get('api_key')}",
            "Content-Type": "application/json",
        }

    def get_data(self, endpoint: str) -> list[dict]:
        params = {"page": 1, "page_size": 250}
        data = []
        while True:
            response = self.request_api("GET", endpoint, params=params)
            data.extend(response.json().get("data", []))
            if response.json().get("next") is not None:
                params["page"] += 1
            else:
                break
        return data

    @property
    def subsidiaries(self) -> list[dict]:
        if self._target.reference_data.get("subsidiaries") is None:
            subsidiaries = self.get_data("/subsidiaries/")
            self._target.reference_data["subsidiaries"] = subsidiaries
        return self._target.reference_data["subsidiaries"]
    
    @property
    def currencies(self) -> list[dict]:
        if self._target.reference_data.get("currencies") is None:
            currencies = self.get_data("/currencies/")
            self._target.reference_data["currencies"] = currencies
        return self._target.reference_data["currencies"]
    

    @property
    def vendors(self) -> list[dict]:
        if self._target.reference_data.get("vendors") is None:
            vendors = self.get_data("/vendors/")
            self._target.reference_data["vendors"] = [{
                "airbase_id": v.get("airbase_id"),
                "name": v.get("name"),
                "erp_reference_id": v.get("erp_reference_id"),
            } for v in vendors]
        return self._target.reference_data["vendors"]

    def get_subsidiary(self, subsidiary_ref: str) -> dict:
        mapped_subsidiaries = []
        for sub in subsidiary_ref:
            subsidiary = None
            if sub.get("id"):
                subsidiary = next(
                    (
                        s
                        for s in self.subsidiaries
                        if s.get("airbase_id") == sub.get("id")
                    ),
                    None
                )
            if sub.get("subsidiaryNumber"):
                subsidiary = next(
                    (
                        s
                        for s in self.subsidiaries
                        if s.get("erp_reference_id") == sub.get("subsidiaryNumber")
                    ),
                    None
                )
            if not subsidiary and sub.get("name"):
                subsidiary = next(
                    (
                        s
                        for s in self.subsidiaries
                        if s.get("name") == sub.get("name")
                    ),
                    None,
                )

            if not subsidiary:
                raise ValueError(f"Subsidiary {sub} not found")

            mapped_subsidiaries.append(
                {
                    "airbase_id": subsidiary.get("airbase_id"),
                    "erp_reference_id": subsidiary.get("erp_reference_id"),
                }
            )
        return mapped_subsidiaries

    def get_currency(self, currency: str) -> dict:
        currency = next(
            (c for c in self.currencies if c.get("iso_code") == currency),
            None,
        )
        if not currency:
            raise ValueError(f"Currency {currency} not found")

        return currency.get("erp_reference_id")

    def upsert_record(self, record: dict, context: dict):
        record_id = record.pop("id", None)
        is_update = record_id is not None

        endpoint = self.endpoint
        method = "POST"

        if is_update:
            endpoint = f"{self.endpoint}{record_id}/"
            method = "PATCH"

            # erp_reference_id can't be updated
            record.pop("erp_reference_id", None)

        response = self.request_api(method, endpoint, request_data=record)
        id = response.json().get("airbase_id")

        state_updates = {}
        if is_update and response.ok:
            state_updates = {"is_updated": True}

        return id, response.ok, state_updates
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record