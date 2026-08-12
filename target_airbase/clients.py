from __future__ import annotations

from copy import deepcopy

from hotglue_singer_sdk.sinks.batch import BatchSink
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink, HotglueSink


def get_base_url(config: dict) -> str:
    if config.get("sandbox") is True:
        return "https://api-stage.airbase.io/v1/accounting"
    return "https://api.airbase.io/v1/accounting"


class AirbaseSink(HotglueSink):
    
    @property
    def base_url(self) -> str:
        return get_base_url(self.config)
    
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
            if not subsidiary and sub.get("subsidiaryNumber"):
                subsidiary = next(
                    (
                        s
                        for s in self.subsidiaries
                        if s.get("erp_reference_id") == sub.get("subsidiaryNumber")
                    ),
                    None
                )
            if not subsidiary and sub.get("erp_reference_id"):
                subsidiary = next(
                    (
                        s
                        for s in self.subsidiaries
                        if s.get("erp_reference_id") == sub.get("erp_reference_id")
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
        # Prefer a row with a non-blank erp_reference_id when Airbase has
        # duplicates (e.g. transaction-created + ETL-created currency). Currencies
        # created via transactions will have a blank erp_reference_id.
        matches = [c for c in self.currencies if c.get("iso_code") == currency]
        if not matches:
            raise ValueError(f"Currency {currency} not found")

        for match in matches:
            erp_reference_id = match.get("erp_reference_id")
            if erp_reference_id and str(erp_reference_id).strip():
                return erp_reference_id

        raise ValueError(
            f"Currency {currency} found on Airbase but has no erp_reference_id, "
            f"so we can't create {self.name} that reference it. "
            "This usually means it was auto-created from a transaction. "
        )

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


class AirbaseBatchSink(HotglueBatchSink, AirbaseSink):
    process_record = BatchSink.process_record
    max_size = 100

    @property
    def current_size(self) -> int:
        # RecordSink sets current_size=0; the SDK uses this to know when to flush a batch.
        return self._batch_records_read

    def _build_state(self, entry: dict, success: bool, **extra) -> dict:
        state = {"success": success, "hash": entry["hash"], **extra}
        if entry.get("external_id"):
            state["externalId"] = entry["external_id"]
        return state

    def _record_for_hash(self, payload: dict) -> dict:
        record = deepcopy(payload)
        airbase_id = record.pop("airbase_id", None)
        if airbase_id is not None:
            return {"id": airbase_id, **record}
        return record

    def process_batch_record(self, record: dict, index: int) -> dict:
        payload = deepcopy(record)
        airbase_id = payload.pop("id", None)
        if airbase_id:
            payload["airbase_id"] = airbase_id
        return payload

    def make_batch_request(self, records: list[dict]):
        return self.request_api("POST", self.endpoint, request_data={"entities": records})

    def _match_bulk_response_entity(self, payload: dict, entities: list[dict]) -> dict | None:
        """
        Match a payload to an entity in the bulk API response.
        """
        airbase_id = payload.get("airbase_id") or payload.get("id")
        erp_reference_id = payload.get("erp_reference_id")

        for entity in entities:
            entity_id = entity.get("airbase_id") or entity.get("id")
            if airbase_id and entity_id == airbase_id:
                return entity
            if erp_reference_id and entity.get("erp_reference_id") == erp_reference_id:
                return entity
        return None

    def _failed_states(self, staging: list[dict], body: dict) -> list[dict]:
        errors_by_index = {
            item["index"]: item
            for item in body.get("validation_errors", [])
        }
        return [
            self._build_state(entry, False, error=errors_by_index.get(index, body))
            for index, entry in enumerate(staging)
        ]

    def _success_states(self, staging: list[dict], body: dict) -> list[dict]:
        created = body.get("created") or []
        updated = body.get("updated") or []

        states = []
        for entry in staging:
            entity = self._match_bulk_response_entity(entry["payload"], updated)
            is_updated = entity is not None
            if not entity:
                entity = self._match_bulk_response_entity(entry["payload"], created)

            extra = {}
            record_id = None
            if entity:
                record_id = entity.get("airbase_id") or entity.get("id")
            success = bool(record_id)
            if record_id:
                extra["id"] = record_id
            if is_updated and success:
                extra["is_updated"] = True
            if not success:
                extra["error"] = "No Airbase id in bulk response"
            states.append(self._build_state(entry, success, **extra))
        return states

    def handle_batch_response(self, response, staging: list[dict]) -> dict:
        body = response.json()
        if not response.ok:
            return {"state_updates": self._failed_states(staging, body)}
        return {"state_updates": self._success_states(staging, body)}

    def _update_batch_duplicates(
        self,
        batch_duplicates: list[dict],
        updates: list[dict],
    ) -> None:
        for dup in batch_duplicates:
            existing = next(
                (s for s in updates if s.get("hash") == dup["hash"] and s.get("success")),
                None,
            ) or self.get_existing_state(dup["hash"])
            if existing:
                self.update_state(existing, is_duplicate=True, record=dup["record"])
                continue

            self.logger.info(
                f"Skipping duplicate record of type {self.name} in batch "
                f"(first attempt did not succeed): {dup['hash']}"
            )

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        staging = []
        batch_duplicates = []
        seen_in_batch: set[str] = set()
        external_id_key = self._target.EXTERNAL_ID_KEY

        for index, raw_record in enumerate(context.get("records", [])):
            external_id = None
            try:
                if self.name not in self.allows_externalid and raw_record.get(external_id_key):
                    external_id = raw_record.pop(external_id_key, None)
                payload = self.process_batch_record(raw_record, index)
            except Exception as e:
                self.logger.exception(f"Batch record error {str(e)}")
                self.update_state(
                    self._build_record_error_state(
                        e,
                        record=raw_record,
                        external_id=external_id,
                    ),
                    record=raw_record,
                )
                continue

            record_hash = self.build_record_hash(self._record_for_hash(payload))
            if record_hash in self.processed_hashes:
                self.logger.info(f"Record of type {self.name} already exists with hash: {record_hash}")
                continue

            existing_state = self.get_existing_state(record_hash)
            if self.name in self.allows_externalid:
                external_id = raw_record.get(external_id_key)
            else:
                external_id = external_id or raw_record.pop(external_id_key, None)

            if existing_state:
                self.update_state(
                    existing_state,
                    is_duplicate=True,
                    record=self._record_for_hash(payload),
                )
                continue

            if record_hash in seen_in_batch:
                batch_duplicates.append({
                    "hash": record_hash,
                    "record": self._record_for_hash(payload),
                })
                continue

            seen_in_batch.add(record_hash)
            staging.append({
                "payload": payload,
                "hash": record_hash,
                "external_id": external_id,
            })

        if not staging:
            return

        try:
            payloads = [entry["payload"] for entry in staging]
            response = self.make_batch_request(payloads)
            updates = self.handle_batch_response(response, staging)["state_updates"]
        except Exception as e:
            self.logger.exception(f"Bulk upsert error {str(e)}")
            updates = [self._build_state(entry, False, error=str(e)) for entry in staging]

        for state in updates:
            if state.get("success"):
                self.logger.info(f"{self.name} processed id: {state.get('id')}")
            self.update_state(state)

        self._update_batch_duplicates(batch_duplicates, updates)
