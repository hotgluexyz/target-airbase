"""airbase target class."""

from typing import Type
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.sinks import Sink
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_airbase.sync_session_helpers import notify_entity_sync_complete, notify_entity_sync_start
from target_airbase.sinks import AccountsSink, SuppliersSink, SubsidiariesSink, CurrenciesSink


class TargetAirbase(TargetHotglue):
    """Sample target for airbase."""

    name = "target-airbase"
    reference_data = {}

    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True),
    ).to_dict()

    SINK_TYPES = [AccountsSink, SuppliersSink, SubsidiariesSink, CurrenciesSink]

    def listen(self, file_input=None):
        notify_entity_sync_start(dict(self.config))
        super().listen(file_input)

    def _process_endofpipe(self) -> None:
        super()._process_endofpipe()
        notify_entity_sync_complete(dict(self.config))


if __name__ == "__main__":
    TargetAirbase.cli()
