ENTITY_SYNC_PATH = "/entity/sync/"

from target_airbase.clients import AirbaseSink
import requests
import logging

LOGGER = logging.getLogger(__name__)

def _entity_sync_url() -> str:
    return f"{AirbaseSink.base_url.rstrip('/')}{ENTITY_SYNC_PATH}"


def _entity_sync_headers(config: dict) -> dict:
    return {
        "Authorization": f"Token {config.get('api_key')}",
        "Content-Type": "application/json",
    }


def notify_entity_sync_start(config: dict) -> None:
    response = requests.post(
        _entity_sync_url(),
        json={"action": "START"},
        headers=_entity_sync_headers(config),
        timeout=300,
    )

    if response.status_code == 409:
        LOGGER.info("GL onboarding is already completed. Skipping entity sync start.")
        return
    
    response.raise_for_status()


def notify_entity_sync_complete(config: dict) -> None:
    response = requests.patch(
        _entity_sync_url(),
        json={"action": "COMPLETE"},
        headers=_entity_sync_headers(config),
        timeout=300,
    )

    if response.status_code == 404 and response.json().get("detail") == "No active sync session found.":
        LOGGER.info("GL onboarding is already completed. Skipping entity sync complete.")
        return
    response.raise_for_status()