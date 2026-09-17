from target_airbase.clients import get_base_url
import backoff
import logging
import requests

from hotglue_singer_sdk.exceptions import RetriableAPIError

ENTITY_SYNC_PATH = "/entity/sync/"
LOGGER = logging.getLogger(__name__)


def _entity_sync_url(config: dict) -> str:
    return f"{get_base_url(config).rstrip('/')}{ENTITY_SYNC_PATH}"


def _entity_sync_headers(config: dict) -> dict:
    return {
        "Authorization": f"Token {config.get('api_key')}",
        "Content-Type": "application/json",
    }


def _should_retry(response: requests.Response) -> bool:
    return response.status_code == 429 or 500 <= response.status_code < 600


@backoff.on_exception(
    backoff.expo,
    (RetriableAPIError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError),
    factor=2,
    max_tries=5,
    on_backoff=lambda details: LOGGER.warning(
        "Backing off entity sync request for %.1f seconds after %s tries.",
        details["wait"],
        details["tries"],
    ),
)
def _entity_sync_request(method: str, config: dict, payload: dict) -> requests.Response:
    response = requests.request(
        method,
        _entity_sync_url(config),
        json=payload,
        headers=_entity_sync_headers(config),
        timeout=300,
    )
    if _should_retry(response):
        raise RetriableAPIError(
            f"{response.status_code} Client Error: {response.reason} for url: {response.url}",
            response,
        )
    return response


def notify_entity_sync_start(config: dict) -> None:
    response = _entity_sync_request("POST", config, {"action": "START"})

    if response.status_code == 409:
        LOGGER.info("GL onboarding is already completed. Skipping entity sync start.")
        return

    response.raise_for_status()


def notify_entity_sync_complete(config: dict) -> None:
    response = _entity_sync_request("PATCH", config, {"action": "COMPLETE"})

    if response.status_code == 404 and response.json().get("detail") == "No active sync session found.":
        LOGGER.info("GL onboarding is already completed. Skipping entity sync complete.")
        return
    response.raise_for_status()
