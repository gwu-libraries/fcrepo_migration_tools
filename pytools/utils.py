from datetime import datetime
from typing import List


def uri_to_id(uri: str | List[str]):
    if isinstance(uri, list):
        return [uri_to_id(element) for element in uri]
    return uri.split("/")[-1]


def convert_date(date_str: str) -> str:
    # Format date without timestamp for Bulkrax
    return datetime.fromisoformat(date_str).replace(tzinfo=None).strftime("%Y-%m-%d")


def is_active_embargo(record) -> bool:
    return (
        datetime.fromisoformat(record["embargo_release_date"]).replace(tzinfo=None)
        >= datetime.now()
    )
