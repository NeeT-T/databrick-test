"""ETL — Load layer.

Sends the transformed address string to an AWS SQS queue.
When AWS credentials or the queue URL are not configured the message is
printed to stdout as a fallback, which is useful for local development
and Databricks Community Edition environments.
"""

import json
import os
from typing import Optional

try:
    import boto3  # type: ignore
    _BOTO3_AVAILABLE = True
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore
    _BOTO3_AVAILABLE = False


def load(message: str, queue_url: Optional[str] = None) -> dict:
    """Send the transformed address string to an SQS queue.

    The function tries to publish to SQS using boto3.  If boto3 is not
    installed, or the queue URL / AWS credentials are not available, it
    falls back to printing the message and returns a mock response so the
    pipeline can still be demonstrated end-to-end.

    Args:
        message: The address string produced by the Transform layer.
        queue_url: SQS queue URL.  Falls back to the ``SQS_QUEUE_URL``
            environment variable if not provided.

    Returns:
        A dictionary with ``"MessageId"`` and ``"status"`` keys.
    """
    resolved_url = queue_url or os.environ.get("SQS_QUEUE_URL", "")

    if resolved_url and _BOTO3_AVAILABLE and boto3 is not None:
        try:
            client = boto3.client("sqs")
            response = client.send_message(
                QueueUrl=resolved_url,
                MessageBody=json.dumps({"address": message}),
            )
            return {
                "MessageId": response.get("MessageId", ""),
                "status": "sent_to_sqs",
                "queue_url": resolved_url,
            }
        except Exception as exc:  # pragma: no cover
            print(f"[WARN] Could not send to SQS: {exc}. Falling back to stdout.")

    # Fallback — print to stdout (works in Databricks Community Edition)
    print(f"[LOAD] {message}")
    return {
        "MessageId": None,
        "status": "printed_to_stdout",
        "message": message,
    }
