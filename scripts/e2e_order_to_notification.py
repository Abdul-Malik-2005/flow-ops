"""End-to-end smoke: one real order reaches a real notification in Mailpit.

This is the cross-service hand-off the whole async pipeline was built for. It
drives the *real* stack — Order's HTTP API (and therefore its saga, outbox and
relay), the broker, the Notification consumer, its sender and SMTP — and checks
the result where a customer would see it. Nothing here re-implements a service:
the script only mints a token, creates an order and asks Mailpit what arrived.

Why a script and not a pytest: this span belongs to no single service. The Order
API is exercised over HTTP, the Notification side runs as its own containers, and
a system smoke is the tool for exactly that shape.

Run it against the compose stack:

    docker compose up -d
    cd services/order
    poetry run python ../../scripts/e2e_order_to_notification.py

Why mint the token here: Order verifies access tokens locally with Identity's
public key, so the E2E needs a valid signature, not a running Identity service.
The key is the same one the compose `order-api` mounts.

Configuration (defaults match `compose.yaml`):
    ORDER_API_URL            http://localhost:8002
    MAILPIT_API_URL          http://localhost:8025/api/v1/messages
    IDENTITY_PRIVATE_KEY_PATH  <repo>/services/identity/keys/jwt_private.pem
    JWT_KID                  identity-1
    RECIPIENT_DOMAIN         flowops.local
    POLL_TIMEOUT_SECONDS     60

Exit codes: 0 delivered, 1 an assertion/timeout, 2 misconfiguration.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import jwt

REPO_ROOT = Path(__file__).resolve().parents[1]

ORDER_API_URL = os.getenv("ORDER_API_URL", "http://localhost:8002").rstrip("/")
MAILPIT_API_URL = os.getenv(
    "MAILPIT_API_URL",
    "http://localhost:8025/api/v1/messages",
)
PRIVATE_KEY_PATH = Path(
    os.getenv(
        "IDENTITY_PRIVATE_KEY_PATH",
        str(REPO_ROOT / "services" / "identity" / "keys" / "jwt_private.pem"),
    )
)
JWT_KID = os.getenv("JWT_KID", "identity-1")
RECIPIENT_DOMAIN = os.getenv("RECIPIENT_DOMAIN", "flowops.local")
POLL_TIMEOUT_SECONDS = float(os.getenv("POLL_TIMEOUT_SECONDS", "60"))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1.0"))

#: The three notifications the composer can produce. Whichever one arrives proves
#: the pipeline; which one depends on Inventory's stock, which this script must
#: not (and cannot) influence.
ALLOWED_SUBJECTS = frozenset(
    {
        "Your FlowOps order is confirmed",
        "Your FlowOps order could not be placed",
        "Your FlowOps order was cancelled",
    }
)

#: Any well-formed ids work: a valid reserve yields `order.reserved`, an unknown
#: product yields `order.failed`. Both are terminal and both emit an event.
PRODUCT_ID = "11111111-1111-4111-8111-111111111111"
WAREHOUSE_ID = "33333333-3333-4333-8333-333333333333"

#: Statuses that mean the saga reached a terminal state and staged an event.
#: `202` is excluded: it means "Inventory was unreachable", so no event exists.
TERMINAL_STATUSES = frozenset({200, 201, 409, 422, 502})


def access_token(user_id: uuid.UUID) -> str:
    """Mint an access token the Order service will accept.

    Claims match Identity's contract (`docs/architecture.md` §4): `sub`, `role`,
    `type="access"`, `exp`, `iat`, `jti`, plus the `kid` header Order checks.
    """
    now = dt.datetime.now(dt.UTC)
    claims = {
        "sub": str(user_id),
        "role": "user",
        "type": "access",
        "iat": now,
        "exp": now + dt.timedelta(minutes=5),
        "jti": str(uuid.uuid4()),
    }
    return jwt.encode(
        claims,
        PRIVATE_KEY_PATH.read_bytes(),
        algorithm="RS256",
        headers={"kid": JWT_KID},
    )


def post_order(token: str) -> tuple[int, dict[str, object]]:
    """Create one order; return the status and the decoded body."""
    body = json.dumps(
        {
            "items": [
                {
                    "product_id": PRODUCT_ID,
                    "warehouse_id": WAREHOUSE_ID,
                    "quantity": 1,
                }
            ]
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{ORDER_API_URL}/api/v1/orders",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Idempotency-Key": str(uuid.uuid4()),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, _decode(response.read())
    except urllib.error.HTTPError as error:
        return error.code, _decode(error.read())


def _decode(raw: bytes) -> dict[str, object]:
    try:
        decoded = json.loads(raw.decode("utf-8") or "{}")
    except ValueError:
        return {"raw": raw.decode("utf-8", errors="replace")}
    return decoded if isinstance(decoded, dict) else {"raw": decoded}


def fetch_messages() -> dict[str, object]:
    with urllib.request.urlopen(MAILPIT_API_URL, timeout=5) as response:
        decoded = json.loads(response.read().decode("utf-8"))
    return decoded if isinstance(decoded, dict) else {}


def find_notification(recipient: str) -> dict[str, object] | None:
    """The first message addressed to `recipient` with a known subject."""
    for message in fetch_messages().get("messages", []):
        if not isinstance(message, dict):
            continue
        if message.get("Subject") not in ALLOWED_SUBJECTS:
            continue
        addresses = [
            entry.get("Address")
            for entry in message.get("To", [])
            if isinstance(entry, dict)
        ]
        if recipient in addresses:
            return message
    return None


def wait_for_notification(recipient: str) -> dict[str, object] | None:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            found = find_notification(recipient)
        except Exception as error:  # noqa: BLE001 - a poll must not abort the wait
            last_error = error
        else:
            if found is not None:
                return found
        time.sleep(POLL_INTERVAL_SECONDS)
    if last_error is not None:
        print(f"last Mailpit error: {last_error}", file=sys.stderr)
    return None


def main() -> int:
    if not PRIVATE_KEY_PATH.is_file():
        print(f"missing Identity private key: {PRIVATE_KEY_PATH}", file=sys.stderr)
        return 2

    user_id = uuid.uuid4()
    recipient = f"{user_id}@{RECIPIENT_DOMAIN}"

    try:
        status, body = post_order(access_token(user_id))
    except Exception as error:  # noqa: BLE001 - a smoke reports, it does not raise
        print(
            f"could not reach the Order API at {ORDER_API_URL}: {error}",
            file=sys.stderr,
        )
        return 1

    print(f"POST /api/v1/orders -> {status} (user={user_id})")
    if status == 202:
        print(
            "order stayed pending: Inventory is unreachable, so no event is "
            "emitted. Start `inventory-api` (docker compose up -d inventory-api).",
            file=sys.stderr,
        )
        return 1
    if status not in TERMINAL_STATUSES:
        print(f"unexpected create status {status}: {body}", file=sys.stderr)
        return 1

    print(
        f"waiting for a notification to {recipient} "
        f"(up to {POLL_TIMEOUT_SECONDS:.0f}s)..."
    )
    message = wait_for_notification(recipient)
    if message is None:
        print(
            f"timeout: no notification for {recipient} within "
            f"{POLL_TIMEOUT_SECONDS:.0f}s. Check `docker compose logs "
            "order-outbox-relay notification-event-consumer notification-sender`.",
            file=sys.stderr,
        )
        return 1

    print(f"notification delivered: subject={message.get('Subject')!r} to={recipient}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
