"""Recipient resolution: a `user_id` becomes an address, behind a port.

The local resolver is the dev stand-in; its only contract is that it is stable and
obviously fake, so a misconfigured sender cannot email a stranger and the composer
never has to know where an address comes from.
"""

from __future__ import annotations

import uuid

from app.services.recipient_resolver import LocalRecipientResolver

USER_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")


async def test_the_local_resolver_builds_an_address_from_the_id() -> None:
    address = await LocalRecipientResolver(domain="example.test").resolve(
        user_id=USER_ID,
    )

    assert address == f"{USER_ID}@example.test"


async def test_the_default_domain_is_flowops_local() -> None:
    address = await LocalRecipientResolver().resolve(user_id=USER_ID)

    assert address == f"{USER_ID}@flowops.local"
