"""ORM models.

Every model module is imported here so that Alembic's autogenerate sees the
full `Base.metadata`.
"""

from app.models.notification_inbox import NotificationInbox

__all__ = ["NotificationInbox"]
