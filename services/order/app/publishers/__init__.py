"""Adapters that publish committed order events outside the database.

One module per transport: `log` today, a broker adapter when a consumer exists.
The port lives in `app.services.outbox_relay`.
"""
