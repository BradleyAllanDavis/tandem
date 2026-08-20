"""HubClient over direct ledger calls — used ONLY by the hub's in-process
gateway worker (same process, same trust domain as the ledger itself; a
token would be theater). Implements the same interface as
spoke.hub_http.HttpHubClient, so SpokeCore can't tell the difference.
"""

from __future__ import annotations

from .api import DEFAULT_LEASE_SECONDS
from .ledger import Ledger, Principal


class DirectHubClient:
    def __init__(self, ledger: Ledger, principal: Principal,
                 lease_seconds: float = DEFAULT_LEASE_SECONDS):
        self.ledger = ledger
        self.principal = principal
        self.lease_seconds = lease_seconds

    def push_transfer(self, to: str, src_uuid: str, payload: dict, rev: int = 1) -> dict:
        return self.ledger.push_transfer(self.principal, to, src_uuid, payload, rev)

    def deliveries(self, limit: int = 10) -> list:
        return self.ledger.lease_deliveries(self.principal, limit, self.lease_seconds)

    def ack(self, delivery_id: str, dst_uuid=None) -> dict:
        return self.ledger.ack_delivery(self.principal, delivery_id, dst_uuid=dst_uuid)

    def nack(self, delivery_id: str, error: str) -> dict:
        return self.ledger.nack_delivery(self.principal, delivery_id, error)

    def watch(self) -> list:
        return self.ledger.watchlist(self.principal)

    def observe(self, transfer_id: str, state: str) -> dict:
        return self.ledger.observe(self.principal, transfer_id, state)

    def mark_retagged(self, transfer_id: str) -> dict:
        return self.ledger.mark_retagged(self.principal, transfer_id)
