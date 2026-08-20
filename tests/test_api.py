"""HTTP surface tests — a real in-process server on an ephemeral port,
exercised through the same HttpHubClient a deployed spoke uses."""

import os
import tempfile
import threading
import time
import unittest

from hub.api import make_server
from hub.ledger import Ledger
from spoke.hub_http import HttpHubClient, HubHTTPError

PAYLOAD = {"schema": "tandem.todo/1", "title": "水 filter", "notes": "",
           "checklist": ["a", "b"], "when": None, "deadline": None,
           "context_url": None}


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.ledger = Ledger(os.path.join(cls.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        tenant = cls.ledger.create_tenant("davis")["id"]
        bradley = cls.ledger.create_member(tenant, "bradley", "B", can_admin=True)
        jill = cls.ledger.create_member(tenant, "jill", "J")
        cls.b_token = cls.ledger.create_device(bradley["id"], "gw")["token"]
        cls.j_token = cls.ledger.create_device(jill["id"], "air")["token"]
        cls.server = make_server(cls.ledger, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.ledger.close()
        cls.tmp.cleanup()

    def client(self, token):
        return HttpHubClient(self.url, lambda: token)

    def test_auth_rejected(self):
        with self.assertRaises(HubHTTPError) as ctx:
            self.client("garbage").health()
        self.assertEqual(ctx.exception.status, 401)

    def test_full_loop_over_http(self):
        b = self.client(self.b_token)
        j = self.client(self.j_token)
        rec = b.push_transfer("jill", "SRC-HTTP-1", PAYLOAD)
        self.assertFalse(rec["deduped"])
        rec2 = b.push_transfer("jill", "SRC-HTTP-1", PAYLOAD)
        self.assertTrue(rec2["deduped"])

        d = j.deliveries()[0]
        self.assertEqual(d["kind"], "create")
        self.assertEqual(d["payload"]["title"], "水 filter")
        self.assertEqual(d["provenance_tag"], "from-bradley 👨")
        j.ack(d["id"], dst_uuid="DST-HTTP-1")

        watch_b = b.watch()
        self.assertEqual(watch_b[0]["state"], "applied")
        self.assertEqual(watch_b[0]["role"], "sender")

        j.observe(rec["id"], "completed")
        echo = b.deliveries()[0]
        self.assertEqual(echo["kind"], "complete")
        self.assertEqual(echo["uuid"], "SRC-HTTP-1")
        b.ack(echo["id"])
        self.assertEqual(b.watch(), [])
        self.assertTrue(b.health()["ok"])

    def test_nack_over_http(self):
        b = self.client(self.b_token)
        j = self.client(self.j_token)
        b.push_transfer("jill", "SRC-HTTP-2", PAYLOAD)
        d = j.deliveries()[0]
        j.nack(d["id"], "correlation timeout")
        time.sleep(0.02)  # past the test's injected 0.001s backoff floor
        d2 = j.deliveries()[0]
        self.assertEqual(d["id"], d2["id"])
        j.ack(d2["id"], dst_uuid="DST-HTTP-2")

    def test_admin_requires_can_admin(self):
        j = self.client(self.j_token)
        with self.assertRaises(HubHTTPError) as ctx:
            j._request("POST", "/v1/admin/members",
                       {"handle": "eve", "display_name": "Eve"})
        self.assertEqual(ctx.exception.status, 403)

    def test_admin_member_and_device_lifecycle(self):
        b = self.client(self.b_token)
        m = b._request("POST", "/v1/admin/members",
                       {"handle": "aaron", "display_name": "Aaron"})
        dev = b._request("POST", "/v1/admin/devices",
                         {"member_id": m["id"], "name": "phone"})
        self.assertIn("token", dev)
        # the new token works…
        aaron = self.client(dev["token"])
        self.assertTrue(aaron.health()["ok"])
        # …until revoked
        b._request("POST", f"/v1/admin/devices/{dev['id']}/revoke", {})
        with self.assertRaises(HubHTTPError) as ctx:
            aaron.health()
        self.assertEqual(ctx.exception.status, 401)


if __name__ == "__main__":
    unittest.main()
