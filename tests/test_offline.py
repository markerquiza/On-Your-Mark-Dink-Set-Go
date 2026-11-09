import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from server import Handler, ThreadingHTTPServer, backup_database, connect, initialize


class OfflineDeskTest(unittest.TestCase):
    def setUp(self):
        folder = self.enter_folder()
        self.path = folder / "queue.sqlite3"
        initialize(self.path, 6)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.database = self.path
        self.addCleanup(self.server.server_close)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)

    def enter_folder(self):
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        return Path(folder.name)

    def request(self, route, data=None):
        url = f"http://127.0.0.1:{self.server.server_port}{route}"
        body = json.dumps(data).encode() if data is not None else None
        request = Request(url, body, {"Content-Type": "application/json"})
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    def action(self, action, expected=200, **fields):
        status, result = self.request("/api/action", dict(action=action, **fields))
        self.assertEqual(status, expected, result)
        return result

    def test_session_and_restore(self):
        self.action("assignment_mode", enabled=False)
        for n in range(4):
            self.action("join", name=f"Player {n}", phone=f"0917000000{n}", level="Beginner")
        _, board = self.request("/api/state")
        self.assertEqual(len(board["courts"]), 6)
        self.assertTrue(all(not court["match_id"] for court in board["courts"]))
        group = board["next_group"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [pool.submit(self.request, "/api/action", dict(action="call", court=c, group=group))
                     for c in (1, 2)]
        self.assertEqual(sorted(call.result()[0] for call in calls), [200, 409])
        _, board = self.request("/api/state")
        court = next(court for court in board["courts"] if court["match_id"])
        target = dict(court=court["id"], match=court["match_id"])
        self.action("start", **target)
        self.action("end_session", expected=409, session=board["session"]["id"])
        result = self.action("finish", **target, score_a=11, score_b=8)
        self.assertEqual(len(result["queue"]), 4)
        self.action("finish", expected=409, **target, score_a=11, score_b=8)
        result = self.action("end_session", session=board["session"]["id"])
        self.assertIn("Backup verified", result["backup_message"])
        saved, = (self.path.parent / "backups").glob("*.sqlite3")
        restored = backup_database(saved, self.path.parent / "restored.sqlite3")
        initialize(restored, 6)
        with closing(connect(restored)) as db:
            self.assertEqual(db.execute("SELECT score_a FROM matches").fetchone()[0], 11)
            self.assertEqual(db.execute("SELECT count(*) FROM profiles").fetchone()[0], 4)

    def test_profile_identity_survives_phone_change_and_reuse(self):
        self.action("assignment_mode", enabled=False)
        for n in range(4):
            self.action("join", name=f"Player {n}", phone=f"0917000000{n}", level="Beginner")
        _, players = self.request("/api/members?q=0000")
        player_id = players[0]["id"]
        _, board = self.request("/api/state")
        result = self.action("call", court=1, group=board["next_group"])
        match = result["courts"][0]["match_id"]
        self.action("start", court=1, match=match)
        self.action("finish", court=1, match=match, score_a=11, score_b=7)
        self.action("edit_member", member=player_id, name="Renamed", phone="09170000008",
                    level="Intermediate")
        self.action("add_member", name="New player", phone="09170000000", level="Beginner")
        initialize(self.path, 6)
        _, renamed = self.request("/api/members?q=0008")
        _, new = self.request("/api/members?q=0000")
        self.assertEqual(renamed[0]["id"], player_id)
        self.assertNotEqual(new[0]["id"], player_id)
        _, history = self.request("/api/member-history?id=" + player_id)
        self.assertEqual(history[0]["score"], 11)
        self.assertEqual(history[0]["partners"], ["Player 1"])
        _, new_history = self.request("/api/member-history?id=" + new[0]["id"])
        self.assertEqual(new_history, [])
        with closing(connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM session_participants").fetchone()[0], 4)


    def register(self, number, level="Beginner"):
        return self.action("join", name=f"Player {number}", phone=f"09170000{number:03d}", level=level)

    def test_automatic_assignment_capacity(self):
        for number in range(30):
            board = self.register(number)
            if number + 1 in (12, 20, 30):
                expected = min(6, (number + 1) // 4)
                self.assertEqual(sum(bool(court["match_id"]) for court in board["courts"]), expected)
                self.assertEqual(len(board["queue"]), number + 1 - 4 * expected)
        assigned = [name for court in board["courts"] for name in court["players"]]
        self.assertEqual(len(set(assigned)), 24)

    def test_finish_refills_with_first_time_players_before_returners(self):
        for number in range(27):
            board = self.register(number)
        match = board["courts"][0]["match_id"]
        self.action("start", court=1, match=match)
        result = self.action("finish", court=1, match=match, score_a=11, score_b=8)
        court = result["courts"][0]
        self.assertNotEqual(court["match_id"], match)
        self.assertTrue({"Player 24", "Player 25", "Player 26"} <= set(court["players"]))
        self.assertIn("First games", court["selection_reason"])
        self.action("finish", expected=409, court=1, match=match, score_a=11, score_b=8)

    def test_rest_exclusion_and_mixed_level_fallback(self):
        self.action("rotation", split=True)
        self.register(0)
        self.action("rest", ticket=1)
        for number, level in enumerate(("Beginner", "Advanced", "Advanced", "Beginner"), 1):
            board = self.register(number, level)
        self.assertEqual(board["courts"][0]["players"], ["Player 1", "Player 2", "Player 3", "Player 4"])
        self.assertTrue(board["queue"][0]["resting"])
        match = board["courts"][0]["match_id"]
        returned = self.action("cancel", court=1, match=match)
        self.assertFalse(returned["courts"][0]["open"])
        self.assertEqual(len(returned["queue"]), 5)


class MigrationTest(unittest.TestCase):
    def test_restore_and_migrate_legacy_database(self):
        with TemporaryDirectory() as directory:
            original = Path(directory) / "legacy.sqlite3"
            with closing(connect(original)) as db, db:
                from server import SCHEMA
                db.executescript(SCHEMA)
                db.execute("ALTER TABLE tickets ADD COLUMN phone TEXT")
                db.execute("INSERT INTO courts(id) VALUES (1)")
                db.execute("INSERT INTO matches(court, started, ended) VALUES (1, 100, 200)")
                db.execute("""
                    INSERT INTO tickets(name, name_key, phone, match_id, closed)
                    VALUES ('Legacy', 'legacy', '+639170000000', 1, 1),
                           ('Unlinked', 'unlinked', NULL, 1, 1),
                           ('Legacy', 'legacy', '+639170000000', NULL, 0)
                """)
            rehearsal = backup_database(original, Path(directory) / "rehearsal.sqlite3")
            initialize(rehearsal, 6)
            initialize(rehearsal, 6)
            with closing(connect(rehearsal)) as db:
                self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
                self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
                self.assertEqual(db.execute("SELECT count(*) FROM tickets").fetchone()[0], 3)
                self.assertEqual(db.execute("SELECT count(*) FROM session_participants").fetchone()[0], 1)
                self.assertIsNone(db.execute("SELECT player_id FROM tickets WHERE id = 2").fetchone()[0])
            with closing(connect(original)) as db:
                self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 0)
            self.assertEqual(len(list((Path(directory) / "backups").glob("*.sqlite3"))), 1)
