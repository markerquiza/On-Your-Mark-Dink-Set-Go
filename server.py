"""Single-venue pickleball queue with transactional court assignments."""
import argparse
import json
import os
import re
import sqlite3
import tempfile
import time
from datetime import datetime
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent
# Base tables: sessions group play, courts hold capacity, matches track games,
# and tickets represent each player’s entry and roster seat. Later migrations add fields.
SCHEMA = """
CREATE TABLE IF NOT EXISTS play_sessions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    started INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
    ended INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_session ON play_sessions((1)) WHERE ended IS NULL;
CREATE TABLE IF NOT EXISTS rotation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    split INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO rotation(id) VALUES (1);
CREATE TABLE IF NOT EXISTS courts (
    id INTEGER PRIMARY KEY, open INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY, court INTEGER NOT NULL REFERENCES courts(id),
    called INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
    started INTEGER, ended INTEGER, canceled INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS one_match_per_court
    ON matches(court) WHERE ended IS NULL;
CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, name_key TEXT NOT NULL,
    joined INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
    match_id INTEGER REFERENCES matches(id), closed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS waiting_order ON tickets(id)
    WHERE closed = 0 AND match_id IS NULL;
"""


LEVELS = ('Beginner', 'Advanced Beginner', 'Low Intermediate', 'Intermediate', 'High Intermediate', 'Advanced')


# A user-facing rule violation, such as trying to start an occupied court or submit an old match ID.
class QueueError(Exception):
    pass


# Open the local SQLite file, return named columns, and enforce foreign keys on each connection.
def connect(path):
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


# Copy a consistent database snapshot to a temporary file, verify its integrity, then publish it without overwriting a backup.
def backup_database(source, destination):
    """Create and verify a snapshot without overwriting an existing backup."""
    source, destination = Path(source), Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=destination.parent)
    os.close(descriptor)
    try:
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            with closing(sqlite3.connect(temporary)) as copy:
                db.backup(copy)
                copy.execute("PRAGMA journal_mode = DELETE")
                if copy.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise QueueError("Backup integrity check failed.")
        os.link(temporary, destination)
    finally:
        os.unlink(temporary)
    return destination


# Choose a unique timestamped backup name beside the database.
def snapshot(path, label):
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    destination = path.parent / "backups" / f"{path.stem}-{label}-{timestamp}.sqlite3"
    return backup_database(path, destination)


# Return the only session that has no end time. A unique database index enforces this invariant.
def active_session(db):
    return db.execute("SELECT * FROM play_sessions WHERE ended IS NULL").fetchone()


# Give saved profiles stable IDs and link older tickets to profiles and sessions where the old data supports it.
def migrate_player_identity(db):
    db.execute("ALTER TABLE profiles ADD COLUMN player_id TEXT")
    db.execute("UPDATE profiles SET player_id = lower(hex(randomblob(16)))")
    db.execute("CREATE UNIQUE INDEX profiles_player_id ON profiles(player_id)")
    db.execute("ALTER TABLE tickets ADD COLUMN player_id TEXT REFERENCES profiles(player_id)")
    db.execute("ALTER TABLE tickets ADD COLUMN session_id INTEGER REFERENCES play_sessions(id)")
    db.execute("""
        UPDATE tickets SET player_id = (SELECT player_id FROM profiles WHERE profiles.phone = tickets.phone),
            session_id = CASE WHEN match_id IS NOT NULL
                THEN (SELECT session_id FROM matches WHERE matches.id = tickets.match_id)
                WHEN closed = 0 THEN (SELECT id FROM play_sessions WHERE ended IS NULL)
            END
    """)
    db.execute("CREATE INDEX tickets_player_history ON tickets(player_id, match_id)")
    db.execute("CREATE UNIQUE INDEX one_active_player ON tickets(player_id) WHERE closed = 0")
    db.execute("""
        CREATE TABLE session_participants (
            session_id INTEGER NOT NULL REFERENCES play_sessions(id),
            player_id TEXT NOT NULL REFERENCES profiles(player_id) ON DELETE CASCADE,
            joined INTEGER NOT NULL,
            PRIMARY KEY (session_id, player_id)
        )
    """)
    db.execute("""
        INSERT INTO session_participants(session_id, player_id, joined)
        SELECT session_id, player_id, min(joined) FROM tickets
        WHERE session_id IS NOT NULL AND player_id IS NOT NULL GROUP BY session_id, player_id
    """)
    db.execute("PRAGMA user_version = 1")


# Create or upgrade the local schema. Back up existing databases before migration and seed courts only when none exist.
def initialize(path, courts):
    path = Path(path)
    if path.is_file() and path.stat().st_size:
        with closing(connect(path)) as existing:
            upgrading = existing.execute("PRAGMA user_version").fetchone()[0] < 2 and existing.execute("""
                SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'matches'
            """).fetchone()
        if upgrading:
            snapshot(path, "before-queue-upgrade")
    with closing(connect(path)) as db, db:
        db.execute("PRAGMA journal_mode = WAL")
        db.executescript(SCHEMA)
        db.execute("BEGIN IMMEDIATE")
        columns = {column["name"] for column in db.execute("PRAGMA table_info(tickets)")}
        if "phone" not in columns:
            db.execute("ALTER TABLE tickets ADD COLUMN phone TEXT")
        for name, definition in (("seat", "INTEGER"), ("previous_game", "INTEGER"),
                                 ("lane", "TEXT"), ("level", "TEXT"),
                                 ("resting", "INTEGER NOT NULL DEFAULT 0")):
            if name not in columns:
                db.execute(f"ALTER TABLE tickets ADD COLUMN {name} {definition}")
        db.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                phone TEXT PRIMARY KEY, name TEXT NOT NULL, name_key TEXT NOT NULL, level TEXT, email TEXT
            )
        """)
        profile_columns = {column["name"] for column in db.execute("PRAGMA table_info(profiles)")}
        if "email" not in profile_columns:
            db.execute("ALTER TABLE profiles ADD COLUMN email TEXT")
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version < 1:
            db.execute("""
                INSERT OR IGNORE INTO profiles(phone, name, name_key, level)
                SELECT phone, name, name_key, level FROM tickets
                WHERE phone IS NOT NULL ORDER BY id DESC
            """)
        court_columns = {column["name"] for column in db.execute("PRAGMA table_info(courts)")}
        if "levels" not in court_columns:
            db.execute("ALTER TABLE courts ADD COLUMN levels TEXT NOT NULL DEFAULT '[]'")
        match_columns = {column["name"] for column in db.execute("PRAGMA table_info(matches)")}
        if "session_id" not in match_columns:
            db.execute("ALTER TABLE matches ADD COLUMN session_id INTEGER REFERENCES play_sessions(id)")
            # Group legacy matches without creating a session on a clean install.
            if db.execute("SELECT 1 FROM matches LIMIT 1").fetchone():
                session = db.execute("INSERT INTO play_sessions(name) VALUES ('Open play')").lastrowid
                db.execute("UPDATE matches SET session_id = ?", (session,))
        db.execute("CREATE INDEX IF NOT EXISTS matches_session ON matches(session_id)")
        if "winner" not in match_columns:
            db.execute("ALTER TABLE matches ADD COLUMN winner TEXT CHECK (winner IN ('A', 'B'))")
        for column in ("score_a", "score_b"):
            if column not in match_columns:
                db.execute(f"ALTER TABLE matches ADD COLUMN {column} INTEGER")
        db.execute("CREATE INDEX IF NOT EXISTS ticket_phone_history ON tickets(phone, id)")
        db.execute("DROP INDEX IF EXISTS one_active_ticket")
        db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS one_active_phone ON tickets(phone)
            WHERE closed = 0 AND phone IS NOT NULL
        """)
        if version < 1:
            migrate_player_identity(db)
        if version < 2:
            db.execute("ALTER TABLE rotation ADD COLUMN auto_assign INTEGER NOT NULL DEFAULT 1")
            db.execute("ALTER TABLE tickets ADD COLUMN paused_seconds INTEGER NOT NULL DEFAULT 0")
            db.execute("ALTER TABLE tickets ADD COLUMN rest_started INTEGER")
            db.execute("ALTER TABLE matches ADD COLUMN selection_reason TEXT")
            db.execute("UPDATE tickets SET rest_started = ? WHERE resting = 1", (int(time.time()),))
            db.execute("PRAGMA user_version = 2")
        if not db.execute("SELECT 1 FROM courts LIMIT 1").fetchone():
            db.executemany("INSERT INTO courts(id) VALUES (?)",
                           [(index + 1,) for index in range(courts)])
        if automatic_assignment(db):
            fill_courts(db)


# Collect unassigned, open tickets. Order eligible players by first-game priority, active wait, games played and ticket ID.
def waiting(db):
    return db.execute("""
        SELECT ticket.id, ticket.name, ticket.joined, ticket.previous_game, ticket.lane,
               ticket.level, ticket.resting, ticket.rest_started,
               ticket.joined + ticket.paused_seconds AS wait_since,
               (SELECT count(*) FROM tickets played JOIN matches game ON game.id = played.match_id
                WHERE played.player_id = ticket.player_id AND game.session_id = ticket.session_id
                  AND game.ended IS NOT NULL AND game.canceled = 0) AS games
        FROM tickets ticket
        WHERE ticket.closed = 0 AND ticket.match_id IS NULL
        ORDER BY ticket.resting, games > 0, wait_since, games, ticket.id
    """).fetchall()


# Filter by allowed levels, select four eligible tickets, then compare the possible doubles pairings for those four.
def next_group(queue, split, levels=()):
    eligible = [player for player in queue
                if not player["resting"] and (not levels or player["level"] in levels)]
    if len(eligible) < 4:
        return [], None

    def priority(player):
        return (player["games"] > 0, player["wait_since"], player["games"])

    eligible.sort(key=lambda player: (*priority(player), player["id"]))
    lane = eligible[0]["lane"] if split else None
    if lane:
        eligible.sort(key=lambda player: (*priority(player), player["lane"] != lane, player["id"]))
    selected = eligible[:4]
    reason = ("First games take priority, then longest active wait."
              if any(not player["games"] for player in selected)
              else "Selected by longest active wait, then fewest games played.")
    if lane and all(player["lane"] == lane for player in selected):
        reason += f" All four are in the {lane}s group."

    def team_cost(teams):
        def rating(index):
            level = selected[index]["level"]
            return LEVELS.index(level) if level in LEVELS else (len(LEVELS) - 1) / 2

        def prior_pair(a, b):
            first, second = selected[a], selected[b]
            return (first["lane"] is not None and first["lane"] == second["lane"]
                    and first["previous_game"] == second["previous_game"])

        a, b, c, d = teams
        imbalance = abs(rating(a) + rating(b) - rating(c) - rating(d))
        repeats = int(prior_pair(a, b)) + int(prior_pair(c, d))
        return (imbalance, -repeats if split else repeats)

    seats = min(((0, 1, 2, 3), (0, 2, 1, 3), (0, 3, 1, 2)), key=team_cost)
    return [selected[index]["id"] for index in seats], reason


# Create a match and set its four seat numbers. Seats 1–2 are Team A and 3–4 are Team B.
def assign_group(db, court_id, session_id, group, reason):
    match = db.execute("""
        INSERT INTO matches(court, session_id, selection_reason) VALUES (?, ?, ?)
    """, (court_id, session_id, reason)).lastrowid
    db.executemany("UPDATE tickets SET match_id = ?, seat = ? WHERE id = ?",
                   [(match, seat, ticket) for seat, ticket in enumerate(group, 1)])


# Read the saved preference for filling open courts without a manual call.
def automatic_assignment(db):
    return bool(db.execute("SELECT auto_assign FROM rotation WHERE id = 1").fetchone()[0])


# Assign eligible groups to available courts, refreshing the waiting list as assignments are made.
def fill_courts(db):
    session = active_session(db)
    if not session:
        return
    queue = waiting(db)
    split = split_rotation(db)
    courts = db.execute("""
        SELECT id, levels FROM courts WHERE open = 1 AND NOT EXISTS
            (SELECT 1 FROM matches WHERE court = courts.id AND ended IS NULL)
        ORDER BY id
    """).fetchall()
    courts = [(court["id"], json.loads(court["levels"])) for court in courts]
    for court_id, levels in sorted(courts, key=lambda court: len(court[1]) or len(LEVELS) + 1):
        group, reason = next_group(queue, split, levels)
        if group:
            assign_group(db, court_id, session["id"], group, reason)
            queue = [player for player in queue if player["id"] not in group]


# Read whether recent wins and losses should influence grouping.
def split_rotation(db):
    return bool(db.execute("SELECT split FROM rotation WHERE id = 1").fetchone()[0])


# Assemble the browser board: court rosters, waiting players, suggested groups, settings and session totals.
def state(db):
    courts = [dict(row) for row in db.execute("""
        SELECT courts.id, courts.open, courts.levels, matches.id AS match_id, matches.called, matches.started, matches.selection_reason
        FROM courts LEFT JOIN matches ON courts.id = matches.court AND matches.ended IS NULL
        ORDER BY courts.id
    """)]
    players = {}
    for row in db.execute("""
        SELECT tickets.match_id, tickets.name FROM tickets
        JOIN matches ON matches.id = tickets.match_id
        WHERE matches.ended IS NULL ORDER BY coalesce(tickets.seat, tickets.id)
    """):
        players.setdefault(row["match_id"], []).append(row["name"])
    queue = waiting(db)
    split = split_rotation(db)
    for court in courts:
        court["levels"] = json.loads(court["levels"])
        group, reason = (next_group(queue, split, court["levels"])
                         if court["open"] and not court["match_id"] else ([], None))
        court["next_group"] = group
        if reason:
            court["selection_reason"] = reason
        court["players"] = players.get(court["match_id"], [])
    session = active_session(db)
    latest = session or db.execute("SELECT * FROM play_sessions ORDER BY id DESC LIMIT 1").fetchone()
    completed = db.execute("""
        SELECT COUNT(*) FROM matches WHERE session_id = ? AND ended IS NOT NULL AND canceled = 0
    """, (latest["id"] if latest else None,)).fetchone()[0]
    return {"courts": courts, "queue": [dict(row) for row in queue],
            "completed": completed, "split": split, "next_group": next_group(queue, split)[0], "auto_assign": automatic_assignment(db),
            "session": dict(latest) if latest else None}


# Store equivalent Philippine numbers in the same format so one person cannot receive duplicate active tickets.
def normalize_phone(raw):
    if not isinstance(raw, str):
        raise QueueError("Enter a mobile number.")
    phone = re.sub(r"[ ().-]", "", raw.strip())
    if re.fullmatch(r"09[0-9]{9}", phone):
        return "+63" + phone[1:]
    if re.fullmatch(r"639[0-9]{9}", phone):
        return "+" + phone
    if re.fullmatch(r"\+639[0-9]{9}", phone):
        return phone
    if not phone.startswith("+63") and re.fullmatch(r"\+[1-9][0-9]{7,14}", phone):
        return phone
    raise QueueError("Enter a Philippine mobile number or an international number starting with +.")


# Search saved profiles by name or the last four digits of a number.
def members(db, query):
    query = query.strip().casefold()
    return [dict(row) for row in db.execute("""
        SELECT profile.player_id AS id, profile.name, profile.level, profile.email,
               substr(profile.phone, -4) AS last_four,
               EXISTS (SELECT 1 FROM tickets active
                       WHERE active.player_id = profile.player_id AND active.closed = 0) AS active
        FROM profiles profile
        WHERE instr(profile.name_key, ?) > 0 OR substr(profile.phone, -4) = ?
        ORDER BY profile.name_key, profile.player_id LIMIT 50
    """, (query, query))]


# Read completed matches for one player and derive their score, result, partner and opponents from the roster order.
def member_history(db, member_id):
    rows = db.execute("""
        SELECT match.id, match.court, match.started, match.ended, match.winner,
               match.score_a, match.score_b, player.name, player.id = entry.id AS is_member
        FROM tickets entry JOIN matches match ON match.id = entry.match_id
        JOIN tickets player ON player.match_id = match.id
        WHERE entry.player_id = coalesce(
            (SELECT player_id FROM profiles WHERE player_id = ?),
            (SELECT player_id FROM tickets WHERE id = ?)
        )
          AND match.ended IS NOT NULL AND match.canceled = 0 AND match.started IS NOT NULL
        ORDER BY match.ended DESC, match.id DESC, coalesce(player.seat, player.id)
    """, (member_id, member_id))
    games = {}
    for row in rows:
        games.setdefault(row["id"], []).append(row)
    history = []
    for roster in games.values():
        first = roster[0]
        member_seat = next(index for index, row in enumerate(roster) if row["is_member"])
        team = member_seat // 2
        winner = first["winner"]
        result = None if winner is None else ("Win" if winner == ("A", "B")[team] else "Loss")
        scores = [first["score_a"], first["score_b"]]
        history.append({
            "court": first["court"], "started": first["started"], "ended": first["ended"],
            "result": result, "score": scores[team], "opponent_score": scores[1 - team],
            "partners": [row["name"] for index, row in enumerate(roster)
                         if index // 2 == team and not row["is_member"]],
            "opponents": [row["name"] for index, row in enumerate(roster) if index // 2 != team],
        })
    return history


# Apply a requested action and refill courts automatically after actions that can make players or courts available.
def change(db, data):
    apply_change(db, data)
    if data.get("action") in (
        "start_session", "join", "join_member", "ready", "undo_done", "toggle",
        "court_levels", "finish", "assignment_mode", "rotation", "edit_member",
    ) and automatic_assignment(db):
        fill_courts(db)


# Validate and execute a staff action. The HTTP caller holds a write transaction so changes commit together.
def apply_change(db, data):
    action = data.get("action")
    # Session actions create or close the active session. Closing preserves completed match history.
    if action == "start_session":
        if active_session(db):
            raise QueueError("End the current session before starting another.")
        name = data.get("name")
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 80:
            raise QueueError("Enter a session name of 1 to 80 characters.")
        db.execute("INSERT INTO play_sessions(name) VALUES (?)", (name.strip(),))
        return
    if action == "end_session":
        session = active_session(db)
        if session is None or session["id"] != data.get("session"):
            raise QueueError("The active session changed. Refresh and try again.")
        if db.execute("SELECT 1 FROM matches WHERE ended IS NULL").fetchone():
            raise QueueError("Finish or return all court assignments before ending the session.")
        db.execute("UPDATE tickets SET closed = 1 WHERE match_id IS NULL AND closed IN (0, 2)")
        db.execute("""
            UPDATE play_sessions SET ended = CAST(strftime('%s', 'now') AS INTEGER) WHERE id = ?
        """, (session["id"],))
        return
    if action in ("join", "join_member", "call", "undo_done") and not active_session(db):
        raise QueueError("Start an open-play session first.")
    if action == "assignment_mode":
        enabled = data.get("enabled")
        if type(enabled) is not bool:
            raise QueueError("Choose an assignment mode.")
        db.execute("UPDATE rotation SET auto_assign = ? WHERE id = 1", (enabled,))
        return
    if action == "rotation":
        if type(data.get("split")) is not bool:
            raise QueueError("Choose a rotation mode.")
        db.execute("UPDATE rotation SET split = ? WHERE id = 1", (data["split"],))
        if not data["split"]:
            db.execute("UPDATE tickets SET lane = NULL WHERE closed = 0")
        return
    # Resolve saved profiles by their stable ID. Deletion anonymizes historical roster entries.
    if action in ("join_member", "edit_member", "delete_member"):
        member = db.execute("""
            SELECT * FROM profiles WHERE player_id = coalesce(
                (SELECT player_id FROM profiles WHERE player_id = ?),
                (SELECT player_id FROM tickets WHERE id = ?)
            )
        """,
                            (data.get("member"), data.get("member"))).fetchone()
        if member is None:
            raise QueueError("Saved player not found.")
        if action == "delete_member":
            if data.get("confirmed") is not True:
                raise QueueError("Confirm permanent deletion.")
            player_id = member["player_id"]
            active = db.execute("""
                SELECT id, match_id FROM tickets WHERE player_id = ? AND closed = 0
            """, (player_id,)).fetchone()
            if active and active["match_id"] is not None:
                raise QueueError("Finish or return this player's court assignment before deleting.")
            if active:
                change(db, {"action": "remove", "ticket": active["id"]})
            db.execute("DELETE FROM tickets WHERE player_id = ? AND match_id IS NULL", (player_id,))
            db.execute("""
                UPDATE tickets SET phone = NULL, player_id = NULL, name = 'Deleted player',
                    name_key = 'deleted player', level = NULL
                WHERE player_id = ?
            """, (player_id,))
            db.execute("DELETE FROM profiles WHERE player_id = ?", (player_id,))
            return
        if action == "join_member":
            data = dict(member)
    # Validate profile fields before saving. Joining also creates a queue ticket and a session participation record.
    if action in ("join", "join_member", "edit_member", "add_member"):
        raw = data.get("name")
        name = " ".join(raw.split()) if isinstance(raw, str) else ""
        if not 1 <= len(name) <= 40:
            raise QueueError("Enter a name of 1 to 40 characters.")
        level = data.get("level")
        if level not in LEVELS and (action != "join_member" or level is not None):
            raise QueueError("Select a player level.")
        raw_email = data.get("email", "")
        if raw_email is None and action == "join_member":
            raw_email = ""
        if not isinstance(raw_email, str):
            raise QueueError("Enter a valid email address.")
        email = raw_email.strip() or None
        if email and (len(email) > 254 or not re.fullmatch(
            r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+", email
        )):
            raise QueueError("Enter a valid email address.")
        if action == "edit_member":
            old_phone = member["phone"]
            raw_phone = data.get("phone")
            phone = old_phone if raw_phone == "" else normalize_phone(raw_phone)
            if phone != old_phone and db.execute("SELECT 1 FROM profiles WHERE phone = ?", (phone,)).fetchone():
                raise QueueError("That mobile number belongs to another saved player.")
            db.execute("UPDATE profiles SET phone = ?, name = ?, name_key = ?, level = ?, email = ? WHERE phone = ?",
                       (phone, name, name.casefold(), level, email, old_phone))

            db.execute("""
                UPDATE tickets SET phone = ? WHERE player_id = ? AND closed IN (0, 2)
            """, (phone, member["player_id"]))
            db.execute("""
                UPDATE tickets SET name = ?, name_key = ?, level = ?
                WHERE player_id = ? AND closed IN (0, 2) AND match_id IS NULL
            """, (name, name.casefold(), level, member["player_id"]))
            return
        phone = normalize_phone(data.get("phone"))
        if action == "add_member" and db.execute("SELECT 1 FROM profiles WHERE phone = ?", (phone,)).fetchone():
            raise QueueError("That mobile number already belongs to a saved player.")
        if db.execute("SELECT 1 FROM tickets WHERE phone = ? AND closed = 0", (phone,)).fetchone():
            raise QueueError("That mobile number is already waiting or assigned to a court.")
        db.execute("""
            INSERT INTO profiles(phone, name, name_key, level, email, player_id)
            VALUES (?, ?, ?, ?, ?, lower(hex(randomblob(16))))
            ON CONFLICT(phone) DO UPDATE SET name = excluded.name, name_key = excluded.name_key,
                                             level = excluded.level, email = coalesce(excluded.email, profiles.email)
        """, (phone, name, name.casefold(), level, email))
        if action != "add_member":
            session_id = active_session(db)["id"]
            ticket = db.execute("""
                INSERT INTO tickets(name, name_key, phone, level, player_id, session_id)
                SELECT name, name_key, phone, level, player_id, ? FROM profiles WHERE phone = ?
            """, (session_id, phone)).lastrowid
            db.execute("""
                INSERT OR IGNORE INTO session_participants(session_id, player_id, joined)
                SELECT session_id, player_id, joined FROM tickets WHERE id = ?
            """, (ticket,))
        return

    # Rest pauses eligibility and the wait clock. Ready returns the ticket to active waiting.
    if action in ("rest", "ready"):
        now = int(time.time())
        if action == "rest":
            updated = db.execute("""
                UPDATE tickets SET resting = 1, rest_started = coalesce(rest_started, ?)
                WHERE id = ? AND closed = 0 AND match_id IS NULL
            """, (now, data.get("ticket")))
        else:
            updated = db.execute("""
                UPDATE tickets SET resting = 0, rest_started = NULL,
                    paused_seconds = paused_seconds + max(0, ? - coalesce(rest_started, ?))
                WHERE id = ? AND closed = 0 AND match_id IS NULL
            """, (now, now, data.get("ticket")))
        if not updated.rowcount:
            raise QueueError("That player is no longer in the queue.")
        return
    if action == "undo_done":
        restored = db.execute("""
            UPDATE tickets SET closed = 0
            WHERE id = ? AND closed = 2 AND match_id IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM tickets active
                  WHERE (active.player_id = tickets.player_id OR active.phone = tickets.phone)
                    AND active.closed = 0
              )
        """, (data.get("ticket"),))
        if not restored.rowcount:
            raise QueueError("That player can no longer be restored from this entry.")
        return
    if action in ("remove", "done"):
        ticket = db.execute("""
            SELECT id, previous_game, lane FROM tickets
            WHERE id = ? AND closed = 0 AND match_id IS NULL
        """, (data.get("ticket"),)).fetchone()
        if ticket is None:
            raise QueueError("That player is no longer waiting.")
        db.execute("""
            UPDATE tickets SET closed = ?, lane = NULL, previous_game = NULL WHERE id = ?
        """, (2 if action == "done" else 1, ticket["id"]))
        db.execute("""
            UPDATE tickets SET lane = NULL, previous_game = NULL
            WHERE previous_game = ? AND lane = ? AND closed = 0 AND match_id IS NULL
        """, (ticket["previous_game"], ticket["lane"]))
        return

    court = db.execute("SELECT * FROM courts WHERE id = ?", (data.get("court"),)).fetchone()
    if court is None:
        raise QueueError("Court not found.")
    match = db.execute("SELECT * FROM matches WHERE court = ? AND ended IS NULL",
                       (court["id"],)).fetchone()
    if action == "toggle":
        if match or data.get("open") != court["open"]:
            raise QueueError("The court changed. Refresh and try again.")
        db.execute("UPDATE courts SET open = 1 - open WHERE id = ?", (court["id"],))
        return
    if action == "court_levels":
        levels = data.get("levels")
        if match:
            raise QueueError("Change eligible levels once the court is empty.")
        if not isinstance(levels, list) or any(level not in LEVELS for level in levels):
            raise QueueError("Choose valid player levels.")
        db.execute("UPDATE courts SET levels = ? WHERE id = ?",
                   (json.dumps([level for level in LEVELS if level in levels]), court["id"]))
        return
    # Compare the requested roster with the current next group to reject a stale call from another tab.
    if action == "call":
        if match or not court["open"]:
            raise QueueError("That court is unavailable.")
        group, reason = next_group(waiting(db), split_rotation(db), json.loads(court["levels"]))
        if not group:
            raise QueueError("No complete group is ready.")
        if data.get("group") != group:
            raise QueueError("The next group changed. Review the updated queue.")
        assign_group(db, court["id"], active_session(db)["id"], group, reason)
        return
    if not match or match["id"] != data.get("match"):
        raise QueueError("The court assignment changed. Review the updated board.")
    if action == "start" and match["started"] is None:
        db.execute("UPDATE matches SET started = CAST(strftime('%s', 'now') AS INTEGER) WHERE id = ?", (match["id"],))
    elif action == "cancel" and match["started"] is None:
        db.execute("""
            UPDATE tickets SET match_id = NULL, seat = NULL,
                paused_seconds = paused_seconds + max(0, ? - ?)
            WHERE match_id = ?
        """, (int(time.time()), match["called"], match["id"]))
        if automatic_assignment(db):
            db.execute("UPDATE courts SET open = 0 WHERE id = ?", (court["id"],))
        db.execute("UPDATE matches SET ended = CAST(strftime('%s', 'now') AS INTEGER), canceled = 1 WHERE id = ?",
                   (match["id"],))
    # Validate the result, end the match, and put eligible players back in the queue.
    elif action == "finish" and match["started"] is not None:
        split = split_rotation(db)
        winner = data.get("winner")
        score_a, score_b = data.get("score_a"), data.get("score_b")
        if score_a is not None or score_b is not None:
            if any(type(score) is not int or not 0 <= score <= 999 for score in (score_a, score_b)):
                raise QueueError("Enter both team scores as whole numbers from 0 to 999.")
            if score_a == score_b:
                raise QueueError("Enter a final score with a winning team.")
            scored_winner = "A" if score_a > score_b else "B"
            if winner is not None and winner != scored_winner:
                raise QueueError("The selected winner does not match the score.")
            winner = scored_winner
        if split and winner not in ("A", "B"):
            raise QueueError("Select the winning team before finishing.")
        roster = db.execute("""
            SELECT ticket.id, coalesce(profile.name, ticket.name) AS name,
                   coalesce(profile.name_key, ticket.name_key) AS name_key,
                   coalesce(profile.phone, ticket.phone) AS phone, ticket.player_id,
                   coalesce(profile.level, ticket.level) AS level
            FROM tickets ticket LEFT JOIN profiles profile ON profile.player_id = ticket.player_id
            WHERE ticket.match_id = ?
            ORDER BY coalesce(ticket.seat, ticket.id)
        """, (match["id"],)).fetchall()
        db.execute("UPDATE tickets SET closed = 1 WHERE match_id = ?", (match["id"],))
        for index, player in enumerate(roster):
            db.execute("UPDATE tickets SET seat = ? WHERE id = ?", (index + 1, player["id"]))
            team = "A" if index < 2 else "B"
            lane = ("winner" if team == winner else "loser") if split else None
            db.execute("""
                INSERT INTO tickets(name, name_key, phone, previous_game, lane, level, player_id, session_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (player["name"], player["name_key"], player["phone"], match["id"], lane,
                  player["level"], player["player_id"], match["session_id"]))
        db.execute("""
            UPDATE matches SET ended = CAST(strftime('%s', 'now') AS INTEGER), winner = ?, score_a = ?, score_b = ?
            WHERE id = ?
        """, (winner if winner in ("A", "B") else None, score_a, score_b, match["id"]))
    else:
        raise QueueError("That action is unavailable for this match.")


# Return either filtered session summaries or the completed match rosters for one selected session.
def session_history(db, session_id=None, player="", court=""):
    player = player.strip().casefold()
    matches = """
        FROM matches game
        WHERE game.ended IS NOT NULL AND game.started IS NOT NULL AND game.canceled = 0
          AND (? = '' OR game.court = ?)
          AND (? = '' OR EXISTS (
              SELECT 1 FROM tickets roster LEFT JOIN profiles profile ON profile.player_id = roster.player_id
              WHERE roster.match_id = game.id
                AND (instr(roster.name_key, ?) > 0 OR instr(profile.name_key, ?) > 0)
          ))
    """
    filters = (court, court, player, player, player)
    if session_id is None:
        return [dict(row) for row in db.execute(f"""
            WITH counts AS (
                SELECT game.session_id, count(*) AS games {matches} GROUP BY game.session_id
            )
            SELECT session.id, session.name, session.started, session.ended,
                   coalesce(counts.games, 0) AS games
            FROM play_sessions session LEFT JOIN counts ON counts.session_id = session.id
            WHERE (? = '' AND ? = '') OR counts.games > 0
            ORDER BY session.started DESC, session.id DESC
        """, (*filters, player, court))]

    games = {}
    rows = db.execute(f"""
        WITH selected AS (
            SELECT game.id, game.court, game.started, game.ended,
                   game.winner, game.score_a, game.score_b {matches} AND game.session_id = ?
        )
        SELECT selected.*, roster.name FROM selected
        LEFT JOIN tickets roster ON roster.match_id = selected.id
        ORDER BY selected.started, selected.id, coalesce(roster.seat, roster.id)
    """, (*filters, session_id))
    for row in rows:
        game = games.setdefault(row["id"], {
            key: row[key] for key in ("id", "court", "started", "ended", "winner", "score_a", "score_b")
        })
        roster = game.setdefault("players", [])
        if row["name"] is not None:
            roster.append(row["name"])
    return list(games.values())


# Route local HTTP requests, validate input, and send the HTML page or JSON responses.
class Handler(BaseHTTPRequestHandler):
    # Send a typed response with caching disabled so the desk receives current state.
    def reply(self, status, body, content_type="application/json; charset=utf-8"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # Encode a Python object as JSON before sending it to the browser.
    def json_reply(self, status, data):
        self.reply(status, json.dumps(data).encode())

    # Serve one HTML shell for all admin pages, or read SQLite for a board, directory, or history request.
    def do_GET(self):
        url = urlsplit(self.path)
        path = url.path
        if path in ("/", "/courts", "/courts/", "/players", "/players/", "/history", "/history/"):
            self.reply(200, (ROOT / "index.html").read_bytes(), "text/html; charset=utf-8")
        elif path == "/api/session-history":
            query = parse_qs(url.query)
            session_id = query.get("session", [None])[0]
            player = query.get("player", [""])[0][:80]
            court = query.get("court", [""])[0]
            with closing(connect(self.server.database)) as db:
                self.json_reply(200, session_history(db, session_id, player, court))
        elif path in ("/api/members", "/api/member-history"):
            query = parse_qs(url.query)
            with closing(connect(self.server.database)) as db:
                if path == "/api/members":
                    self.json_reply(200, members(db, query.get("q", [""])[0][:80]))
                else:
                    member_id = query.get("id", [""])[0]
                    self.json_reply(200, member_history(db, member_id))
        elif path == "/api/state":
            with closing(connect(self.server.database)) as db:
                db.execute("BEGIN")
                self.json_reply(200, state(db))
        else:
            self.json_reply(404, {"error": "Page not found."})

    # Validate the JSON action and take a SQLite write lock before changing any queue or match state.
    def do_POST(self):
        if self.path != "/api/action":
            self.json_reply(404, {"error": "Page not found."})
            return
        if self.headers.get_content_type() != "application/json":
            self.json_reply(415, {"error": "Send JSON."})
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 4096:
                raise ValueError()
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict) or any(
                isinstance(data.get(key), (dict, list)) for key in ("court", "ticket", "match", "member")
            ):
                raise ValueError()
        except ValueError:
            self.json_reply(400, {"error": "Invalid request."})
            return
        try:
            with closing(connect(self.server.database)) as db, db:
                db.execute("BEGIN IMMEDIATE")
                if data.get("action") != "backup":
                    change(db, data)
                result = state(db)
        except QueueError as error:
            self.json_reply(409, {"error": str(error)})
        except sqlite3.OperationalError:
            self.json_reply(503, {"error": "The queue is temporarily unavailable. Try again."})
        else:
            if data.get("action") in ("end_session", "backup"):
                try:
                    result["backup_message"] = f"Backup verified and saved as {snapshot(self.server.database, 'session' if data['action'] == 'end_session' else 'manual').name}."
                except (OSError, sqlite3.Error, QueueError):
                    result["backup_message"] = "Backup failed. Use Back up now to retry."
            self.json_reply(200, result)


# Command-line entry point. Bind only to loopback so the offline desk stays on this computer.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--courts", type=int, choices=range(1, 25), default=8)
    parser.add_argument("--db", type=Path, default=ROOT / "queue.sqlite3")
    parser.add_argument("--backup-to", type=Path, help="Verify and copy --db to a new file, then exit")
    args = parser.parse_args()
    if args.backup_to:
        print(backup_database(args.db, args.backup_to))
        raise SystemExit(0)
    initialize(args.db, args.courts)
    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        server.database = args.db
        print(f"Queue desk at http://127.0.0.1:{server.server_port}", flush=True)
        server.serve_forever()
