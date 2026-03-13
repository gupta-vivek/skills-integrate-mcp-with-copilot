"""Database utilities for persistent storage of activities, users, and enrollments."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


DEFAULT_ACTIVITIES = {
    "Chess Club": {
        "description": "Learn strategies and compete in chess tournaments",
        "schedule": "Fridays, 3:30 PM - 5:00 PM",
        "max_participants": 12,
        "participants": ["michael@mergington.edu", "daniel@mergington.edu"],
    },
    "Programming Class": {
        "description": "Learn programming fundamentals and build software projects",
        "schedule": "Tuesdays and Thursdays, 3:30 PM - 4:30 PM",
        "max_participants": 20,
        "participants": ["emma@mergington.edu", "sophia@mergington.edu"],
    },
    "Gym Class": {
        "description": "Physical education and sports activities",
        "schedule": "Mondays, Wednesdays, Fridays, 2:00 PM - 3:00 PM",
        "max_participants": 30,
        "participants": ["john@mergington.edu", "olivia@mergington.edu"],
    },
    "Soccer Team": {
        "description": "Join the school soccer team and compete in matches",
        "schedule": "Tuesdays and Thursdays, 4:00 PM - 5:30 PM",
        "max_participants": 22,
        "participants": ["liam@mergington.edu", "noah@mergington.edu"],
    },
    "Basketball Team": {
        "description": "Practice and play basketball with the school team",
        "schedule": "Wednesdays and Fridays, 3:30 PM - 5:00 PM",
        "max_participants": 15,
        "participants": ["ava@mergington.edu", "mia@mergington.edu"],
    },
    "Art Club": {
        "description": "Explore your creativity through painting and drawing",
        "schedule": "Thursdays, 3:30 PM - 5:00 PM",
        "max_participants": 15,
        "participants": ["amelia@mergington.edu", "harper@mergington.edu"],
    },
    "Drama Club": {
        "description": "Act, direct, and produce plays and performances",
        "schedule": "Mondays and Wednesdays, 4:00 PM - 5:30 PM",
        "max_participants": 20,
        "participants": ["ella@mergington.edu", "scarlett@mergington.edu"],
    },
    "Math Club": {
        "description": "Solve challenging problems and participate in math competitions",
        "schedule": "Tuesdays, 3:30 PM - 4:30 PM",
        "max_participants": 10,
        "participants": ["james@mergington.edu", "benjamin@mergington.edu"],
    },
    "Debate Team": {
        "description": "Develop public speaking and argumentation skills",
        "schedule": "Fridays, 4:00 PM - 5:30 PM",
        "max_participants": 12,
        "participants": ["charlotte@mergington.edu", "henry@mergington.edu"],
    },
}


class ActivityNotFoundError(Exception):
    """Raised when an activity does not exist."""


class AlreadySignedUpError(Exception):
    """Raised when a user is already enrolled in an activity."""


class NotSignedUpError(Exception):
    """Raised when a user is not enrolled in an activity."""


class ActivityFullError(Exception):
    """Raised when an activity has no remaining capacity."""


def _guess_name_from_email(email: str) -> str:
    return email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()


def _resolve_db_path() -> Path:
    path_from_env = os.getenv("ACTIVITIES_DB_PATH")
    if path_from_env:
        return Path(path_from_env)
    return Path(__file__).parent / "data" / "activities.db"


def _connect() -> sqlite3.Connection:
    db_path = _resolve_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_database() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                email TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                grade_level TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS activities (
                name TEXT PRIMARY KEY,
                description TEXT NOT NULL,
                schedule TEXT NOT NULL,
                max_participants INTEGER NOT NULL CHECK(max_participants > 0)
            );

            CREATE TABLE IF NOT EXISTS enrollments (
                activity_name TEXT NOT NULL,
                user_email TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (activity_name, user_email),
                FOREIGN KEY (activity_name) REFERENCES activities(name) ON DELETE CASCADE,
                FOREIGN KEY (user_email) REFERENCES users(email) ON DELETE CASCADE
            );
            """
        )

        activity_count = conn.execute("SELECT COUNT(*) AS total FROM activities").fetchone()["total"]
        if activity_count == 0:
            _seed_default_data(conn)


def _seed_default_data(conn: sqlite3.Connection) -> None:
    for activity_name, details in DEFAULT_ACTIVITIES.items():
        conn.execute(
            """
            INSERT OR IGNORE INTO activities (name, description, schedule, max_participants)
            VALUES (?, ?, ?, ?)
            """,
            (
                activity_name,
                details["description"],
                details["schedule"],
                details["max_participants"],
            ),
        )

        for email in details["participants"]:
            conn.execute(
                """
                INSERT OR IGNORE INTO users (email, name)
                VALUES (?, ?)
                """,
                (email, _guess_name_from_email(email)),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO enrollments (activity_name, user_email)
                VALUES (?, ?)
                """,
                (activity_name, email),
            )


def get_activities_with_participants() -> dict[str, dict[str, object]]:
    with _connect() as conn:
        activities = conn.execute(
            """
            SELECT name, description, schedule, max_participants
            FROM activities
            ORDER BY name
            """
        ).fetchall()

        activity_map: dict[str, dict[str, object]] = {}
        for activity in activities:
            participants = conn.execute(
                """
                SELECT user_email
                FROM enrollments
                WHERE activity_name = ?
                ORDER BY user_email
                """,
                (activity["name"],),
            ).fetchall()

            activity_map[activity["name"]] = {
                "description": activity["description"],
                "schedule": activity["schedule"],
                "max_participants": activity["max_participants"],
                "participants": [row["user_email"] for row in participants],
            }

        return activity_map


def signup_for_activity(activity_name: str, email: str) -> None:
    with _connect() as conn:
        activity = conn.execute(
            """
            SELECT max_participants
            FROM activities
            WHERE name = ?
            """,
            (activity_name,),
        ).fetchone()
        if activity is None:
            raise ActivityNotFoundError()

        existing = conn.execute(
            """
            SELECT 1
            FROM enrollments
            WHERE activity_name = ? AND user_email = ?
            """,
            (activity_name, email),
        ).fetchone()
        if existing:
            raise AlreadySignedUpError()

        participant_count = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM enrollments
            WHERE activity_name = ?
            """,
            (activity_name,),
        ).fetchone()["total"]
        if participant_count >= activity["max_participants"]:
            raise ActivityFullError()

        conn.execute(
            """
            INSERT OR IGNORE INTO users (email, name)
            VALUES (?, ?)
            """,
            (email, _guess_name_from_email(email)),
        )
        conn.execute(
            """
            INSERT INTO enrollments (activity_name, user_email)
            VALUES (?, ?)
            """,
            (activity_name, email),
        )


def unregister_from_activity(activity_name: str, email: str) -> None:
    with _connect() as conn:
        activity = conn.execute(
            """
            SELECT 1
            FROM activities
            WHERE name = ?
            """,
            (activity_name,),
        ).fetchone()
        if activity is None:
            raise ActivityNotFoundError()

        enrollment = conn.execute(
            """
            SELECT 1
            FROM enrollments
            WHERE activity_name = ? AND user_email = ?
            """,
            (activity_name, email),
        ).fetchone()
        if enrollment is None:
            raise NotSignedUpError()

        conn.execute(
            """
            DELETE FROM enrollments
            WHERE activity_name = ? AND user_email = ?
            """,
            (activity_name, email),
        )
