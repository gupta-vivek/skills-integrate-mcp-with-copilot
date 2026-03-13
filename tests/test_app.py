from pathlib import Path
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app import app  # noqa: E402
from db import initialize_database  # noqa: E402


@pytest.fixture
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "activities.db"
    monkeypatch.setenv("ACTIVITIES_DB_PATH", str(path))
    initialize_database()
    return path


def test_get_activities_returns_seeded_data(db_path: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/activities")

    assert response.status_code == 200
    payload = response.json()
    assert "Chess Club" in payload
    assert payload["Chess Club"]["max_participants"] == 12


def test_signup_prevents_duplicate_enrollment(db_path: Path) -> None:
    with TestClient(app) as client:
        signup = client.post(
            "/activities/Chess%20Club/signup",
            params={"email": "new.student@mergington.edu"},
        )
        duplicate = client.post(
            "/activities/Chess%20Club/signup",
            params={"email": "new.student@mergington.edu"},
        )

    assert signup.status_code == 200
    assert duplicate.status_code == 400
    assert duplicate.json()["detail"] == "Student is already signed up"


def test_signup_persists_after_restart(db_path: Path) -> None:
    email = "persisted.student@mergington.edu"

    with TestClient(app) as client:
        signup = client.post("/activities/Art%20Club/signup", params={"email": email})
        assert signup.status_code == 200

    with TestClient(app) as client:
        activities = client.get("/activities").json()

    assert email in activities["Art Club"]["participants"]


def test_unregister_removes_participant(db_path: Path) -> None:
    email = "removable.student@mergington.edu"

    with TestClient(app) as client:
        signup = client.post("/activities/Debate%20Team/signup", params={"email": email})
        assert signup.status_code == 200

        unregister = client.delete(
            "/activities/Debate%20Team/unregister",
            params={"email": email},
        )
        assert unregister.status_code == 200

        activities = client.get("/activities").json()

    assert email not in activities["Debate Team"]["participants"]
