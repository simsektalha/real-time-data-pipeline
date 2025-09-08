from src.ingestion.producer import normalize


def test_normalize_bool_and_types():
    raw = {
        "station_id": 72,
        "num_bikes_available": "3",
        "num_ebikes_available": None,
        "num_docks_available": "10",
        "is_installed": 1,
        "is_renting": 0,
        "is_returning": True,
        "last_reported": "1715012345",
    }
    out = normalize(raw)
    assert out["station_id"] == "72"
    assert out["num_bikes_available"] == 3
    assert out["num_docks_available"] == 10
    assert out["is_installed"] is True
    assert out["is_renting"] is False
    assert out["is_returning"] is True
    assert out["last_reported"] == 1715012345


