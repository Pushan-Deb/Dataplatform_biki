from pathlib import Path

import pandas as pd


def main() -> None:
    target = Path("/opt/feast/repo/data/customer_features.parquet")
    target.parent.mkdir(parents=True, exist_ok=True)

    frame = pd.DataFrame(
        [
            {
                "customer_id": 1001,
                "event_timestamp": "2026-03-30T10:00:00Z",
                "created_timestamp": "2026-03-30T10:05:00Z",
                "account_age_days": 420,
                "avg_order_value": 185.4,
                "segment": "enterprise",
            },
            {
                "customer_id": 1002,
                "event_timestamp": "2026-03-30T10:00:00Z",
                "created_timestamp": "2026-03-30T10:05:00Z",
                "account_age_days": 95,
                "avg_order_value": 42.7,
                "segment": "growth",
            },
            {
                "customer_id": 1003,
                "event_timestamp": "2026-03-30T10:00:00Z",
                "created_timestamp": "2026-03-30T10:05:00Z",
                "account_age_days": 12,
                "avg_order_value": 11.2,
                "segment": "starter",
            },
        ]
    )
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    frame["created_timestamp"] = pd.to_datetime(frame["created_timestamp"], utc=True)
    frame.to_parquet(target, index=False)


if __name__ == "__main__":
    main()
