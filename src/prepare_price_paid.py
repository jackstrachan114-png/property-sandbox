"""Prepare and clean price paid data."""

from src.config import INTERIM_DIR, RAW_DIR


def main() -> None:
    INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Preparing data from {RAW_DIR} to {INTERIM_DIR}")


if __name__ == "__main__":
    main()
