"""Download raw datasets into data/raw."""

from src.config import RAW_DIR


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Raw data directory ready: {RAW_DIR}")


if __name__ == "__main__":
    main()
