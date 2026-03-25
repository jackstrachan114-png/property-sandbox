"""Run the project pipeline."""

from src.download_data import main as download_main
from src.prepare_price_paid import main as prepare_main


def main() -> None:
    download_main()
    prepare_main()
    print("Pipeline run complete.")


if __name__ == "__main__":
    main()
