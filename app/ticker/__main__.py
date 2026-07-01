from app.core.config import get_settings
from app.core.logging import configure_logging
from app.ticker.runner import run_forever


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    run_forever(settings)


if __name__ == "__main__":
    main()
