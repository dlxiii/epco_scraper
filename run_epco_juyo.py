from datetime import datetime, timedelta
import sys

from epco_scraper import epco


AREAS = [
    "hokkaido",
    "tohoku",
    "tokyo",
    "chubu",
    "chugoku",
    "hokuriku",
    "kansai",
    "shikoku",
    "kyushu",
    "okinawa",
]


if __name__ == "__main__":
    yesterday = datetime.today() - timedelta(days=1)

    scraper = epco()
    for area in AREAS:
        try:
            paths = scraper.juyo(date=yesterday.strftime("%Y%m%d"), area=area)
            for path in paths:
                print(path)
        except RuntimeError as err:
            print(err)
            sys.exit(1)
