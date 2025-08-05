from datetime import datetime, timedelta
import sys

from epco_scraper import epco


if __name__ == "__main__":
    yesterday = datetime.today() - timedelta(days=1)

    scraper = epco()
    try:
        paths = scraper.juyo(date=yesterday.strftime("%Y%m%d"))
        for path in paths:
            print(path)
    except RuntimeError as err:
        print(err)
        sys.exit(1)
