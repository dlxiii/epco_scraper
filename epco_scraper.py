import datetime as dt
import re
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin
import requests
from zipfile import ZipFile
import chardet


class epco:
    """Scraper for EPCO electricity usage data.

    The landing page for the Hokkaido area is
    https://denkiyoho.hepco.co.jp/area_download.html.
    The Tohoku area provides yearly CSV files at
    https://setsuden.tohoku-epco.co.jp/.
    The Tokyo area provides monthly ZIP archives at
    https://www.tepco.co.jp/forecast/.
    """

    BASE_URLS = {
        "hokkaido": "https://denkiyoho.hepco.co.jp/",
        "tohoku": "https://setsuden.nw.tohoku-epco.co.jp/",
        "tokyo": "https://www.tepco.co.jp/forecast/",
        "chubu": "https://powergrid.chuden.co.jp/denkiyoho/",
        "hokuriku": "https://www.rikuden.co.jp/nw/denki-yoho/csv/",
    }

    def juyo(self, date, area="hokkaido"):
        """Download and extract electricity usage data.

        Parameters
        ----------
        date : str | datetime.date | datetime.datetime
            Date used to determine which dataset to download. An ISO formatted
            string (``YYYY-MM-DD``) is also accepted.
        area : str, optional
            Electricity area. Supports ``"hokkaido"``, ``"tohoku"``, ``"tokyo"``,
            ``"chubu"``, and ``"hokuriku"``.

        Returns
        -------
        list[str]
            Paths to the extracted CSV files. For the Hokkaido area files are
            saved under ``csv/juyo/hok/YYYY`` where ``YYYY`` is the calendar
            year with empty lines removed. For the Tohoku area files are saved
            under ``csv/juyo/toh`` with empty lines removed. For the Chubu area
            files are saved under ``csv/juyo/chb/YYYY`` with empty lines removed.
            For the Hokuriku area files are saved under ``csv/hrk/YYYY`` with
            empty lines removed.
        """
        if isinstance(date, str):
            date = dt.date.fromisoformat(date)
        elif isinstance(date, dt.datetime):
            date = date.date()
        elif not isinstance(date, dt.date):
            raise TypeError("date must be a date, datetime, or ISO format string")

        base_url = self.BASE_URLS.get(area)
        if base_url is None:
            raise ValueError(f"Unsupported area: {area}")

        year = date.year

        if area == "tohoku":
            csv_name = f"juyo_{year}_tohoku.csv"
            csv_url = urljoin(base_url, f"common/demand/{csv_name}")
            res = requests.get(csv_url, headers={"User-Agent": "Mozilla/5.0"})
            res.raise_for_status()

            target_dir = Path("csv") / "juyo" / "toh"
            target_dir.mkdir(parents=True, exist_ok=True)
            dest_path = target_dir / csv_name

            encoding = chardet.detect(res.content).get("encoding") or "shift_jis"
            text = res.content.decode(encoding)
            # Remove empty lines from the CSV text
            lines = [line for line in text.splitlines() if line.strip()]
            cleaned = "\n".join(lines) + "\n"
            with open(dest_path, "w", encoding="utf-8") as dst:
                dst.write(cleaned)
            return [str(dest_path)]

        if area == "hokuriku":
            csv_name = f"juyo_05_{date:%Y%m%d}.csv"
            csv_url = urljoin(base_url, csv_name)
            res = requests.get(csv_url, headers={"User-Agent": "Mozilla/5.0"})
            res.raise_for_status()

            target_dir = Path("csv") / "hrk" / "hrk" / f"{year}"
            target_dir.mkdir(parents=True, exist_ok=True)
            dest_path = target_dir / csv_name

            encoding = chardet.detect(res.content).get("encoding") or "shift_jis"
            text = res.content.decode(encoding)
            lines = [line for line in text.splitlines() if line.strip()]
            cleaned = "\n".join(lines) + "\n"
            with open(dest_path, "w", encoding="utf-8") as dst:
                dst.write(cleaned)
            return [str(dest_path)]

        if area == "tokyo":
            filename = f"{year}{date.month:02d}_power_usage.zip"
            zip_url = urljoin(base_url, f"html/images/{filename}")
        elif area == "chubu":
            filename = f"{year}{date.month:02d}_power_usage.zip"
            zip_url = urljoin(base_url, f"/denki_yoho_content_data/download_csv/{filename}")
        else:
            start_months = {1: 1, 2: 1, 3: 1, 4: 4, 5: 4, 6: 4, 7: 7, 8: 7, 9: 7, 10: 10, 11: 10, 12: 10}
            end_months = {1: 3, 2: 3, 3: 3, 4: 6, 5: 6, 6: 6, 7: 9, 8: 9, 9: 9, 10: 12, 11: 12, 12: 12}

            start_month = start_months[date.month]
            end_month = end_months[date.month]

            filename = f"{year}{start_month:02d}-{end_month:02d}_{area}_denkiyohou.zip"

            page_url = urljoin(base_url, "area_download.html")
            res = requests.get(page_url)
            res.raise_for_status()
            html = res.text

            pattern = re.escape(f"area/data/zip/{filename}")
            match = re.search(pattern, html)
            if not match:
                raise ValueError(f"No data link found for {date}")
            href = match.group(0)
            zip_url = urljoin(base_url, href)

        zres = requests.get(zip_url, headers={"User-Agent": "Mozilla/5.0"})
        zres.raise_for_status()

        area_map = {"hokkaido": "hok", "tokyo": "tok", "chubu": "chb"}
        area_path = area_map.get(area, area)
        target_dir = Path("csv") / "juyo" / area_path / f"{year}"
        target_dir.mkdir(parents=True, exist_ok=True)

        extracted_files: list[str] = []
        with ZipFile(BytesIO(zres.content)) as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                name = Path(member.filename).name
                dest_path = target_dir / name
                with zf.open(member) as src:
                    data = src.read()

                encoding = (chardet.detect(data).get("encoding") or "").lower()
                text = data.decode(encoding)
                if area == "tokyo":
                    # Remove only single blank lines while keeping groups of
                    # consecutive blank lines intact. This avoids altering
                    # blocks that use multiple blank lines as separators.
                    lines = text.splitlines()
                    cleaned: list[str] = []
                    i = 0
                    while i < len(lines):
                        if not lines[i].strip():
                            j = i
                            while j < len(lines) and not lines[j].strip():
                                j += 1
                            if j - i > 1:
                                cleaned.extend([""] * (j - i))
                            i = j
                            continue
                        cleaned.append(lines[i])
                        i += 1
                    text = "\n".join(cleaned) + "\n"
                elif area in {"hokkaido", "chubu"}:
                    # Remove all empty lines
                    lines = [line for line in text.splitlines() if line.strip()]
                    text = "\n".join(lines) + "\n"
                with open(dest_path, "w", encoding="utf-8") as dst:
                    dst.write(text)
                extracted_files.append(str(dest_path))

        return extracted_files


if __name__ == "__main__":
    from datetime import date
    from dateutil.relativedelta import relativedelta

    scraper = epco()

    for k in range(365*10):
        months_ago = date.today() - relativedelta(days=1 * k)
        result = scraper.juyo(months_ago, "hokuriku")
        print(f"{months_ago}: {result}")

    # for k in range(84):
    #     months_ago = date.today() - relativedelta(months=1 * k)
    #     result = scraper.juyo(months_ago, "chubu")
    #     print(f"{months_ago}: {result}")

    # for k in range(48):
    #     months_ago = date.today() - relativedelta(months=1 * k)
    #     result = scraper.juyo(months_ago, "tokyo")
    #     print(f"{months_ago}: {result}")

    # for k in range(11):
    #     months_ago = date.today() - relativedelta(months=12 * k)
    #     result = scraper.juyo(months_ago, "tohoku")
    #     print(f"{months_ago}: {result}")

    # for k in range(30):
    #     months_ago = date.today() - relativedelta(months=3 * k)
    #     result = scraper.juyo(months_ago, "hokkaido")
    #     print(f"{months_ago}: {result}")
