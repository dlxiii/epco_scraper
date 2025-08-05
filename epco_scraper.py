import datetime as dt
import re
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin
import shutil
import requests
from zipfile import ZipFile
import chardet


class epco:
    """Scraper for EPCO electricity usage data."""

    BASE_URL = "https://denkiyoho.hepco.co.jp/"

    def juyo(self, date, area="hokkaido"):
        """Download and extract electricity usage data.

        Parameters
        ----------
        date : str | datetime.date | datetime.datetime
            Date used to determine which dataset to download. An ISO formatted
            string (``YYYY-MM-DD``) is also accepted.
        area : str, optional
            Electricity area. Currently only ``"hokkaido"`` is supported.

        Returns
        -------
        list[str]
            Paths to the extracted CSV files. Files are saved under
            ``csv/juyo/area/YYYY`` where ``YYYY`` is the calendar year.
        """
        if isinstance(date, str):
            date = dt.date.fromisoformat(date)
        elif isinstance(date, dt.datetime):
            date = date.date()
        elif not isinstance(date, dt.date):
            raise TypeError("date must be a date, datetime, or ISO format string")

        start_months = {1: 1, 2: 1, 3: 1, 4: 4, 5: 4, 6: 4, 7: 7, 8: 7, 9: 7, 10: 10, 11: 10, 12: 10}
        end_months = {1: 3, 2: 3, 3: 3, 4: 6, 5: 6, 6: 6, 7: 9, 8: 9, 9: 9, 10: 12, 11: 12, 12: 12}

        start_month = start_months[date.month]
        end_month = end_months[date.month]
        year = date.year

        filename = f"{year}{start_month:02d}-{end_month:02d}_{area}_denkiyohou.zip"

        page_url = urljoin(self.BASE_URL, "area_download.html")
        res = requests.get(page_url)
        res.raise_for_status()
        html = res.text

        pattern = re.escape(f"area/data/zip/{filename}")
        match = re.search(pattern, html)
        if not match:
            raise ValueError(f"No data link found for {date}")
        href = match.group(0)
        zip_url = urljoin(self.BASE_URL, href)

        zres = requests.get(zip_url)
        zres.raise_for_status()

        if area == "hokkaido":
            area_path = "hok"
        else:
            area_path = area
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
                if encoding.replace("-", "") in {"shiftjis", "cp932"}:
                    text = data.decode(encoding)
                    with open(dest_path, "w", encoding="utf-8") as dst:
                        dst.write(text)
                else:
                    with open(dest_path, "wb") as dst:
                        dst.write(data)
                extracted_files.append(str(dest_path))

        return extracted_files


if __name__ == "__main__":
    scraper = epco()
    print(scraper.juyo(dt.date.today(), "hokkaido"))
