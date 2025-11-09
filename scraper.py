"""
books_scraper.scraper

Скрипт для автоматического сбора информации о книгах
с сайта https://books.toscrape.com.

Функциональность:
- получение данных об одной книге (get_book_data);
- парсинг всех страниц каталога (scrape_books);
- опциональная многопоточность для ускорения;
- сохранение результата в artifacts/books_data.txt;
- автоматический запуск по расписанию (19:00).

Автор: Ekaterina Kuchina, МФТИ, 2025 г.
"""

import os
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import schedule
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_URL = "https://books.toscrape.com/"
OUTPUT_PATH = os.path.join(BASE_DIR, "artifacts", "books_data.txt")


def get_book_data(book_url: str) -> dict:
    """
    Извлекает полную информацию о книге с её страницы.

    Parameters
    ----------
    book_url : str
        Полный URL страницы книги.

    Returns
    -------
    dict
        Словарь с ключами:
        title, price, availability, rating, description,
        upc, product_type, price_excl_tax, price_incl_tax,
        tax, availability_count, number_of_reviews.
    """
    with requests.get(book_url, timeout=15) as response:
        response.encoding = (
            "utf-8"
            if "utf" in response.apparent_encoding.lower()
            else "ISO-8859-1"
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

    title = soup.find("div", class_="product_main").h1.get_text(strip=True)
    price = soup.find("p", class_="price_color").get_text(strip=True)
    availability = soup.find(
        "p", class_="instock availability"
    ).get_text(strip=True)

    rating = soup.find("p", class_="star-rating")["class"][1]

    description_tag = soup.find("div", id="product_description")
    description = (
        description_tag.find_next_sibling("p").get_text(strip=True)
        if description_tag
        else ""
    )

    info_table = soup.find("table", class_="table table-striped")
    info = {
        row.th.get_text(strip=True): row.td.get_text(strip=True)
        for row in info_table.find_all("tr")
    }

    return {
        "title": title,
        "price": price,
        "availability": availability,
        "rating": rating,
        "description": description,
        "upc": info.get("UPC", ""),
        "product_type": info.get("Product Type", ""),
        "price_excl_tax": info.get("Price (excl. tax)", ""),
        "price_incl_tax": info.get("Price (incl. tax)", ""),
        "tax": info.get("Tax", ""),
        "availability_count": info.get("Availability", ""),
        "number_of_reviews": info.get("Number of reviews", ""),
    }


def _fetch_page(
    session: requests.Session,
    page_url: str,
    timeout: int
) -> list[str]:

    """
    Возвращает список ссылок на книги с указанной страницы.
    """
    with session.get(page_url, timeout=timeout) as response:
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        return [urljoin(page_url, a["href"]) for a in soup.select("h3 > a")]


def scrape_books(  # pylint: disable=too-many-branches, too-many-locals
    is_save: bool = True,
    use_threads: bool = False,
    max_pages: int | None = None,
    output_path: str | None = None,
    per_request_timeout: int = 30,
) -> list[dict]:
    """
    Собирает данные о книгах со всех страниц каталога Books to Scrape.
    """
    all_books = []
    page = 1
    session = requests.Session()

    while True:
        page_url = (
            f"{BASE_URL}catalogue/page-{page}.html"
            if page > 1
            else f"{BASE_URL}index.html"
        )
        try:
            book_links = _fetch_page(session, page_url, per_request_timeout)
        except requests.RequestException as exc:
            print(f"⚠️ Ошибка при загрузке {page_url}: {exc}")
            break

        if not book_links:
            break

        print(f"📄 Page {page} → {len(book_links)} books")

        if use_threads:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(get_book_data, link)
                    for link in book_links
                ]

                for f in as_completed(futures):
                    try:
                        all_books.append(f.result())
                    except requests.RequestException as exc:
                        print(f"⚠️ Ошибка сети в потоке: {exc}")
                    except (RuntimeError, ExceptionGroup) as exc:
                        print(f"⚠️ Неожиданная ошибка в потоке: {exc}")
        else:
            for link in book_links:
                try:
                    all_books.append(get_book_data(link))
                    time.sleep(0.05)
                except requests.RequestException as exc:
                    print(f"⚠️ Ошибка сети при обработке {link}: {exc}")

        if max_pages and page >= max_pages:
            break

        next_button = _fetch_page(session, page_url, per_request_timeout)
        if not next_button:
            break
        page += 1

    if is_save:
        save_path = output_path or OUTPUT_PATH
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as file:
            for book in all_books:
                file.write(str(book) + "\n")
        print(f"\n✅ Сохранено {len(all_books)} книг в {save_path}")

    return all_books


def job() -> None:
    """Единичный запуск задачи парсинга каталога книг."""
    print("\n🕖 Запуск задачи парсинга...")
    scrape_books(is_save=True, use_threads=True)
    print("✅ Задача завершена.")


def run_scheduler() -> None:
    """Настраивает ежедневный запуск парсера в 19:00."""
    schedule.every().day.at("19:00").do(job)
    print("📅 Планировщик запущен. Ожидаем 19:00 для запуска задачи...")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    job()
    # run_scheduler()
