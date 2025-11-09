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

BASE_URL = "https://books.toscrape.com/"
OUTPUT_DIR = "artifacts"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "books_data.txt")


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

    Examples
    --------
    >>> get_book_data(
    ... "http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"
    ... )
    {'title': 'A Light in the Attic', 'price': '£51.77', ...}
    """
    response = requests.get(book_url, timeout=15)
    response.encoding = "utf-8" if "utf" in response.apparent_encoding.lower() else "ISO-8859-1"
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    title = soup.find("div", class_="product_main").h1.get_text(strip=True)
    price = soup.find("p", class_="price_color").get_text(strip=True)
    availability = soup.find("p", class_="instock availability").get_text(strip=True)
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


def scrape_books(is_save: bool = True, use_threads: bool = False) -> list[dict]:
    """
    Собирает данные о книгах со всех страниц каталога Books to Scrape.

    Parameters
    ----------
    is_save : bool, optional
        Сохранять ли результат в файл (по умолчанию True).
    use_threads : bool, optional
        Использовать ли многопоточность для ускорения парсинга
        (по умолчанию False — безопасный режим).

    Returns
    -------
    list[dict]
        Список словарей с информацией о книгах.

    Examples
    --------
    >>> scrape_books(is_save=False, use_threads=True)
    [{'title': 'A Light in the Attic', 'price': '£51.77', ...}, ...]
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
        response = session.get(page_url, timeout=10)
        if response.status_code != 200:
            break

        soup = BeautifulSoup(response.text, "lxml")
        book_links = [urljoin(page_url, a["href"]) for a in soup.select("h3 > a")]
        if not book_links:
            break

        print(f"📄 Page {page} → {len(book_links)} books")

        if use_threads:
            # многопоточный режим
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(get_book_data, link) for link in book_links]
                for f in as_completed(futures):
                    try:
                        all_books.append(f.result())
                    except Exception as exc:
                        print(f"⚠️ Ошибка в потоке: {exc}")
        else:
            # последовательный режим
            for link in book_links:
                try:
                    all_books.append(get_book_data(link))
                    time.sleep(0.05)
                except Exception as exc:
                    print(f"⚠️ Ошибка при обработке {link}: {exc}")

        next_button = soup.select_one("li.next > a")
        if not next_button:
            break
        page += 1

    if is_save:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
            for book in all_books:
                file.write(str(book) + "\n")
        print(f"\n✅ Сохранено {len(all_books)} книг в {OUTPUT_PATH}")

    return all_books


def job() -> None:
    """
    Единичный запуск задачи парсинга каталога книг.

    Запускает scrape_books() и сохраняет данные в файл.
    """
    print("\n🕖 Запуск задачи парсинга...")
    scrape_books(is_save=True, use_threads=True)
    print("✅ Задача завершена.")


def run_scheduler() -> None:
    """
    Настраивает ежедневный запуск парсера в 19:00.

    Функция выполняет бесконечный цикл ожидания
    и проверяет расписание раз в минуту.
    """
    schedule.every().day.at("19:00").do(job)
    print("📅 Планировщик запущен. Ожидаем 19:00 для запуска задачи...")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    # По умолчанию однократный запуск с многопоточностью
    job()
    # Для постоянного расписания раскомментируйте:
    # run_scheduler()
