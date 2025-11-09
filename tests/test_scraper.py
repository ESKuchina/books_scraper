"""
tests/test_scraper.py

Набор автотестов для проверки функций парсинга из модуля scraper.py.

Проверяются:
- корректность структуры данных, возвращаемых get_book_data();
- валидность содержимого (ключи, типы, значения);
- успешность сборки всех книг и создание файла при сохранении.

Для запуска:
pytest tests/test_scraper.py -v
"""

import os
import pytest
from scraper import get_book_data, scrape_books


@pytest.fixture
def example_url() -> str:
    """
    Возвращает тестовую ссылку на книгу для проверки.

    Returns
    -------
    str
        URL страницы книги.
    """
    return "http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"


def test_get_book_data_returns_dict(example_url):
    """
    Проверяет, что get_book_data возвращает словарь с нужными ключами.
    """
    data = get_book_data(example_url)
    assert isinstance(data, dict), "Ожидался словарь"
    required_keys = {
        "title",
        "price",
        "availability",
        "rating",
        "description",
        "upc",
    }
    assert required_keys.issubset(data.keys()), "Отсутствуют обязательные поля"


def test_get_book_data_content(example_url):
    """
    Проверяет корректность содержимого данных книги.
    """
    data = get_book_data(example_url)
    assert data["title"] == "A Light in the Attic"
    assert "£" in data["price"] or "Â£" in data["price"], "Цена некорректна"
    assert data["rating"] in ["One", "Two", "Three", "Four", "Five"], \
        "Некорректное значение рейтинга"


def test_scrape_books_creates_file(tmp_path):
    """
    Проверяет, что scrape_books создаёт файл и возвращает список данных о книгах.
    """
    test_file = tmp_path / "books_data.txt"

    # Выполняем парсинг в безопасном режиме (без потоков)
    result = scrape_books(is_save=True, use_threads=False)

    # Проверяем, что результат — список
    assert isinstance(result, list), "Функция должна возвращать список"
    assert len(result) > 0, "Парсер не собрал книги"

    # Проверяем, что файл сохранён
    assert os.path.exists("artifacts/books_data.txt") or test_file.exists(), \
        "Файл books_data.txt не создан"

    # Проверяем, что внутри файла есть строки
    with open("artifacts/books_data.txt", "r", encoding="utf-8") as f:
        content = f.read().strip()
    assert len(content) > 0, "Файл пуст"
