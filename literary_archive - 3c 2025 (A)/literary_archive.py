import sqlite3
import os
from datetime import datetime

# --- 1. Настройка и подключение к базе данных ---

def setup_library_db(filename="literary_archive.db"):
    """
    Устанавливает соединение с базой данных и активирует поддержку внешних ключей.
    Удаляет существующий файл БД, чтобы начать с чистого листа.
    """
    if os.path.exists(filename):
        os.remove(filename)

    db_conn = sqlite3.connect(filename)
    db_cursor = db_conn.cursor()
    # Активация внешних ключей
    db_cursor.execute("PRAGMA foreign_keys = ON;")
    
    return db_conn, db_cursor

# --- 2. Создание структуры (Схемы) ---

def define_schema(cursor):
    """Определяет структуру всех пяти связанных таблиц для каталога."""
    
    # Таблица 1: Members (Пользователи/Читатели)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Members (
        member_id INTEGER PRIMARY KEY AUTOINCREMENT,
        display_name TEXT NOT NULL,
        login_email TEXT UNIQUE NOT NULL,
        secure_hash TEXT NOT NULL,
        join_date TEXT NOT NULL
    );
    """)

    # Таблица 2: Creators (Авторы)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Creators (
        creator_id INTEGER PRIMARY KEY AUTOINCREMENT,
        display_full_name TEXT NOT NULL,
        year_born INTEGER
    );
    """)

    # Таблица 3: Categories (Жанры)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Categories (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_title TEXT UNIQUE NOT NULL
    );
    """)

    # Таблица 4: LibraryEntries (Книги)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS LibraryEntries (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_title TEXT NOT NULL,
        publication_year INTEGER,
        creator_ref_id INTEGER NOT NULL,
        category_ref_id INTEGER NOT NULL,
        FOREIGN KEY (creator_ref_id) REFERENCES Creators(creator_id),
        FOREIGN KEY (category_ref_id) REFERENCES Categories(category_id)
    );
    """)

    # Таблица 5: UserFeedback (Отзывы)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS UserFeedback (
        feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_ref_id INTEGER NOT NULL,
        entry_ref_id INTEGER NOT NULL,
        grade INTEGER CHECK (grade >= 1 AND grade <= 5),
        feedback_text TEXT,
        feedback_date TEXT NOT NULL,
        FOREIGN KEY (member_ref_id) REFERENCES Members(member_id),
        FOREIGN KEY (entry_ref_id) REFERENCES LibraryEntries(entry_id)
    );
    """)
    print("Структура базы данных для цифрового каталога создана.")

# --- 3. Вставка данных ---

def populate_data(cursor):
    """Заполняет все таблицы тестовыми данными."""

    # 3.1. Members
    members_data = [
        ("Anna Ivanova", "anna@archive.com", "hash_a123", "2025-01-15"),
        ("Igor Smirnov", "igor@archive.com", "hash_b456", "2025-03-22"),
        ("Elena Kuznetsova", "elena@archive.com", "hash_c789", "2025-07-10")
    ]
    cursor.executemany("INSERT INTO Members (display_name, login_email, secure_hash, join_date) VALUES (?, ?, ?, ?);", members_data)

    # 3.2. Creators
    creators_data = [
        ("Leo Tolstoy", 1828),
        ("Fyodor Dostoevsky", 1821),
        ("Alexander Pushkin", 1799)
    ]
    cursor.executemany("INSERT INTO Creators (display_full_name, year_born) VALUES (?, ?);", creators_data)

    # 3.3. Categories
    categories_data = [
        ("Classic Literature",),
        ("Science Fiction",),
        ("Mystery",)
    ]
    cursor.executemany("INSERT INTO Categories (category_title) VALUES (?);", categories_data)

    # 3.4. LibraryEntries
    entries_data = [
        ("War and Peace", 1869, 1, 1),
        ("Crime and Punishment", 1866, 2, 1),
        ("Eugene Onegin", 1833, 3, 1)
    ]
    cursor.executemany("INSERT INTO LibraryEntries (entry_title, publication_year, creator_ref_id, category_ref_id) VALUES (?, ?, ?, ?);", entries_data)

    # 3.5. UserFeedback
    feedback_data = [
        # user_id 1, book_id 1
        (1, 1, 5, "Magnificent novel, read in one breath!", "2025-08-01"),
        # user_id 2, book_id 2
        (2, 2, 4, "Complex, but deep work.", "2025-08-15"),
        # user_id 3, book_id 3
        (3, 3, 5, "I love Pushkin! Reading again and again.", "2025-09-01")
    ]
    cursor.executemany("""
    INSERT INTO UserFeedback (member_ref_id, entry_ref_id, grade, feedback_text, feedback_date)
    VALUES (?, ?, ?, ?, ?);
    """, feedback_data)
    print("Таблицы успешно заполнены тестовыми данными.")

# --- 4. Выполнение ---

def main():
    conn, cursor = setup_library_db()
    
    try:
        define_schema(cursor)
        populate_data(cursor)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка при работе с БД: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
