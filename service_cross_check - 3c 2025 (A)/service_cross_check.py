import sqlite3
import os

def check_user_overlap():
    """
    Создает две таблицы пользователей для разных платформ и выполняет 
    операцию INNER JOIN для поиска общих ID.
    """
    db_filename = "service_cross_check.db"
    
    # Удаляем старый файл для чистого старта
    if os.path.exists(db_filename):
        os.remove(db_filename)

    conn_handle = sqlite3.connect(db_filename)
    db_cursor = conn_handle.cursor()

    # Создаем таблицы с новыми именами и структурой
    db_cursor.executescript("""
    CREATE TABLE PlatformA_Users (
        PID INTEGER PRIMARY KEY,
        Alias TEXT NOT NULL
    );

    CREATE TABLE PlatformB_Users (
        PID INTEGER PRIMARY KEY,
        Alias TEXT NOT NULL
    );
    """)

    # --- Вставляем данные ---
    
    # 1. Пользователи PlatformA
    platform_a_data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David")
    ]

    # 2. Пользователи PlatformB
    platform_b_data = [
        (3, "Charlie"),
        (4, "David"),
        (5, "Eve"),
        (6, "Frank")
    ]

    db_cursor.executemany("INSERT INTO PlatformA_Users VALUES (?, ?);", platform_a_data)
    db_cursor.executemany("INSERT INTO PlatformB_Users VALUES (?, ?);", platform_b_data)
    conn_handle.commit()

    # --- Анализ: Пересечение данных (INNER JOIN) ---

    # Запрос на поиск общих записей, используя псевдонимы (A, B)
    db_cursor.execute("""
    SELECT 
        A.PID, A.Alias
    FROM 
        PlatformA_Users A
    INNER JOIN 
        PlatformB_Users B 
    ON 
        A.PID = B.PID
    ORDER BY 
        A.PID;
    """)

    print("--- Результат кросс-проверки ---")
    print("Идентификаторы и имена пользователей, присутствующих на обеих платформах:")
    for row in db_cursor.fetchall():
        print(f"ID: {row[0]}, Имя: {row[1]}")

if __name__ == "__main__":
    check_user_overlap()
