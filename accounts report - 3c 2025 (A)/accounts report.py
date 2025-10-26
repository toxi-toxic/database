import sqlite3
import os

def identify_shared_accounts():
    """
    Создает две таблицы для различных клиентских платформ и использует 
    INNER JOIN для выявления аккаунтов, присутствующих в обеих системах.
    """
    db_name = "accounts report.db"
    
    # Очищаем старый файл для гарантии чистого запуска
    if os.path.exists(db_name):
        os.remove(db_name)

    db_handle = sqlite3.connect(db_name)
    sql_runner = db_handle.cursor()

    # Создаем таблицы для двух клиентских групп с новыми именами
    sql_runner.executescript("""
    CREATE TABLE DesktopClients (
        AccountID INTEGER PRIMARY KEY,
        ScreenName TEXT NOT NULL
    );

    CREATE TABLE MobileClients (
        AccountID INTEGER PRIMARY KEY,
        ScreenName TEXT NOT NULL
    );
    """)

    # --- Подготовка данных ---
    
    # 1. Аккаунты настольной версии (DesktopClients)
    desktop_data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David")
    ]

    # 2. Аккаунты мобильной версии (MobileClients)
    mobile_data = [
        (3, "Charlie"),
        (4, "David"),
        (5, "Eve"),
        (6, "Frank")
    ]

    sql_runner.executemany("INSERT INTO DesktopClients VALUES (?, ?);", desktop_data)
    sql_runner.executemany("INSERT INTO MobileClients VALUES (?, ?);", mobile_data)
    db_handle.commit()

    # --- Анализ: Поиск общих аккаунтов (INNER JOIN) ---

    # Запрос на поиск общих записей, используя INNER JOIN
    sql_runner.execute("""
    SELECT 
        DC.AccountID, DC.ScreenName
    FROM 
        DesktopClients DC
    JOIN 
        MobileClients MC 
    ON 
        DC.AccountID = MC.AccountID;
    """)

    print("--- Отчет по общим аккаунтам ---")
    print("Аккаунты, используемые и на десктопе, и на мобильной платформе:")
    for record in sql_runner.fetchall():
        print(f"Аккаунт №: {record[0]}, Имя пользователя: {record[1]}")

    db_handle.close()
    print("\nПроверка завершена.")

if __name__ == "__main__":
    identify_shared_accounts()
