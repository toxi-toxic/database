# Импорт необходимых модулей
import sqlite3
import os

# --- Константы ---
DB_PATH = "company_contacts.db"
TABLE_SALES = "Dept_Sales"
TABLE_HR = "Dept_HR"
TABLE_OPS = "Dept_Operations"

def setup_database(db_file):
    """Настраивает подключение, удаляет старый файл и создает таблицы."""
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Создаем три таблицы для разных отделов
    cursor.executescript(f"""
    CREATE TABLE {TABLE_SALES} (
        Contact_Alias TEXT,
        Mobile_Num TEXT
    );

    CREATE TABLE {TABLE_HR} (
        Contact_Alias TEXT,
        Mobile_Num TEXT
    );

    CREATE TABLE {TABLE_OPS} (
        Contact_Alias TEXT,
        Mobile_Num TEXT
    );
    """)
    return conn, cursor

def insert_sample_data(cursor, conn):
    """Вставляет синтетические данные с дубликатами и NULL-значениями."""
    
    # Контакты отдела Продаж
    sales_data = [
        ("Виктор Смирнов", "444111"),
        ("Олег Дымов", None),
        ("Виктор Смирнов", "444111"), # Дубликат 1
    ]
    
    # Контакты отдела Кадров (HR)
    hr_data = [
        ("Екатерина Левина", "888222"),
        ("Олег Дымов", None),         # Дубликат 2 (с NULL)
        ("Анна Зайцева", "666333"),
    ]
    
    # Контакты отдела Операций (Ops)
    ops_data = [
        ("Глеб Орлов", "999000"),
        ("Анна Зайцева", "666333"),   # Дубликат 3
        ("Марина Кротова", None),
    ]

    # Вставляем данные
    cursor.executemany(f"INSERT INTO {TABLE_SALES} VALUES (?, ?);", sales_data)
    cursor.executemany(f"INSERT INTO {TABLE_HR} VALUES (?, ?);", hr_data)
    cursor.executemany(f"INSERT INTO {TABLE_OPS} VALUES (?, ?);", ops_data)
    conn.commit()

def run_consolidation_query(cursor):
    """
    Выполняет запрос для объединения всех таблиц, 
    замены NULL и удаления дубликатов.
    """
    
    # Используем CTE для объединения данных перед применением DISTINCT и COALESCE.
    cursor.execute(f"""
    WITH AllContacts AS (
        SELECT Contact_Alias, Mobile_Num FROM {TABLE_SALES}
        UNION ALL
        SELECT Contact_Alias, Mobile_Num FROM {TABLE_HR}
        UNION ALL
        SELECT Contact_Alias, Mobile_Num FROM {TABLE_OPS}
    )
    
    SELECT 
        DISTINCT Contact_Alias, 
        COALESCE(Mobile_Num, 'UNKNOWN') AS Mobile_Number
    FROM AllContacts
    ORDER BY Contact_Alias;
    """)

    # Вывод результатов
    print("Список контактов всех отделов:")
    print("-" * 50)
    print(f"{'Имя':<20} | {'Телефон':<15}")
    print("-" * 50)
    
    for row in cursor.fetchall():
        # Форматированный вывод
        print(f"{row[0]:<20} | {row[1]:<15}")
    print("-" * 50)


def consolidate_company_contacts():
    """Главная функция для выполнения."""
    conn = None
    try:
        conn, cursor = setup_database(DB_PATH)
        insert_sample_data(cursor, conn)
        run_consolidation_query(cursor)
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    consolidate_company_contacts()
