# Импорт необходимых модулей
import sqlite3
import os

# --- Константы БД ---
DB_FILE = "product.db"
TECH_TABLE = "Tech_Inventory"
HOME_TABLE = "Home_Inventory"

def setup_database(db_file):
    """Настраивает подключение, удаляет старый файл и создает таблицы."""
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Создаем две таблицы: Техника и Товары для дома
    cursor.executescript(f"""
    CREATE TABLE {TECH_TABLE} (
        Prod_ID INTEGER PRIMARY KEY,
        Item_Name TEXT NOT NULL,
        Mfg_City TEXT NOT NULL,
        Mfg_Region TEXT NOT NULL
    );

    CREATE TABLE {HOME_TABLE} (
        Prod_ID INTEGER PRIMARY KEY,
        Item_Name TEXT NOT NULL,
        Mfg_City TEXT NOT NULL,
        Mfg_Region TEXT NOT NULL
    );
    """)
    return conn, cursor

def insert_sample_data(cursor, conn):
    """Вставляет синтетические данные в обе таблицы."""
    
    # Товары из категории "Техника"
    tech_items = [
        (101, "Smartwatch X5", "Shenzhen", "Asia"),
        (102, "E-Reader Pro", "Munich", "Europe"),
        (103, "Gaming Mouse", "Taipei", "Asia"),
        (104, "Wireless Charger", "Paris", "Europe")
    ]
    
    # Товары из категории "Дом"
    home_items = [
        (201, "Coffee Machine", "Milan", "Europe"),
        (202, "Vacuum Cleaner", "Kyoto", "Asia"),
        (203, "Smart Speaker", "New York", "Americas"), # Будет исключен фильтром
        (204, "Electric Kettle", "Shenzhen", "Asia")
    ]

    # Вставляем данные
    cursor.executemany(f"INSERT INTO {TECH_TABLE} VALUES (?, ?, ?, ?);", tech_items)
    cursor.executemany(f"INSERT INTO {HOME_TABLE} VALUES (?, ?, ?, ?);", home_items)
    conn.commit()

def run_union_query(cursor):
    """
    Выполняет UNION ALL для объединения данных из двух таблиц,
    фильтруя по регионам Europe и Asia, и добавляя столбец Category.
    """
    
    # Запрос использует UNION ALL для объединения двух SELECT-запросов.
    # Обратите внимание на совпадение количества и типов столбцов в обеих частях.
    cursor.execute(f"""
    SELECT Prod_ID, Item_Name, Mfg_City, Mfg_Region, 'Tech' AS Category
    FROM {TECH_TABLE}
    WHERE Mfg_Region IN ('Europe', 'Asia')

    UNION ALL

    SELECT Prod_ID, Item_Name, Mfg_City, Mfg_Region, 'Home' AS Category
    FROM {HOME_TABLE}
    WHERE Mfg_Region IN ('Europe', 'Asia')

    ORDER BY Mfg_City, Item_Name;
    """)

    # Вывод результатов
    print("Список всех товаров из Европы и Азии, объединенных по категории:")
    print("-" * 50)
    
    results = cursor.fetchall()
    for row in results:
        # Форматированный вывод
        print(f"ID: {row[0]:<5} | Товар: {row[1]:<20} | Город: {row[2]:<10} | Категория: {row[4]}")
    print("-" * 50)


def combine_product_lists():
    """Главная функция для выполнения всей логики."""
    conn = None
    try:
        conn, cursor = setup_database(DB_FILE)
        insert_sample_data(cursor, conn)
        run_union_query(cursor)
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    combine_product_lists()