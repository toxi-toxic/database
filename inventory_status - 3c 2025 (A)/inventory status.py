import sqlite3
import os

def status_lookup_demo():
    """
    Демонстрирует использование SQL-оператора CASE для преобразования 
    числовых кодов статуса в читаемый текстовый формат.
    """
    db_file_name = "inventory_status.db"
    
    # Убедимся, что начинаем с чистого листа
    if os.path.exists(db_file_name):
        os.remove(db_file_name)

    db_connection = sqlite3.connect(db_file_name)
    sql_cursor = db_connection.cursor()

    # Создаем таблицу для учета товаров (Items)
    sql_cursor.execute("""
    CREATE TABLE Items (
        item_id INTEGER PRIMARY KEY,
        code_inventory INTEGER
    );
    """)

    # --- Вставляем данные инвентаризации ---
    
    inventory_items = [
        (101, 0),
        (102, 1),
        (103, 2),
        (104, 3),
        (105, 99)  # Неизвестный код статуса
    ]
    sql_cursor.executemany("INSERT INTO Items VALUES (?, ?);", inventory_items)
    db_connection.commit()

    # --- Запрос с CASE (перевод кода в текст) ---
    
    sql_cursor.execute("""
    SELECT 
        item_id,
        CASE code_inventory
            WHEN 0 THEN 'На складе'
            WHEN 1 THEN 'Проверка качества'
            WHEN 2 THEN 'Готов к отправке'
            WHEN 3 THEN 'В пути'
            ELSE 'Списано/Неизвестно'
        END AS InventoryStatus
    FROM Items;
    """)

    print("--- Отчет по статусам инвентаризации ---")
    for row in sql_cursor.fetchall():
        print(f"Товар ID: {row[0]}, Статус: {row[1]}")

if __name__ == "__main__":
    status_lookup_demo()
