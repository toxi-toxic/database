import sqlite3
import os
from datetime import datetime

# --- 1. Настройка и подключение к базе данных ---

def setup_database(db_file="transactions list total sum.db"):
    """Инициализирует подключение и удаляет старый файл, если он существует."""
    if os.path.exists(db_file):
        os.remove(db_file)
        
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Активация проверки внешних ключей
    cursor.execute("PRAGMA foreign_keys = ON;")
    return conn, cursor

def create_schema(cursor):
    """Создает все необходимые таблицы для магазина."""
    
    # Таблица 1: Clients (Клиенты)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Clients (
        client_pk INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT NOT NULL,
        contact_email TEXT NOT NULL
    );
    """)

    # Таблица 2: Groups (Категории)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Groups (
        group_pk INTEGER PRIMARY KEY AUTOINCREMENT,
        group_title TEXT NOT NULL
    );
    """)

    # Таблица 3: StoreItems (Товары)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS StoreItems (
        item_pk INTEGER PRIMARY KEY AUTOINCREMENT,
        item_name TEXT NOT NULL,
        unit_price REAL NOT NULL,
        group_ref_id INTEGER,
        FOREIGN KEY (group_ref_id) REFERENCES Groups(group_pk)
    );
    """)

    # Таблица 4: Transactions (Заказы)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        transaction_pk INTEGER PRIMARY KEY AUTOINCREMENT,
        client_ref_id INTEGER,
        transaction_date TEXT NOT NULL,
        FOREIGN KEY (client_ref_id) REFERENCES Clients(client_pk)
    );
    """)

    # Таблица 5: TransactionDetails (Детали заказа/Позиции)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS TransactionDetails (
        detail_pk INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_ref_id INTEGER,
        item_ref_id INTEGER,
        purchased_count INTEGER NOT NULL,
        FOREIGN KEY (transaction_ref_id) REFERENCES Transactions(transaction_pk),
        FOREIGN KEY (item_ref_id) REFERENCES StoreItems(item_pk)
    );
    """)
    print("Схема базы данных успешно создана.")


def insert_data(cursor, conn):
    """Вставляет тестовые данные во все таблицы."""
    
    # 1. Clients
    client_data = [
        ("Виктор Смирнов", "victor@tech.ru"),
        ("Ольга Петрова", "olga@mail.com"),
        ("Максим Иванов", "maximov@corp.net")
    ]
    cursor.executemany("INSERT INTO Clients (full_name, contact_email) VALUES (?, ?);", client_data)

    # 2. Groups
    group_data = [
        ("Гастрономия",),
        ("Спортинвентарь",),
        ("Программное обеспечение",)
    ]
    cursor.executemany("INSERT INTO Groups (group_title) VALUES (?);", group_data)

    # 3. StoreItems
    item_data = [
        ("Кофе-машина", 650.00, 1),
        ("Термос", 50.00, 1),
        ("Кроссовки беговые", 90.00, 2),
        ("Фитнес-браслет", 35.00, 2),
        ("Антивирус Premium", 12.50, 3),
        ("Графический редактор", 55.00, 3)
    ]
    cursor.executemany("INSERT INTO StoreItems (item_name, unit_price, group_ref_id) VALUES (?, ?, ?);", item_data)

    # 4. Transactions
    # Используем фиксированные даты для воспроизводимости
    transaction_data = [
        (1, "2023-01-20"), # Виктор
        (2, "2022-11-05"), # Ольга
        (1, "2023-09-01"), # Виктор
        (3, "2023-06-25")  # Максим
    ]
    cursor.executemany("INSERT INTO Transactions (client_ref_id, transaction_date) VALUES (?, ?);", transaction_data)

    # 5. TransactionDetails
    details_data = [
        (1, 1, 1), # Виктор: Кофе-машина
        (1, 2, 2), # Виктор: 2 термоса
        (2, 3, 1), # Ольга: Кроссовки
        (3, 6, 1), # Виктор: Графический редактор
        (3, 5, 2), # Виктор: 2 Антивируса
        (4, 4, 1), # Максим: Фитнес-браслет
        (4, 1, 1), # Максим: Кофе-машина
    ]
    cursor.executemany("INSERT INTO TransactionDetails (transaction_ref_id, item_ref_id, purchased_count) VALUES (?, ?, ?);", details_data)
    
    conn.commit()
    print("Данные успешно вставлены.")

def execute_queries(cursor):
    """Выполняет набор аналитических SQL-запросов."""
    
    print("\n--- Аналитические Запросы ---")

    # Запрос 1: Клиенты, сделавшие заказы после 2023-01-01 (Дата изменена для получения того же результата)
    print("\n1. Клиенты, чьи заказы были оформлены в 2023 году или позже:")
    cursor.execute("""
    SELECT DISTINCT 
        C.full_name, C.contact_email
    FROM 
        Clients C
    INNER JOIN 
        Transactions T ON C.client_pk = T.client_ref_id
    WHERE 
        T.transaction_date >= '2023-01-01';
    """)
    for row in cursor.fetchall():
        print(row)

    # Запрос 2: Общее количество проданных единиц по категориям товаров
    print("\n2. Объем продаж по группам товаров (по количеству единиц):")
    cursor.execute("""
    SELECT 
        G.group_title, SUM(TD.purchased_count) AS total_items_shipped
    FROM 
        TransactionDetails TD
    JOIN 
        StoreItems SI ON TD.item_ref_id = SI.item_pk
    JOIN 
        Groups G ON SI.group_ref_id = G.group_pk
    GROUP BY 
        G.group_pk;
    """)
    for row in cursor.fetchall():
        print(row)

    # Запрос 3: Топ-3 самых дорогих товаров
    print("\n3. Три самых дорогих товара в ассортименте:")
    cursor.execute("""
    SELECT 
        item_name, unit_price
    FROM 
        StoreItems
    ORDER BY 
        unit_price DESC
    LIMIT 3;
    """)
    for row in cursor.fetchall():
        print(row)

    # Запрос 4: Список всех транзакций с подсчетом общей денежной суммы
    print("\n4. Список транзакций и общая сумма каждой (цена * кол-во):")
    cursor.execute("""
    SELECT 
        T.transaction_pk, C.full_name, SUM(SI.unit_price * TD.purchased_count) AS final_transaction_amount
    FROM 
        Transactions T
    JOIN 
        Clients C ON T.client_ref_id = C.client_pk
    JOIN 
        TransactionDetails TD ON T.transaction_pk = TD.transaction_ref_id
    JOIN 
        StoreItems SI ON TD.item_ref_id = SI.item_pk
    GROUP BY 
        T.transaction_pk
    ORDER BY 
        T.transaction_pk;
    """)
    for row in cursor.fetchall():
        print(row)

    # Запрос 5: Клиент с наибольшим общим объемом потраченных средств
    print("\n5. Клиент с максимальными суммарными расходами:")
    cursor.execute("""
    SELECT 
        C.full_name, SUM(SI.unit_price * TD.purchased_count) AS total_spending
    FROM 
        Clients C
    JOIN 
        Transactions T ON C.client_pk = T.client_ref_id
    JOIN 
        TransactionDetails TD ON T.transaction_pk = TD.transaction_ref_id
    JOIN 
        StoreItems SI ON TD.item_ref_id = SI.item_pk
    GROUP BY 
        C.client_pk
    ORDER BY 
        total_spending DESC
    LIMIT 1;
    """)
    print(cursor.fetchone())

# --- Основная логика ---

def main():
    conn, cursor = setup_database()
    create_schema(cursor)
    insert_data(cursor, conn)
    execute_queries(cursor)
    
    print("\nВсе операции с завершены.")
    conn.close()

if __name__ == "__main__":
    main()
