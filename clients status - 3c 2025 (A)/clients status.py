# Импорт необходимых модулей для работы с БД и датами
import sqlite3
import os
from datetime import datetime, timedelta

# --- Настройки БД и схемы ---
DB_NAME = "clients status.sqlite"
TABLE_NAME = "UserActivity"
VIP_THRESHOLD = 50000
RECENT_DAYS = 60
CHURN_DAYS = 180

def setup_database(db_file):
    """Инициализирует подключение и создает таблицу."""
    # Удаляем старый файл для чистого запуска
    if os.path.exists(db_file):
        os.remove(db_file)
        
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Создаем таблицу с новыми именами столбцов
    cursor.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        user_pk INTEGER PRIMARY KEY,
        alias_name TEXT NOT NULL,
        last_txn_date TEXT,
        gross_revenue REAL
    );
    """)
    return conn, cursor

def insert_data(cursor):
    """Вставляет синтетические данные в таблицу UserActivity."""
    current_datetime = datetime.today()
    
    # Данные для вставки: user_pk, alias_name, last_txn_date, gross_revenue
    user_data = [
        # Пользователь 1: Соответствует VIP-условию
        (101, "Phoenix", (current_datetime - timedelta(days=10)).strftime('%Y-%m-%d'), 60000), 
        
        # Пользователь 2: Соответствует Удержанию (прошло > 180 дней)
        (102, "Rider_77", (current_datetime - timedelta(days=200)).strftime('%Y-%m-%d'), 30000), 
        
        # Пользователь 3: Соответствует Обычному (Сумма < 50k, или активность > 60 дней)
        (103, "ShadowLord", (current_datetime - timedelta(days=100)).strftime('%Y-%m-%d'), 40000), 
        
        # Пользователь 4: Соответствует Обычному (Активность < 60 дней, но Сумма < 50k)
        (104, "Viktoria_S", (current_datetime - timedelta(days=30)).strftime('%Y-%m-%d'), 20000), 
        
        # Пользователь 5: Соответствует Удержанию (last_txn_date IS NULL)
        (105, "Anna_K", None, 70000), 
    ]
    
    cursor.executemany(f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?);", user_data)

def execute_segmentation_query(cursor):
    """Выполняет SQL-запрос с CASE для статуса пользователей."""
    
    # Обратите внимание на переписанный CASE-оператор и использование f-строки
    query = f"""
    SELECT alias_name,
           CASE
                -- Условие 1: Премиум (VIP)
                WHEN gross_revenue > {VIP_THRESHOLD} AND last_txn_date IS NOT NULL AND julianday('now') - julianday(last_txn_date) <= {RECENT_DAYS} THEN 'VIP'
                
                -- Условие 2: Риск оттока
                WHEN (last_txn_date IS NULL) OR (julianday('now') - julianday(last_txn_date) > {CHURN_DAYS}) THEN 'Риск оттока'
                
                -- Условие 3: Обычный
                ELSE 'Обычный'
           END AS user_segment
    FROM {TABLE_NAME}
    ORDER BY alias_name;
    """
    
    cursor.execute(query)
    
    print("Результаты статуса пользователей:")
    print("---------------------------------")
    for row in cursor.fetchall():
        print(f"Имя: {row[0]:<15} | Статус: {row[1]}")
    print("---------------------------------")


def segment_users_by_loyalty():
    """Главная функция для выполнения всех шагов."""
    conn = None
    try:
        conn, cursor = setup_database(DB_NAME)
        insert_data(cursor)
        conn.commit()
        execute_segmentation_query(cursor)
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    segment_users_by_loyalty()
