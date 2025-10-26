# Импорт необходимых модулей
import sqlite3
import os

# --- Константы ---
DB_PATH = "clients were weren’t.db"
TABLE_A = "InitialMembers"
TABLE_B = "FinalMembers"

def execute_comparison_query(cursor, source_table, target_table, title):
    """
    Выполняет SQL-запрос с EXCEPT для сравнения двух таблиц
    и выводит результат.
    """
    print(f"\n--- {title} ---")
    
    # Запрос для нахождения уникальных записей в исходной таблице (Source)
    # по сравнению с целевой таблицей (Target)
    query = f"""
    SELECT UserID_PK, DisplayName FROM {source_table}
    EXCEPT
    SELECT UserID_PK, DisplayName FROM {target_table};
    """
    cursor.execute(query)
    
    results = cursor.fetchall()
    
    if results:
        for row in results:
            print(f"ID: {row[0]:<5} Имя: {row[1]}")
    else:
        print("Не найдено.")

def run_flow_analysis():
    """Главная функция для настройки БД, вставки данных и выполнения анализа."""

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Создаем две таблицы с новыми именами и структурой
        cursor.executescript(f"""
        CREATE TABLE {TABLE_A} (
            UserID_PK INTEGER PRIMARY KEY,
            DisplayName TEXT NOT NULL
        );

        CREATE TABLE {TABLE_B} (
            UserID_PK INTEGER PRIMARY KEY,
            DisplayName TEXT NOT NULL
        );
        """)

        # Данные для Group A (Изначальный список)
        group_a_data = [
            (1, "Maxim Galkin"),
            (2, "Darya Kozlova"),
            (3, "Kirill Semenov"),
            (4, "Polina Volkova")
        ]

        # Данные для Group B (Финальный список)
        group_b_data = [
            (3, "Kirill Semenov"),
            (4, "Polina Volkova"),
            (5, "Stanislav Larin"),
            (6, "Elena Romanova")
        ]

        # Вставляем данные
        cursor.executemany(f"INSERT INTO {TABLE_A} VALUES (?, ?);", group_a_data)
        cursor.executemany(f"INSERT INTO {TABLE_B} VALUES (?, ?);", group_b_data)
        conn.commit()

        # Клиенты, которые были в 2024, но нет в 2025
        execute_comparison_query(
            cursor, 
            TABLE_A, 
            TABLE_B, 
            "Клиенты, которые были в 2024, но отсутствуют в 2025"
        )

        # Клиенты, которые появились только в 2025, отсутствуют в 2024
        execute_comparison_query(
            cursor, 
            TABLE_B, 
            TABLE_A, 
            "Клиенты, которые появились только в 2025, отсутствуют в 2024"
        )

    except sqlite3.Error as e:
        print(f"Произошла ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_flow_analysis()
