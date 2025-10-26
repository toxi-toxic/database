import sqlite3
import os

def compare_staff_datasets():
    """
    Создает две таблицы (текущий штат и исторические записи) и использует 
    оператор EXCEPT для выполнения взаимного сравнения.
    """
    db_file_name = "staff division.db"
    
    # Очистка файла БД для гарантированного чистого запуска
    if os.path.exists(db_file_name):
        os.remove(db_file_name)

    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()

    # Создаем таблицы с новыми именами
    cursor.executescript("""
    CREATE TABLE CurrentStaff (
        StaffID INTEGER PRIMARY KEY,
        WorkerName TEXT NOT NULL,
        Division TEXT NOT NULL
    );

    CREATE TABLE PastRecords (
        StaffID INTEGER PRIMARY KEY,
        WorkerName TEXT NOT NULL,
        Division TEXT NOT NULL
    );
    """)

    # --- Вставляем данные ---
    
    # 1. Данные о текущем штате (CurrentStaff)
    current_staff_data = [
        (1, "Иван Иванов", "Отдел продаж"),
        (2, "Мария Петрова", "Маркетинг"),
        (3, "Сергей Сидоров", "ИТ"),
        (4, "Ольга Смирнова", "ИТ")
    ]

    # 2. Данные о бывших сотрудниках (PastRecords)
    past_staff_data = [
        (5, "Анна Кузнецова", "Отдел продаж"),
        (6, "Петр Петров", "Маркетинг"),
        (3, "Сергей Сидоров", "ИТ") 
    ]

    cursor.executemany("INSERT INTO CurrentStaff VALUES (?, ?, ?);", current_staff_data)
    cursor.executemany("INSERT INTO PastRecords VALUES (?, ?, ?);", past_staff_data)
    conn.commit()

    # --- Анализ: Сравнение множеств ---

    # Запрос 1: Найти записи, которые есть только в CurrentStaff
    # Задача: Сотрудники, которые работают сейчас, но не имеют записей в историческом списке (т.е. никогда не увольнялись или их ID уникальны)
    print("--- 1. Уникальные сотрудники (Только в CurrentStaff): ---")
    cursor.execute("""
    SELECT StaffID, WorkerName, Division FROM CurrentStaff
    EXCEPT
    SELECT StaffID, WorkerName, Division FROM PastRecords;
    """)
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Имя: {row[1]}, Подразделение: {row[2]}")

    # Запрос 2: Найти записи, которые есть только в PastRecords
    # Задача: Сотрудники, которые есть в историческом списке, но отсутствуют в текущем штате (т.е. они не были повторно наняты)
    print("\n--- 2. Невозвращенные сотрудники (Только в PastRecords): ---")
    cursor.execute("""
    SELECT StaffID, WorkerName, Division FROM PastRecords
    EXCEPT
    SELECT StaffID, WorkerName, Division FROM CurrentStaff;
    """)
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Имя: {row[1]}, Подразделение: {row[2]}")

    conn.close()
    print("\nОперация завершена.")

if __name__ == "__main__":
    compare_staff_datasets()