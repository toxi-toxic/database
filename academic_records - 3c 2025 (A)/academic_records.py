# Импорт необходимых библиотек
import sqlite3
import os
import sys

# --- КОНСТАНТЫ ---
DB_NAME = "academic_records.db"
# Паттерн для фильтрации: ищем фамилии, начинающиеся на букву 'И'
SURNAME_FILTER_PATTERN = "И%" 

def initialize_database_schema(db_file):
    """Удаляет старый файл БД и создает новую схему с тремя таблицами."""
    
    # 1. Очистка старого файла
    if os.path.exists(db_file):
        os.remove(db_file)
        
    # 2. Подключение к новой/созданной БД
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        sys.exit(1)

    # 3. Создание таблиц (переименованы для уникальности)
    cursor.executescript("""
    CREATE TABLE Pupils (
        PupilID INTEGER PRIMARY KEY,
        Name TEXT NOT NULL,
        Surname TEXT NOT NULL
    );

    CREATE TABLE Disciplines (
        DisciplineID INTEGER PRIMARY KEY,
        Title TEXT NOT NULL
    );

    CREATE TABLE Assignments (
        AssignmentID INTEGER PRIMARY KEY,
        PupilID INTEGER,
        DisciplineID INTEGER,
        FOREIGN KEY (PupilID) REFERENCES Pupils(PupilID),
        FOREIGN KEY (DisciplineID) REFERENCES Disciplines(DisciplineID)
    );
    """)
    conn.commit()
    return conn, cursor

def load_initial_records(conn):
    """Загружает стартовые данные в таблицы Pupils, Disciplines и Assignments."""
    cursor = conn.cursor()

    # Данные учеников
    pupils_data = [
        (1, "Иван", "Иванов"),
        (2, "Мария", "Петрова"),
        (3, "Игорь", "Исаев"),
        (4, "Алексей", "Смирнов"),
        (5, "Ирина", "Ильина"),
    ]

    # Данные дисциплин
    disciplines_data = [
        (1, "Математика"),
        (2, "Физика"),
        (3, "Информатика")
    ]

    # Данные о назначении (записи на курсы)
    assignments_data = [
        (1, 1, 1),  # Иван Иванов - Математика
        (2, 1, 3),  # Иван Иванов - Информатика
        (3, 2, 2),  # Мария Петрова - Физика
        (4, 3, 3),  # Игорь Исаев - Информатика
        (5, 4, 1),  # Алексей Смирнов - Математика
        (6, 5, 2),  # Ирина Ильина - Физика
        (7, 5, 3)   # Ирина Ильина - Информатика
    ]

    cursor.executemany("INSERT INTO Pupils VALUES (?, ?, ?);", pupils_data)
    cursor.executemany("INSERT INTO Disciplines VALUES (?, ?);", disciplines_data)
    cursor.executemany("INSERT INTO Assignments VALUES (?, ?, ?);", assignments_data)
    conn.commit()
    print("-> Данные успешно загружены.")

def perform_filtered_join(cursor, filter_key):
    """
    Выполняет запрос INNER JOIN через три таблицы (Pupils, Assignments, Disciplines)
    и фильтрует результат по фамилии (Surname), используя LIKE.
    """
    print(f"\n--- Отчет по ученикам с фамилией, начинающейся на '{filter_key}' ---")
    
    # Запрос
    sql_query = f"""
    SELECT 
        P.PupilID, 
        P.Name, 
        P.Surname, 
        D.Title 
    FROM 
        Pupils P
    INNER JOIN 
        Assignments A ON P.PupilID = A.PupilID
    INNER JOIN 
        Disciplines D ON A.DisciplineID = D.DisciplineID
    WHERE 
        P.Surname LIKE ?
    ORDER BY 
        P.PupilID, D.Title;
    """
    
    # Выполнение запроса с параметром для безопасности
    cursor.execute(sql_query, (filter_key,))
    
    results = cursor.fetchall()
    
    # Форматированный вывод
    print(f"| ID | Имя         | Фамилия     | Курс")
    print("-" * 45)
    
    for row in results:
        pupil_id, name, surname, course = row
        print(f"| {pupil_id:<2} | {name:<10} | {surname:<10} | {course}")
        
    if not results:
        print("Нет данных, соответствующих условиям фильтрации.")


def main_execution_flow():
    """Основной поток выполнения программы."""
    
    conn, cursor = initialize_database_schema(DB_NAME)
    load_initial_records(conn)
    perform_filtered_join(cursor, SURNAME_FILTER_PATTERN)


if __name__ == "__main__":
    main_execution_flow()
