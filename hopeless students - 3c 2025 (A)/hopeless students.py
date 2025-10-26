# Импорт необходимых модулей
import sqlite3
import os

# --- Константы Базы Данных ---
DB_FILE = "hopeless_students.db"
TABLE_STUDENTS = "StudentsList"
TABLE_SUBMISSIONS = "SubmissionsLog"

def setup_database(db_file):
    """Устанавливает соединение с БД, удаляет старый файл и создает таблицы."""
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Создаем основную таблицу: StudentsList (список всех студентов)
    cursor.executescript(f"""
    CREATE TABLE {TABLE_STUDENTS} (
        student_id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        group_num TEXT
    );

    -- Создаем детальную таблицу: SubmissionsLog (записи о сданных работах)
    CREATE TABLE {TABLE_SUBMISSIONS} (
        submission_pk INTEGER PRIMARY KEY,
        assignment_code TEXT,
        student_ref_id INTEGER,
        grade REAL,
        FOREIGN KEY (student_ref_id) REFERENCES StudentsList(student_id)
    );
    """)
    return conn, cursor

def insert_sample_data(cursor, conn):
    """Вставляет тестовые данные в таблицы."""
    
    # Студенты в списке
    students = [
        (1, "Иванов П.К.", "Группа А"),
        (2, "Сидорова В.А.", "Группа Б"), # Нет сданных работ
        (3, "Петров А.Н.", "Группа А"),
        (4, "Кузнецова Е.Д.", "Группа Б"), # Нет сданных работ
        (5, "Смирнов Р.В.", "Группа А"),
    ]
    cursor.executemany(f"INSERT INTO {TABLE_STUDENTS} VALUES (?, ?, ?);", students)

    # Сданные работы (только для студентов 1, 3, 5)
    submissions_log = [
        (101, "A1", 1, 95.5),
        (102, "A2", 3, 88.0),
        (103, "A1", 5, 75.0),
        (104, "A3", 1, 90.0),
    ]
    cursor.executemany(f"INSERT INTO {TABLE_SUBMISSIONS} VALUES (?, ?, ?, ?);", submissions_log)
    conn.commit()

def run_missing_submissions_query(cursor):
    """
    Выполняет анти-джойн запрос для поиска студентов без сданных работ.
    """
    
    # Запрос: Найти записи в левой таблице (StudentsList), которые не имеют совпадений 
    # в правой таблице (SubmissionsLog).
    cursor.execute(f"""
    SELECT s.student_id, s.full_name, s.group_num
    FROM {TABLE_STUDENTS} s
    LEFT JOIN {TABLE_SUBMISSIONS} l ON s.student_id = l.student_ref_id
    WHERE l.submission_pk IS NULL; -- Ключевое условие для анти-джойна
    """)

    # Вывод результатов
    print("Студенты, не сдавшие ни одной работы:")
    print("-" * 60)
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"ID: {row[0]:<5} | ФИО: {row[1]:<20} | Группа: {row[2]}")
    else:
        print("Все студенты в списке имеют записи о сданных работах.")
    print("-" * 60)


def audit_student_assignments():
    """Основная функция, запускающая всю логику скрипта."""
    conn = None
    try:
        conn, cursor = setup_database(DB_FILE)
        insert_sample_data(cursor, conn)
        run_missing_submissions_query(cursor)
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    audit_student_assignments()
