# Импорт необходимых модулей
import sqlite3
import os

# --- Константы Базы Данных ---
DB_FILE = "teachers without classes.db" # имя файла
TABLE_FACULTY = "Faculty"        # Таблица преподавателей
TABLE_ASSIGNMENTS = "ClassAssignments" # Таблица назначений

def setup_database(db_file):
    """Устанавливает соединение, удаляет старый файл и создает таблицы."""
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Создаем основную таблицу: Faculty (Список всех преподавателей)
    cursor.executescript(f"""
    CREATE TABLE {TABLE_FACULTY} (
        teacher_id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        department TEXT
    );

    -- Создаем детальную таблицу: ClassAssignments (Записи о назначенных классах)
    CREATE TABLE {TABLE_ASSIGNMENTS} (
        assignment_id INTEGER PRIMARY KEY,
        teacher_ref_id INTEGER,
        class_code TEXT,
        semester TEXT,
        FOREIGN KEY (teacher_ref_id) REFERENCES Faculty(teacher_id)
    );
    """)
    return conn, cursor

def insert_sample_data(cursor, conn):
    """Вставляет тестовые данные в таблицы."""
    
    # Преподаватели в штате
    faculty_list = [
        (10, "Смирнова И.В.", "Математика"),
        (20, "Петров С.Н.", "Физика"),     # Этот преподаватель не ведет классов
        (30, "Иванов А.П.", "История"),
        (40, "Васильева Е.А.", "Информатика"), # Этот преподаватель не ведет классов
    ]
    cursor.executemany(f"INSERT INTO {TABLE_FACULTY} VALUES (?, ?, ?);", faculty_list)

    # Назначенные классы (только для преподавателей 10 и 30)
    assignments_log = [
        (1001, 10, "MATH101", "Осень 2024"),
        (1002, 30, "HIST205", "Осень 2024"),
        (1003, 10, "MATH210", "Осень 2024"),
    ]
    cursor.executemany(f"INSERT INTO {TABLE_ASSIGNMENTS} VALUES (?, ?, ?, ?);", assignments_log)
    conn.commit()

def run_unassigned_teachers_query(cursor):
    """
    Выполняет анти-джойн запрос для поиска преподавателей без назначенных классов.
    """
    
    # Запрос: Найти записи в левой таблице (Faculty), которые не имеют совпадений 
    # в правой таблице (ClassAssignments).
    cursor.execute(f"""
    SELECT t.teacher_id, t.full_name, t.department
    FROM {TABLE_FACULTY} t
    LEFT JOIN {TABLE_ASSIGNMENTS} a ON t.teacher_id = a.teacher_ref_id
    WHERE a.assignment_id IS NULL; -- Ключевое условие для анти-джойна
    """)

    # Вывод результатов
    print("Преподаватели, не имеющие назначенных классов:")
    print("-" * 70)
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"ID: {row[0]:<5} | ФИО: {row[1]:<20} | Отдел: {row[2]}")
    else:
        print("Все преподаватели имеют назначенные классы.")
    print("-" * 70)


def audit_faculty_load():
    """Основная функция, запускающая весь скрипт."""
    conn = None
    try:
        conn, cursor = setup_database(DB_FILE)
        insert_sample_data(cursor, conn)
        run_unassigned_teachers_query(cursor)
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    audit_faculty_load()