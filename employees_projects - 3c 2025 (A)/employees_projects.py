# Импорт необходимых модулей для работы с БД и ОС
import sqlite3
import os
import sys

# Глобальная константа для названия файла базы данных
DB_FILENAME = "employees_projects.db"

def initialize_database(db_file):
    """Устанавливает соединение и создает таблицы 'employees' и 'projects'."""
    
    # Очистка среды: удаление старых файлов
    if os.path.exists(db_file):
        os.remove(db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Скрипт создания таблиц с исправленной ошибкой PRIMARY KEY
    cursor.executescript("""
    CREATE TABLE employees (
        employee_id INTEGER PRIMARY KEY,
        employee_name TEXT NOT NULL,
        department TEXT
    );

    CREATE TABLE projects (
        project_id INTEGER PRIMARY KEY,
        project_name TEXT NOT NULL,
        assigned_employee_id INTEGER,
        FOREIGN KEY (assigned_employee_id) REFERENCES employees(employee_id)
    );
    """)
    conn.commit()
    return conn, cursor

def populate_initial_data(cursor, conn):
    """Загружает тестовые данные о сотрудниках и проектах."""
    
    employees_list = [
        (1, "Иван Иванов", "Отдел продаж"),
        (2, "Мария Петрова", "Разработка"),
        (3, "Алексей Смирнов", "Разработка"),
        (4, "Ольга Кузнецова", "Маркетинг"),
        (5, "Дмитрий Васильев", "Отдел продаж"),
        (6, "Елена Фёдорова", "Финансы"), # Сотрудник без проекта
    ]

    projects_list = [
        (101, "Проект Alpha", 1),       # Иван
        (102, "Проект Beta", 2),        # Мария
        (103, "Проект Gamma", None),    # Проект без сотрудника
        (104, "Проект Delta", 3),       # Алексей
        (105, "Проект Epsilon", None),   # Проект без сотрудника
    ]

    cursor.executemany("INSERT INTO employees VALUES (?, ?, ?);", employees_list)
    cursor.executemany("INSERT INTO projects VALUES (?, ?, ?);", projects_list)
    conn.commit()

def print_report_results(cursor, title, sql_query, headers):
    """
    Выполняет SQL-запрос и форматированно выводит результат в консоль.
    """
    
    print("\n" + "#" * 90)
    print(f"| Отчет: {title}")
    print("#" * 90)
    
    try:
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Печать заголовков
        header_line = f"| {' | '.join(h.ljust(len(h)) for h in headers)}"
        print(header_line)
        print("-" * 90)
        
        # Вывод данных
        for row in results:
            formatted_row = []
            for item in row:
                s = 'НЕТ' if item is None else str(item)
                formatted_row.append(s)
            
            # предположение, что вывод в консоль не требует сложного выравнивания
            print(f"| {' | '.join(formatted_row)}") 
            
    except sqlite3.Error as e:
        print(f"Критическая ОШИБКА SQL: {e}")
        sys.exit(1)


def generate_hr_report():
    """Главная функция, которая запускает все этапы отчета."""
    
    conn, cursor = initialize_database(DB_FILENAME)
    populate_initial_data(cursor, conn)

    # ----------------------------------------------------------------------
    # Запрос 1: Все сотрудники и проекты, в которых они участвуют (включая сотрудников без проектов)
    # Использует LEFT JOIN, чтобы сохранить все записи из левой таблицы (employees)
    # ----------------------------------------------------------------------
    print_report_results(
        cursor,
        "1. Сотрудники и их проекты (Все сотрудники)",
        """
        SELECT 
            e.employee_name, 
            e.department,
            p.project_name
        FROM 
            employees e
        LEFT JOIN 
            projects p ON e.employee_id = p.assigned_employee_id
        ORDER BY 
            e.employee_name;
        """,
        ["Имя Сотрудника", "Отдел", "Проект"]
    )

    # ----------------------------------------------------------------------
    # Запрос 2: Все проекты и сотрудники, которые над ними работают (включая проекты без сотрудников)
    # Использует LEFT JOIN, чтобы сохранить все записи из левой таблицы (projects)
    # ----------------------------------------------------------------------
    print_report_results(
        cursor,
        "2. Проекты и назначенные сотрудники (Все проекты)",
        """
        SELECT 
            p.project_name, 
            e.employee_name
        FROM 
            projects p
        LEFT JOIN 
            employees e ON p.assigned_employee_id = e.employee_id
        ORDER BY 
            p.project_name;
        """,
        ["Название Проекта", "Назначенный Сотрудник"]
    )

    # ----------------------------------------------------------------------
    # Запрос 3: Список сотрудников, которые не работают ни над одним проектом (Anti-JOIN)
    # Использует LEFT JOIN + WHERE IS NULL
    # ----------------------------------------------------------------------
    print_report_results(
        cursor,
        "3. Сотрудники без проектов",
        """
        SELECT 
            e.employee_name, 
            e.department
        FROM 
            employees e
        LEFT JOIN 
            projects p ON e.employee_id = p.assigned_employee_id
        WHERE 
            p.project_id IS NULL 
        ORDER BY 
            e.employee_name;
        """,
        ["Имя Сотрудника", "Отдел"]
    )

    # ----------------------------------------------------------------------
    # Запрос 4: Все сотрудники и все проекты (FULL OUTER JOIN, имитация через UNION)
    # Объединяет (LEFT JOIN employees) и (RIGHT JOIN projects, или LEFT JOIN projects с условием NULL)
    # ----------------------------------------------------------------------
    print_report_results(
        cursor,
        "4. Полный обзор",
        """
        -- Часть 1: Все сотрудники и их проекты
        SELECT 
            e.employee_name AS Entity1, 
            p.project_name AS Entity2
        FROM 
            employees e
        LEFT JOIN 
            projects p ON e.employee_id = p.assigned_employee_id
        
        UNION
        
        -- Часть 2: Все проекты без сотрудников (исключая те, что уже есть в Части 1)
        SELECT 
            e.employee_name AS Entity1, 
            p.project_name AS Entity2
        FROM 
            projects p
        LEFT JOIN 
            employees e ON p.assigned_employee_id = e.employee_id
        WHERE 
            e.employee_id IS NULL
        ORDER BY 
            Entity1 NULLS LAST, Entity2 NULLS LAST;
        """,
        ["Сотрудник", "Проект"]
    )

    conn.close()

if __name__ == "__main__":
    generate_hr_report()