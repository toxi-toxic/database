# Импорт необходимых модулей
import sqlite3
import os

# --- Константы Базы Данных ---
DB_FILE = "devops_productivity.db"
TABLE_DEVS = "Developers"           # Таблица разработчиков
TABLE_TOOLS = "SoftwareTools"       # Таблица программных инструментов (аналог 'Sessions')
TABLE_LOGS = "ActivityLog"          # Таблица журнала активности (аналог 'Tickets')

def initialize_db(db_path):
    """Устанавливает соединение, удаляет старый файл и создает новую схему."""
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Создаем таблицу инструментов
    cursor.execute("""
    CREATE TABLE SoftwareTools (
        ToolID INTEGER PRIMARY KEY,
        ToolName TEXT NOT NULL,
        UsageArea TEXT
    );
    """)

    # 2. Создаем таблицу разработчиков
    cursor.execute("""
    CREATE TABLE Developers (
        DevID INTEGER PRIMARY KEY,
        DevName TEXT NOT NULL,
        Seniority TEXT
    );
    """)

    # 3. Создаем таблицу журнала активности (логи использования инструментов)
    cursor.execute(f"""
    CREATE TABLE {TABLE_LOGS} (
        LogID INTEGER PRIMARY KEY,
        DevID INTEGER,
        ToolID INTEGER,
        LogDate TEXT, -- Дата и время лога
        Description TEXT,
        FOREIGN KEY (DevID) REFERENCES Developers(DevID),
        FOREIGN KEY (ToolID) REFERENCES SoftwareTools(ToolID)
    );
    """)
    conn.commit()
    return conn, cursor

def populate_data(cursor, conn):
    """Вставляет тестовые данные в таблицы."""

    # Программные инструменты
    tools = [
        (1, "Git (Version Control)", "Code Management"),
        (2, "Jira (Task Tracker)", "Project Planning"),
        (3, "Grafana (Monitoring)", "Operations"),
        (4, "VS Code (Editor)", "Development"),
    ]
    cursor.executemany("INSERT INTO SoftwareTools VALUES (?, ?, ?);", tools)

    # Разработчики
    devs = [
        (10, "Светлана", "Senior"),
        (20, "Дмитрий", "Junior"),
        (30, "Елена", "Middle")
    ]
    cursor.executemany("INSERT INTO Developers VALUES (?, ?, ?);", devs)

    # Журнал активности. Описания изменены по вашему запросу.
    logs = [
        # Светлана (10) использовала Git (1) и Jira (2) -> 2 уникальных инструмента (PASS)
        (100, 10, 1, "2024-10-25 09:30:00", "Обновление документации API"),             # <-- Изменено
        (101, 10, 2, "2024-10-25 11:00:00", "Настройка нового мониторингового дашборда"), # <-- Изменено

        # Дмитрий (20) использовал только Grafana (3) -> 1 уникальный инструмент (FAIL)
        (102, 20, 3, "2024-10-25 14:00:00", "Проверка серверной нагрузки"),

        # Елена (30) использовала VS Code (4) дважды -> 1 уникальный инструмент (FAIL)
        (103, 30, 4, "2024-10-25 15:30:00", "Написание юнит-тестов"),
        (104, 30, 4, "2024-10-25 17:00:00", "Локальный запуск сборки"),
    ]
    cursor.executemany("INSERT INTO ActivityLog VALUES (?, ?, ?, ?, ?);", logs)
    conn.commit()

def run_multi_tool_devs_query(cursor):
    """
    Выполняет аналитический запрос: находит разработчиков, 
    которые использовали не менее 2 различных программных инструментов.
    """
    
    cursor.execute(f"""
    SELECT 
        d.DevName,
        d.Seniority,
        t.ToolName,
        l.LogDate,
        l.Description
    FROM {TABLE_LOGS} l
    JOIN Developers d ON l.DevID = d.DevID
    JOIN SoftwareTools t ON l.ToolID = t.ToolID
    WHERE l.DevID IN (
        -- Подзапрос находит DevID, у которых COUNT(DISTINCT ToolID) >= 2
        SELECT DevID
        FROM {TABLE_LOGS}
        GROUP BY DevID
        HAVING COUNT(DISTINCT ToolID) >= 2
    )
    ORDER BY d.DevName, l.LogDate;
    """)

    # Вывод результатов
    print("Разработчики, активно использующие не менее двух различных инструментов:")
    print("=" * 100)
    
    results = cursor.fetchall()
    if results:
        # Форматированный вывод
        print(f"{'Разработчик':<15} | {'Уровень':<8} | {'Инструмент':<25} | {'Дата лога':<20} | {'Описание'}")
        print("-" * 100)
        for row in results:
            print(f"{row[0]:<15} | {row[1]:<8} | {row[2]:<25} | {row[3]:<20} | {row[4]}")
    else:
        print("Не найдено разработчиков, использующих два и более уникальных инструмента.")
    print("=" * 100)


def audit_dev_metrics():
    """Основная функция для запуска аудита."""
    conn = None
    try:
        conn, cursor = initialize_db(DB_FILE)
        populate_data(cursor, conn)
        run_multi_tool_devs_query(cursor)
    except sqlite3.Error as e:
        print(f"Ошибка БД: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    audit_dev_metrics()