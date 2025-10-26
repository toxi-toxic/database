import sqlite3
import os

def run_database_operation():
    """
    Выполняет все операции с базой данных:
    создание, заполнение и выполнение запроса INNER JOIN.
    """
    # 1. Определение имени файла базы данных
    data_file = "company_records.db"

    # 2. Удаление старого файла БД для чистого запуска
    if os.path.exists(data_file):
        os.remove(data_file)
        
    # 3. Установление соединения и получение курсора
    db_connection = sqlite3.connect(data_file)
    db_cursor = db_connection.cursor()

    # 4. Создание таблиц
    # Таблица для отделов/команд
    db_cursor.execute("""
    CREATE TABLE Teams (
        TeamID INTEGER PRIMARY KEY,
        GroupName TEXT NOT NULL,
        TeamLeadID INTEGER
    );
    """)

    # Таблица для сотрудников
    db_cursor.execute("""
    CREATE TABLE Workers (
        WorkerID INTEGER PRIMARY KEY,
        Name TEXT NOT NULL,
        Surname TEXT NOT NULL,
        TeamRefID INTEGER,
        FOREIGN KEY (TeamRefID) REFERENCES Teams(TeamID)
    );
    """)

    # 5. Подготовка и вставка в таблицу Teams
    teams_data = [
        (1, "Development", 501),
        (2, "Marketing", 502),
        (3, "Sales", 503)
    ]
    db_cursor.executemany("INSERT INTO Teams VALUES (?, ?, ?);", teams_data)

    # 6. Подготовка и вставка в таблицу Workers
    workers_data = [
        (1, "Марина", "Коваль", 1), # Связан с TeamID 1
        (2, "Илья", "Буров", 2),   # Связан с TeamID 2
        (3, "Джессика", "Мур", 5), # Не связан (TeamID 5 не существует)
        (4, "Тимур", "Сафиуллин", None) # Не связан (Нет TeamID)
    ]
    db_cursor.executemany("INSERT INTO Workers VALUES (?, ?, ?, ?);", workers_data)
    
    # Сохранение изменений
    db_connection.commit()

    # 7. Выполнение запроса INNER JOIN
    # Соединяем таблицу Workers и таблицу Teams по общему полю ID
    db_cursor.execute("""
    SELECT 
        W.WorkerID, W.Name, W.Surname, T.GroupName
    FROM 
        Workers W
    INNER JOIN 
        Teams T 
    ON 
        W.TeamRefID = T.TeamID;
    """)

    # 8. Извлечение и вывод результатов
    results = db_cursor.fetchall()
    print("--- Результат запроса INNER JOIN ---")
    for row in results:
        print(row)

    # 9. Закрытие соединения
    db_connection.close()

if __name__ == "__main__":
    run_database_operation()