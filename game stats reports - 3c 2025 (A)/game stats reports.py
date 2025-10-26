import sqlite3
import os
from datetime import datetime

# --- 1. Настройка и подключение к базе данных ---

def initialize_db(db_name="game stats reports.db"):
    """
    Устанавливает соединение с БД и очищает среду, удаляя старые таблицы 
    и обеспечивая чистый запуск.
    """
    if os.path.exists(db_name):
        os.remove(db_name)

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    # Активация внешних ключей
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    return conn, cursor

# --- 2. Создание структуры (Схемы) ---

def create_game_schema(cursor):
    """Определяет структуру всех таблиц для хранения статистики."""
    
    # Сначала удаляем, чтобы избежать конфликтов при повторном запуске (хотя файл удаляется)
    cursor.executescript("""
    DROP TABLE IF EXISTS PerformanceLog;
    DROP TABLE IF EXISTS GameSessions;
    DROP TABLE IF EXISTS Users;
    DROP TABLE IF EXISTS Titles;

    CREATE TABLE Users (
        UserID INTEGER PRIMARY KEY AUTOINCREMENT,
        Username TEXT NOT NULL,
        JoinedDate TEXT NOT NULL
    );

    CREATE TABLE Titles (
        TitleID INTEGER PRIMARY KEY AUTOINCREMENT,
        GameName TEXT NOT NULL
    );

    CREATE TABLE GameSessions (
        SessionID INTEGER PRIMARY KEY AUTOINCREMENT,
        TitleRefID INTEGER NOT NULL,
        SessionTime TEXT NOT NULL,
        FOREIGN KEY (TitleRefID) REFERENCES Titles(TitleID)
    );

    CREATE TABLE PerformanceLog (
        LogID INTEGER PRIMARY KEY AUTOINCREMENT,
        SessionRefID INTEGER NOT NULL,
        UserRefID INTEGER NOT NULL,
        FinalScore INTEGER NOT NULL,
        Frags INTEGER NOT NULL,
        Deaths INTEGER NOT NULL,
        IsVictory BOOLEAN NOT NULL,
        FOREIGN KEY (SessionRefID) REFERENCES GameSessions(SessionID),
        FOREIGN KEY (UserRefID) REFERENCES Users(UserID)
    );
    """)
    print("Структура БД для игровой платформы создана.")

# --- 3. Вставка данных ---

def insert_game_data(cursor, conn):
    """Заполняет таблицы тестовыми данными."""
    
    # 3.1. Users (5)
    users_data = [
        ("Phoenix", "2024-01-10"),
        ("Rider", "2023-11-15"),
        ("Warden", "2024-05-20"),
        ("Valkyrie", "2024-03-05"),
        ("Ghost", "2023-09-30")
    ]
    cursor.executemany("INSERT INTO Users (Username, JoinedDate) VALUES (?, ?);", users_data)

    # 3.2. Titles (3)
    titles_data = [
        ("Cyber Arena",),
        ("Space Battle",),
        ("Mystic Quest",)
    ]
    cursor.executemany("INSERT INTO Titles (GameName) VALUES (?);", titles_data)

    # 3.3. GameSessions (9)
    sessions_data = [
        (1, "2024-04-01"),
        (1, "2024-04-05"),
        (2, "2024-03-15"),
        (2, "2024-03-20"),
        (3, "2024-02-10"),
        (3, "2024-02-15"),
        (1, "2024-04-10"),
        (2, "2024-03-25"),
        (3, "2024-02-20"),
    ]
    cursor.executemany("INSERT INTO GameSessions (TitleRefID, SessionTime) VALUES (?, ?);", sessions_data)

    # 3.4. PerformanceLog (Результаты игроков)
    log_data = [
        # Session 1 (Cyber Arena)
        (1, 1, 1500, 10, 5, True),   # Phoenix won
        (1, 2, 1200, 8, 7, False),   # Rider
        (1, 3, 1300, 9, 6, False),   # Warden

        # Session 2 (Cyber Arena)
        (2, 2, 1400, 12, 4, True),   # Rider won
        (2, 4, 1100, 7, 8, False),   # Valkyrie
        (2, 5, 900, 5, 9, False),    # Ghost

        # Session 3 (Space Battle)
        (3, 1, 1600, 15, 3, True),   # Phoenix won
        (3, 3, 1000, 6, 10, False),  # Warden

        # Session 4 (Space Battle)
        (4, 2, 1300, 11, 5, True),   # Rider won
        (4, 4, 1200, 10, 6, False),  # Valkyrie

        # Session 5 (Mystic Quest)
        (5, 3, 1100, 8, 7, True),    # Warden won
        (5, 5, 1050, 7, 8, False),   # Ghost

        # Session 6 (Mystic Quest)
        (6, 1, 1400, 12, 4, True),   # Phoenix won
        (6, 4, 1000, 5, 9, False),   # Valkyrie

        # Session 7 (Cyber Arena)
        (7, 3, 1350, 11, 6, True),   # Warden won
        (7, 5, 900, 5, 12, False),   # Ghost

        # Session 8 (Space Battle)
        (8, 2, 1250, 10, 7, True),   # Rider won
        (8, 1, 1150, 9, 8, False),   # Phoenix

        # Session 9 (Mystic Quest)
        (9, 4, 1300, 12, 3, True),   # Valkyrie won
        (9, 5, 1100, 8, 7, False),   # Ghost
    ]
    cursor.executemany("""
    INSERT INTO PerformanceLog (SessionRefID, UserRefID, FinalScore, Frags, Deaths, IsVictory)
    VALUES (?, ?, ?, ?, ?, ?);
    """, log_data)

    conn.commit()
    print("Тестовые данные успешно загружены.")

# --- 4. Аналитические Запросы ---

def run_analytics(cursor):
    """Выполняет аналитические запросы по игровой статистике."""
    
    print("\n--- Отчеты по игровой статистике ---")

    # Задание 1: Игроки, зарегистрировавшиеся в 2024 году (Используем LIKE)
    print("\n1. Пользователи, присоединившиеся в 2024 году:")
    cursor.execute("""
    SELECT 
        UserID, Username, JoinedDate 
    FROM 
        Users 
    WHERE 
        JoinedDate LIKE '2024%';
    """)
    for row in cursor.fetchall():
        print(row)

    # Задание 2: Средний балл игрока с UserID=1 (Phoenix)
    print("\n2. Средний балл пользователя с UserID=1:")
    cursor.execute("""
    SELECT 
        AVG(FinalScore) 
    FROM 
        PerformanceLog 
    WHERE 
        UserRefID = 1;
    """)
    print(cursor.fetchone()[0])

    # Задание 3: 5 самых популярных игр по количеству сессий
    print("\n3. Топ-5 самых популярных игр по количеству проведенных Сессий:")
    cursor.execute("""
    SELECT 
        T.GameName, COUNT(GS.SessionID) AS TotalSessions
    FROM 
        Titles T
    JOIN 
        GameSessions GS ON T.TitleID = GS.TitleRefID
    GROUP BY 
        T.TitleID
    ORDER BY 
        TotalSessions DESC
    LIMIT 5;
    """)
    for row in cursor.fetchall():
        print(row)

    # Задание 4: Игрок с самым высоким средним K/D (Убийства/Смерти)
    print("\n4. Пользователь с максимальным средним коэффициентом K/D:")
    cursor.execute("""
    SELECT 
        U.UserID, U.Username, AVG(CAST(PL.Frags AS REAL) / NULLIF(PL.Deaths, 0)) AS Avg_KDRatio
    FROM 
        Users U
    JOIN 
        PerformanceLog PL ON U.UserID = PL.UserRefID
    GROUP BY 
        U.UserID
    ORDER BY 
        Avg_KDRatio DESC
    LIMIT 1;
    """)
    print(cursor.fetchone())

    # Задание 5: Игроки, которые играли в 'Cyber Arena', но не выиграли в ней ни одной сессии
    print("\n5. Пользователи, которые играли в 'Cyber Arena', но не одержали в ней ни одной победы:")
    cursor.execute("""
    SELECT DISTINCT 
        U.UserID, U.Username
    FROM 
        Users U
    JOIN 
        PerformanceLog PL ON U.UserID = PL.UserRefID
    JOIN 
        GameSessions GS ON PL.SessionRefID = GS.SessionID
    JOIN 
        Titles T ON GS.TitleRefID = T.TitleID
    WHERE 
        T.GameName = 'Cyber Arena'
        AND U.UserID NOT IN (
            SELECT 
                UserRefID 
            FROM 
                PerformanceLog PL_INNER
            JOIN 
                GameSessions GS_INNER ON PL_INNER.SessionRefID = GS_INNER.SessionID
            JOIN 
                Titles T_INNER ON GS_INNER.TitleRefID = T_INNER.TitleID
            WHERE 
                T_INNER.GameName = 'Cyber Arena' AND PL_INNER.IsVictory = 1
        );
    """)
    for row in cursor.fetchall():
        print(row)

    # Задание 6: Полная статистика сессии с SessionID=2
    print("\n6. Детализированный лог результатов сессии с SessionID=2:")
    cursor.execute("""
    SELECT 
        GS.SessionID, T.GameName AS Title, U.Username AS Player,
        PL.FinalScore, PL.Frags, PL.Deaths,
        CASE 
            WHEN PL.Deaths = 0 THEN CAST(PL.Frags AS REAL) 
            ELSE CAST(PL.Frags AS REAL) / PL.Deaths 
        END AS K_D_Ratio,
        PL.IsVictory
    FROM 
        GameSessions GS
    JOIN 
        Titles T ON GS.TitleRefID = T.TitleID
    JOIN 
        PerformanceLog PL ON GS.SessionID = PL.SessionRefID
    JOIN 
        Users U ON PL.UserRefID = U.UserID
    WHERE 
        GS.SessionID = 2
    ORDER BY 
        U.Username;
    """)
    for row in cursor.fetchall():
        print(row)

# --- 5. Выполнение ---

def main():
    conn, cursor = initialize_db()
    create_game_schema(cursor)
    insert_game_data(cursor, conn)
    run_analytics(cursor)
    
    print("\nанализа данных завершено.")
    conn.close()

if __name__ == "__main__":
    main()
