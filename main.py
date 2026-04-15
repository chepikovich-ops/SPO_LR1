import mysql.connector
import csv
import os
class MySQLAutomation:
    def __init__(self, config):
        self.config = config
        try:
            self.conn = mysql.connector.connect(**config)
            self.cursor = self.conn.cursor(dictionary=True)
            print("подключено к БД")
        except mysql.connector.Error as err:
            print(f"ошибка подключения: {err}")
    def _execute(self, query, params=None, commit=False):
        try:
            self.cursor.execute(query, params or ())
            if commit: self.conn.commit()
            return self.cursor.fetchall() if self.cursor.description else None
        except mysql.connector.Error as err:
            print(f"SQL ошибка: {err}")
            return None
    def create(self, table, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self._execute(query, list(data.values()), commit=True)
    def read(self, table, conditions=None):
        query = f"SELECT * FROM {table}"
        if conditions:
            where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
            query += f" WHERE {where_clause}"
            return self._execute(query, list(conditions.values()))
        return self._execute(query)
    def update(self, table, data, conditions):
        set_clause = ", ".join([f"{k}=%s" for k in data.keys()])
        where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self._execute(query, list(data.values()) + list(conditions.values()), commit=True)
    def delete(self, table, conditions):
        where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        self._execute(query, list(conditions.values()), commit=True)
    # 1 Сортировка столбца
    def get_column_sorted(self, table, col, desc=False):
        order = "DESC" if desc else "ASC"
        return self._execute(f"SELECT {col} FROM {table} ORDER BY {col} {order}")
    # 2 Диапазон по ID
    def get_range_by_id(self, table, start, end):
        return self._execute(f"SELECT * FROM {table} WHERE id BETWEEN %s AND %s", (start, end))
    # 3 Удаление диапазона по ID
    def delete_range_by_id(self, table, start, end):
        self._execute(f"DELETE FROM {table} WHERE id BETWEEN %s AND %s", (start, end),
                      commit=True)
    # 4 Структура таблицы
    def get_structure(self, table):
        return self._execute(f"DESCRIBE {table}")
    # 5 Поиск по значению
    def find_by_value(self, table, col, val):
        return self._execute(f"SELECT * FROM {table} WHERE {col} = %s", (val,))
    # 6 Удаление таблицы
    def drop_table(self, table):
        self._execute(f"DROP TABLE IF EXISTS {table}", commit=True)
    # 7 Управление столбцами
    def add_column(self, table, col_name, col_type):
        self._execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}", commit=True)
    def drop_column(self, table, col_name):
        self._execute(f"ALTER TABLE {table} DROP COLUMN {col_name}", commit=True)
    # 8 CSV Экспорт/Импорт
    def export_csv(self, table, path):
        data = self._execute(f"SELECT * FROM {table}")
        if data:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
    def import_csv(self, table, path):
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cols = ", ".join(row.keys())
                vals = ", ".join(["%s"] * len(row))
                self._execute(f"INSERT INTO {table} ({cols}) VALUES ({vals})", list(row.values()), commit=True)
    # 9 INNER JOIN (Только совпадающие записи)
    def inner_join(self, table1, table2, col1, col2, select_cols="*"):
        query = f"SELECT {select_cols} FROM {table1} INNER JOIN {table2} ON {table1}.{col1} = {table2}.{col2}"
        return self._execute(query)

    # 10 LEFT JOIN (Все из левой + совпадения из правой)
    def left_join(self, table1, table2, col1, col2, select_cols="*"):
        query = f"SELECT {select_cols} FROM {table1} LEFT JOIN {table2} ON {table1}.{col1} = {table2}.{col2}"
        return self._execute(query)

    # 11 RIGHT JOIN (Все из правой + совпадения из левой)
    def right_join(self, table1, table2, col1, col2, select_cols="*"):
        query = f"SELECT {select_cols} FROM {table1} RIGHT JOIN {table2} ON {table1}.{col1} = {table2}.{col2}"
        return self._execute(query)

    # 12 UNION (Объединение без дубликатов)
    def union(self, table1, table2, select_cols="*"):
        query = f"SELECT {select_cols} FROM {table1} UNION SELECT {select_cols} FROM {table2}"
        return self._execute(query)

    # 13 UNION ALL (Объединение с дубликатами)
    def union_all(self, table1, table2, select_cols="*"):
        query = f"SELECT {select_cols} FROM {table1} UNION ALL SELECT {select_cols} FROM {table2}"
        return self._execute(query)


db_config = {
    'user': 'root',
    'password': 'pass',
    'host': '127.0.0.1',
    'database': 'jino_db'
}

db = MySQLAutomation(db_config)
T = "test_users" # Имя тестовой таблицы

print("\nТесты")

# 0 Подготовка: Создаем таблицу и наполняем её
db._execute(f"CREATE TABLE IF NOT EXISTS {T} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), age INT)", commit=True)
db._execute(f"INSERT INTO {T} (name, age) VALUES ('Ivan', 20), ('Anna', 30), ('Oleg', 25), ('Daria', 22)", commit=True)
print(" Таблица создана и наполнена.")

# 1 Сортировка (возрастание возраста)
print("1 Сортировка (age ASC):", db.get_column_sorted(T, 'age'))

# 2 Вывод диапазона (ID от 1 до 3)
print("2 Диапазон ID (1-3):", db.get_range_by_id(T, 1, 3))

# 4 Структура таблицы
print("4 Структура таблицы:", db.get_structure(T))

# 5 Поиск конкретной строки (где name = 'Anna')
print("5 Поиск (name='Anna'):", db.find_by_value(T, 'name', 'Anna'))

# 6. Удаление всей таблицы
# db.drop_table(T)
# print("6. Таблица удалена.")

# 7 Добавление нового столбца (email)
db.add_column(T, 'email', 'VARCHAR(100)')
print("7 Столбец 'email' добавлен.")

# 8 Экспорт в CSV
db.export_csv(T, 'users_backup.csv')
print("8 Данные экспортированы в users_backup.csv")


