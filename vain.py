import csv
import os


class UniversalSQLAutomation:
    def __init__(self, db_type, config):

        self.db_type = db_type.lower()
        self.config = config
        self.conn = None
        self.cursor = None

        try:
            if self.db_type == 'mysql':
                import mysql.connector
                self.conn = mysql.connector.connect(**config)
                self.cursor = self.conn.cursor(dictionary=True)
            elif self.db_type == 'postgres':
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.conn = psycopg2.connect(**config)
                self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            else:
                raise ValueError("Поддерживаются только 'mysql' или 'postgres'")
            print(f"--- Успешное подключение к {self.db_type.upper()} ---")
        except Exception as err:
            print(f"ОШИБКА ПОДКЛЮЧЕНИЯ: {err}")

    def _execute(self, query, params=None, commit=False):
        if not self.cursor: return None
        try:
            self.cursor.execute(query, params or ())
            if commit: self.conn.commit()
            if self.cursor.description:
                result = self.cursor.fetchall()
                return [dict(row) for row in result] if self.db_type == 'postgres' else result
            return None
        except Exception as err:
            if self.db_type == 'postgres': self.conn.rollback()
            print(f"SQL Ошибка: {err}")
            return None

    def _build_where(self, filters):
        """Вспомогательный метод для генерации WHERE из словаря"""
        if not filters:
            return "", []
        clauses = [f"{k} = %s" for k in filters.keys()]
        return " WHERE " + " AND ".join(clauses), list(filters.values())

    # --- Базовый CRUD с поддержкой фильтров ---
    def create(self, table, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self._execute(query, list(data.values()), commit=True)

    def read(self, table, filters=None):
        where_str, params = self._build_where(filters)
        return self._execute(f"SELECT * FROM {table}{where_str}", params)

    def update(self, table, data, filters):
        set_clause = ", ".join([f"{k}=%s" for k in data.keys()])
        where_str, params = self._build_where(filters)
        query = f"UPDATE {table} SET {set_clause}{where_str}"
        self._execute(query, list(data.values()) + params, commit=True)

    def delete(self, table, filters):
        where_str, params = self._build_where(filters)
        self._execute(f"DELETE FROM {table}{where_str}", params, commit=True)

    # --- Дополнительные функции ---
    def get_column_sorted(self, table, col, desc=False, filters=None):
        order = "DESC" if desc else "ASC"
        where_str, params = self._build_where(filters)
        return self._execute(f"SELECT {col} FROM {table}{where_str} ORDER BY {col} {order}", params)

    def get_range_by_id(self, table, start, end):
        return self._execute(f"SELECT * FROM {table} WHERE id BETWEEN %s AND %s", (start, end))

    def get_structure(self, table):
        if self.db_type == 'mysql':
            return self._execute(f"DESCRIBE {table}")
        query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s"
        return self._execute(query, (table,))

    def drop_table(self, table):
        self._execute(f"DROP TABLE IF EXISTS {table}", commit=True)

    # --- JOINS с поддержкой фильтров ---
    def inner_join(self, t1, t2, c1, c2, select="*", filters=None):
        where_str, params = self._build_where(filters)
        query = f"SELECT {select} FROM {t1} INNER JOIN {t2} ON {t1}.{c1} = {t2}.{c2}{where_str}"
        return self._execute(query, params)

    def left_join(self, t1, t2, c1, c2, select="*", filters=None):
        where_str, params = self._build_where(filters)
        query = f"SELECT {select} FROM {t1} LEFT JOIN {t2} ON {t1}.{c1} = {t2}.{c2}{where_str}"
        return self._execute(query, params)

    def full_join(self, t1, t2, c1, c2, select="*", filters=None):
        where_str, params = self._build_where(filters)
        if self.db_type == 'postgres':
            query = f"SELECT {select} FROM {t1} FULL OUTER JOIN {t2} ON {t1}.{c1} = {t2}.{c2}{where_str}"
            return self._execute(query, params)
        # MySQL эмуляция
        q_left = f"SELECT {select} FROM {t1} LEFT JOIN {t2} ON {t1}.{c1} = {t2}.{c2}{where_str}"
        q_right = f"SELECT {select} FROM {t1} RIGHT JOIN {t2} ON {t1}.{c1} = {t2}.{c2}{where_str}"
        return self._execute(f"{q_left} UNION {q_right}", params + params)

    # --- UNIONS ---
    def union(self, t1, t2, select="*", filters=None):
        where_str, params = self._build_where(filters)
        query = f"SELECT {select} FROM {t1}{where_str} UNION SELECT {select} FROM {t2}{where_str}"
        return self._execute(query, params + params)

    # --- CSV ---
    def export_csv(self, table, path):
        data = self.read(table)
        if data:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)

    def import_csv(self, table, path):
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.create(table, row)

db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'pass',
    'database': 'lab_db',
    'port': 3306
}

db = UniversalSQLAutomation('mysql', db_config)
# Соединяем Employees и Departments по полю department_id (inner join)
employees_with_dept = db.inner_join(
    t1="Employees",
    t2="Departments",
    c1="department_id", # Поле в Employees
    c2="department_id", # Поле в Departments (теперь правильно)
    select="Employees.first_name, Employees.last_name, Departments.dept_name"
)

for emp in employees_with_dept:
    print(f"{emp['first_name']} {emp['last_name']} — {emp['dept_name']}")


