import sqlite3

conn = sqlite3.connect("mydatabase_Stoit_Kompani.db")  # или :memory: чтобы сохранить в RAM
cursor = conn.cursor()

# Создание таблицы
cursor.execute("""CREATE TABLE pars
                  (name text, mail text, city text, number text, hrefs text, description text)
               """)
