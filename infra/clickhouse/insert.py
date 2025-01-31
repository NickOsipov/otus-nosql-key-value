"""
Setup ClickHouse database with randomly generated users using Faker.
"""

from datetime import datetime, timedelta
from random import randint

from faker import Faker
from sqlalchemy import create_engine, text


# Инициализация Faker
fake = Faker()
# Добавляем несколько локалей для разнообразия
Faker.seed(0)  # Для воспроизводимости результатов
fake = Faker(['en_US'])

engine = create_engine("clickhouse+native://default@localhost:9099/default")

def generate_random_user(user_id):
    """
    Генерация случайного пользователя с помощью Faker
    """
    # Случайно выбираем тип профиля: простой или с компанией
    is_company_profile = fake.boolean(chance_of_getting_true=30)
    
    if is_company_profile:
        # Профиль сотрудника компании
        company = fake.company()
        name = fake.name()
        email = fake.company_email()
    else:
        # Обычный профиль
        name = fake.name()
        email = fake.email()
    
    # Генерация даты регистрации за последние 2 дня
    days_ago = randint(0, 2)
    registration_date = (datetime.now() - timedelta(days=days_ago))
    
    # Добавляем случайное время
    registration_date = registration_date.replace(
        hour=randint(0, 23),
        minute=randint(0, 59),
        second=randint(0, 59)
    )
    
    return {
        'user_id': user_id,
        'name': name,
        'email': email,
        'registration_date': registration_date.strftime('%Y-%m-%d %H:%M:%S')
    }

def generate_insert_query(users):
    """
    Генерация INSERT запроса для списка пользователей
    """
    values = []
    for user in users:
        values.append(
            f"({user['user_id']}, '{user['name']}', '{user['email']}', "
            f"'{user['registration_date']}')"
        )
    
    return f"""
    INSERT INTO default.users_info 
    (user_id, name, email, registration_date) 
    VALUES {','.join(values)};
    """

# SQL запросы
drop_table = """
DROP TABLE IF EXISTS users_info;
"""

create_table = """
CREATE TABLE IF NOT EXISTS users_info
(
    user_id UInt32,
    name String,
    email String,
    registration_date DateTime
) ENGINE = MergeTree()
ORDER BY (registration_date, user_id);
"""

verify_query = """
SELECT 
    user_id,
    name,
    email,
    registration_date
FROM users_info
ORDER BY registration_date DESC;
"""

statistics_query = """
SELECT
    toDate(registration_date) as date,
    count() as users_count,
    uniqExact(email) as unique_emails
FROM users_info
GROUP BY date
ORDER BY date DESC;
"""

if __name__ == "__main__":
    # Количество пользователей для генерации
    num_users = int(input("Введите количество пользователей для генерации: "))
    
    print(f"\nГенерация {num_users} случайных пользователей...")
    
    # Генерация пользователей
    users = [generate_random_user(i+1) for i in range(num_users)]
    
    # Формирование запроса для вставки
    insert_data = generate_insert_query(users)
    
    # Выполнение запросов
    with engine.connect() as conn:
        print("\nСоздание таблицы...")
        conn.execute(text(drop_table))
        conn.execute(text(create_table))
        
        print("Вставка данных...")
        conn.execute(text(insert_data))
        
        print("\nСтатистика по дням:")
        print("-" * 80)
        result = conn.execute(text(statistics_query))
        for row in result:
            print(f"Дата: {row.date}")
            print(f"Количество регистраций: {row.users_count}")
            print(f"Уникальных email: {row.unique_emails}")
            print("-" * 40)
        