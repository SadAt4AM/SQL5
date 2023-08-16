import psycopg2
import json

def print_bd(bd):
    for i in bd:
        print(f'name: {i[0]}, surname: {i[1]}, email: {i[2]}, phone: {i[3]}')
#Без with
def make_base(cur):
    cur.execute('''  
    CREATE TABLE IF NOT EXISTS users(
    id SERIAL PRIMARY KEY,
    name_u VARCHAR(30) NOT NULL,
    surname_u VARCHAR(30) NOT NULL,
    email VARCHAR(30) NOT NULL UNIQUE
    );                  
    ''')
    conn.commit()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS phones(
    id SERIAL PRIMARY KEY,
    u_phone BIGINT UNIQUE,
    user_id INTEGER REFERENCES users(id)
    );        
    ''')
    conn.commit()
#новый юзер
def add_user(cur, name, surname, email, phone):
    cur.execute('''
    INSERT INTO users(name_u, surname_u, email)
    VALUES (%s, %s, %s) RETURNING id;
    ''', (name, surname, email))
    id = cur.fetchone()
    if len(phone) > 0:
        for i in phone:
            cur.execute('''
            INSERT INTO phones(u_phone, user_id)
            VALUES (%s, %s);            
            ''', (i, id[0]))
            conn.commit()
    conn.commit()
#добавление номера существующему пользователю (по имени и id)
def add_number_by_name (cur, name, phone):
    try:
        cur.execute('''
        SELECT id FROM users
        WHERE name_u LIKE %s;
        ''', (name, ))
        name_id = cur.fetchall()[0][0]
        cur.execute('''
            INSERT INTO phones(u_phone, user_id)
            VALUES (%s, %s);            
            ''', (phone, name_id))
        conn.commit()
    except IndexError:
        print('Данный пользователь не найден')
def add_number (cur, id, phone):
    cur.execute('''
        INSERT INTO phones(u_phone, user_id)
        VALUES (%s, %s);            
        ''', (phone, id))
    conn.commit()
#Смена пользователя
def alt_user (cur, id, name_u = None, surname_u = None, email = None, phone = None, old_phone = None):
    if name_u != None:
        cur.execute('''
            UPDATE users SET name_u = %s WHERE id = %s;            
            ''', (name_u, id))
        conn.commit()
    if surname_u != None:
        cur.execute('''
            UPDATE users SET surname_u = %s WHERE id = %s;            
            ''', (surname_u, id))
        conn.commit()
    if email != None:
        cur.execute('''
            UPDATE users SET email = %s WHERE id = %s;            
            ''', (email, id))
        conn.commit()
    if phone != None and id and old_phone != None:
        cur.execute('''
        SELECT id FROM phones
        WHERE user_id = %s and u_phone = %s;
        ''', (id, old_phone))
        phone_id = cur.fetchall()[0][0]
        cur.execute('''
            UPDATE phones SET u_phone = %s WHERE id = %s;            
            ''', (phone, phone_id))
        conn.commit()
#Удаление номера (по id)
def delete_phone(cur, id, phone):
    cur.execute('''
    SELECT id FROM phones
    WHERE user_id = %s and u_phone = %s;
    ''', (id, phone))
    phone_id = cur.fetchall()[0][0]
    cur.execute('''
        DELETE FROM phones WHERE id = %s;            
        ''', (phone_id, ))
    conn.commit()
#Удаление пользователя (по id)
def delete_user(cur, id):
    cur.execute('''
    SELECT id FROM phones
    WHERE user_id = %s;
    ''', (id,))
    phone_id = cur.fetchall()
    for i in phone_id:
        cur.execute('''
            DELETE FROM phones WHERE id = %s;
            ''', (i[0],))
        conn.commit()
    cur.execute('''
                DELETE FROM users WHERE id = %s;
                ''', (id,))
    conn.commit()
#Поиск
def find_out(cur, type, data):
    if type == 'name':
        cur.execute('''
        SELECT name_u, surname_u, email, u_phone FROM users AS u
        LEFT JOIN phones AS p ON u.id = p.user_id
        WHERE name_u = %s;
        '''
        , (data,))
        print_bd(cur.fetchall())
    elif type == 'surname':
        cur.execute('''
        SELECT name_u, surname_u, email, u_phone FROM users AS u
        LEFT JOIN phones AS p ON u.id = p.user_id
        WHERE surname_u = %s;
        '''
        , (data,))
        print_bd(cur.fetchall())
    elif type == 'email':
        cur.execute('''
        SELECT name_u, surname_u, email, u_phone FROM users AS u
        LEFT JOIN phones AS p ON u.id = p.user_id
        WHERE email = %s;
        '''
        , (data,))
        print_bd(cur.fetchall())
    elif type == 'phone':
        cur.execute('''
        SELECT name_u, surname_u, email, u_phone FROM users AS u
        LEFT JOIN phones AS p ON u.id = p.user_id
        WHERE u_phone = %s;
        '''
        , (data,))
        print_bd(cur.fetchall())

def terminate(cur):
    cur.execute('''
    DROP TABLE phones CASCADE;
    DROP TABLE users CASCADE;
    ''')

if __name__ == '__main__':
    with open('db.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
#Делаем заранее, чтобы не заполнять тут

with psycopg2.connect(database="psytask", user="postgres", password="111") as conn:  # подключение к базе
    with conn.cursor() as cur:
        terminate(cur) #Для проверки повторного запуска
        make_base(cur)
        for i in data:
             add_user(cur, name = i.get('name'), surname = i.get('surname'), email = i.get('email'), phone = i.get('phone'))
        add_number(cur, '1', '88053535')
        alt_user(cur, id = '1', name_u = 'Test', surname_u = 'Test2', email = 'Test3')
        alt_user(cur, id = 4, phone='555555', old_phone='895424658')
        delete_phone(cur, id = 3, phone='895424656')
        delete_user(cur, id = '3')
        find_out(cur, type = 'phone', data = '123556754')
    conn.commit() #Лучше конечно поменять на close