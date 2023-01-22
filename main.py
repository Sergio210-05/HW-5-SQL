import psycopg2
import ast


def delete_tables(cur, table_clients, table_emails, table_phones):
    cur.execute(f"""
    DROP TABLE {table_phones};
    DROP TABLE {table_emails};
    DROP TABLE {table_clients};
    """)


def create_tables(cur, table_clients, table_emails, table_phones):
    delete_tables(cur, table_clients, table_emails, table_phones)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_clients} (
        PRIMARY KEY (client_id),
        client_id   SERIAL,
        first_name  VARCHAR(30)     NOT NULL,
        last_name   VARCHAR(30)     NOT NULL
    );
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_emails} (
        PRIMARY KEY (email_id),
        email_id    SERIAL,
        client_id   INTEGER             NOT NULL    REFERENCES clients(client_id),
        email      VARCHAR(30) UNIQUE   NOT NULL
    );
    """)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_phones} (
        PRIMARY KEY (phone_id),
        phone_id     SERIAL,
        client_id    INTEGER              NOT NULL  REFERENCES clients(client_id),
        phone_number VARCHAR(15)  UNIQUE  NOT NULL
    );
    """)


def new_client(cur, first_name, last_name, table_clients='clients'):
    cur.execute(f"""
    INSERT INTO {table_clients} (first_name, last_name)
    VALUES  (%s, %s);
    """, (first_name, last_name)
    )


def find_id(cur, first_name, last_name, table_clients='clients'):
    cur.execute(f"""
    SELECT client_id 
      FROM {table_clients}
     WHERE first_name = %s AND last_name = %s
    """, (first_name, last_name))
    return cur.fetchone()[0]


def add_phone(cur, phone_number, first_name=None, last_name=None,
              client_id=None, table_clients='clients', table_phones='phone_book'):
    """Function to add phone number to client.
    May use first_name, last_name to find person (client_id is None)
    OR client_id (first_name, last_name are None)"""
    if client_id is None:
        client_id = find_id(cur, first_name, last_name, table_clients)

    cur.execute(f"""
    INSERT INTO {table_phones} (client_id, phone_number)
    VALUES  (%s, %s);
    """, (client_id, str(phone_number))
    )


def add_email(cur, email, first_name=None, last_name=None,
              client_id=None, table_clients='clients', table_emails='email_adresses'):
    """Function to add e-mail to client.
    May use first_name, last_name to find person (client_id is None)
    OR client_id (first_name, last_name are None)"""
    if client_id is None:
        client_id = find_id(cur, first_name, last_name, table_clients)

    cur.execute(f"""
    INSERT INTO {table_emails} (client_id, email)
    VALUES  (%s, %s);
    """, (client_id, email)
    )


def change_data(cur, first_name, last_name, changing_field,  new_value, old_value=None,
                table_clients='clients', table_emails='email_adresses', table_phones='phone_book'):
    client_id = find_id(cur, first_name, last_name, table_clients)
    field_table = {
        'email': table_emails,
        'phone_number': table_phones,
        'first_name': table_clients,
        'last_name': table_clients
    }
    table = field_table[changing_field]
    if changing_field == 'first_name':
        old_value = first_name
    elif changing_field == 'last_name':
        old_value = last_name

    def change_name(table=table, changing_field=changing_field,
                    new_value=new_value, client_id=client_id):
        cur.execute(f"""
        UPDATE {table}
           SET {changing_field} = %s
         WHERE client_id = %s AND {changing_field} = %s;
        """, (new_value, client_id, old_value)
        )
    change_name()


def delete_phone(cur, first_name, last_name, phone_number,
                 table_phones='phone_book', table_clients='clients', client_id=None):
    if client_id is None:
        client_id = find_id(cur, first_name, last_name, table_clients)
    cur.execute(f"""
    DELETE FROM {table_phones}
    WHERE client_id = %s AND phone_number = %s;
    """, (client_id, phone_number))


def delete_client(cur, first_name, last_name, client_id=None,
                  table_phones='phone_book', table_emails='email_adresses', table_clients='clients'):
    if client_id is None:
        client_id = find_id(cur, first_name, last_name, table_clients)
    cur.execute(f"""
    DELETE FROM {table_phones}
    WHERE client_id = %s;
    
    DELETE FROM {table_emails}
    WHERE client_id = %s;
    
    DELETE FROM {table_clients}
    WHERE client_id = %s;
    """, (client_id, client_id, client_id))


def find_client(cur, first_name=None, last_name=None, email=None, phone_number=None, client_id=None,
                table_phones='phone_book', table_emails='email_adresses', table_clients='clients'):
    if first_name is not None and last_name is not None:
        client_id = find_id(cur, first_name, last_name, table_clients)
    select_from = """
    SELECT c.client_id, c.first_name, c.last_name, ea.email, pb.phone_number
      FROM clients c
           FULL JOIN email_adresses ea ON c.client_id = ea.client_id
           FULL JOIN phone_book pb ON c.client_id = pb.client_id"""
    group_by = """  GROUP BY c.client_id, c.first_name, c.last_name, ea.email, pb.phone_number;"""
    if client_id is not None:
        cur.execute(select_from + """
        WHERE c.client_id = %s
        """ + group_by, (client_id,))
    else:
        cur.execute(select_from + """
         WHERE c.first_name = %s
            OR c.last_name = %s
            OR ea.email = %s
            OR pb.phone_number = %s
        """ + group_by, (first_name, last_name, email, str(phone_number)))
    print(cur.fetchall())
    return cur.fetchall()


with open('authentication.txt', 'rt', encoding='utf8') as au:
    login_pass = ast.literal_eval(au.read())

conn = psycopg2.connect(database='clients_db', user=login_pass['login'], password=login_pass['password'])
table_clients = 'clients'
table_emails = 'email_adresses'
table_phones = 'phone_book'

with conn.cursor() as cur:
    create_tables(cur=cur, table_clients=table_clients, table_emails=table_emails, table_phones=table_phones)
    conn.commit()

    users = (('Will', 'Smith'), ('Brad', 'Pitt'), ('Angelina', 'Jolie'), ('Mark', 'Wahlberg'),
             ('Kristian', 'Bale'), ('Ben', 'Affleck'), ('Gal', 'Gadot'),
             ('Anne', 'Hathaway'), ('Emma', 'Watson'), ('Robert', 'Downey Jr.'))

    phone_numbers = ((1, 147852369), (2, 9851423658), (3, 852156743),
                     (5, 632102746), (6, 961459782), (7, 328740964),  (7, 967541285),
                     (8, 654147069), (10, 250364952), (10, 456258987), (10, 952367069))

    e_mails = ((1, 'Will@gmail.com'), (2, 'Brad@gmail.com'), (3, 'Angel@gmail.com'), (4, 'Mark@gmail.com'),
               (5, 'Kristian@gmail.com'), (6, 'Ben@gmail.com'), (7, 'GalG@gmail.com'),
               (8, 'Ann@gmail.com'), (9, 'Emma@gmail.com'), (10, 'Robert@gmail.com'))

    for user in users:
        new_client(cur=cur, first_name=user[0], last_name=user[1], table_clients=table_clients)
    for number in phone_numbers:
        add_phone(cur, phone_number=number[1], client_id=number[0],
                  table_clients=table_clients, table_phones=table_phones)

    add_phone(cur, phone_number='000000000', first_name='Robert', last_name='Downey Jr.',
              table_clients=table_clients, table_phones=table_phones)

    for em in e_mails:
        add_email(cur, em[1], client_id=em[0], table_clients=table_clients, table_emails=table_emails)

    conn.commit()
    #
    # change_data(cur=cur, first_name='Will', last_name='Smith',
    #             changing_field='first_name', new_value='William')
    # change_data(cur=cur, first_name='Robert', last_name='Downey Jr.',
    #             changing_field='email', new_value='IronMan@gmail.com', old_value='Robert@gmail.com')
    # change_data(cur=cur, first_name='Robert', last_name='Downey Jr.',
    #             changing_field='phone_number', new_value='112233445', old_value='000000000')
    # conn.commit()

    # delete_phone(cur=cur, first_name='Robert', last_name='Downey Jr.', phone_number='250364952')
    # conn.commit()

    # delete_client(cur=cur, first_name='Brad', last_name='Pitt')
    # conn.commit()

    # search_first_last = find_client(cur=cur, first_name='Angelina', last_name='Jolie')
    # print(search_first_last)

    # new_client(cur=cur, first_name='Emma', last_name='Stone', table_clients=table_clients)
    # conn.commit()

    # for num in phone_numbers:
    #     search_phone = find_client(cur=cur, phone_number=num[1])
    #     print(search_phone)

    # for em in e_mails:
    #     search_email = find_client(cur=cur, email=em[1])
    #     print(search_email)

    commands = {
        'ct': create_tables,
        'ac': new_client,
        'ap': add_phone,
        'cd': change_data,
        'dp': delete_phone,
        'dc': delete_client,
        'fc': find_client,
        'e': 'exit'
    }

    while True:
        com = input("""
        Input one of several combination if symbols:
        'ct' - A function that creates a database structure (tables)
        'ac' - A function that allows you to add a new client
        'ap' - A feature that allows you to add a phone for an existing client
        'cd' - A function that allows you to change customer data
        'dp' - A feature that allows you to delete a phone for an existing client
        'dc' - A function that allows you to delete an existing client
        'fc' - A function that allows you to find a client by his data (first name, last name, email or phone)
        'e' - Escape from this program
        """)
        if com == 'e':
            break
        else:
            if com == 'ct':
                args = [cur, table_clients, table_emails, table_phones]
            elif com == 'ac':
                args = [cur, input('Input first_name: '), input('Input last_name: '), table_clients]
            elif com == 'ap':
                args = [cur, input('Input phone number: '), input('Input first name: '), input('Input last name: '),
                        None, table_clients, table_phones]
            elif com == 'cd':
                args = [cur, input('Input first name: '), input('Input last name: '),
                        input('Input changing_field (first_name/last_name/email/phone_number): '),
                        input('Input new_value: '), input('Input old_value: ')]
            elif com == 'dp':
                args = [cur, input('Input first name: '), input('Input last name: '),
                        input('Input number to delete: ')]
            elif com == 'dc':
                args = [cur, input('Input first name: '), input('Input last name: '),
                        None, table_phones, table_emails, table_clients]
            elif com == 'fc':
                args = [cur, None, None, None, None,
                        None, table_phones, table_emails, table_clients]
                search_arg = input('Input searching argument (first_name/last_name/email/phone_number): ')
                if search_arg == 'first_name':
                    args[1] = input('Input first name: ')
                elif search_arg == 'last_name':
                    args[2] = input('Input last name: ')
                elif search_arg == 'email':
                    args[3] = input('Input email: ')
                elif search_arg == 'phone_number':
                    args[4] = input('Input phone number: ')
            run = commands[com](*args)
            conn.commit()

conn.close()
