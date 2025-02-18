import psycopg2
from Collect_Data_User import Transform_Data_User

DB_CONFIG = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

def CreateTable(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
       id_user SERIAL PRIMARY KEY,
       first_name VARCHAR(255),
       last_name VARCHAR(255),
       gender VARCHAR(255),
       street VARCHAR(255),
       city VARCHAR(255),
       country VARCHAR(255),
       postcode VARCHAR(255),
       latitude FLOAT,
       longitude FLOAT,
       email VARCHAR(255),
       phone VARCHAR(255),
       username VARCHAR(255),
       password_hash VARCHAR(255),
       date_registered VARCHAR(255)
       );
    """)
    print("Create table user successfully!!!")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS news (
       id_news SERIAL PRIMARY KEY,
       id_user INT REFERENCES users(id_user),
       author VARCHAR(255),
       title VARCHAR(255),
       description TEXT,
       url VARCHAR(255),
       urlToImage VARCHAR(255),
       date_published VARCHAR(255),
       content TEXT
       );
    """)
    print("Create table news successfully!!!")

def LoadData(cur):
    df = Transform_Data_User()
    for _, user in df.iterrows():
        first_name = user['first_name']
        last_name = user['last_name']
        gender = user['gender']
        street = user['street']
        city = user['city']
        country = user['country']
        postcode = user['postcode']
        latitude = user['latitude']
        longitude = user['longitude']
        email = user['email']
        phone = user['phone']
        username = user['username']
        password_hash = user['password_hash']
        date_registered = user['date_registered']
        author = user['author']
        title = user['title']
        description = user['description']
        url = user['url']
        urlToImage = user['urlToImage']
        date_published = user['date_published']
        content = user['content']

        cur.execute("""
                   INSERT INTO users(first_name, last_name, gender, street, city, country, postcode, latitude, longitude, email, phone, username, password_hash, date_registered)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id_user;
                   """, (
            first_name, last_name, gender, street, city, country, postcode, latitude, longitude, email, phone, username, password_hash, date_registered))
        print("Insert to table user successfully!!!!")

        id_user = cur.fetchone()[0]
        cur.execute("""
                   INSERT INTO news(id_user, author, title, description, url, urlToImage, date_published, content)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   """, (id_user, author, title, description, url, urlToImage, date_published, content))
        print("Insert to table news successfully!!!!")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connect to postgres successfully!!!")
        cur = conn.cursor()
        CreateTable(cur)
        LoadData(cur)
        conn.commit()
        print("Load data to postgres successfully!!!")
    except Exception as e:
        print(f"Fail! Please check again: {e}")
    finally:
        cur.close()
        conn.close()

if __name__=="__main__":
    main()









