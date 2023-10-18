import psycopg2
import requests
from minio import Minio
from dotenv import load_dotenv

import io
import os

load_dotenv()

# this function create table in PostgreSQL to store the name and link of images
def creating_table():

    connection = psycopg2.connect(
        host='localhost',
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port=5432
    )

    cursor = connection.cursor()

    query = """CREATE TABLE IF NOT EXISTS images(
               id SERIAL PRIMARY KEY NOT NULL,
               name VARCHAR(50),
               link VARCHAR(100)
               )"""

    cursor.execute(query)
    connection.commit()

    connection.close()

# this function save the name and link of your image in PostgreSQL
def save_to_db(url: str, name: str):
    
    connection = psycopg2.connect(
        host='localhost',
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port=5432
    )

    cursor = connection.cursor()

    query = f"INSERT INTO images(name, link) VALUES('{name}', '{url}')"
    cursor.execute(query)

    connection.commit()
    connection.close()

# this function get all names of images from PostgreSQL
def get_all_names():

    connection = psycopg2.connect(
        host='localhost',
        port=5432,
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    
    cursor = connection.cursor()

    query = "SELECT name FROM images"
    cursor.execute(query)

    lst = cursor.fetchall()
    result = [name[0] for name in lst]
    return result

# this function get image url for loading it into MinIO
def get_image_url(query: str, orientation: str, size: str):

    API_KEY = os.getenv('API_KEY')

    # API that's used
    url = "https://api.pexels.com/v1/search"
    
    headers = {'Authorization': API_KEY}

    params={
        'query': query,
        'orientation': orientation,
        'size': size,
        'page': 1,
        'per_page': 1
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        info = response.json()
        url = info['photos'][0]['src']['original']
        return url


"""def create_image(url: str, data=None, *, image_name="downloaded_image.jpeg", type_loading="url"):
    
    if type_loading == 'url':
        response = requests.get(url)
        image_data = response.content

        with open(image_name, "wb") as image_file:
            image_file.write(image_data)

    elif type_loading == 'data':
        
        with open(image_name, "wb") as image_file:
            image_file.write(data)"""

# this function load image in MinIO
def load_to_minio(url: str, obj_name: str):

    ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

    client = Minio(
        endpoint='localhost:9000',
        secret_key=SECRET_KEY,
        access_key=ACCESS_KEY,
        secure=False
    )

    response = requests.get(url)
    image = io.BytesIO(response.content)
    length = len(response.content)

    found = client.bucket_exists('images')

    if not found:
        client.make_bucket('images')

    client.put_object(
        bucket_name='images',
        object_name=obj_name,
        data=image,
        length=-1,
        part_size=length
    )

# this function load image from MinIO
def load_from_minio(obj_name: str, fileName='image.jpeg'):
    
    ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

    client = Minio(
        endpoint='localhost:9000',
        secret_key=SECRET_KEY,
        access_key=ACCESS_KEY,
        secure=False
    )

    client.fget_object('images', obj_name, fileName)

query = 'Here you write query for image API'
size = 'Here you write size of your image'
orientation = 'Here you write orientation for your image'
name = 'Here you write name of your image'

url = get_image_url(query=query, size=size, orientation=orientation)
load_to_minio(url=url, obj_name=name)
save_to_db(name=name, url=url)
load_from_minio(name)