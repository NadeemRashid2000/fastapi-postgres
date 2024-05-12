"""
Use: https://jsonplaceholder.typicode.com/

Do: 
  - read and store the first 100 posts using the API
  - read and store teh first 10 users using the API
  - read and store the first 10 comments for each post

  - store the above data in a Postgres DB
  - write a FastAPI to read these from the DB
    - fetch posts
    - fetch users
    - fetch comments
    - create a post   
    - create a comment
    - create a user
    - delete a post
    - delete a comment
    - delele a user

  - deleting user should delete all user data

Reading:
 - REST APIs
 - RESTFUL API design
 - HTTP verbs and Rest design

Tools:
 - Request
 - FastApi
 - Postgres
 - anything else

"""


from fastapi import FastAPI
import psycopg2
import requests
from contextlib import asynccontextmanager
import json

app = FastAPI()

url = "https://jsonplaceholder.typicode.com"

database = {
    "dbname": "api-learning",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": 5432,
}


async def lifespan(app: FastAPI):
    yield

def fetch_posts():
    response = requests.get(f"{url}/posts?_limit=100")
    return response.json()


def fetch_users():
    response = requests.get(f"{url}/users?_limit=10")
    return response.json()


def fetch_comments(post_id):
    response = requests.get(f"{url}/comments?postId={post_id}&_limit=10")
    return response.json()


def create_table():
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS posts(
        id SERIAL PRIMARY KEY,
        post_id INT,
        user_id INT,
        title TEXT,
        body TEXT
        );

        CREATE TABLE IF NOT EXISTS users(
        id SERIAL PRIMARY KEY,
        user_id INT,
        name TEXT,
        username TEXT,
        email TEXT,
        address JSONB,
        phone TEXT,
        website TEXT,
        company JSONB
        
        );


        CREATE TABLE IF NOT EXISTS comments(
        id SERIAL PRIMARY KEY,
        post_id INT,
        name TEXT,
        email TEXT, 
        body TEXT
    
        );


        """
        cursor.execute(create_table_query)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error in creating tables: {e}")
    finally:
        cursor.close()
        conn.close()


def store_data_in_database():
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()

        posts = fetch_posts()
        for post in posts:
            cursor.execute(
                "INSERT INTO posts (post_id, user_id, title, body) VALUES (%s, %s, %s, %s)",
                (post["id"], post["userId"], post["title"], post["body"]),
            )
            comments = fetch_comments(post["id"])
            for comment in comments:
                cursor.execute(
                    "INSERT INTO comments(post_id, name, email, body) VALUES (%s, %s, %s, %s)",
                    (post["id"], comment["name"], comment["email"], comment["body"]),
                )

        users = fetch_users()
        for user in users:
            cursor.execute(
                "INSERT INTO users (user_id, name, username, email, address, phone, website, company) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (user["id"], 
                 user["name"],
                 user["username"],
                 user["email"],
                 json.dumps(user['address']),
                 user['phone'],
                 user['website'], 
                 json.dumps(user['company'])
                ,
            )
            )
        conn.commit()
    except psycopg2.Error as e:
        print(f"Data Storing Error In Database: {e}")
        cursor.close()
        conn.close()

async def startup_event():
    create_table()
    store_data_in_database()
    # posts = fetch_posts()
    # print("posts:")
    # for post in posts:
    #     print(post)


app.add_event_handler("startup", startup_event)


@app.get("/")
async def root():
    return {"message": "Welcome to the API."}


@app.get("/posts")
def fetch_posts_from_database():
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM posts")
        posts = cursor.fetchall()


        
        posts_list = []
        for post in posts:
            post_dict = {
                "id": post[0],
                "post_id": post[1],
                "user_id": post[2],
                "title": post[3],
                "body": post[4],
            }
            posts_list.append(post_dict)

        return {"posts": posts_list}
    except psycopg2.Error as e:
        print(f"Posts fetching Error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/users")
def fetch_users_from_databse():
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
        

        users_list = []
        for user in users:
            user_dict = {
                "id": user[0],
                "user_id": user[1],
                "name": user[2],
                "username": user[3],
                "email": user[5],
                "address": user[6],
                "phone": user[7],
                "website": user[8],
                "company": user[9],
                
            }
            users_list.append(user_dict)
        return {"Users": users_list}
    except psycopg2.Error as e:
        print(f"Users fetching Error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/comments/{post_id}")
def fetch_comments_from_databse(post_id: int):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM comments WHERE post_id = %s", (post_id,))
        comments = cursor.fetchall()


        comments_list = []
        for comment in comments:
            comment_dict = {
                "id": comment[0],
                "post_id": comment[1],
                "name": comment[2],
                "email": comment[3],
                "body": comment[4],
            }
            comments_list.append(comment_dict)
        return {"comments": comments_list}
    except psycopg2.Error as e:
        print(f"comments fetching Error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.post("/create_post")
def create_post(post_id : int, user_id:int, title:str, body:str):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO posts(post_id, user_id, title, body) VALUES(%s, %s, %s, %s)", (post_id, user_id, title, body))
        conn.commit()
        return{"message": "Post Created Successfully."}
    except psycopg2.Error as e:
        print(f"Error in creating post: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.post("/create_user")
def create_user(user_id:int, name:str, user_name:str, email:str, address : dict, phone:str, website:str, company:dict ):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()

        address_json = json.dumps(address)
        company_json = json.dumps(company)
        cursor.execute(
            "INSERT INTO users (user_id, name, username, email, address, phone, website, company) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (user_id,name, user_name, email, address_json, phone, website, company_json,
            ),
        )
        conn.commit()
        return {"message": "User Created Successflly.."}
    except psycopg2.Error as e:
        print(f"Error in creating User: {str(e)}")
    finally:
        cursor.close() 
        conn.close()


@app.post("/create_comment")
def create_user(post_id: int, name: str, email: str, body: str):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO comments (post_id, name, email, body) VALUES(%s, %s, %s, %s)",
            (post_id, name, email, body),
        )
        conn.commit()
        return {"message": "Comment Created Successflly.."}
    except psycopg2.Error as e:
        print(f"Error in creating User: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.delete("/delete_post/{post_id}")
def delete_post(post_id: int):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM posts WHERE post_id = %s", (post_id,))
        conn.commit()
        return {"message":" Post deleted successfully" }
    except psycopg2.Error as e:
        print(f"Error in deleting post: {e}")
    finally:
            cursor.close()
            conn.close()


@app.delete("/delete_comment/{comment_id}")
def delete_comment(comment_id: int):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM comments WHERE id = %s", (comment_id,)) 
        conn.commit()
        return {"message": "Commnent Deleted Sucessfully"}
    except psycopg2.Error as e:
        print(f"Error in deleting Comment: {e}")
    finally:
            cursor.close()
            conn.close()


@app.delete("/delete_user/{user_id}")
def delete_user(user_id: int):
    try:
        conn = psycopg2.connect(**database)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
        cursor.execute("DELETE FROM posts WHERE user_id = %s", (user_id,))
        cursor.execute("DELETE FROM comments WHERE post_id IN (SELECT post_id FROM posts WHERE user_id = %s)", (user_id,))
        conn.commit()
        return {"message": "User Deleted Sucessfully"}
    except psycopg2.Error as e:
        print(f"Error in deleting post: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn
    import requests
    uvicorn.run(app, host="127.0.0.1", port=8000)

 
