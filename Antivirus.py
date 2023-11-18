import psycopg2
from psycopg2 import Error
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

class CustomEventHandler(FileSystemEventHandler):
    def __init__(self, connection):
        self.connection = connection

    def on_any_event(self, event):
        event_type = event.event_type
        file_path = event.src_path
        event_message = f"{event_type}: {file_path}"
        print(f"Processing event: {event_message}")
        insert_event_into_database(self.connection, event_type, file_path)
def connect_to_database():
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="events",
            user="postgres",
            password="test123"
        )
        connection.autocommit = True
        print("Connected to the database")
        return connection
    except Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def insert_event_into_database(connection, event_type, file_path):
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO events (event_type, file_path) VALUES (%s, %s)", (event_type, file_path))
        connection.commit()
        print("Event recorded in the database")
    except Error as e:
        print(f"Error inserting event into the database: {e}")
    finally:
        cursor.close()


if __name__ == "__main__":
    connection = connect_to_database()

    if connection:

        path = '/home/vboxuser/testdir'
        #path = '/home'

        event_handler = CustomEventHandler(connection)

        observer = Observer()
        observer.schedule(event_handler, path, recursive=True)

        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()