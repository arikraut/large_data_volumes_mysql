import mysql.connector as mysql
import os


class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.
    """

    def __init__(
        self,
        PASSWORD="oppgave2",
        HOST="localhost",
        DATABASE="exc2db",
        USER="local_host_user",
    ):
        try:
            self.db_connection = mysql.connect(
                password=PASSWORD, host=HOST, database=DATABASE, user=USER, port=3306
            )
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        self.cursor.close()
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.db_connection.get_server_info())
