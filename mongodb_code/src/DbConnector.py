from pymongo import MongoClient


class DbConnector:
    """
    Connects to the MongoDB server. Default configuration is for a local instance on `localhost`.
    The connection can be customized by providing DATABASE, HOST, USER, and PASSWORD parameters.
    """

    def __init__(
        self,
        DATABASE="exc3db",
        HOST="localhost:27017",
        USER=None,
        PASSWORD=None,
        use_authentication=False,
    ):
        # Determine URI based on authentication requirement
        if use_authentication and USER and PASSWORD:
            uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DATABASE}"
        else:
            uri = (
                f"mongodb://{HOST}/{DATABASE}"  # Use simpler URI for local connections
            )

        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
            print("You are connected to the database:", self.db.name)
            print("-----------------------------------------------\n")
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

    def close_connection(self):
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
