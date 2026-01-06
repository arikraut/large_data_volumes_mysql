from DbConnector import DbConnector
from tabulate import tabulate
from mysql.connector import IntegrityError
import logging
import os
from dataHelper import DataHelper as dh
from datetime import datetime, timedelta
from haversine import haversine, Unit

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

debug_handler = logging.FileHandler("debug_log.log")
debug_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

debug_handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(debug_handler)


class DatabaseManager:

    def __init__(self):
        """
        Initializes the database connection and sets up the cursor.
        """
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.connection.autocommit = False

    def close_connection(self):
        """
        Closes the database connection.
        """
        if self.db_connection:
            self.cursor.close()
            self.db_connection.close()
            logging.info("Database connection closed.")

    def create_tables(self):
        """
        Creates the necessary tables in the database if they do not already exist:
        - User table
        - Activity table
        - TrackPoint table
        """
        create_user_table = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(255) PRIMARY KEY,
            has_labels BOOLEAN
        );
        """
        self.cursor.execute(create_user_table)

        create_activity_table = """
        CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(255),
            transportation_mode VARCHAR(255) DEFAULT NULL,
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE,
            UNIQUE(user_id, start_date_time)
        );
        """
        self.cursor.execute(create_activity_table)

        create_trackpoint_table = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT AUTO_INCREMENT PRIMARY KEY,
            activity_id INT,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_time DATETIME,
            FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE,
            UNIQUE(activity_id, date_time)

        );
        """
        self.cursor.execute(create_trackpoint_table)

        self.db_connection.commit()

    def show_tables(self):
        """
        Prints the names of all tables in the database.
        """
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        logging.info("Tables displayed.")

    def create_user(self, user_id, has_labels):
        try:
            self.cursor.execute(
                "INSERT INTO User (id, has_labels) VALUES (%s, %s)",
                (user_id, has_labels),
            )
            self.db_connection.commit()
            logging.info(f"User {user_id} inserted successfully.")
        except IntegrityError as e:
            if "Duplicate entry" in str(e) or "UNIQUE constraint failed" in str(e):
                logging.warning(f"User {user_id} already exists, skipping insertion.")
            else:
                logging.error(f"Error inserting user {user_id}: {str(e)}")
                self.db_connection.rollback()
        except Exception as e:
            logging.error(f"Unexpected error inserting user {user_id}: {str(e)}")
            self.db_connection.rollback()

    def insert_users(self, users):
        logging.info("Inserting users...")
        for user in users:
            self.create_user(user[0], user[1])
        logging.info("Users inserted.")

    def get_users(self):
        """
        Retrieves all users from the User table.

        Returns:
            list: A list of tuples where each tuple contains:
                - user_id (str): The user ID.
                - has_labels (bool): True if the user has labels, False otherwise.
        """
        self.cursor.execute("SELECT * FROM User")
        return self.cursor.fetchall()

    def insert_activity(
        self, user_id, transportation_mode, start_date_time, end_date_time
    ):
        try:
            insert_query = """
            INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
            VALUES (%s, %s, %s, %s)
            """
            self.cursor.execute(
                insert_query,
                (user_id, transportation_mode, start_date_time, end_date_time),
            )
            self.db_connection.commit()
            logger.info(
                f"Inserted activity for user {user_id} with transportation_mode '{transportation_mode}'"
            )
        except Exception as e:
            logger.error(f"Error inserting activity for user {user_id}: {str(e)}")
            self.db_connection.rollback()

    def insert_activities(self, dataset_path, user):
        user_id = user[0]
        has_labels = user[1]
        activity_folder = os.path.join(dataset_path, user_id, "Trajectory")

        label_lookup = {}
        if has_labels:
            label_file = os.path.join(dataset_path, user_id, "labels.txt")
            if os.path.exists(label_file):
                label_lookup = dh.build_label_lookup(label_file)
            else:
                logging.warning(f"Label file not found for user {user_id}.")

        for file in os.listdir(activity_folder):
            file_path = os.path.join(activity_folder, file)
            try:
                start_time, end_time = dh.get_start_end_time(file_path)
            except ValueError:
                logging.warning(f"Activity file {file_path} contains no valid data.")
                continue

            if has_labels and (start_time, end_time) in label_lookup:
                mode = label_lookup[(start_time, end_time)]
                self.insert_activity(user_id, mode, start_time, end_time)
            else:
                self.insert_activity(user_id, "NULL", start_time, end_time)

    def get_activity_for_user(self, user_id, columns="*"):
        """
        Retrieves specified columns from the Activity table for a given user.

        Args:
            user_id (str): The ID of the user.
            columns (str or list): The columns to retrieve from the Activity table.
                                Pass "*" for all columns, or a list of column names.

        Returns:
            list: A list of tuples where each tuple represents a row from the Activity table.
        """
        if isinstance(columns, list):
            columns = ", ".join(columns)

        query = f"SELECT {columns} FROM Activity WHERE user_id = %s"
        self.cursor.execute(query, (user_id,))
        return self.cursor.fetchall()

    def insert_trackpoints(self, dataset_path, user_id, activity_id, start_date_time):
        """
        Inserts all trackpoints for a specific activity by converting start_date_time into the filename.

        Args:
            dataset_path (str): The path to the dataset folder.
            user_id (str): The user ID.
            activity_id (int): The ID of the activity.
            start_date_time (datetime): The start date and time of the activity.
        """
        # Convert start_date_time to the format YYYYMMDDHHMMSS for the file name
        file_name = start_date_time.strftime("%Y%m%d%H%M%S") + ".plt"
        activity_folder = os.path.join(dataset_path, user_id, "Trajectory")
        file_path = os.path.join(activity_folder, file_name)

        try:
            # Extract trackpoints using DataHelper
            trackpoints = dh.extract_trackpoints(file_path, activity_id)

            # Perform bulk insert of trackpoints
            self.insert_trackpoints_bulk(trackpoints)

        except FileNotFoundError as e:
            logger.error(str(e))
        except Exception as e:
            logger.error(
                f"Error processing trackpoints for activity starting at {start_date_time}: {str(e)}"
            )

    def insert_trackpoints_bulk(self, trackpoints):
        """
        Inserts trackpoints in bulk into the TrackPoint table.

        Args:
            trackpoints (list): A list of tuples where each tuple contains:
                - activity_id (int)
                - lat (float)
                - lon (float)
                - altitude (int)
                - date_time (datetime)
        """
        try:
            insert_query = """
            INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_time)
            VALUES (%s, %s, %s, %s, %s)
            """
            self.cursor.executemany(insert_query, trackpoints)  # Bulk insert
            self.db_connection.commit()
            logger.info(
                f"Inserted {len(trackpoints)} trackpoints in bulk for activity {trackpoints[0][0]}"
            )
        except Exception as e:
            logger.error(
                f"Error in bulk insert: {str(e)}. Falling back to individual inserts."
            )
            self.db_connection.rollback()
            # Fallback to individual inserts for this batch
            self.insert_trackpoints_individual(trackpoints)

    def insert_trackpoints_individual(self, trackpoints):
        """
        Inserts trackpoints individually if batch insertion fails.

        Args:
            trackpoints (list): A list of tuples where each tuple contains:
                - activity_id (int)
                - lat (float)
                - lon (float)
                - altitude (int)
                - date_time (datetime)
        """
        insert_query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        successful_inserts = 0

        for trackpoint in trackpoints:
            try:
                self.cursor.execute(insert_query, trackpoint)
                successful_inserts += 1
            except Exception as e:
                logger.error(f"Error inserting trackpoint {trackpoint}: {str(e)}")

        self.db_connection.commit()
        logger.info(
            f"Inserted {successful_inserts}/{len(trackpoints)} trackpoints in individual mode."
        )

    def get_first_rows(self, table_name, limit=10):
        """
        Retrieves the first rows from a specified table.

        Args:
            table_name (str): The name of the table to query.
            limit (int): The number of rows to retrieve. Default is 10.

        Returns:
            list: A list of tuples representing the rows.
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT %s;"
            self.cursor.execute(query, (limit,))
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            logger.error(f"Error retrieving rows from {table_name}: {str(e)}")
            return []

    ################################################################################
    # The following methods are used to retrieve data from the database for Task 2.#
    ################################################################################

    def get_counts(self):
        """
        Retrieves the counts of users, activities, and trackpoints from the database.

        Returns:
            dict: A dictionary containing the counts with keys 'user_count', 'activity_count', 'trackpoint_count'.
        """
        counts = {}
        try:
            # Count users
            self.cursor.execute("SELECT COUNT(*) FROM User")
            counts["user_count"] = self.cursor.fetchone()[0]

            # Count activities
            self.cursor.execute("SELECT COUNT(*) FROM Activity")
            counts["activity_count"] = self.cursor.fetchone()[0]

            # Count trackpoints
            self.cursor.execute("SELECT COUNT(*) FROM TrackPoint")
            counts["trackpoint_count"] = self.cursor.fetchone()[0]

            logger.info(f"Counts retrieved: {counts}")

        except Exception as e:
            logger.error(f"Error retrieving counts: {str(e)}")
            counts = {
                "user_count": 0,
                "activity_count": 0,
                "trackpoint_count": 0,
            }

        table_data = [
            ["Users", counts["user_count"]],
            ["Activities", counts["activity_count"]],
            ["TrackPoints", counts["trackpoint_count"]],
        ]

        return table_data

    def get_average_activities_per_user(self):
        """
        Retrieves the average number of activities per user from the database.

        Returns:
            float: The average number of activities per user. Returns 0.0 if an error occurs.
        """
        query = """
        SELECT AVG(activity_count) AS avg_activities_per_user
        FROM (
            SELECT COUNT(*) AS activity_count
            FROM Activity
            GROUP BY user_id
        ) AS user_activity_counts;
        """
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()

            return result[0] if result and result[0] is not None else 0.0

        except Exception as e:
            logger.error(f"Error retrieving average activities per user: {str(e)}")
            return 0.0

    def get_top_users_by_activity(self, top_n=20):
        """
        Retrieves the top users with the highest number of activities.

        Args:
            top_n (int): The number of top users to retrieve. Default is 20.

        Returns:
            list: A list of tuples where each tuple contains:
                - user_id (str): The user ID.
                - activity_count (int): The number of activities for the user.
        """
        try:
            query = f"""
            SELECT user_id, COUNT(*) AS activity_count
            FROM Activity
            GROUP BY user_id
            ORDER BY activity_count DESC
            LIMIT {top_n};
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            return results

        except Exception as e:
            logger.error(f"Error retrieving top users by activity count: {str(e)}")
            return []

    def get_users_by_transport_mode(self, transport_mode="taxi"):
        """
        Retrieves all users who have used a specific transportation mode.

        Args:
            transport_mode (str): The transportation mode to filter by. Default is 'taxi'.

        Returns:
            list: A list of user IDs (str) who have used the specified transportation mode.
        """
        try:
            query = """
            SELECT DISTINCT user_id
            FROM Activity
            WHERE transportation_mode = %s;
            """
            self.cursor.execute(query, (transport_mode,))
            results = [row[0] for row in self.cursor.fetchall()]

            return results

        except Exception as e:
            logger.error(
                f"Error retrieving users for transport mode {transport_mode}: {str(e)}"
            )
            return []

    def get_transport_modes_with_counts(self):
        """
        Retrieves all transportation modes and counts how many activities are tagged with each mode.

        Returns:
            list: A list of tuples where each tuple contains:
                - transportation_mode (str): The name of the transportation mode.
                - activity_count (int): The number of activities for that mode.
        """
        try:
            query = """
            SELECT transportation_mode, COUNT(*) AS activity_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL AND transportation_mode != 'NULL'
            GROUP BY transportation_mode;
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results

        except Exception as e:
            logger.error(f"Error retrieving transportation modes with counts: {str(e)}")
            return []

    def get_year_with_most_activities(self):
        """
        Finds the year with the most activities.

        Returns:
            tuple: A tuple containing:
                - year (int): The year with the most activities.
                - activity_count (int): The number of activities in that year.
        """
        try:
            query = """
            SELECT YEAR(start_date_time) AS year, COUNT(*) AS activity_count
            FROM Activity
            GROUP BY year
            ORDER BY activity_count DESC
            LIMIT 1;
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result

        except Exception as e:
            logger.error(f"Error retrieving year with the most activities: {str(e)}")
            return None

    def get_year_with_most_hours(self):
        """
        Finds the year with the most recorded hours.

        Returns:
            tuple: A tuple containing:
                - year (int): The year with the most recorded hours.
                - total_hours (int): The total recorded hours in that year.
        """
        try:
            query = """
            SELECT YEAR(start_date_time) AS year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
            FROM Activity
            GROUP BY year
            ORDER BY total_hours DESC
            LIMIT 1;
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result

        except Exception as e:
            logger.error(
                f"Error retrieving year with the most recorded hours: {str(e)}"
            )
            return None

    def get_total_distance_walked(self, user_id, year=2008):
        """
        Calculates the total distance walked by a user in a specific year using the Haversine formula.

        Args:
            user_id (str): The user ID.
            year (int): The year to filter activities by.

        Returns:
            float: The total distance walked in kilometers.
        """
        try:
            query = """
            SELECT id FROM Activity
            WHERE user_id = %s
            AND transportation_mode = 'walk'
            AND YEAR(start_date_time) = %s;
            """
            self.cursor.execute(query, (user_id, year))
            activity_ids = [row[0] for row in self.cursor.fetchall()]

            total_distance = 0.0

            # Calculate the total distance for each walking activity
            for activity_id in activity_ids:
                self.cursor.execute(
                    """
                SELECT lat, lon FROM TrackPoint
                WHERE activity_id = %s
                ORDER BY date_time ASC;
                """,
                    (activity_id,),
                )

                trackpoints = self.cursor.fetchall()

                # Calculate distance between successive trackpoints using Haversine package
                for i in range(1, len(trackpoints)):
                    point1 = (trackpoints[i - 1][0], trackpoints[i - 1][1])
                    point2 = (trackpoints[i][0], trackpoints[i][1])
                    distance = haversine(point1, point2, unit=Unit.KILOMETERS)
                    total_distance += distance

            return total_distance

        except Exception as e:
            logger.error(
                f"Error calculating total distance walked by user {user_id} in {year}: {str(e)}"
            )
            return 0.0

    def get_top_users_by_altitude_gain(self, top_n=20):
        """
        Finds the top users who have gained the most altitude in meters.

        Args:
            top_n (int): The number of top users to retrieve. Default is 20.

        Returns:
            list: A list of tuples where each tuple contains:
                - user_id (str): The user ID.
                - total_altitude_gain (int): The total altitude gained by the user in meters.
        """
        try:
            # SQL query to calculate altitude gain
            query = f"""
            SELECT user_id, ROUND(SUM(altitude_gain * 0.3048)) AS total_altitude_gain_meters
            FROM (
                SELECT 
                    Activity.user_id,
                    GREATEST(
                        TrackPoint.altitude - LAG(TrackPoint.altitude) 
                        OVER (PARTITION BY TrackPoint.activity_id ORDER BY TrackPoint.date_time), 
                        0
                    ) AS altitude_gain
                FROM TrackPoint
                JOIN Activity ON TrackPoint.activity_id = Activity.id
                WHERE TrackPoint.altitude > -505
            ) gains
            GROUP BY user_id
            ORDER BY total_altitude_gain_meters DESC
            LIMIT {top_n};
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            return results

        except Exception as e:
            logger.error(f"Error calculating altitude gain: {str(e)}")
            return []

    def get_users_with_invalid_activities_count(self):
        """
        Finds all users who have invalid activities and counts the number of invalid activities per user.
        An invalid activity is defined as one with consecutive trackpoints where timestamps deviate by at least 5 minutes.

        Returns:
            list: A list of tuples containing:
                - user_id (str): The user ID.
                - invalid_activity_count (int): The number of invalid activities for that user.
        """
        try:
            query = """
            SELECT user_id, COUNT(DISTINCT activity_id) AS invalid_activity_count
            FROM (
                SELECT 
                    Activity.user_id,
                    TrackPoint.activity_id,
                    TrackPoint.date_time,
                    LAG(TrackPoint.date_time) OVER (
                        PARTITION BY TrackPoint.activity_id 
                        ORDER BY TrackPoint.date_time
                    ) AS previous_date_time,
                    TIMESTAMPDIFF(MINUTE, 
                        LAG(TrackPoint.date_time) OVER (
                            PARTITION BY TrackPoint.activity_id 
                            ORDER BY TrackPoint.date_time
                        ), 
                        TrackPoint.date_time
                    ) AS time_difference
                FROM TrackPoint
                JOIN Activity ON TrackPoint.activity_id = Activity.id
            ) subquery
            WHERE time_difference >= 5
            GROUP BY user_id
            ORDER BY invalid_activity_count DESC;
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            return results

        except Exception as e:
            logger.error(f"Error finding users with invalid activities: {str(e)}")
            return []

    def get_users_in_forbidden_city(self):
        """
        Finds users who have tracked an activity in the Forbidden City of Beijing.
        The Forbidden City is considered to have coordinates around:
        lat 39.916, lon 116.397.

        Returns:
            list: A list of user IDs who have trackpoints near the Forbidden City.
        """
        try:
            query = """
            SELECT DISTINCT Activity.user_id
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE TrackPoint.lat BETWEEN 39.9155 AND 39.9165
            AND TrackPoint.lon BETWEEN 116.3965 AND 116.3975;
            """
            self.cursor.execute(query)
            users = [row[0] for row in self.cursor.fetchall()]

            return users

        except Exception as e:
            logger.error(f"Error finding users in Forbidden City: {str(e)}")
            return []

    def find_users_with_most_used_transportation(self):
        """
        Finds users who have registered a transportation mode and their most used mode.
        The result is sorted by user_id.

        Returns:
            list: A list of tuples where each tuple contains:
                - user_id (str): The user ID.
                - most_used_transportation_mode (str): The most frequently used transportation mode.
        """
        try:
            query = """
            SELECT user_id, transportation_mode
            FROM (
                SELECT 
                    user_id, 
                    transportation_mode, 
                    RANK() OVER (
                        PARTITION BY user_id 
                        ORDER BY COUNT(*) DESC
                    ) as mode_rank
                FROM Activity
                WHERE transportation_mode IS NOT NULL AND transportation_mode != 'NULL'
                GROUP BY user_id, transportation_mode
            ) ranked_modes
            WHERE mode_rank = 1
            ORDER BY user_id;
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            return results

        except Exception as e:
            logger.error(
                f"Error finding users with most used transportation mode: {str(e)}"
            )
            return []
