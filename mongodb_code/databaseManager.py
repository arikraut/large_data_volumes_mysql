from mongodb_code.DbConnector import DbConnector
import logging
import os
from mongodb_code.dataHelper import DataHelper as dh
from pymongo.errors import BulkWriteError, DuplicateKeyError, CollectionInvalid
from haversine import haversine, Unit
from pymongo import ASCENDING
from mongodb_code.mongo_schemas import user_schema, activity_schema, trackpoint_schema

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# File handler setup
debug_handler = logging.FileHandler("debug_log.log")
debug_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
debug_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(debug_handler)


class DatabaseManager:
    def __init__(
        self,
        DATABASE="exc3db",
        HOST="localhost:27017",
        USER=None,
        PASSWORD=None,
        use_authentication=False,
    ):
        """
        Initializes the MongoDB connection and sets up collections with unique indexes
        to prevent duplicate entries. Creates or retrieves `User`, `Activity`, and `TrackPoint`
        collections in the specified database and applies unique constraints.
        """
        self.connection = DbConnector(
            DATABASE=DATABASE,
            HOST=HOST,
            USER=USER,
            PASSWORD=PASSWORD,
            use_authentication=use_authentication,
        )
        self.db = self.connection.db

        # Apply schema validation to collections
        self.user_collection = self._apply_schema("User", user_schema)
        self.activity_collection = self._apply_schema("Activity", activity_schema)
        self.trackpoint_collection = self._apply_schema("TrackPoint", trackpoint_schema)

        # Indexes to enforce uniqueness
        self.user_collection.create_index("_id")
        self.activity_collection.create_index(
            [("user_id", ASCENDING), ("start_date_time", ASCENDING)], unique=True
        )
        self.trackpoint_collection.create_index(
            [("activity_id", ASCENDING), ("date_time", ASCENDING)], unique=True
        )
        self.trackpoint_collection.create_index("user_id", unique=False)
        self.trackpoint_collection.create_index([("location", "2dsphere")])

        logging.info("MongoDB connection established and indexes created.")

    def _apply_schema(self, collection_name, schema):
        """
        Applies a schema to a MongoDB collection. If the collection already exists,
        it catches CollectionInvalid and returns the existing collection.

        Args:
            collection_name (str): The name of the collection.
            schema (dict): The schema to apply.

        Returns:
            Collection: The MongoDB collection with the schema applied.
        """
        try:
            self.db.create_collection(
                collection_name, validator={"$jsonSchema": schema}
            )
            logging.info(f"Collection {collection_name} created with schema.")
        except CollectionInvalid:
            logging.info(
                f"Collection {collection_name} already exists. Using the existing collection."
            )
        except Exception as e:
            logging.error(f"Error creating collection {collection_name}: {e}")

        # Return the collection (newly created or existing)
        return self.db[collection_name]

    def close_connection(self):
        """
        Closes the MongoDB connection.
        """
        self.connection.close_connection()
        logging.info("MongoDB connection closed.")

    # ------------------ Insertion Methods ------------------ #

    def insert_user(self, user_id, has_labels=False):
        """
        Inserts a single user document into the `User` collection with a unique ID.
        Logs success or handles duplicate user warnings.

        Args:
            user_id (str): Unique identifier for the user.
            has_labels (bool): Flag indicating if the user has labeled activities.
        """
        try:
            user_document = {"_id": user_id, "has_labels": has_labels}
            self.user_collection.insert_one(user_document)
            logging.info(f"User {user_id} inserted successfully.")
        except DuplicateKeyError:
            logging.warning(f"User {user_id} already exists, skipping insertion.")
        except Exception as e:
            logging.error(f"Error creating user {user_id}: {str(e)}")

    def insert_activities_bulk(self, activities, user_id):
        """
        Bulk inserts a list of activities for a user into the `Activity` collection,
        handling duplicates and logging each result.

        Args:
            activities (list): List of dictionaries, each representing an activity.
            user_id (str): The user ID for logging purposes.

        Returns:
            list: MongoDB IDs of successfully inserted activities (excluding duplicates).
        """

        result = None

        try:
            # Attempt to insert all activities and allow MongoDB to skip duplicates
            result = self.activity_collection.insert_many(activities, ordered=False)
        except BulkWriteError as e:
            duplicate_count = sum(
                1 for error in e.details["writeErrors"] if error["code"] == 11000
            )
            non_dup_errors = [
                error for error in e.details["writeErrors"] if error["code"] != 11000
            ]

            logging.warning(
                f"Skipped {duplicate_count} duplicate activities for user {user_id}."
            )
            for error in non_dup_errors:
                logging.error(
                    f"Non-duplicate error for user {user_id} activity insertion: {error}"
                )

        return result.inserted_ids if result else []

    def insert_trackpoints_bulk(self, trackpoints, user_id):
        """
        Bulk inserts a list of trackpoints into the `TrackPoint` collection.
        Handles duplicates and logs each result. Also logs any unexpected errors.

        Args:
            trackpoints (list): List of dictionaries, each representing a trackpoint.
            user_id (str): The user ID for logging purposes.

        Returns:
            int: The number of successfully inserted trackpoints (excluding duplicates).
        """

        result = None

        try:
            # Bulk insert trackpoints and allow MongoDB to skip duplicates.
            result = self.trackpoint_collection.insert_many(trackpoints, ordered=False)

        except BulkWriteError as e:
            # Count duplicates and any non-duplicate errors
            duplicate_count = sum(
                1 for error in e.details["writeErrors"] if error["code"] == 11000
            )
            non_dup_errors = [
                error for error in e.details["writeErrors"] if error["code"] != 11000
            ]

            logging.warning(
                f"Skipped {duplicate_count} duplicate trackpoints for user {user_id}."
            )
            for error in non_dup_errors:
                logging.error(
                    f"Non-duplicate error for user {user_id} trackpoint insertion: {error}"
                )

        return len(result.inserted_ids) if result else []

    def process_user_activities_and_trackpoints(self, dataset_path):
        """
        Processes a user's data by inserting the user, activities, and trackpoints in bulk.
        First, it inserts the user, retrieves and inserts activities, then gathers and inserts
        trackpoints for all activities of the user.

        Args:
            dataset_path (str): Path to the dataset directory.
            user (tuple): Tuple containing user_id and has_labels status.
        """

        users = dh.extract_users(dataset_path)
        print("Inserting users, activities, and trackpoints...")
        for user in users:
            user_id, has_labels = user
            self.insert_user(user_id, has_labels)

            activity_documents = dh.extract_activities(dataset_path, user)
            if activity_documents:
                inserted_activity_ids = self.insert_activities_bulk(
                    activity_documents, user_id
                )
                logging.info(
                    f"Inserted {len(inserted_activity_ids)} out of {len(activity_documents)} activities for user {user_id}."
                )

                all_trackpoints_for_user = []
                for activity_doc, activity_id in zip(
                    activity_documents, inserted_activity_ids
                ):
                    file_name = (
                        activity_doc["start_date_time"].strftime("%Y%m%d%H%M%S")
                        + ".plt"
                    )
                    file_path = os.path.join(
                        dataset_path, user_id, "Trajectory", file_name
                    )

                    trackpoints = dh.extract_trackpoints(
                        file_path, activity_id, user_id
                    )
                    all_trackpoints_for_user.extend(trackpoints)

                if all_trackpoints_for_user:
                    inserted_tp_count = self.insert_trackpoints_bulk(
                        all_trackpoints_for_user, user_id
                    )
                    logging.info(
                        f"Inserted {inserted_tp_count} out of {len(all_trackpoints_for_user)} trackpoints for user {user_id}."
                    )
                else:
                    logging.warning(f"No trackpoints found for user {user_id}.")

    def get_first_rows(self, collection_name, limit=10):
        """
        Retrieves the first documents from a specified collection.

        Args:
            collection_name (str): The name of the collection to query.
            limit (int): The number of documents to retrieve. Default is 10.

        Returns:
            list: A list of documents from the specified collection.
        """
        try:
            collection = self.db[collection_name]

            documents = list(collection.find().limit(limit))

            logging.info(f"Retrieved first {limit} documents from {collection_name}.")
            return documents

        except Exception as e:
            logging.error(
                f"Error retrieving documents from {collection_name}: {str(e)}"
            )
            return []

    def show_collections(self):
        """
        Lists all collections in the MongoDB database and shows a sample document from each collection.

        Returns:
            dict: A dictionary with collection names as keys and sample document structures as values.
        """
        try:
            collections = self.db.list_collection_names()
            collection_samples = {}

            for collection_name in collections:
                collection = self.db[collection_name]
                sample_document = collection.find_one()
                collection_samples[collection_name] = (
                    sample_document if sample_document else "No documents found"
                )

            logging.info(f"Collections and sample structures: {collection_samples}")
            return collection_samples

        except Exception as e:
            logging.error(f"Error listing collections: {str(e)}")
            return {}

    def get_users(self):
        """
        Retrieves all users from the User collection.

        Returns:
            list: A list of user documents containing 'id' and 'has_labels'.
        """
        try:
            users = list(self.user_collection.find({}, {"_id": 0}))
            logging.info(f"Retrieved {len(users)} users.")
            return users

        except Exception as e:
            logging.error(f"Error retrieving users: {str(e)}")
            return []

    ################################################################################
    # The following methods are used to retrieve data from the database for Task 2. #
    ################################################################################

    def get_counts(self):
        """
        Retrieves the counts of users, activities, and trackpoints from the database.

        Returns:
            dict: A dictionary containing the counts with keys 'user_count', 'activity_count', 'trackpoint_count'.
        """
        try:
            counts = {
                "user_count": self.user_collection.count_documents({}),
                "activity_count": self.activity_collection.count_documents({}),
                "trackpoint_count": self.trackpoint_collection.count_documents({}),
            }
            logging.info(f"Counts retrieved: {counts}")
            return [
                ["Users", counts["user_count"]],
                ["Activities", counts["activity_count"]],
                ["TrackPoints", counts["trackpoint_count"]],
            ]

        except Exception as e:
            logging.error(f"Error retrieving counts: {str(e)}")
            return [["Users", 0], ["Activities", 0], ["TrackPoints", 0]]

    def get_average_activities_per_user(self):
        """
        Calculates the average number of activities per user.

        Returns:
            float: The average number of activities per user. Returns 0.0 if an error occurs.
        """
        try:
            # Step 1: Group by 'user_id' and count activities per user
            # Step 2: Calculate the average activity count from the grouped results
            pipeline = [
                {
                    "$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}
                },  # Counts activities per user
                {
                    "$group": {
                        "_id": None,
                        "avg_activities_per_user": {"$avg": "$activity_count"},
                    }
                },  # Averages activity counts across users
            ]

            # Execute the aggregation pipeline
            result = list(self.activity_collection.aggregate(pipeline))

            # Extract the average from the result
            avg_activities = result[0]["avg_activities_per_user"] if result else 0.0
            logging.info(f"Average activities per user: {avg_activities}")

            return avg_activities

        except Exception as e:
            logging.error(f"Error retrieving average activities per user: {str(e)}")
            return 0.0

    def get_top_users_by_activity(self, top_n=20):
        """
        Retrieves the top users with the highest number of activities.

        Args:
            top_n (int): The number of top users to retrieve.

        Returns:
            list: A list of dictionaries where each dictionary contains:
                - 'user_id' (str): The user ID.
                - 'activity_count' (int): The number of activities for the user.
        """
        try:
            # Aggregation pipeline to find the top users by activity count
            pipeline = [
                {
                    "$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}
                },  # Group by user_id and count activities
                {
                    "$sort": {"activity_count": -1}
                },  # Sort by activity count in descending order
                {"$limit": top_n},  # Limit the results to top_n users
            ]

            # Execute the aggregation pipeline
            results = list(self.activity_collection.aggregate(pipeline))

            # Format the results as a list of dictionaries
            top_users = [
                {"user_id": result["_id"], "activity_count": result["activity_count"]}
                for result in results
            ]

            logging.info(f"Top {top_n} users by activity count retrieved successfully.")
            return top_users

        except Exception as e:
            logging.error(f"Error retrieving top users by activity count: {str(e)}")
            return []

    def get_users_by_transport_mode(self, transport_mode="taxi"):
        """
        Retrieves all users who have used a specific transportation mode.

        Args:
            transport_mode (str): The transportation mode to filter by.

        Returns:
            list: A list of user IDs who have used the specified transportation mode.
        """
        try:
            # Use distinct to get unique user_id values for the specified transportation mode
            user_ids = self.activity_collection.distinct(
                "user_id", {"transportation_mode": transport_mode}
            )

            logging.info(
                f"Users who used transport mode '{transport_mode}' retrieved successfully."
            )
            return user_ids

        except Exception as e:
            logging.error(
                f"Error retrieving users for transport mode '{transport_mode}': {str(e)}"
            )
            return []

    def get_transport_modes_with_counts(self):
        """
        Retrieves all transportation modes and counts how many activities are tagged with each mode,
        excluding 'NULL' and None values.

        Returns:
            list: A list of dictionaries where each dictionary contains:
                - 'transportation_mode' (str): The transportation mode.
                - 'activity_count' (int): The count of activities for that mode.
        """
        try:
            # Aggregation pipeline to group by transportation mode and count occurrences
            pipeline = [
                {
                    "$match": {"transportation_mode": {"$exists": True}}
                },  # Exclude documents with 'NULL' or None transportation modes
                {
                    "$group": {
                        "_id": "$transportation_mode",
                        "activity_count": {"$sum": 1},
                    }
                },
                {
                    "$sort": {"activity_count": -1}
                },  # Sort modes by activity count in descending order
            ]

            results = list(self.activity_collection.aggregate(pipeline))

            transport_modes = [
                {
                    "transportation_mode": result["_id"],
                    "activity_count": result["activity_count"],
                }
                for result in results
            ]

            logging.info("Transportation modes with counts retrieved successfully.")
            return transport_modes

        except Exception as e:
            logging.error(
                f"Error retrieving transportation modes with counts: {str(e)}"
            )
            return []

    def get_year_with_most_activities(self):
        """
        Finds the year with the most activities.

        Returns:
            dict: A dictionary with 'year' and 'activity_count' for the year with the most activities.
        """
        try:
            # Aggregation pipeline to count activities per year
            pipeline = [
                {
                    "$group": {
                        "_id": {"$year": "$start_date_time"},
                        "activity_count": {"$sum": 1},
                    }
                },
                {"$sort": {"activity_count": -1}},
                {"$limit": 1},
            ]

            result = list(self.activity_collection.aggregate(pipeline))

            if result:
                year_data = {
                    "year": result[0]["_id"],
                    "activity_count": result[0]["activity_count"],
                }
                logging.info(f"Year with most activities: {year_data}")
                return year_data
            else:
                logging.info("No activities found.")
                return None

        except Exception as e:
            logging.error(f"Error retrieving year with the most activities: {str(e)}")
            return None

    def get_year_with_most_hours(self):
        """
        Finds the year with the most recorded hours.

        Returns:
            dict: A dictionary with 'year' and 'total_hours' for the year with the most recorded hours.
        """
        try:
            # Aggregation pipeline to calculate hours and group by year
            pipeline = [
                {
                    "$project": {
                        "year": {"$year": "$start_date_time"},
                        "duration_hours": {
                            "$divide": [
                                {"$subtract": ["$end_date_time", "$start_date_time"]},
                                3600000,
                            ]
                        },
                    }
                },  # Extracts year and calculates duration in hours
                {
                    "$group": {
                        "_id": "$year",
                        "total_hours": {"$sum": "$duration_hours"},
                    }
                },  # Sum hours per year
                {
                    "$sort": {"total_hours": -1}
                },  # Sort by total hours in descending order
                {"$limit": 1},  # Limit to the top year with the most hours
            ]

            result = list(self.activity_collection.aggregate(pipeline))

            if result:
                year_data = {
                    "year": result[0]["_id"],
                    "total_hours": result[0]["total_hours"],
                }
                logging.info(f"Year with most recorded hours: {year_data}")
                return year_data
            else:
                logging.info("No activity hours found.")
                return None

        except Exception as e:
            logging.error(
                f"Error retrieving year with the most recorded hours: {str(e)}"
            )
            return None

    def get_total_distance_walked(self, user_id, year=2008):
        try:
            activities = list(
                self.activity_collection.find(
                    {
                        "user_id": user_id,
                        "transportation_mode": "walk",
                        "$expr": {"$eq": [{"$year": "$start_date_time"}, year]},
                    },
                    {"_id": 1},
                )
            )

            activity_ids = [activity["_id"] for activity in activities]
            total_distance = 0.0

            for activity_id in activity_ids:
                trackpoints = list(
                    self.trackpoint_collection.find(
                        {"activity_id": activity_id}, {"location": 1}
                    ).sort("date_time", 1)
                )

                for i in range(1, len(trackpoints)):
                    point1 = trackpoints[i - 1]["location"]["coordinates"]
                    point2 = trackpoints[i]["location"]["coordinates"]
                    distance = haversine(
                        (point1[1], point1[0]),
                        (point2[1], point2[0]),
                        unit=Unit.KILOMETERS,
                    )
                    total_distance += distance

            logging.info(
                f"Total distance walked by user {user_id} in {year}: {total_distance:.2f} km"
            )
            return total_distance

        except Exception as e:
            logging.error(
                f"Error calculating total distance walked by user {user_id} in {year}: {str(e)}"
            )
            return 0.0

    def get_top_users_by_altitude_gain(self, top_n=20):
        try:
            pipeline = [
                {"$match": {"altitude": {"$gt": -505}}},
                {
                    "$group": {
                        "_id": {"activity_id": "$activity_id", "user_id": "$user_id"},
                        "altitudes": {"$push": "$altitude"},
                    }
                },
                {
                    "$project": {
                        "user_id": "$_id.user_id",
                        "altitude_gain": {
                            "$sum": {
                                "$map": {
                                    "input": {"$range": [1, {"$size": "$altitudes"}]},
                                    "as": "i",
                                    "in": {
                                        "$max": [
                                            {
                                                "$subtract": [
                                                    {
                                                        "$arrayElemAt": [
                                                            "$altitudes",
                                                            "$$i",
                                                        ]
                                                    },
                                                    {
                                                        "$arrayElemAt": [
                                                            "$altitudes",
                                                            {"$subtract": ["$$i", 1]},
                                                        ]
                                                    },
                                                ]
                                            },
                                            0,
                                        ]
                                    },
                                }
                            }
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$user_id",
                        "total_altitude_gain_feet": {"$sum": "$altitude_gain"},
                    }
                },
                {
                    "$project": {
                        "user_id": "$_id",
                        "total_altitude_gain": {
                            "$multiply": ["$total_altitude_gain_feet", 0.3048]
                        },
                    }
                },
                {"$sort": {"total_altitude_gain": -1}},
                {"$limit": top_n},
            ]

            results = list(self.trackpoint_collection.aggregate(pipeline))
            top_users = [
                {
                    "user_id": result["user_id"],
                    "total_altitude_gain": result["total_altitude_gain"],
                }
                for result in results
            ]
            logging.info(f"Top {top_n} users by altitude gain in meters: {top_users}")
            return top_users

        except Exception as e:
            logging.error(f"Error calculating altitude gain: {str(e)}")
            return []

    def get_users_with_invalid_activities_count(self):
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$activity_id",
                        "user_id": {"$first": "$user_id"},
                        "timestamps": {"$push": "$date_time"},
                    }
                },
                {
                    "$project": {
                        "user_id": 1,
                        "invalid": {
                            "$anyElementTrue": {
                                "$map": {
                                    "input": {"$range": [1, {"$size": "$timestamps"}]},
                                    "as": "i",
                                    "in": {
                                        "$gte": [
                                            {
                                                "$subtract": [
                                                    {
                                                        "$arrayElemAt": [
                                                            "$timestamps",
                                                            "$$i",
                                                        ]
                                                    },
                                                    {
                                                        "$arrayElemAt": [
                                                            "$timestamps",
                                                            {"$subtract": ["$$i", 1]},
                                                        ]
                                                    },
                                                ]
                                            },
                                            300000,
                                        ]
                                    },
                                }
                            }
                        },
                    }
                },
                {"$match": {"invalid": True}},
                {"$group": {"_id": "$user_id", "invalid_activity_count": {"$sum": 1}}},
                {"$sort": {"invalid_activity_count": -1}},
            ]

            results = list(self.trackpoint_collection.aggregate(pipeline))
            users_with_invalid_counts = [
                {
                    "user_id": result["_id"],
                    "invalid_activity_count": result["invalid_activity_count"],
                }
                for result in results
            ]
            logging.info(f"Users with invalid activities: {users_with_invalid_counts}")
            return users_with_invalid_counts

        except Exception as e:
            logging.error(f"Error finding users with invalid activities: {str(e)}")
            return []

    def get_users_in_forbidden_city(self):
        try:
            forbidden_city_query = {
                "$match": {
                    "location": {
                        "$geoWithin": {
                            "$box": [[116.3965, 39.9155], [116.3975, 39.9165]]
                        }
                    }
                }
            }

            pipeline = [forbidden_city_query, {"$group": {"_id": "$user_id"}}]

            results = list(self.trackpoint_collection.aggregate(pipeline))
            user_ids = [result["_id"] for result in results]
            logging.info(f"Users in Forbidden City: {user_ids}")
            return user_ids

        except Exception as e:
            logging.error(f"Error finding users in Forbidden City: {str(e)}")
            return []

    def find_users_with_most_used_transportation(self):
        """
        Finds each user's most used transportation mode.

        Returns:
            list: A list of dictionaries where each dictionary contains:
                - 'user_id' (str): The user ID.
                - 'most_used_transportation_mode' (str): The most frequently used transportation mode.
        """
        try:
            # Aggregation pipeline to find each user's most frequently used transportation mode
            pipeline = [
                {"$match": {"transportation_mode": {"$exists": True}}},
                {
                    "$group": {
                        "_id": {"user_id": "$user_id", "mode": "$transportation_mode"},
                        "count": {"$sum": 1},
                    }
                },  # Count occurrences of each transportation mode per user
                {"$sort": {"count": -1}},  # Sort by count in descending order
                {
                    "$group": {
                        "_id": "$_id.user_id",
                        "most_used_transportation_mode": {"$first": "$_id.mode"},
                        "mode_count": {"$first": "$count"},
                    }
                },  # Group by user and select the top mode based on count
                {"$sort": {"_id": 1}},  # Sort by user_id for easier reading
            ]

            results = list(self.activity_collection.aggregate(pipeline))

            user_modes = [
                {
                    "user_id": result["_id"],
                    "most_used_transportation_mode": result[
                        "most_used_transportation_mode"
                    ],
                }
                for result in results
            ]

            logging.info(
                "Users' most used transportation modes retrieved successfully."
            )
            return user_modes

        except Exception as e:
            logging.error(
                f"Error finding users with most used transportation mode: {str(e)}"
            )
            return []
