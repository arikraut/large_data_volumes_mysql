import os
import logging
from datetime import datetime


class DataHelper:

    @staticmethod
    def build_label_lookup(dataset_path, user_id):
        """
        Builds a lookup dictionary for a user's activities from the label file, if available.
        This dictionary maps start and end times of activities to transportation modes.

        Args:
            dataset_path (str): Path to the dataset folder.
            user_id (str): ID of the user.

        Returns:
            dict:   Dictionary with (start_time, end_time) as keys and transportation mode as values.
                    Returns an empty dictionary if the file is missing or no labels are found.
        """
        label_file = os.path.join(dataset_path, user_id, "labels.txt")
        label_lookup = {}

        # Check if label file exists
        if not os.path.exists(label_file):
            logging.warning(
                f"User {user_id} is marked for labels, but the label file: {label_file}, was not found."
            )
            return label_lookup

        try:
            with open(label_file, "r") as f:
                # Skip header
                next(f)
                for line in f:
                    start_time, end_time, mode = line.strip().split("\t")
                    start_time = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
                    end_time = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
                    label_lookup[(start_time, end_time)] = mode
        except Exception as e:
            logging.error(
                f"Error reading label file {label_file} for user {user_id}: {str(e)}"
            )

        return label_lookup

    @staticmethod
    def extract_users(datapath, labeled_ids_path="datasets/dataset/labeled_ids.txt"):
        """
        Prepares the users from the dataset by loading user IDs and labeled IDs.

        Args:
            datapath (str): The path to the dataset folder.
            labeled_ids_path (str): The path to the labeled_ids.txt file.

        Returns:
            list: A list of tuples where each tuple contains:
                - user_id (str): The user ID.
                - has_labels (bool): True if the user has labels, False otherwise.
        """
        logging.info("Preparing users...")
        print("Preparing users...")

        # Get all the user_ids from the dataset
        try:
            user_ids = [
                folder
                for folder in os.listdir(datapath)
                if os.path.isdir(os.path.join(datapath, folder))
            ]
        except OSError as e:
            logging.error(f"Failed to list directories in {datapath}: {str(e)}")
            return []

        logging.info(f"Found {len(user_ids)} users in the dataset")
        print(f"Found {len(user_ids)} users in the dataset")

        # Get the labeled users from the labeled_ids.txt file
        try:
            with open(labeled_ids_path, "r") as f:
                labeled_ids = set(f.read().splitlines())  # Use set for fast lookup
        except FileNotFoundError:
            logging.error(f"Could not find {labeled_ids_path}")
            return []
        except Exception as e:
            logging.error(f"Error reading {labeled_ids_path}: {str(e)}")
            return []

        logging.info(f"Found {len(labeled_ids)} labeled users")

        users_with_labels = [(user_id, user_id in labeled_ids) for user_id in user_ids]

        logging.info(f"{len(users_with_labels)} users prepared successfully")
        return users_with_labels

    @staticmethod
    def extract_activities(dataset_path, user):
        """
        Extracts activities for a user by gathering start and end times from each activity file.
        Checks for transportation mode labels and assigns them if available.

        Args:
            dataset_path (str): Path to the dataset folder.
            user (tuple): Tuple containing user_id and has_labels (bool).

        Returns:
            list:   List of dictionaries, where each dictionary represents an activity with `user_id`,
                    `transportation_mode` (if available), `start_date_time`, and `end_date_time`.
        """

        user_id, has_labels = user
        # Retrieve label lookup if the user has labeled data
        label_lookup = (
            DataHelper.build_label_lookup(dataset_path, user_id) if has_labels else {}
        )

        activity_documents = []
        activity_folder = os.path.join(dataset_path, user_id, "Trajectory")

        for file in os.listdir(activity_folder):
            file_path = os.path.join(activity_folder, file)
            try:
                start_time, end_time = DataHelper.get_start_end_time(file_path)
            except ValueError:
                logging.warning(f"Activity file {file_path} contains no valid data.")
                continue

            transportation_mode = label_lookup.get((start_time, end_time), None)

            # Build the activity document, conditionally including transportation_mode
            activity_doc = {
                "user_id": user_id,
                "start_date_time": start_time,
                "end_date_time": end_time,
            }
            if transportation_mode is not None:
                activity_doc["transportation_mode"] = transportation_mode

            activity_documents.append(activity_doc)

        logging.info(
            f"Total activities extracted for user {user_id}: {len(activity_documents)}"
        )
        return activity_documents

    @staticmethod
    def extract_trackpoints(file_path, activity_id, user_id):
        """
        Extracts trackpoints from a specified file and associates them with an activity ID.
        Each trackpoint includes latitude, longitude, altitude, and timestamp.

        Args:
            file_path (str): Path to the .plt file.
            activity_id (ObjectId): ID of the associated activity.

        Returns:
            list:   List of dictionaries representing trackpoints, where each dictionary includes
                    `activity_id`, `lat`, `lon`, `altitude`, and `date_time`.
        """
        trackpoints = []
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
                for line in lines:
                    lat, lon, altitude, date_time_str = line.strip().split(",")
                    lat, lon = float(lat), float(lon)
                    altitude = int(float(altitude))
                    date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")

                    trackpoints.append(
                        {
                            "activity_id": activity_id,
                            "user_id": user_id,
                            "location": {
                                "type": "Point",
                                "coordinates": [
                                    lon,
                                    lat,
                                ],
                            },
                            "altitude": altitude,
                            "date_time": date_time,
                        }
                    )

            logging.info(f"Extracted {len(trackpoints)} trackpoints from {file_path}")
            return trackpoints

        except FileNotFoundError:
            logging.error(f"Trackpoint file not found: {file_path}")
        except Exception as e:
            logging.error(f"Error extracting trackpoints from {file_path}: {str(e)}")

    @staticmethod
    def get_start_end_time(file_path):
        """
        Extracts start and end times from the first and last entries of an activity file.

        Args:
            file_path (str): Path to the activity file.

        Returns:
            tuple: Start and end times as datetime objects.
        """
        with open(file_path, "r") as f:
            lines = f.readlines()
            start_line = lines[0].strip().split(",")
            start_time = datetime.strptime(start_line[3], "%Y-%m-%d %H:%M:%S")

            end_line = lines[-1].strip().split(",")
            end_time = datetime.strptime(end_line[3], "%Y-%m-%d %H:%M:%S")

        return start_time, end_time
