import logging
import os


class DataPreparation:

    @staticmethod
    def clean_data(datapath):
        """
        Cleans the data by removing files exceeding the max line count.

        Args:
            datapath (str): Path to the dataset folder to clean.
        """
        max_lines = 2507  # 6 preceding lines and 1 line at the end to ignore
        delete_count = 0
        keep_count = 0

        logging.info("Cleaning data...")
        for root, dirs, files in os.walk(datapath):
            if os.path.basename(root) == "Trajectory":
                for file in files:
                    if file.endswith(".plt"):
                        file_path = os.path.join(root, file)

                        # Count the number of lines in the file
                        with open(file_path, "r") as f:
                            line_count = sum(1 for line in f)

                        if line_count > max_lines:
                            delete_count += 1
                            os.remove(file_path)
                            # logging.info(f"Deleted {file_path} with {line_count} lines")
                        else:
                            keep_count += 1
                            # logging.info(f"Kept {file_path} with {line_count} lines")

        logging.info(
            f"Data cleaning complete. Deleted {delete_count} files and kept {keep_count} files"
        )

    @staticmethod
    def prepare_users(
        datapath, labeled_ids_path="datasets/valid_dataset/labeled_ids.txt"
    ):
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
    def fix_negative_alt(datapath):
        """
        Replaces unreasonable altitude values in .plt files with -777.

        Args:
            datapath (str): The path to the dataset containing user folders.
        """
        updated_count = 0
        min_altitude = -505
        invalid_altitude = "-777"

        for root, dirs, files in os.walk(datapath):
            if os.path.basename(root) == "Trajectory":
                for file in files:
                    if file.endswith(".plt"):
                        file_path = os.path.join(root, file)

                        with open(file_path, "r") as f:
                            lines = f.readlines()

                        header = lines[:6]
                        trackpoints = lines[6:]

                        updated = []
                        for tp in trackpoints:
                            data = tp.strip().split(",")

                            if len(data) > 3:
                                try:
                                    alt = float(data[3])
                                    if alt < min_altitude:
                                        data[3] = invalid_altitude
                                        updated_count += 1
                                except ValueError:
                                    data[3] = invalid_altitude
                                    updated_count += 1

                                updated_line = ",".join(data)
                                updated.append(updated_line + "\n")

                        with open(file_path, "w") as f:
                            f.writelines(header + updated)

        logging.info(
            f"Total updated trackpoints with invalid altitudes: {updated_count}"
        )
        print(f"Total updated trackpoints with invalid altitudes: {updated_count}")
