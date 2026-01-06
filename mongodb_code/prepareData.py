from datetime import datetime
import os
import logging


class DataPreparation:
    CLEANING_SETTINGS = {
        "min_altitude": -505,
        "max_altitude": 29035,
        "invalid_altitude": -777,
        "max_trackpoints": 2501,
    }

    @classmethod
    def set_CLEANING_SETTINGS(
        cls,
        min_altitude=None,
        invalid_altitude=None,
        max_altitude=None,
        max_trackpoints=None,
    ):
        if min_altitude is not None:
            cls.CLEANING_SETTINGS["min_altitude"] = min_altitude
        if invalid_altitude is not None:
            cls.CLEANING_SETTINGS["invalid_altitude"] = invalid_altitude
        if max_altitude is not None:
            cls.CLEANING_SETTINGS["max_altitude"] = max_altitude
        if max_trackpoints is not None:
            cls.CLEANING_SETTINGS["max_trackpoints"] = max_trackpoints

    @staticmethod
    def clean_altitude(altitude):
        try:
            alt = float(altitude)
            if (
                alt < DataPreparation.CLEANING_SETTINGS["min_altitude"]
                or alt > DataPreparation.CLEANING_SETTINGS["max_altitude"]
            ):
                return DataPreparation.CLEANING_SETTINGS["invalid_altitude"]
        except ValueError:
            return DataPreparation.CLEANING_SETTINGS["invalid_altitude"]
        return altitude

    @staticmethod
    def combine_datetime(date_str, time_str):
        try:
            datetime_obj = datetime.strptime(
                f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S"
            )
            return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            logging.error(f"Date conversion error: {e}")
            return None

    @staticmethod
    def is_duplicate(datetime_str, unique_datetimes):
        if datetime_str in unique_datetimes:
            return True
        unique_datetimes.add(datetime_str)
        return False

    @staticmethod
    def process_file(file_path):
        """
        Processes a single .plt file to clean trackpoints by fixing altitude, combining date/time,
        and removing duplicates. Writes cleaned data back to the file if processing occurs.

        Args:
            file_path (str): Path to the .plt file.

        Returns:
            tuple: A tuple containing:
                - int: count of deleted (duplicate) trackpoints
                - int: count of updated trackpoints with modified altitudes
                - bool: True if file should be deleted (exceeds trackpoint limit), False otherwise
        """
        with open(file_path, "r") as f:
            # Check if the file has already been processed
            first_line = f.readline().strip()
            if first_line != "Geolife trajectory":
                logging.info(f"File {file_path} already processed; skipping.")
                return 0, 0, False

            lines = f.readlines()

        # Skip header lines and check trackpoint count
        trackpoints = lines[5:]
        if len(trackpoints) > DataPreparation.CLEANING_SETTINGS["max_trackpoints"]:
            return 0, 0, True

        unique_datetimes = set()
        cleaned_trackpoints = []
        num_deleted = 0
        num_updated = 0

        for tp in trackpoints:
            data = tp.strip().split(",")
            if len(data) >= 7:
                lat, lon, altitude, date_str, time_str = (
                    data[0],
                    data[1],
                    data[3],
                    data[5],
                    data[6],
                )

                original_altitude = altitude
                altitude = DataPreparation.clean_altitude(altitude)
                if altitude != original_altitude:
                    num_updated += 1

                datetime_str = DataPreparation.combine_datetime(date_str, time_str)
                if datetime_str is None:
                    continue

                if DataPreparation.is_duplicate(datetime_str, unique_datetimes):
                    num_deleted += 1
                    continue

                cleaned_trackpoints.append(f"{lat},{lon},{altitude},{datetime_str}\n")

        with open(file_path, "w") as f:
            f.writelines(cleaned_trackpoints)
        logging.info(f"File {file_path} cleaned and written back.")

        return num_deleted, num_updated, False

    @staticmethod
    def process_trackpoints(datapath):
        """
        Processes all .plt files in the dataset directory to clean trackpoints.
        It removes duplicates, fixes altitude values, and deletes files with excessive trackpoints.

        Args:
            datapath (str): Path to the dataset directory.

        Logs:
            Summary of total deleted, updated, and removed files due to trackpoint limits.
        """
        logging.info("Processing trackpoints for cleaning...")
        print("Processing trackpoints for cleaning...")
        total_deleted = 0
        total_updated = 0
        total_removed_files = 0

        for root, dirs, files in os.walk(datapath):
            if os.path.basename(root) == "Trajectory":
                for file in files:
                    if file.lower().endswith(".plt"):
                        file_path = os.path.join(root, file)

                        num_deleted, num_updated, should_delete = (
                            DataPreparation.process_file(file_path)
                        )

                        if should_delete:
                            os.remove(file_path)
                            total_removed_files += 1
                            logging.info(
                                f"File deleted due to excessive trackpoints: {file_path}"
                            )
                            continue
                        else:
                            total_deleted += num_deleted
                            total_updated += num_updated
                            logging.info(
                                f"File: {file_path} - Deleted: {num_deleted}, Updated: {num_updated}"
                            )
            else:
                logging.info(f"No 'Trajectory' folder in: {root}")

        logging.info(f"Total deleted trackpoints: {total_deleted}")
        logging.info(f"Total updated trackpoints: {total_updated}")
        logging.info(
            f"Total removed files due to excessive trackpoints: {total_removed_files}"
        )
