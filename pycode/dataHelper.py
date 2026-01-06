from datetime import datetime


class DataHelper:
    @staticmethod
    def get_start_end_time(file_path):
        """
        Extracts the start and end time from a given file.

        Args:
            file_path (str): The file to extract the times from.

        Returns:
            tuple: A tuple containing:
                - start_time (datetime): The start date and time.
                - end_time (datetime): The end date and time.
        """
        with open(file_path, "r") as f:
            lines = f.readlines()
            # Skip the header lines (first 6 lines)
            data_lines = lines[6:]

            if data_lines:
                # Extract start time (from the first data line)
                first_line = data_lines[0].strip().split(",")
                start_date = first_line[5]
                start_time_str = first_line[6]
                start_time = f"{start_date} {start_time_str}"

                # Extract end time (from the last data line)
                last_line = data_lines[-1].strip().split(",")
                end_date = last_line[5]
                end_time_str = last_line[6]
                end_time = f"{end_date} {end_time_str}"

                # Convert strings to datetime objects
                start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

                return start_time, end_time
            else:
                raise ValueError("File contains no data lines")

    @staticmethod
    def build_label_lookup(label_file):
        """
        Builds a lookup table from the labels.txt file for quick access, converting times to datetime objects.

        Args:
            label_file (str): The path to the labels.txt file.

        Returns:
            dict: A dictionary where keys are (start_time, end_time) tuples and values are transportation modes.
        """
        label_lookup = {}
        with open(label_file, "r") as f:
            lines = f.readlines()
            for line in lines[1:]:  # Skip header
                line = line.strip().split("\t")
                label_start_time_str = line[0]
                label_end_time_str = line[1]
                mode = line[2]

                # Convert both start and end times to datetime objects
                label_start_time = datetime.strptime(
                    label_start_time_str, "%Y/%m/%d %H:%M:%S"
                )
                label_end_time = datetime.strptime(
                    label_end_time_str, "%Y/%m/%d %H:%M:%S"
                )

                # Store the label information in the lookup dictionary
                label_lookup[(label_start_time, label_end_time)] = mode

        return label_lookup

    @staticmethod
    def extract_trackpoints(file_path, activity_id):
        """
        Extracts trackpoints from the provided file.

        Args:
            file_path (str): The path to the .plt file.
            activity_id (int): The ID of the activity to associate with the trackpoints.

        Returns:
            list: A list of tuples representing trackpoints for bulk insertion.
        """
        trackpoints = []
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
                data_lines = lines[6:]  # Skip header lines

                for line in data_lines:
                    line = line.strip().split(",")
                    lat = float(line[0])
                    lon = float(line[1])
                    altitude = int(float(line[3]))
                    date_time_str = f"{line[5]} {line[6]}"
                    date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")

                    # Append trackpoint tuple
                    trackpoints.append((activity_id, lat, lon, altitude, date_time))
            return trackpoints

        except FileNotFoundError:
            raise FileNotFoundError(f"Trackpoint file not found: {file_path}")
        except Exception as e:
            raise Exception(f"Error extracting trackpoints from {file_path}: {str(e)}")
