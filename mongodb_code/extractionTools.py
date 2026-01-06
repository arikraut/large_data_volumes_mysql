import os


# Used for extracting example values for the report
# Amount of files in the dataset for a specific user, longest files etc.
def find_largest_file(directory):
    largest_file = None
    largest_size = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            if file_size > largest_size:
                largest_size = file_size
                largest_file = file_path
    return largest_file


def linesLongestFile(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()
        return len(lines)


directory = "datasets/valid_dataset/Data"
largest_file = find_largest_file(directory)
if largest_file is not None:
    print(f"The largest file in {directory} is: {largest_file}")
    print("The amount of lines is:")
    print(linesLongestFile(largest_file))

else:
    print(f"No files found in {directory}")


def countActivities(directory):
    user_file_counts = {}

    for root, dirs, files in os.walk(directory):
        count_of_files = 0
        for file in files:
            if file.endswith(".plt"):
                count_of_files += 1

        if count_of_files > 0:
            user = root.split("/")[-1]
            if user in user_file_counts:
                user_file_counts[user] += count_of_files
            else:
                user_file_counts[user] = count_of_files

    # Find the user with the most .plt files
    max_user = None
    max_count = 0
    for user, count in user_file_counts.items():
        if count > max_count:
            max_user = user
            max_count = count

    return max_user, max_count


# Example usage
directory = "datasets/valid_dataset/Data"
print(countActivities(directory))
"""""
The largest file in datasets/valid_dataset/Data is: datasets/valid_dataset/Data\153\Trajectory\20111003091622.plt
The amount of lines is:
2455
('Data\\128\\Trajectory', 2102)
""" ""
