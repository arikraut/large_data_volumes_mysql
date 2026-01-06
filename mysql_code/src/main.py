import logging
from databaseManager import DatabaseManager
from prepareData import DataPreparation
from tabulate import tabulate


def main():
    dataset_path = "dataset_sample/Data"
    insert_data = True
    do_query = True

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Starting the data preparation and database insertion process...")

    if insert_data:
        dp = DataPreparation()
        dp.clean_data(dataset_path)
        dp.fix_negative_alt(dataset_path)
        users = dp.prepare_users(dataset_path)
        db_manager = None

        try:
            db_manager = DatabaseManager()
            db_manager.create_tables()
            db_manager.show_tables()
            db_manager.insert_users(users)

            users = db_manager.get_users()
            logging.info(f"Retrieved {len(users)} users from the database")

            for user in users:
                db_manager.insert_activities(dataset_path, user)

            for user in users:
                user_id = user[0]
                activities = db_manager.get_activity_for_user(
                    user_id, ["id", "start_date_time"]
                )
                for activity_id, start_date_time in activities:
                    db_manager.insert_trackpoints(
                        dataset_path, user_id, activity_id, start_date_time
                    )

        except Exception as e:
            logging.error(f"ERROR: Failed to use database: {e}")
        finally:
            if db_manager is not None:
                db_manager.connection.close_connection()
                logging.info("Database connection closed.")

    else:
        logging.warning("No users found to insert into the database.")

    if do_query:
        db_manager = DatabaseManager()
        with open("results.txt", "w") as report_file:

            for table_name in ["User", "Activity", "TrackPoint"]:
                rows = db_manager.get_first_rows(table_name)
                if rows:
                    print(f"First 10 rows of the {table_name} table:", file=report_file)
                    column_names = [desc[0] for desc in db_manager.cursor.description]
                    print(
                        tabulate(rows, headers=column_names, tablefmt="grid"),
                        file=report_file,
                    )
                else:
                    print(
                        f"No data found in the {table_name} table or an error occurred.",
                        file=report_file,
                    )
                print("\n", file=report_file)

            # Query 1: Count of entities
            counts = db_manager.get_counts()
            print(
                tabulate(counts, headers=["Entity", "Count"], tablefmt="grid"),
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 2: Average number of activities per user
            avg_activity = db_manager.get_average_activities_per_user()
            print(
                f"Average number of activities per user: {avg_activity}",
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 3: Top 20 users by activity count
            top_activity = db_manager.get_top_users_by_activity(20)
            print("Top 20 users by activity count:", file=report_file)
            print(
                tabulate(
                    top_activity, headers=["User ID", "Activity Count"], tablefmt="grid"
                ),
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 4: Users who have used a taxi
            users_transport = db_manager.get_users_by_transport_mode("taxi")
            print("Users who have used taxi as transport mode:", file=report_file)
            print(users_transport, file=report_file)
            print("\n", file=report_file)

            # Query 5: Transportation modes and their counts
            transport_mode_counts = db_manager.get_transport_modes_with_counts()
            print(
                tabulate(
                    transport_mode_counts,
                    headers=["Transportation Mode", "Activity Count"],
                    tablefmt="grid",
                ),
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 6: Year with the most activities and the most hours
            most_active_year = db_manager.get_year_with_most_activities()
            most_hours_year = db_manager.get_year_with_most_hours()
            if most_active_year and most_hours_year:
                print(
                    f"Year with the most activities: {most_active_year[0]} with {most_active_year[1]} activities.",
                    file=report_file,
                )
                print(
                    f"Year with the most recorded hours: {most_hours_year[0]} with {most_hours_year[1]} hours.",
                    file=report_file,
                )
                if most_active_year[0] == most_hours_year[0]:
                    print(
                        "Yes, the year with the most activities is also the year with the most recorded hours.",
                        file=report_file,
                    )
                else:
                    print(
                        "No, the year with the most activities is different from the year with the most recorded hours.",
                        file=report_file,
                    )
            print("\n", file=report_file)

            # Query 7: Total distance walked by a specific user in 2008
            total_distance = db_manager.get_total_distance_walked("112", 2008)
            print(
                f"Total distance walked by user 112 in 2008: {total_distance} kilometers.",
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 8: Top 20 users by altitude gain
            top_users_altitude_gain = db_manager.get_top_users_by_altitude_gain(20)
            print("Top 20 users by total altitude gain:", file=report_file)
            print(
                tabulate(
                    top_users_altitude_gain,
                    headers=["User ID", "Total Altitude Gain (meters)"],
                    tablefmt="grid",
                ),
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 9: Users with invalid activities
            sorted_invalid_users = db_manager.get_users_with_invalid_activities_count()
            print(
                "Users with invalid activities sorted by invalid count:",
                file=report_file,
            )
            print(
                tabulate(
                    sorted_invalid_users,
                    headers=["User ID", "Invalid Activity Count"],
                    tablefmt="grid",
                ),
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 10: Users who have visited the Forbidden City
            users_in_forbidden_city = db_manager.get_users_in_forbidden_city()
            print("Users who have visited the forbidden city:", file=report_file)
            print(
                tabulate(
                    enumerate(users_in_forbidden_city, start=1),
                    headers=["#", "User ID"],
                    tablefmt="grid",
                ),
                file=report_file,
            )
            print("\n", file=report_file)

            # Query 11: Users with their most used transportation mode
            results = db_manager.find_users_with_most_used_transportation()
            print("Users with their most used transportation mode:", file=report_file)
            print(
                tabulate(
                    results,
                    headers=["User ID", "Most Used Transportation Mode"],
                    tablefmt="grid",
                ),
                file=report_file,
            )
            print("\n", file=report_file)


if __name__ == "__main__":
    main()
