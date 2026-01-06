import logging
from tabulate import tabulate


def execute_queries_and_save_results(db_manager):
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    with open("results.txt", "w") as report_file:
        for table_name in ["User", "Activity", "TrackPoint"]:
            logging.info(f"Querying first rows from {table_name} collection.")
            try:
                rows = db_manager.get_first_rows(table_name)
                if rows and isinstance(rows, list) and isinstance(rows[0], dict):
                    print(f"First 10 rows of the {table_name} table:", file=report_file)
                    print(
                        tabulate(rows, headers="keys", tablefmt="grid"),
                        file=report_file,
                    )
                    logging.info(f"First 10 rows from {table_name}: {rows}")
                else:
                    logging.warning(f"No valid data in {table_name} collection.")
                    print(
                        f"No data found in the {table_name} table or an error occurred.",
                        file=report_file,
                    )
            except Exception as e:
                logging.error(
                    f"Error fetching first rows from {table_name} collection: {str(e)}"
                )
            print("\n", file=report_file)

        # Query 1: Count of entities
        logging.info("Running entity count query.")
        try:
            counts = db_manager.get_counts()
            if counts and isinstance(counts, list):
                print(
                    tabulate(counts, headers=["Entity", "Count"], tablefmt="grid"),
                    file=report_file,
                )
                logging.info(f"Entity counts: {counts}")
            else:
                logging.warning("No valid data returned for entity counts.")
                print("No data available for entity counts.", file=report_file)
        except Exception as e:
            logging.error(f"Error during entity count query: {str(e)}")
        print("\n", file=report_file)

        # Query 2: Average number of activities per user
        logging.info("Calculating average number of activities per user.")
        try:
            avg_activity = db_manager.get_average_activities_per_user()
            print(
                f"Average number of activities per user: {avg_activity}",
                file=report_file,
            )
            logging.info(f"Average activities per user: {avg_activity}")
        except Exception as e:
            logging.error(f"Error calculating average activities per user: {str(e)}")
        print("\n", file=report_file)

        # Query 3: Top 20 users by activity count
        logging.info("Fetching top 20 users by activity count.")
        try:
            top_activity = db_manager.get_top_users_by_activity(20)
            if (
                top_activity
                and isinstance(top_activity, list)
                and isinstance(top_activity[0], dict)
            ):
                print("Top 20 users by activity count:", file=report_file)
                print(
                    tabulate(top_activity, headers="keys", tablefmt="grid"),
                    file=report_file,
                )
                logging.info(f"Top 20 users by activity count: {top_activity}")
            else:
                logging.warning("No valid data for top 20 users by activity count.")
                print(
                    "No valid data for top 20 users by activity count.",
                    file=report_file,
                )
        except Exception as e:
            logging.error(f"Error fetching top 20 users by activity count: {str(e)}")
        print("\n", file=report_file)

        # Query 4: Users who have used a taxi
        logging.info("Fetching users who used 'taxi' as transportation mode.")
        try:
            users_transport = db_manager.get_users_by_transport_mode("taxi")
            if users_transport:
                print("Users who have used taxi as transport mode:", file=report_file)
                print(
                    tabulate(
                        [{"user_id": user_id} for user_id in users_transport],
                        headers="keys",
                        tablefmt="grid",
                    ),
                    file=report_file,
                )
                logging.info(
                    f"Users who used taxi as transport mode: {users_transport}"
                )
            else:
                logging.warning("No users found with taxi as transport mode.")
                print("No users found with taxi as transport mode.", file=report_file)
        except Exception as e:
            logging.error(f"Error fetching users by transport mode 'taxi': {str(e)}")
        print("\n", file=report_file)

        # Query 5: Transportation modes and their counts
        logging.info("Fetching counts for each transportation mode.")
        try:
            transport_mode_counts = db_manager.get_transport_modes_with_counts()
            if (
                transport_mode_counts
                and isinstance(transport_mode_counts, list)
                and isinstance(transport_mode_counts[0], dict)
            ):
                print("Transportation modes with activity counts:", file=report_file)
                print(
                    tabulate(transport_mode_counts, headers="keys", tablefmt="grid"),
                    file=report_file,
                )
                logging.info(
                    f"Transportation modes and counts: {transport_mode_counts}"
                )
            else:
                logging.warning("No valid data for transportation modes with counts.")
                print(
                    "No data available for transportation modes with counts.",
                    file=report_file,
                )
        except Exception as e:
            logging.error(f"Error fetching transportation modes and counts: {str(e)}")
        print("\n", file=report_file)

        # Query 6: Year with the most activities and the most hours
        logging.info("Fetching years with the most activities and hours.")
        try:
            most_active_year = db_manager.get_year_with_most_activities()
            most_hours_year = db_manager.get_year_with_most_hours()
            if most_active_year and most_hours_year:
                print(
                    f"Year with the most activities: {most_active_year['year']} with {most_active_year['activity_count']} activities.",
                    file=report_file,
                )
                print(
                    f"Year with the most recorded hours: {most_hours_year['year']} with {most_hours_year['total_hours']} hours.",
                    file=report_file,
                )
                logging.info(f"Year with most activities: {most_active_year}")
                logging.info(f"Year with most recorded hours: {most_hours_year}")
                if most_active_year["year"] == most_hours_year["year"]:
                    print(
                        "Yes, the year with the most activities is also the year with the most recorded hours.",
                        file=report_file,
                    )
                else:
                    print(
                        "No, the year with the most activities is different from the year with the most recorded hours.",
                        file=report_file,
                    )
            else:
                logging.warning("Data missing for years with most activities or hours.")
                print(
                    "No data available for year with most activities or hours.",
                    file=report_file,
                )
        except Exception as e:
            logging.error(f"Error fetching year with most activities/hours: {str(e)}")
        print("\n", file=report_file)

        # Query 7: Total distance walked by a specific user in 2008
        logging.info("Calculating total distance walked by user 112 in 2008.")
        try:
            total_distance = db_manager.get_total_distance_walked("112", 2008)
            print(
                f"Total distance walked by user 112 in 2008: {total_distance} kilometers.",
                file=report_file,
            )
            logging.info(
                f"Total distance walked by user 112 in 2008: {total_distance} km"
            )
        except Exception as e:
            logging.error(
                f"Error calculating total distance walked by user 112 in 2008: {str(e)}"
            )
        print("\n", file=report_file)

        # Query 8: Top 20 users by altitude gain
        logging.info("Fetching top 20 users by altitude gain.")
        try:
            top_users_altitude_gain = db_manager.get_top_users_by_altitude_gain(20)
            if (
                top_users_altitude_gain
                and isinstance(top_users_altitude_gain, list)
                and isinstance(top_users_altitude_gain[0], dict)
            ):
                print("Top 20 users by total altitude gain:", file=report_file)
                print(
                    tabulate(top_users_altitude_gain, headers="keys", tablefmt="grid"),
                    file=report_file,
                )
                logging.info(
                    f"Top 20 users by altitude gain: {top_users_altitude_gain}"
                )
            else:
                logging.warning("No valid data for top users by altitude gain.")
                print(
                    "No data available for top users by altitude gain.",
                    file=report_file,
                )
        except Exception as e:
            logging.error(f"Error fetching top users by altitude gain: {str(e)}")
        print("\n", file=report_file)

        # Query 9: Users with invalid activities
        logging.info("Fetching users with invalid activities.")
        try:
            sorted_invalid_users = db_manager.get_users_with_invalid_activities_count()
            if (
                sorted_invalid_users
                and isinstance(sorted_invalid_users, list)
                and isinstance(sorted_invalid_users[0], dict)
            ):
                print(
                    "Users with invalid activities sorted by invalid count:",
                    file=report_file,
                )
                print(
                    tabulate(sorted_invalid_users, headers="keys", tablefmt="grid"),
                    file=report_file,
                )
                logging.info(f"Users with invalid activities: {sorted_invalid_users}")
            else:
                logging.warning("No valid data for users with invalid activities.")
                print(
                    "No data available for users with invalid activities.",
                    file=report_file,
                )
        except Exception as e:
            logging.error(f"Error fetching users with invalid activities: {str(e)}")
        print("\n", file=report_file)

        # Query 10: Users who have visited the Forbidden City
        logging.info("Checking for users in Forbidden City.")
        try:
            users_in_forbidden_city = db_manager.get_users_in_forbidden_city()
            if users_in_forbidden_city:
                print("Users who have visited the Forbidden City:", file=report_file)
                print(
                    tabulate(
                        [{"user_id": user_id} for user_id in users_in_forbidden_city],
                        headers="keys",
                        tablefmt="grid",
                    ),
                    file=report_file,
                )
                logging.info(
                    f"Users who visited Forbidden City: {users_in_forbidden_city}"
                )
            else:
                logging.warning("No users found in Forbidden City.")
                print("No users found in the Forbidden City.", file=report_file)
        except Exception as e:
            logging.error(f"Error fetching users in Forbidden City: {str(e)}")
        print("\n", file=report_file)

        # Query 11: Users with their most used transportation mode
        logging.info("Fetching users' most used transportation modes.")
        try:
            results = db_manager.find_users_with_most_used_transportation()
            if results and isinstance(results, list) and isinstance(results[0], dict):
                print(
                    "Users with their most used transportation mode:", file=report_file
                )
                print(
                    tabulate(results, headers="keys", tablefmt="grid"), file=report_file
                )
                logging.info(f"Users with most used transportation mode: {results}")
            else:
                logging.warning(
                    "No data available for users' most used transportation mode."
                )
                print(
                    "No data available for users' most used transportation mode.",
                    file=report_file,
                )
        except Exception as e:
            logging.error(
                f"Error fetching users' most used transportation mode: {str(e)}"
            )
        print("\n", file=report_file)

    db_manager.close_connection()
