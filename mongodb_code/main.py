import logging
from mongodb_code.databaseManager import DatabaseManager
from mongodb_code.prepareData import DataPreparation
from mongodb_code.queryExecutor import execute_queries_and_save_results


def main():
    dataset_path = "dataset_sample/dataset/Data"
    clean_data = False
    insert_data = True
    do_query = True

    db_params = {
        "DATABASE": "exc3db",
        "HOST": "localhost:27017",  # Use "localhost:27017" for local
        "USER": "local_host_user",
        "PASSWORD": "oppgave3",
        "use_authentication": False,  # Set to False for local
    }

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Starting the data preparation and database insertion process...")

    if clean_data:
        dp = DataPreparation()
        dp.process_trackpoints(dataset_path)

    if insert_data:
        db_manager = DatabaseManager(**db_params)
        try:
            db_manager.process_user_activities_and_trackpoints(dataset_path)
            logging.info(f"Users data inserted successfully into the database.")
        except Exception as e:
            logging.error(f"ERROR: Failed to insert data into the database: {e}")
        finally:
            db_manager.close_connection()
            logging.info("Database connection closed.")

    if do_query:
        db_manager = DatabaseManager(**db_params)
        try:
            execute_queries_and_save_results(db_manager)
            logging.info("Queries executed successfully.")
        except Exception as e:
            logging.error(f"ERROR: Failed to execute queries: {e}")
        finally:
            db_manager.close_connection()
            logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
