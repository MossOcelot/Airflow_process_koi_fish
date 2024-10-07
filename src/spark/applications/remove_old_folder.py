import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import cv2
import requests
import os
import shutil
from datetime import datetime, timedelta

# Create spark session
spark = SparkSession.builder.getOrCreate()

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]


def remove_first_folder(parent_folder):
    print("#########################################")
    print("START REMOVE")
    print("#########################################")
    # List all folders inside the parent folder and sort them by name (alphabetical)
    try:
        folders = [
            f
            for f in os.listdir(parent_folder)
            if os.path.isdir(os.path.join(parent_folder, f))
        ]

        if not folders:
            print("No folders to delete.")
            return
    except:
        print("No folders to delete.")
        return
    # Sort folders and select the first one
    folders.sort()
    first_folder = folders[0]
    folder_path = os.path.join(parent_folder, first_folder)

    # Remove the first folder
    try:
        shutil.rmtree(folder_path)
        print(f"Deleted folder: {folder_path}")
    except Exception as e:
        print(f"Failed to delete folder: {folder_path}. Error: {e}")

current_date = (datetime.now()).strftime("%Y%m%d")
print("date:",current_date)
remove_first_folder(
    parent_folder=f"/usr/local/spark/assets/data/koi_images/"
)
