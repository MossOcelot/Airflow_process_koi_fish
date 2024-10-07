import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import cv2
import requests
import time
from datetime import datetime, timedelta
import numpy as np
import os

# Create spark session
spark = SparkSession.builder.getOrCreate()

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

# URL of the real-time video (multipart stream)
image_url = "http://172.30.81.159:5000/video_feed"

end_time = 18  # hours

def save_image_from_stream(url, interval=0.25, max_retries=5):
    current_date = datetime.now().strftime("%Y%m%d")
    print("current_date:", current_date)
    folder_path = f"/usr/local/spark/assets/data/koi_images/{current_date}"
    
    # Create the initial folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    retry_count = 0  # Initialize retry counter

    while (datetime.now() + timedelta(hours=7)).hour < end_time and (
        datetime.now() + timedelta(hours=7)
    ).hour > 6:
        try:
            # Connect to the stream
            stream = requests.get(
                url, stream=True, timeout=5
            )  # Set a timeout for the request

            if stream.status_code == 404:
                retry_count += 1
                print(f"Stream returned 404. Retry {retry_count}/{max_retries}")
                if retry_count >= max_retries:
                    print("Max retries reached. Exiting.")
                    break
                time.sleep(1)  # Wait before retrying
                continue
            elif stream.status_code != 200:
                print(
                    f"Failed to connect to the stream, status code: {stream.status_code}"
                )
                time.sleep(5)  # Wait before retrying
                continue

            byte_data = b""
            for chunk in stream.iter_content(chunk_size=1024):
                byte_data += chunk

                # Extract frame from multipart stream boundary
                a = byte_data.find(b"\xff\xd8")  # Start of JPEG image
                b = byte_data.find(b"\xff\xd9")  # End of JPEG image

                if a != -1 and b != -1:
                    jpg = byte_data[a : b + 2]
                    byte_data = byte_data[b + 2 :]

                    # Convert the data to an image
                    image = cv2.imdecode(
                        np.frombuffer(jpg, dtype=np.uint8), cv2.IMREAD_COLOR
                    )
                    if image is not None:
                        # Check if the date has changed
                        new_date = datetime.now().strftime("%Y%m%d")
                        if new_date != current_date:
                            # Update the current date and create a new folder
                            current_date = new_date

                            os.makedirs(folder_path, exist_ok=True)

                        # Generate the filename with a timestamp
                        timestamp = datetime.now().strftime("%H%M%S_%f")
                        filename = f"{folder_path}/image_{timestamp}.jpg"

                        # Save the image
                        cv2.imwrite(filename, image)
                        print(f"Saved {filename}")
                    else:
                        print("Failed to decode the image")

            retry_count = 0  # Reset retry count on successful connection
            time.sleep(interval)

        except requests.exceptions.RequestException as req_err:
            print(f"Request error: {req_err}. Retrying...")
            retry_count += 1
            if retry_count >= max_retries:
                print("Max retries reached. Exiting.")
                break
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)  # Wait before retrying

    print("TeeesT", (datetime.now() + timedelta(hours=7)).hour)
while True:
    now = datetime.now() + timedelta(hours=7)
    print("#########################################")
    print(f"This time {now}  : H {now.hour} , M {now.minute}")
    print("#########################################")
    if now.hour >= end_time:
        print("#########################################")
        print("It's 6 PM or later. Breaking the process.")
        print("#########################################")

        break
    save_image_from_stream(image_url, interval=1)
