import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import cv2
from datetime import datetime
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


def create_video_from_images(image_folder, folder_video_path, output_video, fps=10):
    try:
        images = [
            img
            for img in os.listdir(image_folder)
            if img.endswith(".jpg") or img.endswith(".png")
        ]
        images.sort()  # เรียงลำดับไฟล์ตามชื่อ
        if not images:
            print("ไม่มีรูปภาพในโฟลเดอร์")
            return
    except:
        print("ไม่มีรูปภาพในโฟลเดอร์")
        return
    # ใช้รูปภาพแรกเพื่อกำหนดขนาดของวิดีโอ
    first_image_path = os.path.join(image_folder, images[0])
    frame = cv2.imread(first_image_path)
    height, width, layers = frame.shape

    # กำหนด codec และสร้าง video writer
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")  # คุณสามารถใช้ "XVID" หรือ "MJPG" ได้
    if not os.path.exists(folder_video_path):
        os.makedirs(folder_video_path)
    output_video = folder_video_path + "/" + output_video
    video = cv2.VideoWriter(output_video, fourcc, fps, (width, height))

    # วนลูปผ่านรูปภาพและเขียนเป็นเฟรมลงวิดีโอ\
    i = 0
    for image in images:
        i += 1
        image_path = os.path.join(image_folder, image)
        frame = cv2.imread(image_path)
        video.write(frame)

        print(f"{i}/{len(images)}")

    video.release()
    print(f"สร้างวิดีโอสำเร็จ: {output_video}")


current_date = datetime.now().strftime("%Y%m%d")
image_folder = f"/usr/local/spark/assets/data/koi_images/{current_date}"
folder_video_path = f"/usr/local/spark/assets/data/koi_video/{current_date}"
output_video = f"output_{current_date}.mp4"
create_video_from_images(image_folder, folder_video_path, output_video)
