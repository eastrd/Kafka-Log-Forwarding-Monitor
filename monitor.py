from time import sleep
import os
import kafka

file_path = 'test.log'

while True:
    try:
        with open(file_path) as f:
            file_size = 0
            while True:
                line = f.readline()
                if line:
                    # Produce the line to Kafka broker
                    print(line)
                file_status_obj = os.stat(file_path)
                if file_size < file_status_obj.st_size:
                    f.seek(0)
                file_size = file_status_obj.st_size
                sleep(1)
    except Exception as e:
        print(e)
        sleep(3)
