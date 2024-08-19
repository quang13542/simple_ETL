import threading
import requests
import os

def download_part(url, start, end, part_number, output_file):
    headers = {'Range': f'bytes={start}-{end}'}
    response = requests.get(url, headers=headers, stream=True)
    
    with open(f"{output_file}.part{part_number}", "wb") as file:
        file.write(response.content)

def combine_parts(output_file, parts):
    with open(output_file, "wb") as output:
        for part_number in range(parts):
            with open(f"{output_file}.part{part_number}", "rb") as part_file:
                output.write(part_file.read())
            os.remove(f"{output_file}.part{part_number}")  # Clean up the part file

def multi_threaded_download(url, output_file, num_threads=4):
    response = requests.head(url)
    file_size = int(response.headers['Content-Length'])
    
    part_size = file_size // num_threads
    
    threads = []
    for i in range(num_threads):
        start = i * part_size
        end = start + part_size - 1 if i < num_threads - 1 else file_size - 1
        
        thread = threading.Thread(target=download_part, args=(url, start, end, i, output_file))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    combine_parts(output_file, num_threads)

