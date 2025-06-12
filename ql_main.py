"""
任务名称
name: 糖豆广场舞视频下载
定时规则
cron: 1 9 * * *
"""
# 配置文件自己在目录中创建 vid.txt 文件，一行一个vid

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import re
import threading
import time
from queue import Queue

import requests
from requests.exceptions import ChunkedEncodingError, RequestException
# from moviepy.editor import *

import tangdou
from get_vid import get_vid_set
from headers import headers

import signal

downloading = False

DEFAULT_PATH = "/my_videos/糖豆广场舞"
# 默认下载vid集合配置，一行一个vid。
# 如果是项目目录下的vid.txt文件，这里可以留空
DEFAULT_VID_FILE = "/ql/data/config/vid.txt"

# 获取当前脚本所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 年份作为下级目录
current_year = str(time.localtime().tm_year)

message_content = []
def send_notify():
    QLAPI.notify("糖豆广场舞视频下载", "\n".join(message_content)) # type: ignore

def downloader(name, url, path, max_retries=3, retry_delay=5):
    global downloading
    if not os.path.exists(path):
        raise ValueError("'{}' does not exist".format(path))
    
    filepath = os.path.join(path, name + ".mp4")
    
    # 检查文件是否已完整存在
    if os.path.exists(filepath):
        try:
            header = headers(url).buildHeader()
            response = requests.head(url, headers=header)
            if response.status_code == 200:
                content_size = int(response.headers.get("content-length", 0))
                if os.path.getsize(filepath) == content_size:
                    msg = f"{name}.mp4 already exists"
                    print(msg)
                    message_content.append(msg)
                    return
        except RequestException:
            pass  # 如果HEAD请求失败，继续正常下载流程
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            start = time.time()
            header = headers(url).buildHeader()
            response = requests.get(url, headers=header, stream=True)
            size = 0
            chunk_size = 1024 * 1024
            content_size = int(response.headers["content-length"])
            
            if response.status_code == 200:
                temp_filepath = filepath + ".temp"
                with open(temp_filepath, "wb") as file:
                    print(f"{name}.mp4 {content_size / 1024 / 1024:.2f}MB downloading... (attempt {retry_count + 1}/{max_retries})")
                    downloading = True
                    for data in response.iter_content(chunk_size=chunk_size):
                        file.write(data)
                        size += len(data)
                    downloading = False
                
                # 下载完成后重命名临时文件
                if os.path.exists(temp_filepath):
                    if os.path.exists(filepath):
                        os.remove(filepath)  # 删除旧文件（如果有）
                    os.rename(temp_filepath, filepath)
                    end = time.time()
                    
                    # print(f"{name}.mp4 download completed, time: {end - start:.2f}s")
                    msg = f"{name}.mp4 download completed, time: {end - start:.2f}s"
                    print(msg)
                    message_content.append(msg)
                    return
                else:
                    raise OSError("Download error, temp file does not exist")
            else:
                raise RuntimeError(f"Request error, status code: {response.status_code}")
                
        except (ChunkedEncodingError, RequestException, OSError) as e:
            retry_count += 1
            if retry_count < max_retries:
                print(f"Error occurred: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                # 删除可能损坏的临时文件
                temp_filepath = filepath + ".temp"
                if os.path.exists(temp_filepath):
                    try:
                        os.remove(temp_filepath)
                    except OSError:
                        pass
            else:
                raise RuntimeError(f"Failed after {max_retries} attempts. Last error: {str(e)}")
        
def download_video(q: Queue):
    while True:
        video_info = q.get()
        vid = video_info.pop("vid")
        try:
            name = video_info["name"]
            urls = video_info["urls"]
            path = video_info["path"]
            hd = list(urls)[0]          # Download highest definition
            name = name + "_" + hd
            url = urls[hd]
            downloader(name, url, path)
        except Exception as e:
            print(f"exception: {e}")
            print(f"{vid} {video_info['name']}.mp4 下载失败！")
        q.task_done()

download_queue = Queue()

def batch_download_vid(vid_set, max_threads=5):
    global download_queue
    
    path = os.path.join(DEFAULT_PATH, current_year)
    if not os.path.exists(path):  # Create the directory if it does not exist
        os.mkdir(path)
        
    td = tangdou.VideoAPI()
    download_queue = Queue()
    for _ in range(max_threads):  # Use 5 threads
        t = threading.Thread(target=download_video, args=(download_queue,))
        t.daemon = True
        t.start()
    for vid in vid_set:
        try:
            video_info = td.get_video_info(vid)
            video_info["path"] = path
            video_info["vid"] = vid
            download_queue.put(video_info)
        except (ValueError, RuntimeError) as e:
            print(e)
            print(f"vid: {vid} 下载失败！")
            continue
    download_queue.join()


if __name__ == "__main__":
    print("===================糖豆视频下载器 By CCBP===================")
    print(f"当前目录 {current_dir}")
    print("============================================================")
    
    vid_file = os.path.join(current_dir, "vid.txt")
    if DEFAULT_VID_FILE:
        vid_file = DEFAULT_VID_FILE
    
    # 判断 json_dir 下是否有 vid.txt 这个文件
    if os.path.exists(vid_file):
        with open(vid_file, "r") as f:
            vid_set = set(map(int, f.read().splitlines()))
            if vid_set:
                batch_download_vid(vid_set)
                send_notify()
            else:
                print(f"{vid_file} 文件为空！")
    