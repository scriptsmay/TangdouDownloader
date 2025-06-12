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

DEFAULT_PATH = "F:\\Media\\视频下载\\downloader"

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
                    print(f"{name}.mp4 already exists")
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
                    print(f"{name}.mp4 download completed, time: {end - start:.2f}s")
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
        

def separate_download():
    while True:
        url = input("请输入视频链接或vid编号:")
        vid = tangdou.get_vid(url)
        if vid is None:
            print("请输入包含vid参数的视频链接或直接输入vid编号！")
        else:
            td = tangdou.VideoAPI()
            try:
                video_info = td.get_video_info(vid)
            except (ValueError, RuntimeError) as e:
                print(e)
                print("请重试！")
                continue
            else:  # Successfully obtained video information
                break
    
    urls_dict = video_info["urls"]
    if not bool(urls_dict):
            raise RuntimeError("URL列表为空！")
    print("检测到以下可选清晰度：")
    urls_list = list(enumerate(urls_dict))
    for url in urls_list:
        print(f"[{url[0]}] {url[1]}")
    while True:
        # index = input("请选择需要下载的清晰度(默认为全部下载):").replace("，", ",").split(",")
        index = ["0"]
        selected_urls = dict()
        if index == ['']:
            selected_urls = urls_dict.copy()
            break
        else:
            for i in index:
                i = str(i)
                if i.isdigit():
                    i = int(i)
                    if i >= len(urls_list):
                        print(f"序号 '{i}' 不存在，请重新输入！")
                        break
                    selected_urls[urls_list[i][1]] = urls_dict[urls_list[i][1]]
                else:
                    print("请输入清晰度序号，多选以逗号分隔！")
                    break
            else:
                break

    # path = input("请输入文件储存目录(默认为当前目录):")
    path = DEFAULT_PATH
    if path == "":
        path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "Download")
    if not os.path.exists(path):  # Create the directory if it does not exist
        os.mkdir(path)
    for clarity,url in selected_urls.items():
        name = video_info["name"] + "_" + clarity
        
        downloader(name, url, path)

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

def batch_download(json_dir, max_threads=5):
    global download_queue
    path = input("请输入文件储存目录(默认为当前目录):")
    if path == "":
        path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "Download")
    if not os.path.exists(path):  # Create the directory if it does not exist
        os.mkdir(path)
    vid_set = get_vid_set(json_dir)
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

def batch_download_vid(vid_set, max_threads=5):
    global download_queue
    path = DEFAULT_PATH
    
    path = os.path.join(path, "Download")
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

def signal_handler(signal, frame):
    global download_queue
    if not download_queue.empty() or downloading:
        while True:
            batch = input("下载尚未完成是否强制退出（y/n）:")
            if batch == "y" or batch == "n":
                break
            print("输入有误，请重新输入！")
        if batch == "n":
            return
    print("\n========================= 下载结束 =========================")
    time.sleep(2)
    sys.exit(0)

signal.signal(signal.SIGINT,signal_handler)

if __name__ == "__main__":
    print("===================糖豆视频下载器 By CCBP===================")
    print("     使用回车键（Enter）选择默认值，使用Ctrl+C退出程序")
    print("============================================================")
    json_dir = "DownloadList"
    vid_file = "DownloadList/vid.txt"
    # Check if the directory exists
    if os.path.isdir(json_dir):
        # 判断 json_dir 下是否有 vid.txt 这个文件
        if os.path.exists(vid_file):
            with open(vid_file, "r") as f:
                vid_set = set(map(int, f.read().splitlines()))
                if vid_set:
                    batch_download_vid(vid_set)
                else:
                    print(f"{vid_file} 文件为空！")

        # List all files in the directory
        files_in_directory = os.listdir(json_dir)
        # Filter out files that end with .json
        json_files = [file for file in files_in_directory if file.endswith('.json')]
        if json_files:
            while True:
                batch = input("检测到批量下载目录非空是否尝试批量下载（y/n）:")
                if batch == "y" or batch == "n":
                    break
                print("输入有误，请重新输入！")
            if batch == "y":
                batch_download(json_dir)

    while True:
        separate_download()
