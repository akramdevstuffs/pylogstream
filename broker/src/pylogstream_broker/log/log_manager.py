import time
import queue
import mmap
import os
import threading
import platform
import _io
from dataclasses import dataclass
from segment_cache import LRUCache
from utility import set_sequential_hint, checksum_verify

RETENSION = 60 # Seconds

GRACE_DELETION_TIME = 5 # Delete file after 5 seconds of being marked

SEGMENT_SIZE = 10*1024*1024 # 10MB, split the segments at 10MB size

SEG_SIZE_INC = 1024*1024 # 1MB, what which size he segments should increase

OLD_SEGMENT_CACHE_SIZE = 1000 # Number of old segments to keep in cache

# Currently not used
@dataclass
class Segment:
    f: object
    mm: mmap.mmap
    create_time: float
    filesize: int
    write_offset: int

topics_log_file = {} # (topic: [Segment, Segment1...], ...)

# release segment caches
def on_segment_evicted(key, seg: Segment):
    #File is opened
    if seg.f:
        # Close mmap and file
        if seg.mm:
            seg.mm.close()
        seg.f.close()
segmentCache = LRUCache(OLD_SEGMENT_CACHE_SIZE, on_segment_evicted)

# Only keeps the latest offset of active segment
segments_write_offset = {} # ('files/topic/seg1.txt': 0230, ...)

delete_file_queue = queue.Queue() # FIFO thread safe queue for deleting the files

LOG_FILE_DIR = 'logs'
if not os.path.isdir(LOG_FILE_DIR):
    os.makedirs(LOG_FILE_DIR)

def get_file_birthtime(st: os.stat_result) -> float:
    """os.stat_result object as input """
    if hasattr(st, "st_birthtime"):
        return st.st_birthtime  # macOS or supported Unix
    else:
        return st.st_ctime       # fallback for Windows

def load_topics_log():
    for topic in os.listdir(LOG_FILE_DIR):
        topic_dir = os.path.join(LOG_FILE_DIR,topic)
        segments = os.listdir(topic_dir)
        files = []
        for seg in segments[:-1]:
            f = open(os.path.join(topic_dir,seg), 'r')
            st = os.stat(f.fileno())
            f.close()
            files.append((f, None,get_file_birthtime(st), st.st_size, -1))
        if len(segments) == 0:
            continue
        f = open(os.path.join(topic_dir, segments[-1]), 'r+b')
        st = os.stat(f.fileno())
        mm = mmap.mmap(f.fileno(),0)
        write_offset = get_write_offset(f,mm)
        files.append((f, mm, get_file_birthtime(st), st.st_size, write_offset))
        topics_log_file[topic] = files

def get_write_offset(f, mm):
    # Find write_offset (end of log)
    write_offset = 0
    while write_offset + 4 <= mm.size():
        length_bytes = mm[write_offset:write_offset+4]
        if not length_bytes or length_bytes == b'\x00\x00\x00\x00':
            break
        msg_len = int.from_bytes(length_bytes, 'big')
        write_offset += 4+msg_len
    return write_offset

def get_topic_log(topic, offset=-1):
    # Return the segment with offset and also it's index
    # offset=-1 returns the active segment
    if topic not in topics_log_file:
        topic_dir = os.path.join(LOG_FILE_DIR, topic)
        if not os.path.isdir(topic_dir):
            os.makedirs(topic_dir)
        rollover_file(topic)
    l,r = 0, len(topics_log_file[topic])-1
    topics_list = topics_log_file[topic]
    index = r
    if offset!=-1:
        while l <= r:
            mid = l + (r-l)//2
            if (get_offset_from_filename(topics_list[mid][0].name) <= offset):
                index = mid
                l = mid + 1
            else:
                r = mid-1
    __segment = topics_list[index]
    # It's old segment
    # Get the cached segment
    if __segment[1] is None:
        cache_key = __segment[0].name
        cached_segment = segmentCache.get(cache_key)
        if cached_segment:
            # we don't need to save it in topics_log_file because it will contain f,mm as None because it's old segment
            return (cached_segment.f, cached_segment.mm, __segment[2], __segment[3], __segment[4], index)
        else:
            # Load from file
            filename = __segment[0].name
            f = open(filename, 'r+b')
            mm = mmap.mmap(f.fileno(), 0)
            # Save to cache
            segmentCache.put(cache_key, Segment(f, mm, __segment[2], __segment[3], __segment[4]))
            return (f, mm, __segment[2], __segment[3], __segment[4], index)

    return __segment + (index,) # Include index in return tuple

def rollover_file(topic):
    segment_list = []
    if topic in topics_log_file:
        segment_list = topics_log_file[topic]
    else:
        topics_log_file[topic] = []
    active_seg = None
    if len(segment_list) > 0:
        active_seg = segment_list[-1]
    # Closing active segment if it's exists and opened
    if (active_seg and active_seg[1] is not None):  # Checking if mmap is None
        """ Instead of closing it now, just put it in the cache it will get closed by itself when it will be not need.
            It will fix the race condition where the file that is in use got closed.
          """
        # active_seg[1].close()
        # active_seg[0].close()
        segmentCache.put(active_seg[0].name, Segment(active_seg[0], active_seg[1], active_seg[2], active_seg[3], active_seg[4]))
        topics_log_file[topic][-1] = (active_seg[0], None, active_seg[2], active_seg[3], active_seg[4])
    start_offset = 0
    # If there's previous segment then updating new write offset
    if active_seg:
        start_offset = active_seg[4]
    filepath = os.path.join(LOG_FILE_DIR,topic,f"{str(start_offset)}_log.txt")
    #Create file if it doesn't exist
    if not os.path.exists(filepath):
        with open(filepath, 'wb') as f:
            f.truncate(SEG_SIZE_INC) # 1MB initial size
    f = open(filepath, 'r+b')
    # Hint to OS for sequential access
    mm = mmap.mmap(f.fileno(), 0)
    set_sequential_hint(mm,f.fileno())
    __segment = (f,mm,time.time(), SEG_SIZE_INC, start_offset)
    new_segment_list = topics_log_file[topic] + [__segment]
    # Reference swap with new list
    topics_log_file[topic] = new_segment_list
    return __segment

""" Take topic, message in bytes, and checksum. Stores it and returns the result code """
# Appended message framing [msg_bytes][4 bytes hash]
def append_message(topic, msg_bytes, hash=None) -> int: # Result code
    if not msg_bytes or msg_bytes==b'':
        return 1 # Invalid message
    if hash and len(hash)!=4:
        return 3 # Invalid hash

    if hash:
        if not checksum_verify(msg_bytes, int.from_bytes(hash, 'big')):
            return 2 # Corrupted message
        # Append the hash at last of message
        msg_bytes += hash

    # We are ignoring index because list size can change while appending the message
    f,mm,create_time,filesize,write_offset, _ = get_topic_log(topic)
    file_start_offset = get_offset_from_filename(f.name)
    file_write_offset = write_offset-file_start_offset
    msg_len = len(msg_bytes)
    # Check if we need to rollover
    #Expiry check
    if (time.time() - create_time) >= RETENSION:
        f,mm,create_time,filesize,write_offset = rollover_file(topic)
        file_start_offset = get_offset_from_filename(f.name)
        file_write_offset = write_offset - file_start_offset
    #Size check
    if file_write_offset + 4 + msg_len > mm.size():
        if mm.size() + SEG_SIZE_INC <= SEGMENT_SIZE:
            # Resize file and mmap here
            f.seek(0, os.SEEK_END)
            f.write(b'\x00'*(SEG_SIZE_INC)) # Add EXTRA_SIZE
            mm.resize(mm.size() + SEG_SIZE_INC)
        else:
            # Size limit reached for the segment
            # Updating vars
            f,mm,create_time,filesize,write_offset = rollover_file(topic)
            file_start_offset = get_offset_from_filename(f.name)
            file_write_offset = write_offset - file_start_offset

    mm[file_write_offset:file_write_offset+4] = msg_len.to_bytes(4,'big')
    mm[file_write_offset+4:file_write_offset+4+msg_len] = msg_bytes
    # #Flush changes to file
    # mm.flush()
    # Updating the last element
    topics_log_file[topic][-1] = (f,mm,create_time,filesize,write_offset+4+msg_len)
    return 0 # Success

def get_oldest_offset(topic):
    if topic not in topics_log_file or len(topics_log_file[topic])==0:
        return 0
    l,r = 0, len(topics_log_file[topic])-1
    while l<r:
        mid = l + (r-l)//2
        created_time = topics_log_file[topic][mid][2]
        if created_time > time.time() - RETENSION:
            r = mid
        else:
            l = mid + 1
    return get_offset_from_filename(topics_log_file[topic][l][0].name)

def get_latest_offset(topic):
    if topic not in topics_log_file or len(topics_log_file[topic])==0:
        return 0
    last_segment = topics_log_file[topic][-1]
    start_offset = get_offset_from_filename(last_segment[0].name)
    write_offset = last_segment[4]
    return write_offset

def check_message_available(topic, offset):
    return offset < get_latest_offset(topic)

def read_message(topic, offset=0, check_hash=False):
    segment = get_topic_log(topic, offset)
    # mmap is save in topic_log means it's active segment
    if(segment[1] is not None):
        if(offset >= segment[4]):
            return None, offset # Offset out of range
    start_offset = get_offset_from_filename(segment[0].name) # Removing _log.txt from f.name
    # Updating offset to match the file offset
    file_offset = offset-start_offset
    index = segment[5]
    mm = segment[1]
    if file_offset + 4 > mm.size():
        # Something wrong, offset out of range
        return None, get_latest_offset(topic) # Returns the latest offset available
    length_bytes = mm[file_offset: file_offset+4]
    retained_time = time.time() - segment[2]
    if not length_bytes or length_bytes==b'\x00\x00\x00\x00':
        return None, offset
    # Check if the segment is expired
    if retained_time > RETENSION:
        return None, get_oldest_offset(topic) # Returns the oldest offset available

    msg_len = int.from_bytes(length_bytes, 'big')
    read_bytes = mm[file_offset+4: file_offset+4+msg_len]
    if check_hash:
        msg_bytes = read_bytes[:-4]
        checksum_bytes = read_bytes[-4:]
        hash = int.from_bytes(checksum_bytes, 'big')
        # Verify checksum
        if not checksum_verify(msg_bytes, hash):
            print("Hash verification failed")
            return None, offset+4+msg_len # Message got corrupted return the next offset
    else:
        msg_bytes = read_bytes
    return msg_bytes.decode(), offset+4+msg_len

def mark_file(f, mm, deletion_time):
    delete_file_queue.put((f.name,deletion_time))

def get_offset_from_filename(filename: str) -> int:
    # filename: /logs/topic/001010_log.txt -> 1010
    # filename: \\logs\\topic\\001010_log.txt -> 1010
    offset = int(filename.split('/')[-1].split('\\')[-1][:-8])
    return offset


def log_cleaner():
    """Runs in background, checks expired logsegments and mark them deletion"""
    while True:
        expired_time = time.time() - RETENSION
        for topic, files in list(topics_log_file.items()):
            old_files = files
            l ,r=0, len(files)-2
            index = -1
            while (l<=r):
                mid = l + (r-l)//2
                created_time = files[mid][2]
                if (created_time < expired_time):
                    l = mid+1
                    index = mid
                else:
                    r = mid-1
            new_files = []
            expired_files = []
            for i in range(0, len(files)):
                if i>index:
                    new_files.append(files[i])
                else:
                    expired_files.append(files[i])
            if topics_log_file[topic] is old_files:
                topics_log_file[topic] = new_files # Perform atomic swap or reference swap
                for fp in expired_files:
                    mark_file(fp[0],fp[1],fp[2]+RETENSION+GRACE_DELETION_TIME)
        time.sleep(1)  # Runs on every 1 seconds

def file_remover():
    """Runs in background, delete files that were marked."""
    while True:
        try:
            path,deletion_time = delete_file_queue.get(timeout=1)
        except queue.Empty:
            continue
        time_pending = deletion_time - time.time()
        if (time_pending > 0):
            time.sleep(time_pending)
        try:
            # Remove from segment cache if exists
            segmentCache.remove(path)
            os.remove(path)
        except Exception as e:
            print(f"Can't delete file {e}")

def lazy_flush():
    """Runs in background, lazy flush active segments"""
    while True:
        for topic,segments in list(topics_log_file.items()):
            if not segments:
                continue
            # Take snapshot of active segment
            active_seg = segments[-1]
            f,mm = active_seg[:2]
            if mm is None:
                continue
            try:
                mm.flush()
                os.fsync(f.fileno())
            except ValueError:
                ## mmap closed, or file closed mid-flush
                continue
            except Exception as e:
                print(f"Lazy flush error: {e}")
        time.sleep(0.5)

def close_all_segments():
    for topic, segments in topics_log_file.items():
        for f, mm, _, _, _ in segments:
            try:
                if mm:
                    mm.flush()
                    os.fsync(f.fileno())
                    mm.close()
                f.close()
            except:
                pass

def start_threads():
    t1 = threading.Thread(target=log_cleaner,daemon=True)
    t2 = threading.Thread(target=file_remover,daemon=True)
    t3 = threading.Thread(target=lazy_flush, daemon=True)
    t1.start()
    t2.start()
    t3.start()
print("File handler Module loaded")
