import apsw
import time
import plistlib
import pprint
import logging
import subprocess

from watchdog.observers.kqueue import KqueueObserver 
from watchdog.events import FileSystemEventHandler

user_folder = subprocess.run(['getconf', 'DARWIN_USER_DIR'], capture_output=True).stdout.decode("utf-8").strip()
db_folder = user_folder + "com.apple.notificationcenter/db2"
db_file = db_folder + "/db"
watch_file = db_folder + "/db-wal"

db = apsw.Connection(db_file)
rec_ids = []

class DBEventHandler(FileSystemEventHandler):
    """Handles notification DB change events"""

    def __init__(self, logger=None):
        super().__init__()
        self.logger = logging.root

    def on_moved(self, event):
        pass

    def on_created(self, event):
        pass

    def on_deleted(self, event):
        pass

    def on_modified(self, event):
        super().on_modified(event)
        cursor = db.cursor()
        sql = f"SELECT rec_id, data FROM record WHERE rec_id NOT IN ({','.join('?' * len(rec_ids))})" 

        while True:
            try:
                new_objs = [(col[0], decode_plist(col[1])) for col in cursor.execute(sql, rec_ids)]
                break
            except apsw.BusyError:
                time.sleep(1)

        self.logger.info(f"-- NEW -- : {len(new_objs)}")
        for obj in new_objs:
            rec_ids.append(obj[0])
            self.logger.info(f"{obj[1]['app']}")


def decode_plist(raw_plist):
    notif_plist = plistlib.loads(raw_plist, fmt=plistlib.FMT_BINARY)

    # try to decode some of the sub-plists, but they might not be there
    try:
        notif_plist['req']['nsdict']['di'] = plistlib.loads(notif_plist['req']['nsdict']['di'], fmt=plistlib.FMT_BINARY)
        notif_plist['req']['usda'] = plistlib.loads(notif_plist['req']['usda'], fmt=plistlib.FMT_BINARY)
        notif_plist['req']['nsdu'] = plistlib.loads(notif_plist['req']['nsdu'], fmt=plistlib.FMT_BINARY)
    except:
        pass

    return notif_plist


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    event_handler = DBEventHandler()
    observer = KqueueObserver()
    observer.schedule(event_handler, watch_file)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
