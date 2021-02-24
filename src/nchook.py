import apsw
import time
import plistlib
import pprint
import logging
import subprocess
import os.path

from watchdog.observers.kqueue import KqueueObserver 
from watchdog.events import FileSystemEventHandler

class DBEventHandler(FileSystemEventHandler):
    """Handles notification DB change events"""

    def __init__(self, db, rec_ids, logger=None):
        super().__init__()
        self.db = db
        self.rec_ids = rec_ids
        self.logger = logging.root

    def on_moved(self, event):
        pass

    def on_created(self, event):
        pass

    def on_deleted(self, event):
        pass

    def on_modified(self, event):
        super().on_modified(event)
        cursor = self.db.cursor()
        sql = f"SELECT rec_id, data FROM record WHERE rec_id NOT IN ({','.join('?' * len(rec_ids))})" 

        while True:
            try:
                new_objs = [(col[0], process_plist(col[1])) for col in cursor.execute(sql, rec_ids)]
                break
            except apsw.BusyError:
                time.sleep(1)

        self.logger.info(f"-- NEW -- : {len(new_objs)}")
        for obj in new_objs:
            self.rec_ids.append(obj[0])
            self.logger.info(f"{obj[1]}")


def process_plist(raw_plist):
    notif_plist = plistlib.loads(raw_plist, fmt=plistlib.FMT_BINARY)

    processed_notif_dict = {
            "app": notif_plist["app"], 
            "title": "",
            "body": notif_plist["req"]["body"],
            # the time in the plist is in the Core Data format, convert it to a unix timestamp
            "time": notif_plist["date"] + 978307200
    }

    # notifications may not have titles.
    if "titl" in notif_plist["req"]:
        processed_notif_dict["title"] = notif_plist["req"]["titl"]

    return processed_notif_dict


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    darwin_user_folder = subprocess.run(['getconf', 'DARWIN_USER_DIR'], capture_output=True).stdout.decode("utf-8").strip()
    db_folder = os.path.join(darwin_user_folder, "com.apple.notificationcenter/db2")
    db_file = os.path.join(db_folder, "db")
    watch_file = os.path.join(db_folder, "db-wal")

    db = apsw.Connection(db_file)
    rec_ids = []

    event_handler = DBEventHandler(db, rec_ids)
    observer = KqueueObserver()
    observer.schedule(event_handler, watch_file)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
