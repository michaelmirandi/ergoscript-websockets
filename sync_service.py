import threading
import asyncio
import logging

from queue import Queue

from cached_requests import CachedRequests
from connection_manager import ConnectionManager
from store import Store


class SyncService:
    def __init__(self, store: Store, manager: ConnectionManager):
        self.store = store
        self.manager = manager
        self.task_queue = Queue()
        self.tombstone_set = set()
        self.url_mapper = dict()
        self.last_task_mapper = dict()
        self.lock = threading.Lock()

    def get_last_task(self, client_id):
        if client_id in self.last_task_mapper:
            return self.last_task_mapper[client_id]
        return None

    def add_task(self, client_id, url):
        self.lock.acquire()
        if url in self.url_mapper:
            self.url_mapper[url].append(client_id)
        else:
            self.url_mapper[url] = [client_id]
        if len(self.url_mapper[url]) == 1:
            self.task_queue.put(url)
        if url in self.tombstone_set:
            self.tombstone_set.remove(url)
        self.last_task_mapper[client_id] = url
        logging.info(
            f"SyncService.add_task::task added for client {client_id} for {url}")
        self.lock.release()

    def remove_task(self, client_id):
        self.lock.acquire()
        for url in self.url_mapper:
            if client_id in self.url_mapper[url]:
                self.url_mapper[url].remove(client_id)
            if len(self.url_mapper[url]) == 0:
                self.tombstone_set.add(url)
        logging.info(
            f"SyncService.remove_task::tasks removed for client {client_id}")
        self.lock.release()

    def _compute(self):
        while True:
            # logging.debug(
            #    f"SyncService._compute::length of current task_queue = {self.task_queue.qsize()}")
            url = self.task_queue.get()
            if url in self.tombstone_set:
                continue
            self.task_queue.put(url)
            last_snapshot = self.store.get(url)
            try:
                res = CachedRequests.get(url)
                if (str(res) != str(last_snapshot)):
                    logging.info(
                        f"SyncService._compute::diff detected for {url} -> broadcast required")
                    loop = asyncio.new_event_loop()
                    client_ids = self.url_mapper[url]
                    for client_id in client_ids:
                        coroutine = self.manager.send_personal_message(
                            client_id, res)
                        loop.run_until_complete(coroutine)
                    loop.close()
                self.store.set(url, res)
            except Exception as e:
                logging.error(f"SyncService._compute::{str(e)}")

    def start(self):
        t = threading.Thread(target=self._compute)
        t.start()
        logging.info("SyncService.start::started")
