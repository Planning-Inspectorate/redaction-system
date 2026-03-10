import tracemalloc
from datetime import datetime
import threading
import time
import os


class MemoryProfiler:
    def __init__(self, trace_delay: int = 30):
        self.trace_delay = trace_delay
        tracemalloc.start()
        if not os.path.exists(os.path.join("memoryProfile")):
            os.mkdir(os.path.join("memoryProfile"))
        self.running = True
        self.worker = threading.Thread(target=self._profile_memory, daemon=True)
        self.worker.start()

    def deactivate(self):
        self.running = False

    def _profile_memory(self):
        while self.running:
            now = datetime.now().replace(microsecond=0).isoformat()
            snapshot = tracemalloc.take_snapshot()
            snapshot.dump(os.path.join("memoryProfile", f"{now}.snapshot"))
            for i in range(0, self.trace_delay):
                time.sleep(1)
                if not self.running:
                    break
        tracemalloc.stop()
