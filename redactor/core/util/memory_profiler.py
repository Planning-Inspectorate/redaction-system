import tracemalloc
from datetime import datetime
import psutil
import threading
import time
import os


class MemoryProfiler:
    def __init__(self, trace_delay: int = 30):
        self.trace_delay = trace_delay
        tracemalloc.start()
        #if not os.path.exists(os.path.join("memoryProfile")):
        #    os.mkdir(os.path.join("memoryProfile"))
        self.min_memory = None
        self.peak_memory_usage = None
        self.memory_snapshot = None
        self.running = True
        self.pid = os.getpid()
        self.python_process = psutil.Process(self.pid)
        self.worker = threading.Thread(target=self._profile_memory, daemon=True)
        self.worker.start()

    def deactivate(self):
        self.running = False
        return self.peak_memory_usage, self.min_memory, self.memory_snapshot

    def _profile_memory(self):
        while self.running:
            #now = datetime.now().replace(microsecond=0).isoformat()
            memory_use = self.python_process.memory_info().rss / 1024**2
            if self.min_memory is None:
                self.min_memory = memory_use
            if self.peak_memory_usage is None or memory_use > self.peak_memory_usage:
                self.peak_memory_usage = memory_use
                self.memory_snapshot = tracemalloc.take_snapshot()
            #print(f"Memory usage: {memory_use:.2f} MiB")
            #snapshot.dump(os.path.join("memoryProfile", f"{now}.snapshot"))
            for i in range(0, self.trace_delay):
                time.sleep(1)
                if not self.running:
                    break
        tracemalloc.stop()
