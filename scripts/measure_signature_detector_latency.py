"""
Measure request latency and throughput of the signature detector's /score endpoint
at increasing levels of client concurrency.

The detector is only reachable from inside the VNet when deployed, so run this against
a local container or from a machine with network access to the app.

With --image, the script starts a container for every combination of --workers
(WEB_CONCURRENCY), --threads (OMP_NUM_THREADS and MKL_NUM_THREADS) and --cpus (the
container's CPU limit), measures it at each client concurrency level, and removes it
afterwards.

# Example usage
`docker build -t signature-detector signature_detector`
`python3 scripts/measure_signature_detector_latency.py --image signature-detector --workers 1 2 4 --threads 1 2 4 --cpus 4 --images sample.pdf -c 1 2 4 8 -n 40`

To measure a detector that is already running instead:
`python3 scripts/measure_signature_detector_latency.py --url http://localhost:8080`

If the throughput stays flat as concurrency rises, the detector is processing requests
one at a time and extra client workers only queue.
"""

import argparse
import io
import itertools
import os
import statistics
import subprocess  # nosec: B404
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import requests
from PIL import Image

PDF_RENDER_DPI = 150  # Matches how the redactor renders pages
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}


def load_images(paths: list[str]) -> list[Image.Image]:
    """Load images from image files, PDFs (every page) or directories of either."""
    files: list[Path] = []
    for path in map(Path, paths):
        files.extend(sorted(path.iterdir()) if path.is_dir() else [path])

    images = []
    for file in files:
        if file.suffix.lower() == ".pdf":
            import pymupdf

            with pymupdf.open(file) as pdf:
                for page in pdf:
                    pix = page.get_pixmap(dpi=PDF_RENDER_DPI)
                    images.append(
                        Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                    )
        elif file.suffix.lower() in IMAGE_SUFFIXES:
            images.append(Image.open(file).convert("RGB"))
    return images


def encode_payloads(images: list[Image.Image], threshold: float) -> list[dict]:
    """Build request bodies in the format the redactor sends: JPEG bytes as hex."""
    payloads = []
    for image in images:
        buffer = io.BytesIO()
        image.convert("RGB").save(buffer, format="jpeg")
        payloads.append({"image": buffer.getvalue().hex(), "threshold": threshold})
    return payloads


def send_request(url: str, payload: dict, timeout: float) -> tuple[float, str]:
    """Send one request and return (seconds taken, status code or exception name)."""
    start = time.perf_counter()
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        outcome = str(response.status_code)
    except requests.RequestException as e:
        outcome = type(e).__name__
    return time.perf_counter() - start, outcome


def percentile(sorted_values: list[float], fraction: float) -> float:
    index = min(len(sorted_values) - 1, round(fraction * (len(sorted_values) - 1)))
    return sorted_values[index]


def run_level(
    url: str,
    payloads: list[dict],
    concurrency: int,
    n_requests: int,
    timeout: float,
    config: str,
) -> dict:
    jobs = [payloads[i % len(payloads)] for i in range(n_requests)]

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(lambda p: send_request(url, p, timeout), jobs))
    wall_time = time.perf_counter() - start

    outcomes = Counter(outcome for _, outcome in results)
    ok_latencies = sorted(seconds for seconds, outcome in results if outcome == "200")
    print(f"\nConcurrency {concurrency} [{config}]")
    print(f"  {n_requests} requests in {wall_time:.1f}s, outcomes: {dict(outcomes)}")
    row = {"config": config, "concurrency": concurrency, "ok": len(ok_latencies)}
    if not ok_latencies:
        print("  No successful requests")
        return row

    throughput = len(ok_latencies) / wall_time
    row |= {
        "p50": percentile(ok_latencies, 0.5),
        "p95": percentile(ok_latencies, 0.95),
        "throughput": throughput,
    }
    print(
        f"  Latency (s): mean {statistics.mean(ok_latencies):.2f}, "
        f"p50 {row['p50']:.2f}, p95 {row['p95']:.2f}, max {ok_latencies[-1]:.2f}"
    )
    print(
        f"  Throughput: {throughput:.2f} req/s "
        f"(one request every {1 / throughput:.2f}s)"
    )
    return row


def docker(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(  # nosec: B603 B607
        ["docker", *args], capture_output=True, text=True, check=False
    )


@contextmanager
def detector_container(
    image: str,
    port: int,
    env: dict[str, str],
    cpus: float | None,
    start_timeout: float,
):
    """Run the detector image with the given environment, yielding its base URL."""
    env_args = [arg for name, value in env.items() for arg in ("-e", f"{name}={value}")]
    cpu_args = ["--cpus", f"{cpus:g}"] if cpus is not None else []
    # A fixed name lets cleanup remove the container even if docker run fails midway
    container = f"signature-detector-latency-{os.getpid()}"
    started = docker(
        "run",
        "-d",
        "--name",
        container,
        "-p",
        f"{port}:8080",
        *cpu_args,
        *env_args,
        image,
    )
    try:
        if started.returncode != 0:
            raise RuntimeError(f"docker run failed: {started.stderr.strip()}")
        container_id = started.stdout.strip()
        base_url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + start_timeout
        while True:
            running = docker("inspect", "-f", "{{.State.Running}}", container_id)
            if running.stdout.strip() != "true":
                logs = docker("logs", "--tail", "20", container_id)
                raise RuntimeError(
                    f"Container exited during startup:\n{logs.stdout}{logs.stderr}"
                )
            try:
                if requests.get(f"{base_url}/health", timeout=2).ok:
                    break
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError(f"No healthy response after {start_timeout}s")
            time.sleep(1)
        yield base_url
    finally:
        docker("rm", "-f", container)


def describe_config(env: dict[str, str], cpus: float | None) -> str:
    settings = [f"{k}={v}" for k, v in env.items()]
    if cpus is not None:
        settings.append(f"--cpus={cpus:g}")
    return ", ".join(settings) or "no docker variables set"


def build_configs(
    workers: list[int | None],
    threads: list[int | None],
    cpus: list[float | None],
) -> list[tuple[dict[str, str], float | None]]:
    """Every combination of workers, threads and CPU limit, as (env vars, cpus)."""
    configs = []
    for n_workers, n_threads, n_cpus in itertools.product(workers, threads, cpus):
        env = {}
        if n_workers is not None:
            env["WEB_CONCURRENCY"] = str(n_workers)
        if n_threads is not None:
            env["OMP_NUM_THREADS"] = str(n_threads)
            env["MKL_NUM_THREADS"] = str(n_threads)
        configs.append((env, n_cpus))
    return configs


def measure(base_url: str, payloads: list[dict], config: str, args) -> list[dict]:
    health = requests.get(f"{base_url}/health", timeout=args.timeout)
    health.raise_for_status()

    score_url = f"{base_url}/score"
    for i in range(args.warmup):
        _, outcome = send_request(score_url, payloads[i % len(payloads)], args.timeout)
        if outcome != "200":
            raise RuntimeError(f"Warm-up request failed: {outcome}")

    return [
        run_level(score_url, payloads, c, args.requests, args.timeout, config)
        for c in args.concurrency
    ]


def print_summary(rows: list[dict]) -> None:
    print("\nSummary")
    print(f"{'Docker settings':<72}{'Conc':>5}{'p50 (s)':>9}{'p95 (s)':>9}{'req/s':>8}")
    for row in rows:
        if "throughput" in row:
            stats = f"{row['p50']:>9.2f}{row['p95']:>9.2f}{row['throughput']:>8.2f}"
        else:
            stats = f"{'failed':>26}"
        print(f"{row['config']:<72}{row['concurrency']:>5}{stats}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--url",
        default="http://localhost:8080",
        help="Detector base URL, used when --image is not given",
    )
    parser.add_argument(
        "--image", help="Docker image to run a container from for each configuration"
    )
    parser.add_argument(
        "--workers",
        nargs="+",
        type=int,
        help="Gunicorn worker counts to test, set as WEB_CONCURRENCY",
    )
    parser.add_argument(
        "--threads",
        nargs="+",
        type=int,
        help="Threads per worker to test, set as OMP_NUM_THREADS and MKL_NUM_THREADS",
    )
    parser.add_argument(
        "--cpus",
        nargs="+",
        type=float,
        help="Container CPU limits to test, set as docker run --cpus",
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Host port for the container"
    )
    parser.add_argument(
        "--start-timeout",
        type=float,
        default=180.0,
        help="Seconds to wait for each container to load the model",
    )
    parser.add_argument(
        "--images",
        nargs="*",
        default=[],
        help="Image files, PDFs or directories to send. Defaults to a blank page",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        nargs="+",
        type=int,
        default=[1, 2, 4, 8],
        help="Client concurrency levels to test",
    )
    parser.add_argument(
        "-n", "--requests", type=int, default=20, help="Requests per concurrency level"
    )
    parser.add_argument(
        "--warmup", type=int, default=2, help="Untimed warm-up requests"
    )
    parser.add_argument("--threshold", type=float, default=0.0)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    if not args.image and (args.workers or args.threads or args.cpus):
        parser.error("--workers, --threads and --cpus need --image to start containers")

    images = load_images(args.images)
    if not images:
        print("No images supplied, using a blank 1240x1754 page (A4 at 150 DPI)")
        images = [Image.new("RGB", (1240, 1754), "white")]
    payloads = encode_payloads(images, args.threshold)
    print(f"Using {len(payloads)} image(s)")

    rows: list[dict] = []
    if not args.image:
        print(f"Measuring the detector already running at {args.url}")
        base_url = args.url.rstrip("/")
        rows = measure(base_url, payloads, "existing detector", args)
    else:
        configs = build_configs(
            args.workers or [None], args.threads or [None], args.cpus or [None]
        )
        for env, cpus in configs:
            config = describe_config(env, cpus)
            print(f"\n=== Starting {args.image}: {config} ===")
            try:
                with detector_container(
                    args.image, args.port, env, cpus, args.start_timeout
                ) as base_url:
                    rows += measure(base_url, payloads, config, args)
            except RuntimeError as e:
                print(f"Skipping this configuration: {e}")
                rows.append({"config": config, "concurrency": "-"})

    print_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
