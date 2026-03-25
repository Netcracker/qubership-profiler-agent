# Java Examples

You can collect tcp dumps from sample java applications that come with profiler agents:

<https://github.com/Netcracker/qubership-profiler-backend/examples>

These applications support multiple endpoints with different functions.
While collecting tcp dumps from them, you need to send requests to these endpoints to create load.

The following python script is suitable for this purpose,
which sends one request for prime number and one request for memory once a minute:

```python
import requests
import time
import random
import string

JAVA_EXAMPLE_URL = "http://localhost:8080"

def is_prime_request():
    num = random.randint(100_000, 1_000_000)
    url = f"{JAVA_EXAMPLE_URL}/custom/prime/{num}"
    try:
        response = requests.get(url, timeout=10)
        print(f"[PRIME] {num} => {response.status_code}, {response.text}")
    except Exception as e:
        print(f"[PRIME] Error: {e}")


def memory_request():
    mem = random.randint(5, 20)
    url = f"{JAVA_EXAMPLE_URL}/custom/memory/{mem}"
    try:
        response = requests.get(url, timeout=10)
        print(f"[MEMORY] {mem} => {response.status_code}, {response.text}")
    except Exception as e:
        print(f"[MEMORY] Error: {e}")

def hash_request():
    rand_str = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 15)))
    url = f"{JAVA_EXAMPLE_URL}/custom/string/hash/{rand_str}"
    try:
        response = requests.get(url, timeout=10)
        print(f"[HASH] {rand_str} => {response.status_code}, {response.text}")
    except Exception as e:
        print(f"[HASH] Error: {e}")

if __name__ == "__main__":
    while True:
        print(">>> Sending requests...")
        is_prime_request()
        memory_request()
        hash_request()
        print(">>> Sleeping for 60 seconds...")
        time.sleep(60)
```
