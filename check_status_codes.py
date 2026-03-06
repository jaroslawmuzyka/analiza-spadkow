import asyncio
from curl_cffi.requests import AsyncSession, RequestsError
import pandas as pd

async def fetch_status_code(session, url):
    """
    Fetch a single URL and return its HTTP Status Code using curl_cffi.
    Strictly follows spec: Timeout 5s, no redirects, impersonate Chrome.
    """
    try:
        response = await session.get(url, allow_redirects=False, timeout=5)
        return {"URL": url, "Status Code": response.status_code}
    except asyncio.TimeoutError:
        return {"URL": url, "Status Code": "Timeout"}
    except RequestsError as e:
        return {"URL": url, "Status Code": f"ReqError"}
    except Exception as e:
        return {"URL": url, "Status Code": f"Error"}

async def analyze_status_codes(urls, progress_callback=None, cancel_flag=None):
    """
    Analyze a list of URLs concurrently returning their HTTP status codes.
    """
    results = []
    sem = asyncio.Semaphore(30)
    
    async with AsyncSession(impersonate="chrome") as session:
        tasks = []
        for url in urls:
             task = asyncio.create_task(bounded_fetch(sem, session, url))
             tasks.append(task)
        
        total = len(tasks)
        completed = 0
        
        for coro in asyncio.as_completed(tasks):
            if cancel_flag and cancel_flag[0]:
                for t in tasks:
                    t.cancel()
                break
                
            try:
                res = await coro
                results.append(res)
            except asyncio.CancelledError:
                pass
                
            completed += 1
            if progress_callback:
                progress_callback(completed / total)
                
    return pd.DataFrame(results)

async def bounded_fetch(sem, session, url):
    async with sem:
        for attempt in range(2):
            res = await fetch_status_code(session, url)
            if isinstance(res["Status Code"], int):
                return res
            if attempt == 0:
                await asyncio.sleep(0.5)
        return res
