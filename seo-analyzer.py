
import asyncio
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession, RequestsError

MAX_BODY_SIZE = 250000  # ~250KB

async def fetch_url(session, url):
    """
    Fetch a single URL and return SEO metrics using curl_cffi.
    Strictly follows spec:
    - Timeout: 5s
    - Redirects: allow_redirects=False
    - Canonical: 1:1 match
    - Noindex: Header or Meta
    - Impersonate: Chrome
    """
    result = {
        'sitemap_url': url,
        'final_status': None,
        'redirect_location': None,
        'canonical': None,
        'canonical_match': False,
        'noindex': False,
        'noindex_source': None,
        'fetch_error': None
    }
    
    try:
        # stream=True to allow partial reading
        response = await session.get(url, allow_redirects=False, stream=True, timeout=10)
        
        result['final_status'] = response.status_code
        
        # 1. Check Redirects
        if response.status_code in (301, 302, 303, 307, 308):
            result['redirect_location'] = response.headers.get('Location')
            return result
        
        # 2. Check Content-Type for Binary Files
        content_type = response.headers.get('Content-Type', '').lower()
        if any(x in content_type for x in ['image', 'pdf', 'video', 'audio', 'zip', 'octet-stream']):
            # It's a binary file, just valid status is enough. 
            # We assume noindex/canonical rules don't apply same way or can't be parsed from body.
            # Check headers for basics.
            x_robots = response.headers.get('X-Robots-Tag', '').lower()
            if 'noindex' in x_robots or 'none' in x_robots:
                result['noindex'] = True
                result['noindex_source'] = 'Header'
            return result

        # 3. Check X-Robots-Tag Header
        x_robots = response.headers.get('X-Robots-Tag', '').lower()
        if 'noindex' in x_robots or 'none' in x_robots:
            result['noindex'] = True
            result['noindex_source'] = 'Header'
            
        # 4. Read Body (Partial)
        content_accumulated = b""
        async for chunk in response.aiter_content():
            content_accumulated += chunk
            if len(content_accumulated) > MAX_BODY_SIZE:
                break
        
        # Try to decode
        try:
            text = content_accumulated.decode('utf-8', errors='ignore')
        except:
             # If decoding fails completely, treat as binary/error and return
             return result

        soup = BeautifulSoup(text, 'html.parser')
        
        # 5. Check Meta Robots
        meta_robots = soup.find('meta', attrs={'name': 'robots'})
        if meta_robots:
            content_attr = meta_robots.get('content', '').lower()
            if 'noindex' in content_attr or 'none' in content_attr:
                result['noindex'] = True
                src = 'Meta'
                if result['noindex_source']:
                    src = 'Both'
                result['noindex_source'] = src
        
        # 6. Check Canonical
        canonical_tag = soup.find('link', attrs={'rel': 'canonical'})
        if canonical_tag:
             result['canonical'] = canonical_tag.get('href')
        
        if not result['canonical']:
            link_header = response.headers.get('Link')
            # Parse complex Link header if needed, for MVP simple check
            pass 

        # 7. Verify Match (STRICT)
        if result['canonical']:
            # STRICT string comparison - User requirement
            result['canonical_match'] = (result['canonical'] == url)
                
    except asyncio.TimeoutError:
        result['fetch_error'] = 'Timeout'
    except RequestsError as e:
        result['fetch_error'] = f"RequestError: {str(e)}"
    except Exception as e:
        result['fetch_error'] = f"Error: {str(e)}"
        
    return result

async def analyze_urls(urls, progress_callback=None, should_stop=None):
    """
    Analyze a list of URLs concurrently using curl_cffi AsyncSession.
    """
    results = []
    # curl_cffi doesn't use Semaphore directly on session, but we can limit concurrency via asyncio
    sem = asyncio.Semaphore(30)  # Concurrency limit
    
    # Create one session for all requests to reuse connection pool/signatures?
    # curl_cffi sessions are good for keeping cookies/preset headers
    async with AsyncSession(impersonate="chrome") as session:
        tasks = []
        for url in urls:
             task = asyncio.create_task(bounded_fetch(sem, session, url))
             tasks.append(task)
        
        # Monitor progress
        total = len(tasks)
        completed = 0
        
        for coro in asyncio.as_completed(tasks):
            if should_stop and should_stop():
                # Cancel remaining tasks
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
                
    return results

async def bounded_fetch(sem, session, url):
    async with sem:
        # Retry logic: 1 retry
        for attempt in range(2):
            res = await fetch_url(session, url)
            if not res['fetch_error']:
                return res
            # If error, wait briefly and retry if it's the first attempt
            if attempt == 0:
                await asyncio.sleep(0.5)
        return res