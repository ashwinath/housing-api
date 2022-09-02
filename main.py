import arrow
import asyncio
import http.client
import json
import urllib.parse

import tornado.web
import tornado.httpclient

from typing import Any, Dict, Generator, Union

RESOURCE_ID_HDB_RESALE = "f1765b54-a209-4718-8d38-a39237f502b3"
cache = {} # cache_key: {data: {}, cache_expire: date}
URL_STRING = "https://data.gov.sg/api/action/datastore_search"

class HealthCheckHandler(tornado.web.RequestHandler):
    async def get(self):
        self.write('{"status": "ok"}')

class HousingHandler(tornado.web.RequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get(self):
        street_name        = self.get_argument("street_name")
        flat_type          = self.get_argument("flat_type")
        start_result_month = self.get_argument("start_result_month") # yyyy-mm format
        start_lease_year   = self.get_argument("start_lease") # yyyy
        end_lease_year     = self.get_argument("end_lease") # yyyy

        cache_key = f"{street_name}-{flat_type}-{start_lease_year}-{end_lease_year}-{start_result_month}"
        if cache_key in cache and cache[cache_key]["cache_expire"] > arrow.now():
            self.write_json(cache[cache_key]["data"])
            return

        data = await query_data(street_name, flat_type, start_result_month, start_lease_year, end_lease_year)
        cache_expire = arrow.now().shift(days=1)
        cache[cache_key] = {"data": data, "cache_expire": cache_expire}

        self.write_json(data)

    def write_json(self, payload: Dict[Any, Any]):
        r = json.dumps(payload)
        self.set_status(http.client.OK)
        self.write(r)

async def query_data(
    street_name: str,
    flat_type: str,
    start_result_month: str, # yyyy-mm
    start_lease_year: str, # yyyy
    end_lease_year: str, # yyyy
) -> Dict[str, Union[str, int]]:
    http_client = tornado.httpclient.AsyncHTTPClient()
    futures = []

    for month_string in get_result_month_generator(start_result_month):
        for lease_year in range(int(start_lease_year), int(end_lease_year)+1):
            ckan_params = {
                "street_name": street_name,
                "month": month_string,
                "lease_commence_date": str(lease_year),
                "flat_type": flat_type,
            }
            query_params = urllib.parse.urlencode({
                "q": json.dumps(ckan_params),
                "resource_id": RESOURCE_ID_HDB_RESALE,
                "sort": "month desc",
            })
            future = http_client.fetch(f"{URL_STRING}?{query_params}")
            futures.append(future)

    results = await asyncio.gather(*futures)

    parsed = []
    for result in results:
        if result.code != 200:
            return None
        body = tornado.escape.json_decode(result.body)
        for record in body["result"]["records"]:
            parsed.append({
                "time": arrow.get(
                    f"{record['month']}-01T00:00:00.000000+08:00"
                ).isoformat(),
                "price": record["resale_price"],
            })

    return parsed

def get_result_month_generator(start_result_month: str) -> Generator[int, None, None]:
    current = arrow.get(f"{start_result_month}-01T00:00:00.000000+08:00")

    while current < arrow.now():
        yield f"{current.year}-{current.month:02}"
        current = current.shift(months=1)

def make_app():
    return tornado.web.Application([
        (r"/query", HousingHandler),
        (r"/", HealthCheckHandler),
    ])

async def main():
    app = make_app()
    app.listen(6000)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
