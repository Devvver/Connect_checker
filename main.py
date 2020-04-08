import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import csv, sys
import urllib.parse
import re
import argparse

start_time = datetime.now()


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('addrs_file', type=argparse.FileType())
    parser.add_argument('-csv', '--csvfile', default=None, help='name of csv, without .csv', type=str)
    parser.add_argument('-t', '--timeout', default=20, type=int, help='Timeout for each connection, default 20 sec')

    return parser


def set_addr(file):  # create list with addresses from file
    addr_list = file.read().splitlines()
    return addr_list


async def write(writer, data):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, writer.writerow, data)


async def connect_to_url(url):
    root_url = urllib.parse.urlsplit(url)
    try:
        if root_url.scheme == 'https':
            await asyncio.open_connection(root_url.hostname, 443, ssl=True)
        else:
            await asyncio.open_connection(root_url.hostname, 80)
        return True, url
    except Exception as e:
        return False, url, e


async def connect_to_ip(addr):
    ip = addr.split(':')
    try:
        await asyncio.open_connection(host=ip[0], port=ip[1])
        return True, addr
    except Exception as e:
        return False, addr, e


async def crawl(addr, timeout):
    check = re.search('[a-zA-z]', addr)
    if check:
        try:
            return await asyncio.wait_for(connect_to_url(addr), timeout=timeout)
        except asyncio.TimeoutError:
            return False, addr, 'Connection timed out'
    else:
        try:
            return await asyncio.wait_for(connect_to_ip(addr), timeout=timeout)
        except asyncio.TimeoutError:
            return False, addr, 'Connection timed out'


async def main(addrs_future, timeout, tablename=None):
    addrs = await addrs_future
    amount = len(addrs)
    con = 0
    fail = 0
    inf = []
    for result in asyncio.as_completed([crawl(addr, timeout) for addr in addrs]):
        res = await result
        if res[0] is True:
            con = con + 1
            inf.append({'Address': res[1], 'state': 'OK'})
        else:
            inf.append({'Address': res[1], 'state': 'FAIL', 'Error': res[2]})
            fail = fail + 1
        un = amount - con - fail
        print(f'\rUnknown: {un} | Failed: {fail} | Connected: {con} ', end='')

    if tablename:
        fieldnames = ['Address', 'state', 'Error']
        writer = csv.DictWriter(open(f'{tablename}.csv', 'w'), fieldnames=fieldnames,
                                delimiter=';')  # delimiter for Excel
        for each in inf:
            await write(writer, each)

    print('\n', datetime.now() - start_time)


if __name__ == '__main__':

    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])

    timeout = namespace.timeout
    csvfile = namespace.csvfile
    addr_file = namespace.addrs_file

    if sys.platform == 'win32':
        main_loop = asyncio.ProactorEventLoop()
    else:
        main_loop = asyncio.get_event_loop()
    asyncio.set_event_loop(main_loop)

    future = main_loop.create_future()
    future.set_result(set_addr(addr_file))

    main_loop.run_until_complete(main(future, timeout, csvfile))
