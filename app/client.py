import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
BUFF_SIZE = int(os.getenv('BUFF_SIZE'))
HOST = os.getenv('HOST')
PORT = int(os.getenv('PORT'))

async def run_client():
    reader, writer = await asyncio.open_connection(HOST, PORT)

    test_str = b'*1\r\n$4\r\nPING\r\n'
    test_str_2 = b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'

    writer.write(test_str_2)
    await writer.drain()

    while x := await reader.read(BUFF_SIZE):
        data = x.decode()
        print(f'{data!r}')

if __name__ == '__main__':
    client_loop = asyncio.new_event_loop()
    client_loop.run_until_complete(run_client())
