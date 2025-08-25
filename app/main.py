import asyncio
import os
from dotenv import load_dotenv
from helper import redis_protocol_parser, executor

load_dotenv()
BUFF_SIZE = int(os.getenv('BUFF_SIZE'))
HOST = os.getenv('HOST')
PORT = int(os.getenv('PORT'))

async def main():
    cache = {}
    print(f'Server started at HOST: {HOST} and PORT: {PORT}')
    async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while x := await reader.read(BUFF_SIZE):
            command, arg_list = redis_protocol_parser(x)
            await executor(command, arg_list, writer, cache)
        
        writer.close()
        await writer.wait_closed() # waits for the writer to be closed properly


    server = await asyncio.start_server(echo_handler, HOST, PORT)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    server_loop = asyncio.new_event_loop()
    server_loop.run_until_complete(main())
