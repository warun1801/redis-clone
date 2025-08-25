import asyncio

async def executor(command: str, arg_list: list, writer: asyncio.StreamWriter, cache: dict) -> None:
    match command:
        case 'ping':
            writer.write(b'+PONG\r\n')
            await writer.drain() # waits for the writer to complete completely
            return
        case 'echo':
            for arg in arg_list:
                writer.write(f'${len(arg)}\r\n{arg}\r\n'.encode())
                await writer.drain()
            return
        case 'set':
            cache[arg_list[0]] = arg_list[1]
            writer.write(b'+OK\r\n')
            await writer.drain()
            return
        case 'get':
            if arg_list[0] in cache:
                writer.write(f'${len(cache[arg_list[0]])}\r\n{cache[arg_list[0]]}\r\n'.encode())
                await writer.drain()
            else:
                writer.write(b'$-1\r\n')
                await writer.drain()
            return
        case '_':
            writer.write(b'Invalid Argument\r\n')
            await writer.drain()
            return
        
def redis_protocol_parser(data) -> tuple:
    entities = data.decode().split('\r\n')
    if entities[-1] == '':
        entities.pop()
    # print(entities, data)
    args = []
    for i in range(1, len(entities)):
        if str(entities[i]).startswith('$'):
            continue
        args.append(entities[i])

    return (args.pop(0).lower(), args) 