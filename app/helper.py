import asyncio

from commands import *

async def executor(command: str, arg_list: list, writer: asyncio.StreamWriter, cache: dict, time_cache: dict, block_list) -> None:
    client_name = writer.get_extra_info('peername')
    command = Command.from_string(command)
    match command:
        case Command.PING:
            await PingCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.ECHO:
            await EchoCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.SET:
            await SetCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.GET:
            await GetCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.RPUSH:
            await RPushCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.LPUSH:
            await LPushCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.LRANGE:
            await LRangeCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.LLEN:
            await LLenCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.LPOP:
            await LPopCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.RPOP:
            await RPopCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.BLPOP:
            await BLPopCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case 'type':
            await TypeCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case '_':
            return ValueError(f"Unknown command {command} from {client_name}")
           
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

def normalize_idx(idx: int, size: int):
    if idx >= 0:
        return idx
    elif abs(idx) >= size:
        return 0
    else:
        return idx % size
