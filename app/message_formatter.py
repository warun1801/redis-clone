from resp import RESPType

def msg(content: any, msg_type: RESPType) -> bytes:
    match msg_type:
        case RESPType.SIMPLE_STRING:
            return f"{RESPType.SIMPLE_STRING.value}{content}\r\n".encode()
        case RESPType.ERROR:
            return f"{RESPType.ERROR.value}{content}\r\n".encode()
        case RESPType.INTEGER:
            return f"{RESPType.INTEGER.value}{content}\r\n".encode()
        case RESPType.BULK_STRING:
            if content is None:
                return f"{RESPType.BULK_STRING.value}-1\r\n".encode()
            return f"{RESPType.BULK_STRING.value}{len(content)}\r\n{content}\r\n".encode()
        case RESPType.ARRAY:
            if content is None:
                return f"{RESPType.ARRAY.value}-1\r\n".encode()
            array_parts = [f"{RESPType.ARRAY.value}{len(content)}\r\n"]
            for item in content:
                if isinstance(item, str):
                    array_parts.append(msg(item, RESPType.BULK_STRING).decode())
                elif isinstance(item, int):
                    array_parts.append(msg(item, RESPType.INTEGER).decode())
                elif item is None:
                    array_parts.append(msg(None, RESPType.BULK_STRING).decode())
                else:
                    raise ValueError("Unsupported item type in array")
            return ''.join(array_parts).encode()
        case RESPType.NULL:
            return f"{RESPType.BULK_STRING.value}-1\r\n".encode()
        case _:
            raise ValueError("Unsupported RESP type")
