class UnknownCommand(ValueError):
    def __init__(self, cmd:str):
        super().__init__(f"Unknown command: {cmd}")

class ChecksumFailed(ValueError):
    def __init__(self, data:bytes, hash:int):
        super().__init__(
            f"checksum failed for data:{data.decode()} with hash: {hash}"
        )