class ReplicaError(Exception):
    pass

class UnauthorisedReplica(ReplicaError):
    def __init__(self, replica_id: str, replica_list: list[str]):
        super().__init__(
            f"Fetch request with replica_id: {replica_id} doesn't belongs in {replica_list}"
        )
        self.replica_id = replica_id
        self.replica_list = replica_list