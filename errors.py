from amazon_kclpy.kcl import CheckpointError


class ShutdownException(CheckpointError):
    pass


class ThrottlingException(CheckpointError):
    pass


class InvalidStateException(CheckpointError):
    pass
