from enum import Enum


class MsgType(Enum):
    INVENTORY = 'inventory'
    POSITION = 'position'
    PARAMS = 'params'


class Action(Enum):
    APPEND = 'append'
    INPLACE = 'inplace'


class Msg():
    def __init__(self, msg_type: MsgType, key, value, action: Action = None):
        self.msg_type = msg_type
        self.key = key
        self.value = value
        self.action = action
        if msg_type in [MsgType.INVENTORY, MsgType.POSITION]:
            assert action is not None, "action should not be None for non-params msg"
