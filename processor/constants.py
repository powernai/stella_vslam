from enum import Enum


class ROUTING_KEY(Enum):
    TO_BE = "to_be_processed"  
    DONE = "processed"

EXCHANGE = 'file_processor'
