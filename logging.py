import logging
import json
from pythonjsonlogger import jsonlogger
from datetime import datetime

class EliteJSONFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = datetime.utcnow().isoformat()
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["module"] = record.module
        log_record["function"] = record.funcName
        
        # Add contextual information for trading
        if hasattr(record, 'symbol'):
            log_record["symbol"] = record.symbol
        if hasattr(record, 'score'):
            log_record["score"] = record.score

def setup_logging(level: str = "INFO"):
    """Setup elite JSON logging for institutional trading"""
    handler = logging.StreamHandler()
    formatter = EliteJSONFormatter(
        "%(timestamp)s %(level)s %(logger)s %(module)s.%(function)s %(message)s"
    )
    handler.setFormatter(formatter)
    
    logging.basicConfig(
        handlers=[handler], 
        level=level, 
        force=True
    )
    
    # Set specific log levels for noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

def get_logger(name: str):
    """Get logger with elite formatting"""
    return logging.getLogger(name)