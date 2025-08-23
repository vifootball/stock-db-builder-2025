import logging
import os
import csv
import inspect
from datetime import datetime
from contextvars import ContextVar
from typing import Optional

_ROOT_FUNC: ContextVar[Optional[str]] = ContextVar("_ROOT_FUNC", default=None)
_FACTORY_INSTALLED = False  # 중복 설치 방지

def _install_record_factory_once():
    global _FACTORY_INSTALLED
    if _FACTORY_INSTALLED:
        return
    
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)

        # 엔트리 함수명(root)
        record.function_name_root = _ROOT_FUNC.get() or record.funcName
        return record
    
    logging.setLogRecordFactory(record_factory)
    _FACTORY_INSTALLED = True


class CSVFormatter(logging.Formatter):
    """표준 흐름: message 계산 → asctime 생성 → CSV 직렬화"""
    def __init__(self, fieldnames, datefmt: str = "%Y-%m-%d %H:%M:%S"):
        super().__init__(datefmt=datefmt)
        self.fieldnames = fieldnames

    def format(self, record: logging.LogRecord) -> str:
        # 1) 표준 처리: 메시지/시간 계산
        record.message = record.getMessage()
        asctime = self.formatTime(record, self.datefmt)

        # 2) 필드 값 매핑(없으면 공백)
        values = {}
        for fn in self.fieldnames:
            if fn == "asctime":
                values[fn] = asctime
            elif fn == "message":
                values[fn] = record.message
            elif fn == "levelname":
                values[fn] = record.levelname
            else:
                values[fn] = getattr(record, fn, "")

        # 3) CSV 안전 인코딩: "..." 감싸고 내부 " → ""
        def q(x):
            s = "" if x is None else str(x)
            return '"' + s.replace('"', '""') + '"'

        return ",".join(q(values[fn]) for fn in self.fieldnames)


def setup_logger(log_root: str = "logs"):
    """
    파일은 CSV(+헤더 1회), 콘솔은 텍스트. RecordFactory로 function_name_root 주입.
    """
    _install_record_factory_once()

    now_month = datetime.now().strftime("%y%m")
    now_day = datetime.now().strftime("%y%m%d")

    # dirpath
    log_dirpath = os.path.join(log_root, now_month)
    os.makedirs(log_dirpath, exist_ok=True)
    # fpath
    log_fpath = os.path.join(log_dirpath, f"{now_day}_log.csv")
    file_exists = os.path.isfile(log_fpath)

    # 파일 핸들러: CSV 필드 정의(헤더 순서와 동일)
    fieldnames = ["asctime", "function_name_root", "funcName", "levelname", "message"]
    # csv_formatter = CSVFormatter(fieldnames, datefmt="%Y-%m-%d %H:%M:%S") # (asctime) 형식 정의
    csv_formatter = CSVFormatter(fieldnames)

    file_handler = logging.FileHandler(log_fpath, encoding="utf-8")
    file_handler.setFormatter(csv_formatter)

    # 콘솔 핸들러 (사람용 텍스트)
    text_formatter = logging.Formatter(
        fmt="%(asctime)s | %(function_name_root)s | %(funcName)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(text_formatter)

    # 기본 로거 세팅
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # 중복 방지
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # 파일이 새로 만들어진 경우 헤더 1회 기록
    if not file_exists:
        with open(log_fpath, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)

    return logger


def enter_root():
    """
    현재 호출한 함수명을 '엔트리(root)'로 세팅.
    최상위/진입 지점에서 한 번만 호출하면 이후 호출 체인 전체에 공유됨.
    """
    caller = inspect.currentframe().f_back.f_code.co_name
    if _ROOT_FUNC.get() is None:
        _ROOT_FUNC.set(caller)


def reset_root():
    """
    작업 단위가 끝난 뒤 루트 초기화가 필요할 때 호출(선택).
    배치 프레임워크에서 Job 경계마다 호출하면 깔끔.
    """
    _ROOT_FUNC.set(None)
