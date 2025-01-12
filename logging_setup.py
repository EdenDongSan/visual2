import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

class SizeTimeRotatingFileHandler(logging.Handler):
    """크기와 시간 모두를 기준으로 로그파일을 관리하는 핸들러"""
    
    def __init__(self, filename: str, max_bytes: int = 100*1024*1024,  # 100MB # 파일은 30개 유지
                 backup_count: int = 30, encoding: str = 'utf-8'):
        super().__init__()
        self.filename = filename
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.encoding = encoding
        self.size_handler = logging.handlers.RotatingFileHandler(
            filename=filename,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding=encoding
        )
        self.time_handler = logging.handlers.TimedRotatingFileHandler(
            filename=filename,
            when='midnight',
            interval=1,
            backupCount=backup_count,
            encoding=encoding
        )

    def emit(self, record):
        """로그 레코드 처리"""
        try:
            # 크기 기반 로테이션 체크
            if os.path.exists(self.filename):
                if os.path.getsize(self.filename) >= self.max_bytes:
                    self.size_handler.emit(record)
                    return

            # 시간 기반 로테이션
            self.time_handler.emit(record)
        except Exception:
            self.handleError(record)

def setup_logging(log_dir: str = "logs", env_path: Optional[str] = None) -> None:
    """
    로깅 설정을 초기화합니다.
    
    Args:
        log_dir (str): 로그 파일을 저장할 디렉토리 경로
        env_path (Optional[str]): .env 파일 경로
    """
    # .env 파일 로드
    load_dotenv(env_path)
    
    # 로그 레벨 설정
    log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))
    
    # 로그 디렉토리 생성
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # 현재 날짜로 로그 파일명 생성
    current_date = datetime.now().strftime("%Y%m%d")
    
    # 각 로그 파일 경로 설정
    log_files = {
        'main': f"{current_date}_trading_bot.log",
        'error': f"{current_date}_error.log",
        'trades': f"{current_date}_trades.log",
        'api': f"{current_date}_api.log",
        'websocket': os.getenv('WS_LOG_FILE', f"{current_date}_websocket.log")
    }
    
    # 로그 파일 경로 생성
    log_paths = {key: os.path.join(log_dir, filename) 
                for key, filename in log_files.items()}
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 기존 핸들러 제거 (중복 방지)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 포맷터 설정
    formatters = {
        'detailed': logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ),
        'api': logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(status_code)s] %(url)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    }
    
    # 핸들러 설정 함수
    def setup_handler(log_path: str, level: int, 
                     formatter_key: str = 'detailed',
                     max_bytes: int = 10*1024*1024) -> logging.Handler:
        handler = SizeTimeRotatingFileHandler(
            filename=log_path,
            max_bytes=max_bytes,
            backup_count=30,
            encoding='utf-8'
        )
        handler.setLevel(level)
        handler.setFormatter(formatters[formatter_key])
        return handler
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatters['detailed'])
    root_logger.addHandler(console_handler)
    
    # 각 로그 타입별 핸들러 설정
    handlers_config = {
        'main': (log_paths['main'], log_level),
        'error': (log_paths['error'], logging.ERROR),
        'trades': (log_paths['trades'], logging.INFO),
        'api': (log_paths['api'], logging.ERROR),
        'websocket': (log_paths['websocket'], logging.INFO)
    }
    
    for log_type, (path, level) in handlers_config.items():
        handler = setup_handler(path, level, 
                              'api' if log_type == 'api' else 'detailed')
        
        # 필터 설정
        if log_type == 'error':
            handler.addFilter(lambda record: record.levelno >= logging.ERROR)
        elif log_type == 'trades':
            handler.addFilter(
                lambda record: 'trading_strategy' in record.name.lower() or
                              'order_execution' in record.name.lower()
            )
        elif log_type == 'api':
            handler.addFilter(
                lambda record: 'bitget_api' in record.name.lower() and 
                              record.levelno >= logging.ERROR
            )
        elif log_type == 'websocket':
            handler.addFilter(
                lambda record: 'websocket' in record.name.lower()
            )
        
        root_logger.addHandler(handler)
    
    # API 로거 특별 설정
    api_logger = logging.getLogger('bitget_api')
    api_handler = setup_handler(log_paths['api'], logging.ERROR, 'api')
    api_logger.propagate = False
    api_logger.addHandler(api_handler)
    
    # 시작 로그 기록
    root_logger.info("Logging system initialized")
    root_logger.info(f"Log level set to {log_level}")

class APILogger:
    """API 호출 로깅을 위한 커스텀 로거"""
    
    def __init__(self, name: str = 'bitget_api'):
        self.logger = logging.getLogger(name)
    
    def log_request(self, method: str, url: str, 
                   status_code: Optional[int] = None, 
                   error: Optional[Exception] = None) -> None:
        """API 요청 로깅"""
        extra = {
            'url': url,
            'status_code': status_code or 0
        }
        
        if error:
            self.logger.error(f"{method} request failed: {str(error)}", 
                            extra=extra)
        elif status_code:
            level = logging.INFO if status_code < 400 else logging.ERROR
            self.logger.log(level, f"{method} request completed", extra=extra)