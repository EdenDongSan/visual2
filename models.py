from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime

@dataclass
class MarketData:
    symbol: str
    last_price: float
    mark_price: float
    index_price: float
    open_interest: float
    volume_24h: float
    high_24h: float
    low_24h: float
    timestamp: int

@dataclass
class Position:
    symbol: str
    side: str  # 'long' or 'short'
    size: float
    entry_price: float
    stop_loss_price: float
    take_profit_price: float
    timestamp: int
    leverage: int
    
    # 기존 필드들
    break_even_price: float
    unrealized_pl: float
    margin_size: float
    available: float
    locked: float
    liquidation_price: float
    margin_ratio: float
    mark_price: float
    achieved_profits: float
    total_fee: float
    margin_mode: str
    
    # 새로 추가되는 필드들
    initial_size: Optional[float] = None  # 초기 포지션 크기
    remaining_size: Optional[float] = None  # 남은 포지션 크기
    average_exit_price: Optional[float] = None  # 평균 청산 가격
    partial_exits: Optional[List[float]] = None  # 부분 청산 기록
    entry_timestamps: Optional[List[int]] = None  # 분할 진입 시간 기록
    
    def __post_init__(self):
        self.initial_size = self.size
        self.remaining_size = self.size
        self.partial_exits = []
        self.entry_timestamps = [self.timestamp]
        
    @property
    def is_fully_closed(self) -> bool:
        """포지션이 완전히 청산되었는지 확인"""
        return self.remaining_size <= 0
        
    @property
    def partial_close_count(self) -> int:
        """부분 청산 횟수"""
        return len(self.partial_exits)
        
    @property
    def average_entry_time(self) -> datetime:
        """평균 진입 시간"""
        if not self.entry_timestamps:
            return datetime.fromtimestamp(self.timestamp / 1000)
        avg_ts = sum(self.entry_timestamps) / len(self.entry_timestamps)
        return datetime.fromtimestamp(avg_ts / 1000)
        
    def add_partial_exit(self, price: float, size: float):
        """부분 청산 기록 추가"""
        self.partial_exits.append(price)
        self.remaining_size -= size
        
        # 평균 청산 가격 업데이트
        total_exit_value = sum(p * s for p, s in zip(self.partial_exits, 
                                                    [size] * len(self.partial_exits)))
        total_exit_size = size * len(self.partial_exits)
        self.average_exit_price = total_exit_value / total_exit_size if total_exit_size > 0 else None

    def add_entry(self, timestamp: int, size: float):
        """분할 진입 기록 추가"""
        self.entry_timestamps.append(timestamp)
        self.initial_size += size
        self.remaining_size += size

@dataclass
class TradingMetrics:
    """트레이딩 지표"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_profit: float = 0.0
    total_loss: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    average_win: float = 0.0
    average_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    
    def update(self, profit: float):
        """거래 결과로 지표 업데이트"""
        self.total_trades += 1
        
        if profit > 0:
            self.winning_trades += 1
            self.total_profit += profit
            self.largest_win = max(self.largest_win, profit)
        else:
            self.losing_trades += 1
            self.total_loss += abs(profit)
            self.largest_loss = min(self.largest_loss, profit)
            
        self.win_rate = (self.winning_trades / self.total_trades) * 100
        self.profit_factor = self.total_profit / self.total_loss if self.total_loss > 0 else float('inf')
        self.average_win = self.total_profit / self.winning_trades if self.winning_trades > 0 else 0
        self.average_loss = self.total_loss / self.losing_trades if self.losing_trades > 0 else 0