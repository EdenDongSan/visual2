import aiomysql
import logging
import os
from typing import Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None

class DatabaseManager:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not DatabaseManager._initialized:
            self.pool = None
            DatabaseManager._initialized = True
    
    async def initialize(self):
        """비동기 DB 풀 초기화"""
        try:
            self.pool = await aiomysql.create_pool(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                db=os.getenv('MYSQL_DATABASE'),
                autocommit=True
            )
            await self._setup_database()
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def _setup_database(self):
        """데이터베이스 및 테이블 설정"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 기존 캔들 테이블
                await cursor.execute("""
                CREATE TABLE IF NOT EXISTS kline_1m (
                    timestamp BIGINT PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume FLOAT,
                    quote_volume FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                await cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_sentiment_data (
                    timestamp BIGINT,
                    symbol VARCHAR(20),
                    open_interest FLOAT DEFAULT 0,
                    oi_rsi FLOAT DEFAULT 50,
                    oi_slope FLOAT DEFAULT 0,
                    oi_change_percent FLOAT DEFAULT 0,
                    long_ratio FLOAT DEFAULT 0,
                    short_ratio FLOAT DEFAULT 0,
                    ls_ratio_slope FLOAT DEFAULT 0,
                    ls_ratio_acceleration FLOAT DEFAULT 0,
                    PRIMARY KEY (timestamp, symbol)
                )
                """)
                
                # 거래 기록 테이블
                await cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_history (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    timestamp BIGINT,
                    symbol VARCHAR(20),
                    side VARCHAR(10),
                    size FLOAT,
                    entry_price FLOAT,
                    exit_price FLOAT,
                    pnl FLOAT,
                    pnl_percentage FLOAT,
                    leverage INT,
                    trade_type VARCHAR(20),
                    entry_type VARCHAR(20),
                    exit_reason VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)

    async def store_candle(self, candle: Candle):
        """단일 캔들 데이터 비동기 저장"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO kline_1m 
                        (timestamp, open, high, low, close, volume, quote_volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
                    """, (
                        candle.timestamp,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                        candle.quote_volume,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                        candle.quote_volume
                    ))
                    
        except Exception as e:
            logger.error(f"Error storing candle data: {e}")
            raise

    async def get_recent_candles(self, limit: int = 200) -> List[Candle]:
        """최근 캔들 데이터 비동기 조회"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT timestamp, open, high, low, close, volume, quote_volume
                        FROM kline_1m
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (limit,))
                    
                    rows = await cursor.fetchall()
                    
                    return [
                        Candle(
                            timestamp=row[0],
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            volume=float(row[5]),
                            quote_volume=float(row[6]) if row[6] else None
                        )
                        for row in rows
                    ]
                    
        except Exception as e:
            logger.error(f"Error fetching recent candles: {e}")
            return []

    async def store_initial_candles(self, candles: List[Dict]):
        """초기 캔들 데이터 일괄 비동기 저장"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for candle_data in candles:
                        await cursor.execute("""
                            INSERT INTO kline_1m 
                            (timestamp, open, high, low, close, volume, quote_volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
                        """, (
                            int(candle_data[0]),
                            float(candle_data[1]),
                            float(candle_data[2]),
                            float(candle_data[3]),
                            float(candle_data[4]),
                            float(candle_data[5]),
                            float(candle_data[6]),
                            float(candle_data[1]),
                            float(candle_data[2]),
                            float(candle_data[3]),
                            float(candle_data[4]),
                            float(candle_data[5]),
                            float(candle_data[6])
                        ))
                        
            logger.info(f"Successfully stored {len(candles)} initial candles")
            
        except Exception as e:
            logger.error(f"Error storing initial candles: {e}")
            raise

    async def store_market_indicators(self, timestamp: int, indicators: dict):
        """시장 지표 비동기 저장"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO market_sentiment_data 
                        (timestamp, symbol, open_interest, oi_rsi, oi_slope, oi_change_percent,
                        long_ratio, short_ratio, ls_ratio_slope, ls_ratio_acceleration)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open_interest=%s, oi_rsi=%s, oi_slope=%s, oi_change_percent=%s,
                        long_ratio=%s, short_ratio=%s, ls_ratio_slope=%s, ls_ratio_acceleration=%s
                    """, (
                        timestamp,
                        'BTCUSDT',  # 현재는 BTCUSDT만 사용
                        indicators.get('open_interest', 0.0),
                        indicators.get('oi_rsi', 0.0),
                        indicators.get('oi_slope', 0.0),
                        indicators.get('oi_change_percent', 0.0),
                        indicators.get('long_ratio', 0.0),
                        indicators.get('short_ratio', 0.0),
                        indicators.get('ls_ratio_slope', 0.0),
                        indicators.get('ls_ratio_acceleration', 0.0),
                        # UPDATE 문을 위한 값 반복
                        indicators.get('open_interest', 0.0),
                        indicators.get('oi_rsi', 0.0),
                        indicators.get('oi_slope', 0.0),
                        indicators.get('oi_change_percent', 0.0),
                        indicators.get('long_ratio', 0.0),
                        indicators.get('short_ratio', 0.0),
                        indicators.get('ls_ratio_slope', 0.0),
                        indicators.get('ls_ratio_acceleration', 0.0)
                    ))
                    
            logger.debug(f"Successfully stored market indicators for timestamp {timestamp}")
                    
        except Exception as e:
            logger.error(f"Error storing market indicators: {e}")
            raise

    async def store_trade(self, trade_data: dict):
        """거래 기록 비동기 저장"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO trade_history 
                        (timestamp, symbol, side, size, entry_price, exit_price,
                        pnl, pnl_percentage, leverage, trade_type, entry_type, exit_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        trade_data['timestamp'],
                        trade_data['symbol'],
                        trade_data['side'],
                        trade_data['size'],
                        trade_data['entry_price'],
                        trade_data['exit_price'],
                        trade_data['pnl'],
                        trade_data['pnl_percentage'],
                        trade_data['leverage'],
                        trade_data['trade_type'],
                        trade_data['entry_type'],
                        trade_data['exit_reason']
                    ))
                    
        except Exception as e:
            logger.error(f"Error storing trade history: {e}")
            raise

    async def close(self):
        """연결 풀 종료"""
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()