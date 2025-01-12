import logging
from typing import Optional, Tuple
from dataclasses import dataclass
import asyncio
import time
from order_execution import OrderExecutor
from market_data_manager import MarketDataManager
from models import Position, TradingMetrics
import math
from models import MarketData

logger = logging.getLogger(__name__)

@dataclass
class TradingConfig:
    leverage: int = 10
    max_leverage: int = 20
    position_size_pct: float = 95.0
    stop_loss_pct: float = 0.8  # 0.8%로 조정
    take_profit_pct: float = 1.5  # 1.5%로 조정
    ratio_threshold: float = 0.02
    min_slope: float = 0.0001
    acceleration_threshold: float = 0.0001

class TradingStrategy:
    def __init__(self, market_data: MarketDataManager, order_executor: OrderExecutor):
        self.market_data = market_data
        self.order_executor = order_executor
        self.config = TradingConfig()
        self.metrics = TradingMetrics()
        self.in_position = False
        self.last_trade_time = 0
        self.min_trade_interval = 1

    async def calculate_position_size(self, current_price: float) -> float:
        """계좌 잔고를 기반으로 포지션 크기 계산"""
        try:
            account_info = await self.order_executor.api.get_account_balance()
            
            if account_info.get('code') != '00000':
                logger.error(f"Failed to get account balance: {account_info}")
                return 0.0
            
            account_data = account_info.get('data', [])[0]
            available_balance = float(account_data.get('available', '0'))
            
            # 전체 잔고의 92% 사용
            trade_amount = available_balance * (self.config.position_size_pct / 100)
            position_size = (trade_amount * self.config.leverage) / current_price
            floor_size = math.floor(position_size * 1000) / 1000  # 소수점 3자리까지
            
            logger.info(f"Calculated position size: {floor_size} (Available balance: {available_balance})")
            return floor_size
                
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0.0

    async def should_open_long(self, indicators: dict) -> bool:
        """롱 포지션 진입 조건 확인"""
        try:
            # 시그널 초기화 확인
            if 'oi_acc_bull' not in self.market_data.signals.columns:
                self.market_data.signals['oi_acc_bull'] = False
            if 'long_extreme_bull' not in self.market_data.signals.columns:
                self.market_data.signals['long_extreme_bull'] = False
            if 'high_liquidity' not in self.market_data.signals.columns:
                self.market_data.signals['high_liquidity'] = False
                
            # 기본 분석 실행
            await self.market_data.analyze_oi_accumulation()
            await self.market_data.analyze_long_ratio_extremes()
            await self.market_data.analyze_market_liquidity()
            
            if self.market_data.signals.empty:
                logger.warning("No signals available yet")
                return False
            
            # 2개 필수 + 1개 선택 조건 확인
            try:
                bull_conditions_primary = (
                    self.market_data.signals['oi_acc_bull'].iloc[-1] and
                    self.market_data.signals['long_extreme_bull'].iloc[-1]
                )
                
                bull_conditions_secondary = (
                    self.market_data.signals['high_liquidity'].iloc[-1] or
                    (indicators.get('oi_acceleration', 0) > 0)
                )
            except IndexError:
                logger.warning("Insufficient signal data")
                return False
                
            if not (bull_conditions_primary and bull_conditions_secondary):
                return False
            
            # 진입 조건 상세 분석
            is_valid_entry, entry_score = await self.market_data.analyze_entry_conditions('long')
            
            if is_valid_entry:
                logger.info(f"Long entry conditions met - Entry score: {entry_score:.2f}")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"롱 진입 조건 확인 중 에러: {e}")
            return False

    async def should_open_short(self, indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        try:
            # 시그널 초기화 확인
            if 'oi_acc_bear' not in self.market_data.signals.columns:
                self.market_data.signals['oi_acc_bear'] = False
            if 'long_extreme_bear' not in self.market_data.signals.columns:
                self.market_data.signals['long_extreme_bear'] = False
            if 'high_liquidity' not in self.market_data.signals.columns:
                self.market_data.signals['high_liquidity'] = False
                
            # 기본 분석 실행
            await self.market_data.analyze_oi_accumulation()
            await self.market_data.analyze_long_ratio_extremes()
            await self.market_data.analyze_market_liquidity()
            
            if self.market_data.signals.empty:
                logger.warning("No signals available yet")
                return False
            
            # 2개 필수 + 1개 선택 조건 확인
            try:
                bear_conditions_primary = (
                    self.market_data.signals['oi_acc_bear'].iloc[-1] and
                    self.market_data.signals['long_extreme_bear'].iloc[-1]
                )
                
                bear_conditions_secondary = (
                    self.market_data.signals['high_liquidity'].iloc[-1] or
                    (indicators.get('oi_acceleration', 0) < 0)
                )
            except IndexError:
                logger.warning("Insufficient signal data")
                return False
                
            if not (bear_conditions_primary and bear_conditions_secondary):
                return False
            
            # 진입 조건 상세 분석
            is_valid_entry, entry_score = await self.market_data.analyze_entry_conditions('short')
            
            if is_valid_entry:
                logger.info(f"Short entry conditions met - Entry score: {entry_score:.2f}")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"숏 진입 조건 확인 중 에러: {e}")
            return False

    async def should_close_position(self, position: Position, indicators: dict) -> Tuple[bool, str]:
        """포지션 청산 조건 확인"""
        try:
            if not position:
                return False, ""
                
            current_price = self.market_data.get_latest_price()
            position_return = (
                (current_price - position.entry_price) / position.entry_price 
                if position.side == 'long'
                else (position.entry_price - current_price) / position.entry_price
            )
            
            # 손절/익절 기준 차등 적용
            stop_loss = -0.008 if position.side == 'long' else -0.007
            take_profit = 0.015 if position.side == 'long' else 0.020
            
            # 손절/익절 확인
            if position_return < stop_loss:
                return True, "stop_loss"
            if position_return > take_profit:
                return True, "take_profit"
                
            # 반대 시그널 발생 확인
            if position.side == 'long' and self.market_data.signals['oi_acc_bear'].iloc[-1]:
                return True, "counter_signal"
            if position.side == 'short' and self.market_data.signals['oi_acc_bull'].iloc[-1]:
                return True, "counter_signal"
                
            # 수익 구간에서 모멘텀 반전 확인
            if position_return > 0:
                price_momentum = indicators.get('price_momentum', 0)
                momentum_threshold = -0.001 if position.side == 'long' else 0.0008
                
                if ((position.side == 'long' and price_momentum < momentum_threshold) or
                    (position.side == 'short' and price_momentum > momentum_threshold)):
                    return True, "momentum_reversal"
            
            return False, ""
            
        except Exception as e:
            logger.error(f"청산 조건 확인 중 에러: {e}")
            return True, "error"

    async def execute_entry(self, side: str, current_price: float) -> bool:
        """포지션 진입 실행"""
        try:
            total_size = await self.calculate_position_size(current_price)
            
            # 진입가격 설정 (현재가 +/- 0.1달러)
            entry_price = current_price + 0.1 if side == "long" else current_price - 0.1
            # 스탑로스 설정 (차등 적용)
            stop_loss_pct = self.config.stop_loss_pct * (1 if side == "long" else 0.875)
            stop_loss_price = (
                entry_price * (1 - stop_loss_pct/100) 
                if side == "long" 
                else entry_price * (1 + stop_loss_pct/100)
            )
            
            # 가격 반올림
            entry_price = round(entry_price * 10) / 10
            stop_loss_price = round(stop_loss_price * 10) / 10
            
            success = await self.order_executor.open_position(
                symbol="BTCUSDT",
                side=side,
                size=str(total_size),
                leverage=self.config.leverage,
                stop_loss_price=stop_loss_price,
                take_profit_price=0.0,  # 동적 익절 사용
                current_price=current_price,
                order_type='limit',
                price=str(entry_price)
            )
            
            if success:
                # 거래 기록 저장
                trade_data = {
                    'timestamp': int(time.time() * 1000),
                    'symbol': "BTCUSDT",
                    'side': side,
                    'size': float(total_size),
                    'entry_price': float(entry_price),
                    'exit_price': 0.0,
                    'pnl': 0.0,
                    'pnl_percentage': 0.0,
                    'leverage': self.config.leverage,
                    'trade_type': 'limit',
                    'entry_type': 'trend_follow',
                    'exit_reason': ''
                }
                
                await self.market_data.db_manager.store_trade(trade_data)
                logger.info(f"Trade entry recorded: {side} {total_size} @ {entry_price}")
                
                self.in_position = True
                self.last_trade_time = int(time.time())
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"진입 실행 중 에러: {e}")
            return False

    async def execute_close(self, position: Position, reason: str) -> bool:
        """포지션 청산 실행"""
        try:
            logger.info(f"Closing position: {position.symbol}, reason: {reason}")
            
            current_price = self.market_data.get_latest_price()
            close_success = await self.order_executor.execute_market_close(position)
            
            if close_success:
                # PnL 계산
                if position.side == 'long':
                    pnl = (current_price - position.entry_price) * position.size
                    pnl_percentage = ((current_price - position.entry_price) / position.entry_price) * 100
                else:
                    pnl = (position.entry_price - current_price) * position.size
                    pnl_percentage = ((position.entry_price - current_price) / position.entry_price) * 100

                # 거래 기록 저장
                trade_data = {
                    'timestamp': int(time.time() * 1000),
                    'symbol': position.symbol,
                    'side': position.side,
                    'size': position.size,
                    'entry_price': position.entry_price,
                    'exit_price': current_price,
                    'pnl': pnl,
                    'pnl_percentage': pnl_percentage,
                    'leverage': position.leverage,
                    'trade_type': 'market',
                    'entry_type': 'trend_follow',
                    'exit_reason': reason
                }
                
                await self.market_data.db_manager.store_trade(trade_data)
                self.metrics.update(pnl)
                self.in_position = False
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"청산 실행 중 에러: {e}")
            return False

    def _adjust_leverage(self, volatility: float) -> int:
        """변동성에 따른 레버리지 동적 조정"""
        try:
            base_volatility = 100
            volatility_ratio = base_volatility / volatility if volatility > 0 else 1
            
            adjusted_leverage = int(self.config.max_leverage * volatility_ratio)
            adjusted_leverage = max(5, min(adjusted_leverage, self.config.max_leverage))
            
                # 이전 값과 다를 때만 로깅
            if not hasattr(self, '_last_logged_leverage') or self._last_logged_leverage != adjusted_leverage:
                logger.info(f"Adjusted leverage: {adjusted_leverage} (volatility: {volatility:.2f})")
                self._last_logged_leverage = adjusted_leverage
            return adjusted_leverage
            
        except Exception as e:
            logger.error(f"Error adjusting leverage: {e}")
            return self.config.leverage

    async def _process_trading_logic(self):
        """트레이딩 로직 처리"""
        try:
            # 데이터 업데이트
            await asyncio.gather(
                self.market_data.update_position_ratio("BTCUSDT"),
                self.market_data.update_open_interest("BTCUSDT")
            )
            
            # 현재 포지션 확인
            position = await self.order_executor.get_position("BTCUSDT")
            
            # 기술적 지표 계산
            indicators = self.market_data.calculate_technical_indicators()
            if not indicators:
                return
                
            # OI 및 시장 상태 분석
            await self.market_data.analyze_oi_accumulation()
            await self.market_data.analyze_long_ratio_extremes()
            await self.market_data.analyze_market_liquidity()

            current_time = int(time.time())
            current_price = indicators.get('last_close')
            
            if not current_price:
                return

            # 포지션이 있는 경우
            if position and position.size > 0:
                should_close, close_reason = await self.should_close_position(position, indicators)
                if should_close:
                    await self.execute_close(position, close_reason)
                    
            # 포지션이 없는 경우
            elif not self.in_position and (current_time - self.last_trade_time) >= self.min_trade_interval:
                # 레버리지 동적 조정
                volatility = self.market_data.calculate_atr(period=14)
                adjusted_leverage = self._adjust_leverage(volatility)
                self.config.leverage = adjusted_leverage
                
                # 진입 조건 확인
                if await self.should_open_long(indicators):
                    await self.execute_entry("long", current_price)
                elif await self.should_open_short(indicators):
                    await self.execute_entry("short", current_price)
            
        except Exception as e:
            logger.error(f"트레이딩 로직 처리 중 에러: {e}")

    async def run(self):
        """전략 실행 메인 루프"""
        try:
            logger.info("Trading strategy started")
            
            # 초기 미체결 주문 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            while True:
                await self._process_trading_logic()
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Trading strategy error: {e}")
        finally:
            logger.info("Trading strategy stopped")