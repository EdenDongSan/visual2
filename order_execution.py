import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime
import asyncio
from data_api import BitgetAPI
import time
from models import Position

logger = logging.getLogger(__name__)

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

class OrderExecutor:
    def __init__(self, api: BitgetAPI):
        self.api = api
        self.positions: Dict[str, Position] = {}
        self.pending_orders: Dict[str, Dict] = {}  # 미체결 주문 관리
        self.order_check_interval = 1  # 주문 체결 확인 간격 (초)
        
    async def wait_for_order_fill(self, symbol: str, order_id: str, timeout: int = 1) -> bool:  #open_position함수에서 호출당한다. filled 즉 채결된 상태가 되면 true 반환. 캔슬되거나 타임아웃내에 filled 상태가 되지않으면 false 반환.
        """주문 체결 대기"""
        start_time = time.time()
        logger.info(f"Waiting for order {order_id} to fill (timeout: {timeout}s)")
        
        while time.time() - start_time < timeout:
            try:
                response = await self.api.get_order_detail(symbol, order_id)  # 와 여기서 쓰인다.
                
                if response.get('code') == '00000':
                    status = response['data']['state']
                    price = response['data'].get('priceAvg', '0')
                    logger.info(f"Order {order_id} status: {status}, avg price: {price}")
                    
                    if status == 'filled':
                        logger.info(f"Order {order_id} filled successfully")
                        return True
                    elif status in ['cancelled', 'canceled']:
                        logger.warning(f"Order {order_id} was cancelled")
                        return False
                        
                await asyncio.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error checking order status: {e}")
                await asyncio.sleep(0.2)
                
        logger.warning(f"Order {order_id} fill timeout after {timeout}s")
        return False
        
    async def _set_position_leverage(self, symbol: str, leverage: int) -> bool:
        """포지션 진입 전 레버리지 설정
        
        Returns:
            bool: 레버리지 설정 성공 여부
        """
        try:
            # 기존 set_leverage 메서드 호출
            response = await self.api.set_leverage(symbol=symbol, leverage=leverage)       # api 함수는 여기서 호출.
            
            # API 응답 검증
            if response and response.get('code') == '00000':
                return True
            
            logger.error(f"Failed to set leverage: {response.get('msg') if response else 'No response'}")
            return False
            
        except Exception as e:
            logger.error(f"Error setting leverage: {e}")
            return False
        
    
    async def cancel_all_symbol_orders(self, symbol: str) -> bool:              # trading_strategy 메인 무한루프 함수의 시작점 부근에서 호출된다. 실제 포지션 진입함수를 실행할때 처음에도 쓰인다. 자주호출된다.
        """특정 심볼의 모든 미체결 주문 취소"""
        try:
            cancel_results = await self.api.cancel_all_pending_orders(symbol)          #api주문을 호출시킨다.
            success = all(result.get('code') == '00000' for result in cancel_results)
            
            if success:
                # pending_orders에서 해당 심볼의 주문 제거. 이건 클래스 내부다.
                self.pending_orders = {
                    order_id: info 
                    for order_id, info in self.pending_orders.items() 
                    if info['symbol'] != symbol
                }
                logger.info(f"Successfully cancelled all pending orders for {symbol}")
            else:
                logger.error(f"Some orders failed to cancel for {symbol}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error cancelling all orders for {symbol}: {e}")
            return False
                
    async def open_position(self, symbol: str, side: str, size: float,                   
                       leverage: int, stop_loss_price: float, 
                       take_profit_price: float, current_price: float,
                       order_type: str = 'limit', price: str = None) -> bool:
        """포지션 오픈"""
        try:
            position = await self.api.get_position(symbol)
            if position:
                logger.warning(f"Position already exists for {symbol}")
                return False
                
            # 레버리지 설정
            await self._set_position_leverage(symbol, leverage)
            
            # API 파라미터 설정
            api_side = 'buy' if side == 'long' else 'sell'
            hold_side = side  # long 또는 short

            # 문자열로 변환된 값들 준비
            str_size = str(size)
            str_stop_loss = str(round(float(stop_loss_price) * 10) / 10)
            str_take_profit = str(round(float(take_profit_price) * 10) / 10)
            str_price = str(round(float(price if price else current_price) * 10) / 10)
            
            # 메인 오더 실행
            response = await self.api.place_order(
                symbol=symbol,
                side=api_side,
                trade_side='open',
                size=str_size,
                margin_coin='USDT',
                order_type=order_type,
                price=str_price if order_type == 'limit' else None
            )
            
            if response.get('code') == '00000':
                order_id = response['data']['orderId']
                logger.info(f"Main order placed successfully: {order_id}")
                
                # 리밋 주문인 경우 체결 대기
                if order_type == 'limit':
                    order_filled = await self.wait_for_order_fill(symbol, order_id)
                    if not order_filled:
                        logger.error("Order not filled within timeout")
                        await self.api.cancel_order(symbol, order_id)
                        return False
                else:
                    # 시장가 주문인 경우 짧은 대기
                    await asyncio.sleep(2)
                
                # 포지션 생성 확인
                position = None
                retry_count = 0
                while retry_count < 5:  # 최대 5회 확인
                    position = await self.get_position(symbol)
                    if position:
                        break
                    retry_count += 1
                    await asyncio.sleep(1)
                
                if not position:
                    logger.error("Position was not created after order fill")
                    return False
                
                # TP/SL 설정 재시도 로직
                sl_set = False
                tp_set = False
                
                # 스탑로스 주문 (최대 3회 시도)
                for attempt in range(3):
                    logger.info(f"Setting stop loss, attempt {attempt + 1}/3")
                    sl_response = await self.api.place_tpsl_order(
                        symbol=symbol,
                        plan_type='loss_plan',
                        trigger_price=str_stop_loss,
                        hold_side=hold_side,
                        size=str_size,
                        execute_price='0'
                    )
                    
                    if sl_response.get('code') == '00000':
                        sl_set = True
                        logger.info("Stop loss set successfully")
                        break
                    
                    logger.error(f"Failed to set stop loss: {sl_response}")
                    await asyncio.sleep(1)
                
                # 테이크프로핏 주문 (최대 3회 시도)
                for attempt in range(3):
                    logger.info(f"Setting take profit, attempt {attempt + 1}/3")
                    tp_response = await self.api.place_tpsl_order(
                        symbol=symbol,
                        plan_type='profit_plan',
                        trigger_price=str_take_profit,
                        hold_side=hold_side,
                        size=str_size,
                        execute_price='0'
                    )
                    
                    if tp_response.get('code') == '00000':
                        tp_set = True
                        logger.info("Take profit set successfully")
                        break
                    
                    logger.error(f"Failed to set take profit: {tp_response}")
                    await asyncio.sleep(1)
                
                # Continue with position even if TP/SL setting fails
                if not (sl_set and tp_set):
                    logger.warning("Failed to set some TP/SL orders, but continuing with position")
                
                # 포지션 정보 업데이트
                self.positions[symbol] = Position(
                    symbol=symbol,
                    side=side,
                    size=float(str_size),
                    entry_price=float(str_price),
                    stop_loss_price=float(str_stop_loss),
                    take_profit_price=float(str_take_profit),
                    timestamp=int(time.time() * 1000),
                    leverage=leverage
                )
                
                logger.info(f"Successfully opened position for {symbol}")
                return True
            
            logger.error(f"Failed to place main order: {response}")
            return False
        
        except Exception as e:
            logger.error(f"Error opening position: {e}")
            return False
        
     # 트레이딩스트레테지에서 첫번째로 호출당한다.   
    async def get_position(self, symbol: str) -> Optional[Position]:
        """현재 포지션 상태 조회"""
        try:
            return await self.api.get_position(symbol)  # await 추가
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None
            
    def update_position_status(self, symbol: str, is_closed: bool = False):          #기본값은 false로 안전장치 설정. 실제 청산로직때는 true값으로 인자 전달해서 기존포지션 삭제가능.
        """포지션 상태 업데이트"""
        if is_closed and symbol in self.positions:
            del self.positions[symbol]

    async def execute_market_close(self, position: Position) -> bool:             # 사용하는 함수. close_position에서 비동기로 호출됨.
        """시장가로 포지션 청산"""
        try:
            logger.info(f"시장가 청산 시작: {position.symbol}")
            
            # 기존 주문들 취소
            await self.cancel_all_symbol_orders(position.symbol)
            
            # 시장가 청산 주문 실행
            response = await self.api.close_position(position.symbol)
            
            if response.get('code') == '00000':
                logger.info(f"포지션 청산 성공: {position.symbol}")
                # 포지션 상태 업데이트
                self.update_position_status(position.symbol, is_closed=True)
                return True
            else:
                logger.error(f"포지션 청산 실패: {response}")
                return False
                
        except Exception as e:
            logger.error(f"시장가 청산 중 오류 발생: {e}")
            return False

    async def execute_limit_close(self, position: Position, limit_price: float, 
                            partial_size: Optional[float] = None) -> bool:
        """지정가로 포지션 청산 (부분 청산 지원)"""
        try:
            logger.info(f"지정가 청산 시작: {position.symbol}, 가격: {limit_price}")
            
            # 청산할 수량 결정
            close_size = partial_size if partial_size else position.size
            
            # 청산 주문의 방향 설정
            side = 'buy' if position.side == 'long' else 'sell'
            
            response = await self.api.place_order(
                symbol=position.symbol,
                side=side,
                trade_side='close',
                size=str(close_size),
                order_type='limit',
                price=str(limit_price)
            )
            
            if response.get('code') == '00000':
                order_id = response['data']['orderId']
                # 주문 체결 대기
                filled = await self.wait_for_order_fill(position.symbol, order_id)
                if filled:
                    logger.info(f"지정가 청산 성공: {position.symbol}")
                    # 전체 청산인 경우에만 상태 업데이트
                    if not partial_size or abs(partial_size - position.size) < 0.000001:
                        self.update_position_status(position.symbol, is_closed=True)
                    return True
                else:
                    logger.warning(f"지정가 청산 주문 미체결: {position.symbol}")
                    return False
            else:
                logger.error(f"지정가 청산 주문 실패: {response}")
                return False
                
        except Exception as e:
            logger.error(f"지정가 청산 중 오류 발생: {e}")
            return False