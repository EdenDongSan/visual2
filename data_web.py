import asyncio
import websockets
import json
import logging
from websockets.protocol import State
from data_api import BitgetAPI
from market_data_manager import MarketDataManager
from database_manager import DatabaseManager, Candle
import pandas as pd
logger = logging.getLogger(__name__)

class BitgetWebsocket:
    def __init__(self, api: BitgetAPI, market_data: MarketDataManager):
        self.WS_URL = "wss://ws.bitget.com/v2/ws/public"
        self.ws = None
        self.api = api
        self.market_data = market_data
        self.db_manager = DatabaseManager()  
        self.connected = False
        self.reconnecting = False
        self.subscriptions = []
        self._processing = False
   
    async def connect(self):
       """WebSocket 연결 설정"""
       while not self.connected and not self.reconnecting:
           try:
               self.reconnecting = True
               logger.info("Attempting to connect to WebSocket...")
               self.ws = await websockets.connect(self.WS_URL)
               self.connected = True
               self.reconnecting = False
               logger.info("WebSocket connected successfully")
               
               # 기존 구독 복구
               for symbol in self.subscriptions:
                   await self.subscribe_kline(symbol)
                   
               asyncio.create_task(self._keep_alive())
               return True
           except Exception as e:
               logger.error(f"WebSocket connection failed: {e}")
               await asyncio.sleep(5)
           finally:
               self.reconnecting = False
       return False

    async def disconnect(self):
        """WebSocket 연결 종료"""
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=5.0)
                self.connected = False
                logger.info("WebSocket disconnected")
            except asyncio.TimeoutError:
                logger.warning("WebSocket close timeout")
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")
                
    async def is_connected(self):
       """웹소켓 연결 상태 확인"""
       return (self.ws is not None and 
               hasattr(self.ws, 'state') and 
               self.ws.state == State.OPEN)

    async def store_initial_candles(self, symbol: str = 'BTCUSDT'):
        """초기 캔들 데이터 저장"""
        try:
            logger.info("Fetching and storing initial candle data")
            
            response = await self.api.get_historical_candles(symbol)
            
            if response and response.get('code') == '00000':
                candles_data = response.get('data', [])
                
                # 직접 DatabaseManager의 메서드 호출
                await self.db_manager.store_initial_candles(candles_data)
                logger.info(f"Successfully stored {len(candles_data)} historical candles")
            else:
                logger.error(f"Failed to fetch historical candles: {response}")
                
        except Exception as e:
            logger.error(f"Error storing initial candles: {e}")

    async def _keep_alive(self):
        """연결 유지 및 재연결 관리"""
        while True:
            try:
                if await self.is_connected():
                    await self.ws.send('ping')
                    await asyncio.sleep(20)
                else:
                    logger.info("WebSocket disconnected, attempting reconnect...")
                    self.connected = False
                    await self.connect()
            except Exception as e:
                logger.error(f"Error in keep_alive: {e}")
                self.connected = False
                await asyncio.sleep(5)
                await self.connect()

    async def subscribe_kline(self, symbol: str = 'BTCUSDT'):
       """K라인 데이터 구독"""
       if symbol not in self.subscriptions:
           self.subscriptions.append(symbol)

       subscribe_data = {
           "op": "subscribe",
           "args": [{
               "instType": "USDT-FUTURES",
               "channel": "candle1m",
               "instId": symbol
           }]
       }
       
       try:
           if await self.is_connected():
               await self.ws.send(json.dumps(subscribe_data))
               logger.info(f"Subscription request sent: {subscribe_data}")
               if not self._processing:
                   asyncio.create_task(self._process_messages())
           else:
               logger.warning("WebSocket not connected, attempting reconnection...")
               await self.connect()
       except Exception as e:
           logger.error(f"Error subscribing to kline: {e}")
           await self.connect()

    async def _process_messages(self):
        """메시지 처리 무한 루프"""
        if self._processing:
            return
           
        self._processing = True
        try:
            while await self.is_connected():
                try:
                    message = await self.ws.recv()
                    if message == 'pong':
                        continue
                       
                    data = json.loads(message)
                    logger.debug(f"Received data: {data}")
                    
                    if not self.reconnecting:  # 재연결 중이 아닐 때만 처리
                        if data.get('action') == 'update' and 'data' in data:
                            await self._handle_kline_data(data['data'])

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket connection closed, attempting reconnect...")
                    self.connected = False
                    self.reconnecting = True  # 재연결 상태 설정
                    await asyncio.sleep(5)
                    await self.connect()
                    self.reconnecting = False  # 재연결 완료
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await asyncio.sleep(1)
                    
        finally:
            self._processing = False

    async def _handle_kline_data(self, candle_data_list):
        """캔들 데이터 처리"""
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']
        
        for candle_data in candle_data_list:
            try:
                if not isinstance(candle_data, list) or len(candle_data) < 6:
                    logger.warning(f"Invalid candle data format: {candle_data}")
                    continue
                    
                # 데이터 타입 변환 및 검증
                candle = Candle(
                    timestamp=int(float(candle_data[0])),
                    open=float(candle_data[1]),
                    high=float(candle_data[2]),
                    low=float(candle_data[3]),
                    close=float(candle_data[4]),
                    volume=float(candle_data[5]),
                    quote_volume=float(candle_data[6]) if len(candle_data) > 6 else 0.0
                )
                
                # DataFrame 업데이트를 포함한 마켓 데이터 캐시 업데이트
                await self.market_data.update_latest_candle(candle)
                logger.debug(f"Successfully processed candle timestamp: {candle.timestamp}")
                
            except (ValueError, IndexError) as e:
                logger.error(f"Data conversion error: {e}, data: {candle_data}")
            except Exception as e:
                logger.error(f"Unexpected error processing candle: {e}, data: {candle_data}")

                  #commit