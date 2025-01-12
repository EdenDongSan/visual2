import asyncio
import signal
import sys
import psutil
from data_web import BitgetWebsocket
from data_api import BitgetAPI
import os
from dotenv import load_dotenv
import logging
from order_execution import OrderExecutor
from trading_strategy_implementation import TradingStrategy
from market_data_manager import MarketDataManager
from database_manager import DatabaseManager
from logging_setup import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self):
        # 환경변수 로드
        load_dotenv()
        
        # API 설정
        self.api_key = os.getenv('BITGET_ACCESS_KEY')
        self.secret_key = os.getenv('BITGET_SECRET_KEY')
        self.passphrase = os.getenv('BITGET_PASSPHRASE')
        
        # DB 매니저 초기화
        self.db_manager = DatabaseManager()
        
        # API 클라이언트 초기화
        self.api = BitgetAPI(self.api_key, self.secret_key, self.passphrase)
        
        # MarketData 매니저 초기화
        self.market_data = MarketDataManager(api=self.api)
        
        # 웹소켓 초기화
        self.ws = BitgetWebsocket(api=self.api, market_data=self.market_data)
        
        # 주문 실행기 초기화
        self.order_executor = OrderExecutor(self.api)
        
        # 트레이딩 전략 초기화
        self.strategy = TradingStrategy(
            market_data=self.market_data,
            order_executor=self.order_executor
        )
        
        self.is_running = False
        self.tasks = []
        self._cleanup_done = asyncio.Event()
    
    async def setup(self):
        """초기 설정 비동기 수행"""
        # DB 초기화
        await self.db_manager.initialize()

    async def cleanup(self):
        """프로그램 종료 시 정리 작업 수행"""
        if not self.is_running:
            return
                
        logger.info("프로그램 종료 시작...")
        self.is_running = False
        
        try:
            # 웹소켓 연결 종료
            if self.ws and self.ws.ws:
                try:
                    await asyncio.wait_for(self.ws.disconnect(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("웹소켓 연결 종료 시간 초과")
                except Exception as e:
                    logger.error(f"웹소켓 연결 종료 중 오류: {e}")
            
            # API 세션 종료
            if hasattr(self.api, 'session') and self.api.session:
                try:
                    await asyncio.wait_for(self.api.session.close(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("API 세션 종료 시간 초과")
                except Exception as e:
                    logger.error(f"API 세션 종료 중 오류: {e}")
        
        finally:
            self._cleanup_done.set()
            logger.info("프로그램 종료 완료")
            
    async def start(self):
        """트레이딩 봇 시작"""
        try:
            self.is_running = True

            # 초기 설정 수행
            await self.setup()
            
            # 웹소켓 연결
            await self.ws.connect()
            
            # 초기 데이터 로드 및 초기화
            await self.ws.store_initial_candles()  # DB에 초기 데이터 저장
            await self.market_data.initialize()    # 캐시 초기화
            
            # 기존 미체결 주문 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            # 태스크 생성
            self.tasks = [
                asyncio.create_task(self.ws.subscribe_kline()),
                asyncio.create_task(self.strategy.run()),
                asyncio.create_task(self._monitor_system())
            ]
            
            # 태스크 완료 대기
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
        except asyncio.CancelledError:
            logger.info("프로그램 실행 취소됨")
        except Exception as e:
            logger.error(f"실행 중 오류 발생: {e}")
        finally:
            await self.cleanup()
            await self._cleanup_done.wait()

    async def _monitor_system(self):
        """시스템 상태 모니터링"""
        while self.is_running:
            try:
                # 연결 상태 확인
                if not await self.ws.is_connected():
                    logger.warning("웹소켓 연결 끊김 감지, 재연결 시도...")
                    await self.ws.connect()
                
                # 메모리 사용량 모니터링
                process = psutil.Process(os.getpid())
                memory_usage = process.memory_info().rss / 1024 / 1024  # MB
                logger.info(f"메모리 사용량: {memory_usage:.2f} MB")
                
                await asyncio.sleep(60)  # 1분마다 체크
                
            except Exception as e:
                logger.error(f"모니터링 중 오류 발생: {e}")
                await asyncio.sleep(5)

def main():
   bot = TradingBot()
   loop = asyncio.new_event_loop()
   asyncio.set_event_loop(loop)
   
   def signal_handler():
       logger.info("종료 시그널 수신...")
       for task in asyncio.all_tasks(loop):
           task.cancel()
   
   try:
       for sig in (signal.SIGINT, signal.SIGTERM):
           loop.add_signal_handler(sig, signal_handler)
   except NotImplementedError:
       # Windows에서는 signal.signal 사용
       signal.signal(signal.SIGINT, lambda s, f: signal_handler())
       signal.signal(signal.SIGTERM, lambda s, f: signal_handler())
   
   try:
       loop.run_until_complete(bot.start())
   except KeyboardInterrupt:
       logger.info("사용자에 의한 프로그램 종료")
       loop.run_until_complete(bot.cleanup())
   except Exception as e:
       logger.error(f"예기치 않은 오류 발생: {e}")
   finally:
       loop.close()

if __name__ == "__main__":
   main()