from api_services import main
from db_services import save_to_database
import asyncio

async def run():
    await main()
    save_to_database()

# 최상위 레벨에서는 asyncio.run()을 사용하여 비동기 함수를 실행
asyncio.run(run())