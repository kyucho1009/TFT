from api_services import process_challenger_data, fetch_challenger_data
import asyncio

async def main():
    await process_challenger_data()

# 최상위 레벨에서는 asyncio.run()을 사용하여 비동기 함수를 실행
asyncio.run(main())