import asyncio
import logging
import lighter

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://mainnet.zklighter.elliot.ai"

async def main():
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    
    try:
        # 获取平台信息
        root_api = lighter.RootApi(api_client)
        info = await root_api.info()
        print("=== Lighter 平台信息 ===")
        print(f"平台信息: {info}")
        
        # 尝试获取不同市场索引的订单簿信息
        order_api = lighter.OrderApi(api_client)
        
        print("\n=== 检查不同市场索引 ===")
        for market_index in range(5):  # 检查前5个市场索引
            try:
                print(f"\n检查市场索引 {market_index}:")
                order_book = await order_api.order_book_orders(market_index, 1)
                print(f"  市场 {market_index} 订单簿: {order_book}")
                
                # 检查是否有买卖盘数据
                if hasattr(order_book, 'bids') and order_book.bids:
                    print(f"  买盘价格: {order_book.bids[0].price}")
                if hasattr(order_book, 'asks') and order_book.asks:
                    print(f"  卖盘价格: {order_book.asks[0].price}")
                    
            except Exception as e:
                print(f"  市场 {market_index} 不可用: {e}")
                
    except Exception as e:
        print(f"获取信息失败: {e}")
    finally:
        await api_client.close()

if __name__ == "__main__":
    asyncio.run(main())
