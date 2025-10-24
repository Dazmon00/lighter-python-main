import asyncio
import logging
import lighter

logging.basicConfig(level=logging.DEBUG)

# The API_KEY_PRIVATE_KEY provided belongs to a dummy account registered on Testnet.
# It was generated using the setup_system.py script, and servers as an example.
# Alternatively, you can go to https://app.lighter.xyz/apikeys for mainnet api keys
BASE_URL = "https://mainnet.zklighter.elliot.ai"
API_KEY_PRIVATE_KEY = ""
ACCOUNT_INDEX = 3156
API_KEY_INDEX = 3


def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


async def main():
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))

    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )

    err = client.check_client()
    if err is not None:
        print(f"CheckClient error: {trim_exception(err)}")
        return

    loop_count = 0
    
    try:
        while True:
            loop_count += 1
            print(f"\n{'='*50}")
            print(f"第 {loop_count} 次循环开始")
            print(f"{'='*50}")

            # 创建0.01 BTC的市价买单
            print("=== 创建BTC市价买单 ===")
            tx = await client.create_market_order_limited_slippage(
                market_index=1,  # BTC/USDC 市场索引
                client_order_index=100 + loop_count,  # 动态索引，避免重复
                base_amount=100,  # 0.01 BTC = 1000 最小单位
                max_slippage=0.05,  # 最大滑点5%
                is_ask=False,  # 买单
                # ideal_price=None,  # 不设置理想价格，让系统自动获取当前市场价格
            )
            print(f"市价买单 {tx=}")
            if tx is not None:
                print("市价买单创建成功！")
            else:
                print("买单创建失败")

            # 创建0.01 BTC的市价卖单
            print("\n=== 创建BTC市价卖单 ===")
            tx = await client.create_market_order_limited_slippage(
                market_index=1,  # BTC/USDC 市场索引
                client_order_index=200 + loop_count,  # 动态索引，避免重复
                base_amount=100,  # 0.01 BTC = 1000 最小单位
                max_slippage=0.05,  # 最大滑点5%
                is_ask=True,  # 卖单
                # ideal_price=None,  # 不设置理想价格，让系统自动获取当前市场价格
            )
            print(f"市价卖单 {tx=}")
            if tx is not None:
                print("市价卖单创建成功！")
            else:
                print("卖单创建失败")

            # 创建认证令牌
            auth, err = client.create_auth_token_with_expiry(lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY)
            print(f"\n认证令牌: {auth=}")
            if err is not None:
                print(f"认证令牌创建失败: {trim_exception(err)}")

            print(f"\n第 {loop_count} 次循环完成，等待2-3秒...")
            await asyncio.sleep(2.5)  # 暂停2.5秒
            
    except KeyboardInterrupt:
        print(f"\n收到中断信号，程序停止。总共执行了 {loop_count} 次循环。")
    except Exception as e:
        print(f"\n发生错误: {e}")
    finally:
        await client.close()
        await api_client.close()
        print("客户端连接已关闭。")


if __name__ == "__main__":
    asyncio.run(main())
