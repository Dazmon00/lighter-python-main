#!/usr/bin/env python3
"""
BTC交易脚本：获取当前价格，挂限价多单(价格-10)，成交后挂限价空单(价格+10)
如果10秒不成交，重新获取价格并重新挂单

使用方法：
1. 在代码中设置 ETHEREUM_PRIVATE_KEY 变量（第55行）
2. 运行脚本：python btc_open_close_position.py

注意：此脚本使用Paradex生产网，请确保有足够的资金
"""

import asyncio
import logging
import os
import sys
import time
import traceback
import aiohttp
from decimal import Decimal

# 添加paradex目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'paradex'))

from shared.api_config import ApiConfig
from shared.paradex_api_utils import Order, OrderSide, OrderType
from shared.api_client import (
    get_jwt_token, 
    get_paradex_config, 
    post_order_payload, 
    sign_order,
    get_open_orders,
    delete_order_payload,
    fetch_positions,
    get_markets,
    get_bbo
)
from onboarding import perform_onboarding
from utils import (
    generate_paradex_account,
    get_l1_eth_account,
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 配置参数
PARADEX_HTTP_URL = "https://api.prod.paradex.trade/v1"
BTC_MARKET = "BTC-USD-PERP"
ORDER_SIZE = Decimal("0.001")  # 0.001 BTC
CLIENT_ID_BUY = "btc-buy-order"
CLIENT_ID_SELL = "btc-sell-order"

# 在这里直接设置您的以太坊私钥（去掉0x前缀）
ETHEREUM_PRIVATE_KEY = "082fd5384ea743fdf51957d3b244bad9ec6f3b6ec56a148655b539f452cdf3d0"

class BTCParadexTrader:
    def __init__(self):
        self.config = None
        self.jwt_token = None
        
    async def initialize(self):
        """初始化Paradex账户和认证"""
        logging.info("开始初始化Paradex交易...")
        
        # 优先使用代码中设置的私钥，如果没有设置则使用环境变量
        eth_private_key = ETHEREUM_PRIVATE_KEY
        if eth_private_key == "your_ethereum_private_key_here":
            eth_private_key = os.getenv("ETHEREUM_PRIVATE_KEY")
        
        if not eth_private_key or eth_private_key == "your_ethereum_private_key_here":
            raise ValueError("请设置私钥：在代码中设置 ETHEREUM_PRIVATE_KEY 变量或设置环境变量 ETHEREUM_PRIVATE_KEY")
        
        # 创建配置
        self.config = ApiConfig()
        self.config.paradex_http_url = PARADEX_HTTP_URL
        self.config.ethereum_private_key = eth_private_key
        
        # 获取Paradex配置
        logging.info("获取Paradex配置...")
        self.config.paradex_config = await get_paradex_config(PARADEX_HTTP_URL)
        
        # 生成Paradex账户
        logging.info("生成Paradex账户...")
        _, eth_account = get_l1_eth_account(eth_private_key)
        self.config.paradex_account, self.config.paradex_account_private_key = generate_paradex_account(
            self.config.paradex_config, eth_account.key.hex()
        )
        
        # 执行onboarding
        logging.info("执行账户onboarding...")
        await perform_onboarding(
            self.config.paradex_config,
            self.config.paradex_http_url,
            self.config.paradex_account,
            self.config.paradex_account_private_key,
            eth_account.address,
        )
        
        # 获取JWT token
        logging.info("获取JWT token...")
        self.jwt_token = await get_jwt_token(
            self.config.paradex_config,
            self.config.paradex_http_url,
            self.config.paradex_account,
            self.config.paradex_account_private_key,
        )
        
        logging.info("Paradex初始化完成！")
        
    async def get_current_price(self, order_side=None):
        """获取当前BTC价格，只使用Paradex BBO API"""
        try:
            # 使用get_bbo函数获取BTC价格，设置超时
            logging.info(f"使用BBO API获取{BTC_MARKET}价格...")
            
            # 设置超时时间（3秒）
            try:
                bbo = await asyncio.wait_for(
                    get_bbo(PARADEX_HTTP_URL, BTC_MARKET), 
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                logging.error("BBO API请求超时（3秒）")
                return None
            
            logging.info(f"BBO响应: {bbo}")
            
            # 检查价格数据的新鲜度
            if 'last_updated_at' in bbo:
                last_updated = bbo['last_updated_at']
                current_time = int(time.time() * 1000)  # 转换为毫秒
                time_diff = current_time - last_updated
                
                # 如果价格数据超过10秒，认为过时
                if time_diff > 10000:  # 10秒 = 10000毫秒
                    logging.warning(f"价格数据过时（{time_diff/1000:.1f}秒前）")
                    return None
            
            # 根据订单类型选择合适的价格
            if order_side == OrderSide.Buy:
                # 买单使用bid价格（买价）
                price = bbo.get("bid")
                price_type = "bid"
            elif order_side == OrderSide.Sell:
                # 卖单使用ask价格（卖价）
                price = bbo.get("ask")
                price_type = "ask"
            else:
                # 默认使用mid价格
                price = bbo.get("mid") or bbo.get("bid") or bbo.get("ask")
                price_type = "mid"
            
            if price is not None:
                price_float = float(price)
                logging.info(f"从BBO获取BTC价格 ({price_type}): {price_float}")
                return price_float
            
            logging.error("BBO API无法获取有效价格")
            return None
            
        except Exception as e:
            logging.error(f"获取BTC价格失败: {e}")
            return None
    
    async def get_current_price_simple(self, order_side=None):
        """简化的价格获取方法，快速获取BBO价格"""
        try:
            logging.info("使用快速BBO API获取BTC价格...")
            # 设置更短的超时时间（2秒）
            try:
                bbo = await asyncio.wait_for(
                    get_bbo(PARADEX_HTTP_URL, BTC_MARKET), 
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                logging.error("快速BBO API请求超时（2秒）")
                return None
            
            # 根据订单类型选择合适的价格
            if order_side == OrderSide.Buy:
                # 买单使用bid价格（买价）
                price = bbo.get("bid")
                price_type = "bid"
            elif order_side == OrderSide.Sell:
                # 卖单使用ask价格（卖价）
                price = bbo.get("ask")
                price_type = "ask"
            else:
                # 默认使用bid价格
                price = bbo.get("bid") or bbo.get("ask") or bbo.get("mid")
                price_type = "bid"
            
            if price is not None:
                price_float = float(price)
                logging.info(f"从快速BBO获取BTC价格 ({price_type}): {price_float}")
                return price_float
            
            logging.error("快速BBO API无法获取有效价格")
            return None
        except Exception as e:
            logging.error(f"快速价格获取失败: {e}")
            return None
    
    async def get_btc_price_from_external_api(self):
        """从外部API获取BTC价格作为备用方案"""
        try:
            # 使用CoinGecko API作为备用，设置超时
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
                async with session.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd') as response:
                    if response.status == 200:
                        data = await response.json()
                        price = data['bitcoin']['usd']
                        logging.info(f"从CoinGecko获取BTC价格: {price}")
                        return price
                    else:
                        logging.error(f"CoinGecko API返回错误: {response.status}")
                        return None
        except asyncio.TimeoutError:
            logging.error("CoinGecko API请求超时（3秒）")
            return None
        except Exception as e:
            logging.error(f"从外部API获取BTC价格失败: {e}")
            return None
    
    async def get_current_positions(self):
        """获取当前持仓"""
        try:
            positions = await fetch_positions(PARADEX_HTTP_URL, self.jwt_token)
            logging.info(f"当前持仓: {positions}")
            return positions
        except Exception as e:
            logging.error(f"获取持仓失败: {e}")
            return []
    
    async def cancel_all_orders(self):
        """取消所有挂单"""
        try:
            open_orders = await get_open_orders(PARADEX_HTTP_URL, self.jwt_token)
            if open_orders:
                logging.info(f"发现 {len(open_orders)} 个挂单，正在取消...")
                for order in open_orders:
                    order_id = order.get("order_id") or order.get("id")
                    if order_id:
                        await delete_order_payload(PARADEX_HTTP_URL, self.jwt_token, order_id)
                        logging.info(f"已取消订单: {order_id}")
                        await asyncio.sleep(0.5)  # 避免请求过快
            else:
                logging.info("没有发现挂单")
        except Exception as e:
            logging.error(f"取消挂单失败: {e}")
    
    async def create_limit_order(self, side: OrderSide, size: Decimal, price: Decimal, client_id: str):
        """创建限价单"""
        try:
            order = Order(
                market=BTC_MARKET,
                order_type=OrderType.Limit,
                order_side=side,
                size=size,
                limit_price=price,
                client_id=client_id,
                signature_timestamp=int(time.time() * 1000),
            )
            
            # 签名订单
            sig = sign_order(self.config, order)
            order.signature = sig
            
            # 提交订单
            logging.info(f"提交{side.name}限价单: {size} BTC @ {price}")
            response = await post_order_payload(PARADEX_HTTP_URL, self.jwt_token, order.dump_to_dict())
            
            if response.get("status_code") == 201:
                logging.info(f"{side.name}限价单提交成功: {response}")
                return True
            else:
                logging.error(f"{side.name}限价单提交失败: {response}")
                return False
                
        except Exception as e:
            logging.error(f"创建{side.name}限价单失败: {e}")
            return False
    
    async def create_market_order(self, side: OrderSide, size: Decimal, client_id: str):
        """创建市价单"""
        try:
            order = Order(
                market=BTC_MARKET,
                order_type=OrderType.Market,
                order_side=side,
                size=size,
                client_id=client_id,
                signature_timestamp=int(time.time() * 1000),
            )
            
            # 签名订单
            sig = sign_order(self.config, order)
            order.signature = sig
            
            # 提交订单
            logging.info(f"提交{side.name}单: {size} BTC")
            response = await post_order_payload(PARADEX_HTTP_URL, self.jwt_token, order.dump_to_dict())
            
            if response.get("status_code") == 201:
                logging.info(f"{side.name}单提交成功: {response}")
                return True
            else:
                logging.error(f"{side.name}单提交失败: {response}")
                return False
                
        except Exception as e:
            logging.error(f"创建{side.name}单失败: {e}")
            return False
    
    async def wait_for_order_fill_with_retry(self, max_wait_time=10, max_retries=5):
        """等待订单成交，如果超时则重新获取价格并重新挂单"""
        logging.info(f"等待订单成交，最多等待{max_wait_time}秒...")
        
        # 直接调用改进后的wait_for_order_fill方法
        return await self.wait_for_order_fill(max_wait_time)
    
    async def execute_buy_order_with_retry(self, max_retries=5):
        """执行买单，如果10秒不成交则重新获取价格并重新挂单"""
        for attempt in range(max_retries):
            logging.info(f"=== 第{attempt + 1}次尝试挂买单 ===")
            
            # 1. 先取消所有现有订单
            logging.info("取消所有现有订单...")
            await self.cancel_all_orders()
            await asyncio.sleep(1)
            
            # 2. 获取当前价格（多次重试）
            current_price = None
            price_retry_count = 0
            while current_price is None and price_retry_count < 3:
                if attempt == 0 and price_retry_count == 0:
                    # 第一次尝试使用完整的价格获取（买单使用bid价格）
                    current_price = await self.get_current_price(OrderSide.Buy)
                else:
                    # 重试时使用快速价格获取（买单使用bid价格）
                    logging.info("重试时使用快速价格获取...")
                    current_price = await self.get_current_price_simple(OrderSide.Buy)
                
                if current_price is None:
                    price_retry_count += 1
                    logging.warning(f"价格获取失败，第{price_retry_count}次重试...")
                    await asyncio.sleep(1)
            
            if not current_price:
                logging.error("多次尝试后仍无法获取BTC价格")
                await asyncio.sleep(2)
                continue
            
            # 3. 计算买单价格
            buy_price = Decimal(str(current_price - 0))
            logging.info(f"当前价格: {current_price}, 买单价格: {buy_price}")
            
            # 4. 挂买单
            buy_success = await self.create_limit_order(
                OrderSide.Buy, 
                ORDER_SIZE, 
                buy_price,
                f"{CLIENT_ID_BUY}-attempt-{attempt + 1}"
            )
            
            if not buy_success:
                logging.error(f"第{attempt + 1}次挂买单失败")
                await asyncio.sleep(2)
                continue
            
            # 5. 等待成交
            fill_success = await self.wait_for_order_fill_with_retry(max_wait_time=10)
            
            if fill_success:
                logging.info(f"买单在第{attempt + 1}次尝试中成交！")
                return True
            else:
                logging.warning(f"买单第{attempt + 1}次尝试未成交，准备重试...")
                await asyncio.sleep(1)
        
        logging.error(f"买单在{max_retries}次尝试后仍未成交")
        return False
    
    async def execute_sell_order_with_retry(self, max_retries=5):
        """执行卖单，如果10秒不成交则重新获取价格并重新挂单"""
        for attempt in range(max_retries):
            logging.info(f"=== 第{attempt + 1}次尝试挂卖单 ===")
            
            # 1. 先取消所有现有订单
            logging.info("取消所有现有订单...")
            await self.cancel_all_orders()
            await asyncio.sleep(1)
            
            # 2. 获取当前价格（多次重试）
            current_price = None
            price_retry_count = 0
            while current_price is None and price_retry_count < 3:
                if attempt == 0 and price_retry_count == 0:
                    # 第一次尝试使用完整的价格获取（卖单使用ask价格）
                    current_price = await self.get_current_price(OrderSide.Sell)
                else:
                    # 重试时使用快速价格获取（卖单使用ask价格）
                    logging.info("重试时使用快速价格获取...")
                    current_price = await self.get_current_price_simple(OrderSide.Sell)
                
                if current_price is None:
                    price_retry_count += 1
                    logging.warning(f"价格获取失败，第{price_retry_count}次重试...")
                    await asyncio.sleep(1)
            
            if not current_price:
                logging.error("多次尝试后仍无法获取BTC价格")
                await asyncio.sleep(2)
                continue
            
            # 3. 计算卖单价格
            sell_price = Decimal(str(current_price + 0))
            logging.info(f"当前价格: {current_price}, 卖单价格: {sell_price}")
            
            # 4. 挂卖单
            sell_success = await self.create_limit_order(
                OrderSide.Sell, 
                ORDER_SIZE, 
                sell_price,
                f"{CLIENT_ID_SELL}-attempt-{attempt + 1}"
            )
            
            if not sell_success:
                logging.error(f"第{attempt + 1}次挂卖单失败")
                await asyncio.sleep(2)
                continue
            
            # 5. 等待成交
            fill_success = await self.wait_for_order_fill_with_retry(max_wait_time=10)
            
            if fill_success:
                logging.info(f"卖单在第{attempt + 1}次尝试中成交！")
                return True
            else:
                logging.warning(f"卖单第{attempt + 1}次尝试未成交，准备重试...")
                await asyncio.sleep(1)
        
        logging.error(f"卖单在{max_retries}次尝试后仍未成交")
        return False
    
    async def wait_for_order_fill(self, max_wait_time=30):
        """等待订单成交"""
        logging.info("等待订单成交...")
        start_time = time.time()
        
        # 记录初始持仓
        initial_positions = await self.get_current_positions()
        logging.info(f"初始持仓: {initial_positions}")
        
        while time.time() - start_time < max_wait_time:
            try:
                open_orders = await get_open_orders(PARADEX_HTTP_URL, self.jwt_token)
                logging.info(f"当前挂单数量: {len(open_orders) if open_orders else 0}")
                
                # 如果没有挂单，检查持仓是否有变化
                if not open_orders:
                    current_positions = await self.get_current_positions()
                    if current_positions != initial_positions:
                        logging.info("持仓发生变化，订单已成交！")
                        return True
                    else:
                        logging.info("没有挂单且持仓无变化，订单可能已成交")
                        return True
                
                # 检查挂单状态
                for order in open_orders:
                    order_id = order.get('id', 'unknown')
                    status = order.get('status', 'unknown')
                    logging.info(f"订单 {order_id} 状态: {status}")
                    
                    # 如果订单状态是filled或completed，认为已成交
                    if status in ['filled', 'completed', 'executed']:
                        logging.info(f"订单 {order_id} 已成交！")
                        return True
                
                # 检查持仓是否有变化（即使还有挂单）
                current_positions = await self.get_current_positions()
                if current_positions != initial_positions:
                    logging.info("持仓发生变化，订单已成交！")
                    return True
                
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"检查订单状态失败: {e}")
                await asyncio.sleep(1)
        
        logging.warning(f"订单在{max_wait_time}秒内未成交")
        return False
    
    async def execute_trading_strategy(self):
        """执行交易策略：获取价格 -> 挂限价多单(价格-10) -> 成交后挂限价空单(价格+10)"""
        try:
            # 1. 检查当前持仓
            logging.info("=== 检查当前持仓 ===")
            positions = await self.get_current_positions()
            
            # 2. 取消所有挂单
            logging.info("=== 取消所有挂单 ===")
            await self.cancel_all_orders()
            
            # 3. 执行买单（带重试机制）
            logging.info("=== 开始执行买单策略 ===")
            buy_success = await self.execute_buy_order_with_retry(max_retries=5)
            
            if not buy_success:
                logging.error("买单策略失败，终止交易")
                return False
            
            # 4. 买单成交后，执行卖单（带重试机制）
            logging.info("=== 买单成交，开始执行卖单策略 ===")
            sell_success = await self.execute_sell_order_with_retry(max_retries=5)
            
            if not sell_success:
                logging.error("卖单策略失败")
                return False
            
            # 5. 最终持仓检查
            logging.info("=== 最终持仓检查 ===")
            final_positions = await self.get_current_positions()
            
            logging.info("交易策略执行完成！")
            return True
            
        except Exception as e:
            logging.error(f"执行交易策略失败: {e}")
            traceback.print_exc()
            return False

async def main():
    """主函数"""
    trader = BTCParadexTrader()
    
    try:
        # 初始化
        await trader.initialize()
        
        # 执行交易策略
        success = await trader.execute_trading_strategy()
        
        if success:
            logging.info("✅ 交易策略执行成功！")
        else:
            logging.error("❌ 交易策略执行失败！")
            
    except Exception as e:
        logging.error(f"程序执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    print("=" * 60)
    print("BTC交易脚本：获取当前价格，挂限价多单(价格-10)，成交后挂限价空单(价格+10)")
    print("如果10秒不成交，重新获取价格并重新挂单")
    print("=" * 60)
    print("注意：此脚本使用Paradex生产网")
    print("请确保已在代码中设置 ETHEREUM_PRIVATE_KEY 变量")
    print("=" * 60)
    
    asyncio.run(main())
