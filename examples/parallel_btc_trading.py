#!/usr/bin/env python3
"""
多线程BTC交易脚本

从CSV读取配置，每组账号对应一条线程，执行以下逻辑：
1. paradex获取当前BTC价格，挂限价空单（当前ask价格），监控成交
2. 如果10秒不成交，重新获取价格并重新挂单
3. 成交后，市价开多单lighter BTC
4. 等待配置时间
5. paradex获取当前BTC价格，挂限价多单（当前bid价格），监控成交
6. 如果10秒不成交，重新获取价格并重新挂单
7. 成交后，市价开空单lighter BTC
"""

import asyncio
import csv
import logging
import os
import random
import sys
import threading
import time
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# 添加paradex目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'paradex'))

import lighter
from paradex.shared.api_config import ApiConfig
from paradex.shared.paradex_api_utils import Order, OrderSide, OrderType
from paradex.shared.api_client import (
    get_jwt_token, 
    get_paradex_config, 
    post_order_payload, 
    sign_order,
    get_open_orders,
    delete_order_payload,
    get_bbo,
    fetch_positions,
    fetch_tokens,
)
from paradex.onboarding import perform_onboarding
from paradex.utils import (
    generate_paradex_account,
    get_l1_eth_account,
)

# 配置日志 - 文件输出详细日志，控制台输出警告和错误
log_filename = f"trading_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# 创建文件处理器（INFO级别）
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)  # 文件记录所有日志
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d | [%(threadName)s] %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# 创建控制台处理器（WARNING级别，减少干扰）
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)  # 控制台只显示警告和错误
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d | [%(threadName)s] %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# 配置根日志记录器
logging.basicConfig(
    level=logging.DEBUG,  # 根级别设为DEBUG，让handlers自己过滤
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)
logger.info(f"日志文件已创建: {log_filename}")

# 配置参数
PARADEX_HTTP_URL = "https://api.prod.paradex.trade/v1"
LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
BTC_MARKET = "BTC-USD-PERP"
ORDER_SIZE = Decimal("0.001")  # 0.0005 BTC
CLIENT_ID_SHORT = "btc-short-order"
CLIENT_ID_LONG = "btc-long-order"
MAX_WAIT_TIME = 10  # 10秒超时
MAX_RETRIES = 5  # 最大重试次数
LIGHTER_BTC_MARKET_INDEX = 1  # BTC市场索引

# 全局状态字典，用于UI显示
account_status = defaultdict(dict)
account_status_lock = threading.Lock()

# 全局退出事件：Ctrl+C 时置位，让各线程优雅退出并执行清理
shutdown_event = threading.Event()


def update_account_status(account_name, key, value):
    """更新账户状态"""
    with account_status_lock:
        account_status[account_name][key] = value


def format_status_table():
    """格式化状态表格 - 使用rich库"""
    try:
        os.system('clear' if os.name != 'nt' else 'cls')
    except:
        pass
    
    with account_status_lock:
        if not account_status:
            print("暂无账户数据...")
            return
        
        # 使用rich创建表格
        try:
            from rich.console import Console
            from rich.table import Table
            from rich.live import Live
            
            console = Console()
            table = Table(title=f"BTC交易监控面板 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                         show_header=True, header_style="bold cyan")
            
            table.add_column("账号", justify="center", width=20)
            table.add_column("初始余额(paradex/lighter)", justify="center", width=35)
            table.add_column("当前余额(paradex/lighter)", justify="center", width=35)
            table.add_column("当前持仓(paradex/lighter)", justify="center", width=30)
            table.add_column("磨损", justify="center", width=20)
            table.add_column("循环次数", justify="center", width=12)
            table.add_column("等待倒计时", justify="center", width=15)
            
            for account_name, status in sorted(account_status.items()):
                init_bal_p = status.get('initial_balance_paradex', 0)
                init_bal_l = status.get('initial_balance_lighter', 0)
                curr_bal_p = status.get('current_balance_paradex', 0)
                curr_bal_l = status.get('current_balance_lighter', 0)
                paradex_pos = status.get('paradex_position', '无')
                lighter_pos = status.get('lighter_position', '无')
                loop_count = status.get('loop_count', 0)
                wait_status = status.get('wait_status', '准备中')
                countdown = status.get('countdown', '')
                
                init_total = f"{init_bal_p:.2f}+{init_bal_l:.2f}={init_bal_p+init_bal_l:.2f}"
                curr_total = f"{curr_bal_p:.2f}+{curr_bal_l:.2f}={curr_bal_p+curr_bal_l:.2f}"
                position_total = f"{paradex_pos}/{lighter_pos}"
                
                status_display = wait_status
                if countdown:
                    status_display = f"{countdown}秒"
                
                # 计算磨损
                init_sum = init_bal_p + init_bal_l
                curr_sum = curr_bal_p + curr_bal_l
                loss = init_sum - curr_sum
                loss_str = f"-{loss:.2f}" if loss > 0 else f"+{abs(loss):.2f}"
                
                table.add_row(
                    account_name,
                    init_total,
                    curr_total,
                    position_total,
                    loss_str,
                    str(loop_count),
                    status_display
                )
            
            console.print(table)
            
        except ImportError:
            # 如果rich未安装，使用简单表格
            print("=" * 100)
            print(f"BTC交易监控面板 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 100)
            
            # 表格头部
            print("┌" + "─" * 20 + "┬" + "─" * 35 + "┬" + "─" * 35 + "┬" + "─" * 30 + "┬" + "─" * 20 + "┬" + "─" * 12 + "┬" + "─" * 15 + "┐")
            print("│" + "账号".center(20) + "│" + "初始余额(paradex/lighter)".center(35) + "│" + 
                  "当前余额(paradex/lighter)".center(35) + "│" + "当前持仓(paradex/lighter)".center(30) + "│" + "磨损".center(20) + "│" + "循环次数".center(12) + "│" + "等待倒计时".center(15) + "│")
            print("├" + "─" * 20 + "┼" + "─" * 35 + "┼" + "─" * 35 + "┼" + "─" * 30 + "┼" + "─" * 20 + "┼" + "─" * 12 + "┼" + "─" * 15 + "┤")
            
            # 表格内容
            for account_name, status in sorted(account_status.items()):
                init_bal_p = status.get('initial_balance_paradex', 0)
                init_bal_l = status.get('initial_balance_lighter', 0)
                curr_bal_p = status.get('current_balance_paradex', 0)
                curr_bal_l = status.get('current_balance_lighter', 0)
                paradex_pos = status.get('paradex_position', '无')
                lighter_pos = status.get('lighter_position', '无')
                loop_count = status.get('loop_count', 0)
                wait_status = status.get('wait_status', '准备中')
                countdown = status.get('countdown', '')
                
                init_total = f"{init_bal_p:.2f}+{init_bal_l:.2f}={init_bal_p+init_bal_l:.2f}"
                curr_total = f"{curr_bal_p:.2f}+{curr_bal_l:.2f}={curr_bal_p+curr_bal_l:.2f}"
                position_total = f"{paradex_pos}/{lighter_pos}"
                
                status_display = wait_status
                if countdown:
                    status_display = f"{countdown}秒"
                
                # 计算磨损
                init_sum = init_bal_p + init_bal_l
                curr_sum = curr_bal_p + curr_bal_l
                loss = init_sum - curr_sum
                loss_str = f"-{loss:.2f}" if loss > 0 else f"+{abs(loss):.2f}"
                
                print(f"│ {account_name.ljust(19)}│ {init_total.center(34)}│ {curr_total.center(34)}│ " +
                      f"{position_total.center(29)}│ {loss_str.center(19)}│ {str(loop_count).center(11)}│ {status_display.center(14)}│")
            
            print("└" + "─" * 20 + "┴" + "─" * 35 + "┴" + "─" * 35 + "┴" + "─" * 30 + "┴" + "─" * 20 + "┴" + "─" * 12 + "┴" + "─" * 15 + "┘")


def run_status_monitor():
    """运行状态监控 - 每2秒更新一次表格"""
    while True:
        try:
            format_status_table()
            time.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"状态监控错误: {e}")
            time.sleep(5)


class AccountTrader:
    """单个账户的交易器"""
    
    def __init__(self, account_info):
        self.account_info = account_info
        self.config = None
        self.jwt_token = None
        self.lighter_client = None
        self.market_info = None
        self.logger = logging.getLogger(f"Trader-{account_info.get('description', 'Unknown')}")
        self.token_refresh_task = None  # Token刷新任务
        self.account_name = account_info.get('description', 'Unknown')
        self.shutting_down = False  # 退出清理标记
        
        # 初始化状态
        update_account_status(self.account_name, 'wait_status', '初始化中')
        update_account_status(self.account_name, 'loop_count', 0)
        
    def _seconds_remaining_in_funding_window(self) -> int:
        """判断是否处于资金费率窗口（北京时间 00:00/08:00/16:00 前后5分钟）。
        返回：处于窗口则返回距离窗口结束的秒数；否则返回0。
        """
        try:
            now_utc = datetime.now(timezone.utc)
            now_cst = now_utc.astimezone(timezone(timedelta(hours=8)))
            minutes = now_cst.hour * 60 + now_cst.minute

            funding_minutes = [0, 8 * 60, 16 * 60]  # 00:00, 08:00, 16:00
            day_minutes = 24 * 60

            for m in funding_minutes:
                start = (m - 5) % day_minutes
                end = (m + 5) % day_minutes

                if start <= end:
                    in_window = (start <= minutes <= end)
                    remaining_min = end - minutes
                else:
                    # 跨天窗口，例如 23:55~00:05
                    in_window = (minutes >= start) or (minutes <= end)
                    # 计算到end的正向距离（取模）
                    remaining_min = (end - minutes) % day_minutes

                if in_window:
                    # 留一点缓冲（+5秒）
                    return max(1, remaining_min * 60 + 5)
        except Exception:
            pass

        return 0

    async def pause_for_funding_window(self) -> bool:
        """若处于资金费率窗口：取消两边挂单并暂停到窗口结束。返回是否发生了暂停。"""
        seconds = self._seconds_remaining_in_funding_window()
        if seconds > 0:
            try:
                msg = f"资金费率窗口内，暂停交易 {seconds} 秒，并取消所有挂单"
                self.logger.warning(msg)
                update_account_status(self.account_name, 'wait_status', '资金费率窗口，取消挂单并暂停...')
                # 取消两边挂单
                await self.cancel_all_orders()
                await self.cancel_lighter_orders()
                await self.close_all_paradex_positions()
                await self.close_all_lighter_positions()
            except Exception as e:
                self.logger.error(f"资金费率窗口处理出错: {e}")

            # 暂停到窗口结束
            try:
                await asyncio.sleep(seconds)
            except Exception:
                pass
            update_account_status(self.account_name, 'wait_status', '恢复交易')
            return True
        return False

    async def initialize_paradex(self):
        """初始化Paradex账户"""
        update_account_status(self.account_name, 'wait_status', '初始化Paradex...')
        self.logger.info("开始初始化Paradex...")
        
        paradex_eth_key = self.account_info['paradex_eth_key']
        
        # 创建配置
        self.config = ApiConfig()
        self.config.paradex_http_url = PARADEX_HTTP_URL
        self.config.ethereum_private_key = paradex_eth_key
        
        # 获取Paradex配置
        self.logger.info("获取Paradex配置...")
        self.config.paradex_config = await get_paradex_config(PARADEX_HTTP_URL)
        
        # 生成Paradex账户
        self.logger.info("生成Paradex账户...")
        _, eth_account = get_l1_eth_account(paradex_eth_key)
        self.config.paradex_account, self.config.paradex_account_private_key = generate_paradex_account(
            self.config.paradex_config, eth_account.key.hex()
        )
        
        # 执行onboarding
        self.logger.info("执行账户onboarding...")
        await perform_onboarding(
            self.config.paradex_config,
            self.config.paradex_http_url,
            self.config.paradex_account,
            self.config.paradex_account_private_key,
            eth_account.address,
        )
        
        # 获取JWT token
        self.logger.info("获取JWT token...")
        self.jwt_token = await get_jwt_token(
            self.config.paradex_config,
            self.config.paradex_http_url,
            self.config.paradex_account,
            self.config.paradex_account_private_key,
        )
        
        self.logger.info("Paradex初始化完成！")
        
        # 获取初始余额
        try:
            initial_balance = await self.get_paradex_balance()
            update_account_status(self.account_name, 'initial_balance_paradex', initial_balance)
            update_account_status(self.account_name, 'current_balance_paradex', initial_balance)
        except Exception as e:
            self.logger.error(f"获取Paradex初始余额失败: {e}")
            update_account_status(self.account_name, 'initial_balance_paradex', 0)
            update_account_status(self.account_name, 'current_balance_paradex', 0)
    
    async def initialize_lighter(self):
        """初始化Lighter客户端"""
        update_account_status(self.account_name, 'wait_status', '初始化Lighter...')
        self.logger.info("开始初始化Lighter...")
        
        # 使用ApiNonceManager以每次都从API获取最新nonce
        import lighter.nonce_manager as nm
        self.lighter_client = lighter.SignerClient(
            url=LIGHTER_URL,
            private_key=self.account_info['api_key_private_key'],
            account_index=int(self.account_info['account_index']),
            api_key_index=int(self.account_info['api_key_index']),
            nonce_management_type=nm.NonceManagerType.API,  # 使用API nonce管理器
        )
        
        # 检查客户端
        err = self.lighter_client.check_client()
        if err is not None:
            raise Exception(f"Lighter客户端检查失败: {err}")
        
        # 记录初始化时的nonce值
        if hasattr(self.lighter_client, 'nonce_manager') and hasattr(self.lighter_client.nonce_manager, 'nonce'):
            self.logger.info(f"Lighter初始化完成！初始nonce值: {self.lighter_client.nonce_manager.nonce}")
            
            # **关键修复：如果nonce长时间不变，说明有pending交易**
            # 强制刷新一次nonce并等待
            api_key_idx = int(self.account_info['api_key_index'])
            self.logger.info(f"预热nonce管理器...")
            self.lighter_client.nonce_manager.hard_refresh_nonce(api_key_idx)
            await asyncio.sleep(1)  # 给一些时间让pending交易处理
        else:
            self.logger.info("Lighter初始化完成！")
        
        # 获取初始余额
        try:
            initial_balance = await self.get_lighter_balance()
            update_account_status(self.account_name, 'initial_balance_lighter', initial_balance)
            update_account_status(self.account_name, 'current_balance_lighter', initial_balance)
        except Exception as e:
            self.logger.error(f"获取Lighter初始余额失败: {e}")
            update_account_status(self.account_name, 'initial_balance_lighter', 0)
            update_account_status(self.account_name, 'current_balance_lighter', 0)
    
    async def get_current_price(self, order_side=None):
        """获取当前BTC价格"""
        try:
            self.logger.info("获取BTC价格...")
            
            # 设置超时
            try:
                bbo = await asyncio.wait_for(
                    get_bbo(PARADEX_HTTP_URL, BTC_MARKET),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                self.logger.error("BBO API请求超时")
                return None
            
            # 根据订单类型选择合适的价格
            if order_side == OrderSide.Buy:
                price = bbo.get("bid")
                price_type = "bid"
            elif order_side == OrderSide.Sell:
                price = bbo.get("ask")
                price_type = "ask"
            else:
                price = bbo.get("bid") or bbo.get("ask")
                price_type = "bid"
            
            if price is not None:
                price_float = float(price)
                self.logger.info(f"获取BTC价格 ({price_type}): {price_float}")
                return price_float
            
            self.logger.error("无法获取有效价格")
            return None
            
        except Exception as e:
            self.logger.error(f"获取BTC价格失败: {e}")
            return None
    
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
            self.logger.info(f"提交{side.name}限价单: {size} BTC @ {price}")
            response = await post_order_payload(PARADEX_HTTP_URL, self.jwt_token, order.dump_to_dict())
            
            if response.get("status_code") == 201:
                self.logger.info(f"{side.name}限价单提交成功")
                return True
            else:
                # 检查是否是token过期
                error_msg = str(response)
                if 'token is expired' in error_msg or 'invalid bearer jwt' in error_msg.lower():
                    self.logger.warning("Token已过期，正在刷新...")
                    if await self.refresh_jwt_token():
                        # 刷新后重新签名并重试
                        sig = sign_order(self.config, order)
                        order.signature = sig
                        response = await post_order_payload(PARADEX_HTTP_URL, self.jwt_token, order.dump_to_dict())
                        if response.get("status_code") == 201:
                            self.logger.info(f"{side.name}限价单提交成功（重试）")
                            return True
                self.logger.error(f"{side.name}限价单提交失败: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"创建{side.name}限价单失败: {e}")
            return False
    
    async def refresh_jwt_token(self):
        """刷新JWT token"""
        try:
            self.logger.info("刷新JWT token...")
            self.jwt_token = await get_jwt_token(
                self.config.paradex_config,
                self.config.paradex_http_url,
                self.config.paradex_account,
                self.config.paradex_account_private_key,
            )
            self.logger.info("JWT token刷新成功")
            return True
        except Exception as e:
            self.logger.error(f"刷新JWT token失败: {e}")
            return False
    
    async def wait_for_order_fill(self, max_wait_time=10):
        """等待订单成交"""
        self.logger.info(f"等待订单成交，最多等待{max_wait_time}秒...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                open_orders = await get_open_orders(PARADEX_HTTP_URL, self.jwt_token)
                
                if not open_orders:
                    self.logger.info("订单已成交！")
                    return True
                
                await asyncio.sleep(0.1)
            except Exception as e:
                error_str = str(e)
                # 检查是否是token过期错误
                if 'token is expired' in error_str or 'invalid bearer jwt' in error_str.lower():
                    self.logger.warning("Token已过期，正在刷新...")
                    if await self.refresh_jwt_token():
                        self.logger.info("Token刷新成功，继续检查订单")
                    else:
                        self.logger.error("Token刷新失败")
                        await asyncio.sleep(1)
                else:
                    self.logger.error(f"检查订单状态失败: {e}")
                    await asyncio.sleep(0.1)
        
        self.logger.warning(f"订单在{max_wait_time}秒内未成交")
        return False
    
    async def cancel_all_orders(self):
        """取消所有挂单"""
        try:
            open_orders = await get_open_orders(PARADEX_HTTP_URL, self.jwt_token)
            if open_orders:
                self.logger.info(f"发现 {len(open_orders)} 个挂单，正在取消...")
                for order in open_orders:
                    order_id = order.get("order_id") or order.get("id")
                    if order_id:
                        await delete_order_payload(PARADEX_HTTP_URL, self.jwt_token, order_id)
                        self.logger.info(f"已取消订单: {order_id}")
                        await asyncio.sleep(0.1)
            else:
                self.logger.info("没有发现挂单")
        except Exception as e:
            error_str = str(e)
            # 检查是否是token过期错误
            if 'token is expired' in error_str or 'invalid bearer jwt' in error_str.lower():
                self.logger.warning("Token已过期，正在刷新...")
                if await self.refresh_jwt_token():
                    # 刷新后重试
                    try:
                        open_orders = await get_open_orders(PARADEX_HTTP_URL, self.jwt_token)
                        if open_orders:
                            self.logger.info(f"发现 {len(open_orders)} 个挂单，正在取消...")
                            for order in open_orders:
                                order_id = order.get("order_id") or order.get("id")
                                if order_id:
                                    await delete_order_payload(PARADEX_HTTP_URL, self.jwt_token, order_id)
                                    self.logger.info(f"已取消订单: {order_id}")
                                    await asyncio.sleep(0.1)
                        else:
                            self.logger.info("没有发现挂单")
                    except:
                        pass
                else:
                    self.logger.error("Token刷新失败，无法取消订单")
            else:
                self.logger.error(f"取消挂单失败: {e}")
    
    async def execute_short_order_with_retry(self):
        """执行空单（卖单），如果10秒不成交则重新获取价格并重新挂单"""
        for attempt in range(MAX_RETRIES):
            self.logger.info(f"=== 第{attempt + 1}次尝试挂空单 ===")
            
            # 1. 取消所有现有订单
            self.logger.info("取消所有现有订单...")
            await self.cancel_all_orders()
            await asyncio.sleep(0.1)
            
            # 2. 获取当前价格
            current_price = None
            price_retry_count = 0
            while current_price is None and price_retry_count < 3:
                current_price = await self.get_current_price(OrderSide.Sell)
                if current_price is None:
                    price_retry_count += 1
                    self.logger.warning(f"价格获取失败，第{price_retry_count}次重试...")
                    await asyncio.sleep(0.1)
            
            if not current_price:
                self.logger.error("多次尝试后仍无法获取BTC价格")
                await asyncio.sleep(0.1)
                continue
            
            # 3. 创建空单（使用ask价格）
            short_price = Decimal(str(current_price))
            self.logger.info(f"当前价格: {current_price}, 空单价格: {short_price}")
            
            short_success = await self.create_limit_order(
                OrderSide.Sell, 
                ORDER_SIZE, 
                short_price,
                f"{CLIENT_ID_SHORT}-attempt-{attempt + 1}"
            )
            
            if not short_success:
                self.logger.error(f"第{attempt + 1}次挂空单失败")
                await asyncio.sleep(0.1)
                continue
            
            # 4. 等待成交
            fill_success = await self.wait_for_order_fill(MAX_WAIT_TIME)
            
            if fill_success:
                self.logger.info(f"空单在第{attempt + 1}次尝试中成交！")
                return True
            else:
                self.logger.warning(f"空单第{attempt + 1}次尝试未成交，准备重试...")
                await asyncio.sleep(1)
        
        self.logger.error(f"空单在{MAX_RETRIES}次尝试后仍未成交")
        return False
    
    async def execute_long_order_with_retry(self):
        """执行多单（买单），如果10秒不成交则重新获取价格并重新挂单"""
        for attempt in range(MAX_RETRIES):
            self.logger.info(f"=== 第{attempt + 1}次尝试挂多单 ===")
            
            # 1. 取消所有现有订单
            self.logger.info("取消所有现有订单...")
            await self.cancel_all_orders()
            await asyncio.sleep(0.1)
            
            # 2. 获取当前价格
            current_price = None
            price_retry_count = 0
            while current_price is None and price_retry_count < 3:
                current_price = await self.get_current_price(OrderSide.Buy)
                if current_price is None:
                    price_retry_count += 1
                    self.logger.warning(f"价格获取失败，第{price_retry_count}次重试...")
                    await asyncio.sleep(1)
            
            if not current_price:
                self.logger.error("多次尝试后仍无法获取BTC价格")
                await asyncio.sleep(2)
                continue
            
            # 3. 创建多单（使用bid价格）
            long_price = Decimal(str(current_price))
            self.logger.info(f"当前价格: {current_price}, 多单价格: {long_price}")
            
            long_success = await self.create_limit_order(
                OrderSide.Buy, 
                ORDER_SIZE, 
                long_price,
                f"{CLIENT_ID_LONG}-attempt-{attempt + 1}"
            )
            
            if not long_success:
                self.logger.error(f"第{attempt + 1}次挂多单失败")
                await asyncio.sleep(0.12)
                continue
            
            # 4. 等待成交
            fill_success = await self.wait_for_order_fill(MAX_WAIT_TIME)
            
            if fill_success:
                self.logger.info(f"多单在第{attempt + 1}次尝试中成交！")
                return True
            else:
                self.logger.warning(f"多单第{attempt + 1}次尝试未成交，准备重试...")
                await asyncio.sleep(0.11)
        
        self.logger.error(f"多单在{MAX_RETRIES}次尝试后仍未成交")
        return False
    
    async def create_lighter_market_order(self, is_long: bool):
        """在Lighter创建市价单，带重试机制处理nonce错误"""
        max_retries = 5  # 增加重试次数
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"在Lighter创建{'多头' if is_long else '空头'}市价单 (尝试 {attempt + 1}/{max_retries})...")
                
                # 转换订单大小 (BTC数量转为基础单位)
                # BTC的最小单位是1e8 (1 BTC = 100,000 最小单位)
                base_amount = int(ORDER_SIZE * 100000)  # 0.001 BTC = 100,000 最小单位
                
                # 使用时间戳和随机数生成唯一索引
                client_order_index = (int(time.time() * 1000) * 1000 + attempt * 100 + random.randint(1, 99)) % 1000000
                
                # ApiNonceManager会每次从API获取最新nonce，无需手动刷新
                if hasattr(self.lighter_client.nonce_manager, 'nonce'):
                    self.logger.info(f"当前nonce: {self.lighter_client.nonce_manager.nonce}")
                
                # 使用限制滑点的市价单函数
                self.logger.info(f"创建Lighter限制滑点市价单: base_amount={base_amount}, client_order_index={client_order_index}, is_long={is_long}")
                
                tx = await self.lighter_client.create_market_order_limited_slippage(
                    market_index=LIGHTER_BTC_MARKET_INDEX,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    max_slippage=0.05,  # 最大滑点5%
                    is_ask=not is_long,  # 多头时是bid (买入)，空头时是ask (卖出)
                )
                
                # 检查是否成功
                if tx and tx[1] and tx[1].code == 200:
                    self.logger.info(f"Lighter市价单创建成功: {tx}")
                    return True
                else:
                    error_msg = tx[2] if tx and len(tx) > 2 else "Unknown error"
                    self.logger.warning(f"Lighter市价单创建返回非成功状态: {error_msg}")
                    
                    if "invalid nonce" in str(error_msg):
                        self.logger.warning(f"Nonce错误！后端期望的nonce与本地不匹配")
                        self.logger.warning(f"  - API Key: {self.account_info.get('api_key_index', 0)}")
                        self.logger.warning(f"  - Account: {self.account_info.get('account_index', 0)}")
                        
                        # 等待更长的时间让pending交易完成，并强制刷新nonce
                        self.logger.warning(f"等待pending交易完成（10秒）...")
                        api_key_idx = int(self.account_info.get('api_key_index', 0))
                        self.lighter_client.nonce_manager.hard_refresh_nonce(api_key_idx)
                        await asyncio.sleep(10)  # 等待10秒
                        continue
                    else:
                        return False
                
            except Exception as e:
                error_str = str(e)
                self.logger.error(f"创建Lighter市价单失败 (尝试 {attempt + 1}): {error_str}")
                
                if "invalid nonce" in error_str or "nonce" in error_str.lower():
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Nonce错误！等待pending交易完成...")
                        
                        # 等待pending交易完成
                        await asyncio.sleep(3)
                        continue
                
                if attempt == max_retries - 1:
                    import traceback
                    traceback.print_exc()
                
                return False
        
        self.logger.error(f"Lighter市价单创建在{max_retries}次尝试后失败")
        return False
    
    async def get_lighter_positions(self):
        """获取Lighter当前持仓"""
        try:
            self.logger.info("获取Lighter当前持仓...")
            
            # 创建AccountApi实例
            from lighter.api.account_api import AccountApi
            account_api = AccountApi(self.lighter_client.api_client)
            
            # 使用 "index" 而不是 "account_index"
            account_info = await account_api.account(
                by="index",
                value=str(self.account_info['account_index'])
            )
            
            self.logger.info(f"账户信息: {account_info}")
            
            # 从日志可以看到账户信息在 accounts 数组中
            if hasattr(account_info, 'accounts') and account_info.accounts:
                account = account_info.accounts[0]  # 取第一个账户
                if hasattr(account, 'positions') and account.positions:
                    self.logger.info(f"总持仓数: {len(account.positions)}")
                    btc_positions = []
                    for i, pos in enumerate(account.positions):
                        self.logger.info(f"持仓 {i}: market_id={getattr(pos, 'market_id', 'N/A')}, position={getattr(pos, 'position', 'N/A')}, sign={getattr(pos, 'sign', 'N/A')}")
                        if hasattr(pos, 'market_id') and pos.market_id == LIGHTER_BTC_MARKET_INDEX:
                            btc_positions.append(pos)
                            self.logger.info(f"找到BTC持仓: position={pos.position}, sign={pos.sign}")
                    self.logger.info(f"找到 {len(btc_positions)} 个BTC持仓")
                    return btc_positions
                else:
                    self.logger.info("账户没有持仓数据")
                    return []
            else:
                self.logger.info("账户信息中没有accounts字段")
                return []
        except Exception as e:
            self.logger.error(f"获取持仓失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def close_lighter_position(self, position):
        """关闭单个Lighter持仓"""
        try:
            if not hasattr(position, 'position') or not hasattr(position, 'sign'):
                return False
            
            # position字段是BTC单位的字符串（如'0.00100'），需要转换为最小单位
            position_btc = abs(float(str(position.position)))  # 转换为BTC数量
            position_size = int(position_btc * 100000)  # 转换为最小单位
            # 如果有正仓位但换算后为0，兜底为1个最小单位
            if position_btc > 0 and position_size == 0:
                position_size = 1
            is_long = position.sign == 1  # sign: 1 for Long, -1 for Short
            close_is_ask = is_long  # 如果是多头，需要卖出（ask）；如果是空头，需要买入（bid）
            
            self.logger.info(f"平仓: {'多头' if is_long else '空头'}, 数量: {position_btc} BTC ({position_size} 最小单位)")
            
            # 使用市价单平仓（reduce_only=True 防止误加仓）
            base_amount = position_size
            client_order_index = (int(time.time() * 1000) * 1000 + random.randint(1, 99)) % 1000000
            
            tx = await self.lighter_client.create_market_order_limited_slippage(
                market_index=LIGHTER_BTC_MARKET_INDEX,
                client_order_index=client_order_index,
                base_amount=base_amount,
                max_slippage=0.05,
                is_ask=close_is_ask,
                reduce_only=True,
            )
            
            if tx and tx[1] and tx[1].code == 200:
                self.logger.info(f"平仓成功: {tx}")
                return True
            else:
                self.logger.error(f"平仓失败: {tx}")
                return False
                
        except Exception as e:
            self.logger.error(f"平仓失败: {e}")
            return False

    async def cancel_lighter_orders(self):
        """取消Lighter的所有挂单"""
        try:
            if not self.lighter_client:
                return
            self.logger.info("正在取消Lighter所有订单...")
            result = await self.lighter_client.cancel_all_orders(
                time_in_force=self.lighter_client.CANCEL_ALL_TIF_IMMEDIATE,
                time=0
            )
            if result and len(result) >= 2 and result[1] and result[1].code == 200:
                self.logger.info("Lighter所有订单已取消")
            else:
                self.logger.warning(f"取消Lighter订单返回: {result}")
        except Exception as e:
            self.logger.error(f"取消Lighter订单失败: {e}")
    
    async def get_paradex_positions(self):
        """获取Paradex当前持仓"""
        try:
            self.logger.info("获取Paradex当前持仓...")
            positions = await fetch_positions(PARADEX_HTTP_URL, self.jwt_token)
            
            # 过滤BTC持仓
            btc_positions = []
            for pos in positions:
                if pos.get('market') == BTC_MARKET:
                    btc_positions.append(pos)
            
            if btc_positions:
                self.logger.info(f"找到 {len(btc_positions)} 个BTC持仓")
            else:
                self.logger.info("没有BTC持仓")
            return btc_positions
            
        except Exception as e:
            self.logger.error(f"获取Paradex持仓失败: {e}")
            return []
    
    async def close_paradex_position(self, position):
        """关闭单个Paradex持仓"""
        try:
            size = abs(float(position.get('size', 0)))
            side = position.get('side', '')
            
            self.logger.info(f"持仓详情: size={position.get('size')}, side={side}, market={position.get('market')}")
            
            if size == 0:
                self.logger.info("持仓大小为0，无需关闭")
                return True
            
            # 判断是空仓还是多仓
            is_short = float(position.get('size', 0)) < 0
            is_long = float(position.get('size', 0)) > 0
            
            if is_short:
                self.logger.info(f"平Paradex空头仓位: {abs(size)} BTC")
                order_side = OrderSide.Buy
            elif is_long:
                self.logger.info(f"平Paradex多头仓位: {abs(size)} BTC")
                order_side = OrderSide.Sell
            else:
                self.logger.warning("无法确定持仓方向")
                return False
            
            # 创建市价单平仓
            order = Order(
                market=BTC_MARKET,
                order_type=OrderType.Market,
                order_side=order_side,
                size=Decimal(str(abs(size))),
                client_id=f"close-position-{int(time.time() * 1000)}",
                signature_timestamp=int(time.time() * 1000),
            )
            
            # 签名订单
            sig = sign_order(self.config, order)
            order.signature = sig
            
            # 提交订单
            self.logger.info(f"提交{order_side.name}市价平仓单: {abs(size)} BTC")
            response = await post_order_payload(PARADEX_HTTP_URL, self.jwt_token, order.dump_to_dict())
            
            self.logger.info(f"平仓响应: {response}")
            
            if response.get("status_code") == 201:
                self.logger.info(f"{order_side.name}平仓单提交成功")
                return True
            else:
                self.logger.error(f"{order_side.name}平仓单提交失败: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"关闭Paradex持仓失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def close_all_paradex_positions(self):
        """关闭所有Paradex持仓"""
        try:
            positions = await self.get_paradex_positions()
            
            if not positions:
                self.logger.info("没有Paradex持仓需要关闭")
                return True
            
            self.logger.info(f"开始关闭 {len(positions)} 个Paradex持仓...")
            
            for i, position in enumerate(positions, 1):
                self.logger.info(f"关闭第 {i}/{len(positions)} 个Paradex持仓...")
                success = await self.close_paradex_position(position)
                if success:
                    self.logger.info(f"第 {i} 个Paradex持仓关闭成功")
                else:
                    self.logger.warning(f"第 {i} 个Paradex持仓关闭失败")
                
                # 持仓之间间隔
                if i < len(positions):
                    await asyncio.sleep(1)
            
            self.logger.info("所有Paradex持仓处理完成")
            return True
            
        except Exception as e:
            self.logger.error(f"关闭Paradex持仓失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def close_all_lighter_positions(self):
        """关闭所有Lighter持仓"""
        try:
            positions = await self.get_lighter_positions()
            
            if not positions:
                self.logger.info("没有持仓需要关闭")
                return True
            
            self.logger.info(f"开始关闭 {len(positions)} 个持仓...")
            
            for i, position in enumerate(positions, 1):
                self.logger.info(f"关闭第 {i}/{len(positions)} 个持仓...")
                success = await self.close_lighter_position(position)
                if success:
                    self.logger.info(f"第 {i} 个持仓关闭成功")
                else:
                    self.logger.warning(f"第 {i} 个持仓关闭失败")
                
                # 持仓之间间隔
                if i < len(positions):
                    await asyncio.sleep(1)
            
            self.logger.info("所有持仓处理完成")
            return True
            
        except Exception as e:
            self.logger.error(f"关闭持仓失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def update_positions_status(self):
        """更新持仓信息到UI状态（不检查对等性）"""
        try:
            # 获取两边持仓
            paradex_positions = await self.get_paradex_positions()
            lighter_positions = await self.get_lighter_positions()
            
            # 提取BTC持仓
            paradex_btc_pos = None
            for pos in paradex_positions:
                if pos.get('market') == BTC_MARKET:
                    paradex_btc_pos = pos
                    break
            
            lighter_btc_pos = None
            if lighter_positions and len(lighter_positions) > 0:
                lighter_btc_pos = lighter_positions[0]
            
            # 解析持仓数量
            paradex_size = 0.0
            paradex_is_short = False
            if paradex_btc_pos:
                try:
                    raw_size = paradex_btc_pos.get('size', 0)
                    paradex_size = abs(float(raw_size))
                    paradex_is_short = float(raw_size) < 0
                except Exception as e:
                    self.logger.error(f"更新状态: 解析Paradex持仓数量失败: {e}")
                    paradex_size = 0.0
            
            lighter_size = 0.0
            lighter_is_long = False
            if lighter_btc_pos:
                try:
                    position_value = getattr(lighter_btc_pos, 'position', None)
                    sign_value = getattr(lighter_btc_pos, 'sign', None)
                    if position_value is not None:
                        # position字段已经是BTC单位的字符串，直接转换为float
                        lighter_size = abs(float(str(position_value)))
                        lighter_is_long = (sign_value == 1) if sign_value is not None else False
                except Exception as e:
                    self.logger.error(f"更新状态: 解析Lighter持仓数量失败: {e}")
                    lighter_size = 0.0
            
            # 更新持仓信息到UI状态
            paradex_position_str = ""
            if paradex_size > 0:
                paradex_position_str = f"{'空' if paradex_is_short else '多'}{paradex_size:.4f}"
            else:
                paradex_position_str = "无"
            
            lighter_position_str = ""
            if lighter_size > 0:
                lighter_position_str = f"{'多' if lighter_is_long else '空'}{lighter_size:.4f}"
            else:
                lighter_position_str = "无"
            
            update_account_status(self.account_name, 'paradex_position', paradex_position_str)
            update_account_status(self.account_name, 'lighter_position', lighter_position_str)
            
        except Exception as e:
            self.logger.error(f"更新持仓状态失败: {e}")
            update_account_status(self.account_name, 'paradex_position', '错误')
            update_account_status(self.account_name, 'lighter_position', '错误')
    
    async def check_positions_balanced(self):
        """检查两边持仓是否对等
        返回: (is_balanced: bool, paradex_position: dict, lighter_position: object)
        """
        try:
            self.logger.info("=== 检查持仓对等性 ===")
            
            # 获取两边持仓
            self.logger.info("正在获取Paradex持仓...")
            paradex_positions = await self.get_paradex_positions()
            self.logger.info(f"Paradex持仓查询结果: {len(paradex_positions)} 个BTC持仓")
            
            self.logger.info("正在获取Lighter持仓...")
            lighter_positions = await self.get_lighter_positions()
            self.logger.info(f"Lighter持仓查询结果: {len(lighter_positions)} 个BTC持仓")
            
            # 提取BTC持仓
            paradex_btc_pos = None
            for pos in paradex_positions:
                if pos.get('market') == BTC_MARKET:
                    paradex_btc_pos = pos
                    self.logger.info(f"找到Paradex BTC持仓: {pos}")
                    break
            
            lighter_btc_pos = None
            if lighter_positions and len(lighter_positions) > 0:
                lighter_btc_pos = lighter_positions[0]  # 取第一个BTC持仓
                self.logger.info(f"找到Lighter BTC持仓对象: position={getattr(lighter_btc_pos, 'position', 'N/A')}, sign={getattr(lighter_btc_pos, 'sign', 'N/A')}")
            else:
                self.logger.info("Lighter持仓列表为空或没有BTC持仓")
            
            # 解析持仓数量
            paradex_size = 0.0
            paradex_is_short = False
            if paradex_btc_pos:
                try:
                    raw_size = paradex_btc_pos.get('size', 0)
                    paradex_size = abs(float(raw_size))
                    paradex_is_short = float(raw_size) < 0
                    self.logger.info(f"Paradex持仓: {'空头' if paradex_is_short else '多头'} {paradex_size} BTC (原始值: {raw_size})")
                except Exception as e:
                    self.logger.error(f"解析Paradex持仓数量失败: {e}, 持仓数据: {paradex_btc_pos}")
                    paradex_size = 0.0
            else:
                self.logger.info("Paradex无BTC持仓")
            
            lighter_size = 0.0
            lighter_is_long = False
            if lighter_btc_pos:
                try:
                    position_value = getattr(lighter_btc_pos, 'position', None)
                    sign_value = getattr(lighter_btc_pos, 'sign', None)
                    self.logger.info(f"Lighter持仓原始值: position={position_value}, sign={sign_value}")
                    
                    if position_value is not None:
                        # position字段已经是BTC单位的字符串，直接转换为float
                        lighter_size = abs(float(str(position_value)))
                        lighter_is_long = (sign_value == 1) if sign_value is not None else False
                        self.logger.info(f"Lighter持仓: {'多头' if lighter_is_long else '空头'} {lighter_size} BTC")
                    else:
                        self.logger.warning("Lighter持仓position属性为None")
                except Exception as e:
                    self.logger.error(f"解析Lighter持仓数量失败: {e}, 持仓对象: {lighter_btc_pos}")
                    import traceback
                    traceback.print_exc()
                    lighter_size = 0.0
            else:
                self.logger.info("Lighter无BTC持仓")
            
            # 更新持仓信息到UI状态
            paradex_position_str = ""
            if paradex_size > 0:
                paradex_position_str = f"{'空' if paradex_is_short else '多'}{paradex_size:.4f}"
            else:
                paradex_position_str = "无"
            
            lighter_position_str = ""
            if lighter_size > 0:
                lighter_position_str = f"{'多' if lighter_is_long else '空'}{lighter_size:.4f}"
            else:
                lighter_position_str = "无"
            
            update_account_status(self.account_name, 'paradex_position', paradex_position_str)
            update_account_status(self.account_name, 'lighter_position', lighter_position_str)
            
            # 判断是否对等
            # 对等条件：
            # 1. 两边都没有持仓
            # 2. Paradex多头 + Lighter空头，且数量相等（允许小误差0.0001）
            # 3. Paradex空头 + Lighter多头，且数量相等（允许小误差0.0001）
            tolerance = 0.0001
            
            if paradex_size == 0 and lighter_size == 0:
                self.logger.info("✓ 两边都没有持仓，持仓对等")
                return True, paradex_btc_pos, lighter_btc_pos
            elif not paradex_is_short and not lighter_is_long and abs(paradex_size - lighter_size) < tolerance:
                # Paradex多头 + Lighter空头
                self.logger.info(f"✓ 持仓对等: Paradex多头 {paradex_size} BTC = Lighter空头 {lighter_size} BTC")
                return True, paradex_btc_pos, lighter_btc_pos
            elif paradex_is_short and lighter_is_long and abs(paradex_size - lighter_size) < tolerance:
                # Paradex空头 + Lighter多头
                self.logger.info(f"✓ 持仓对等: Paradex空头 {paradex_size} BTC = Lighter多头 {lighter_size} BTC")
                return True, paradex_btc_pos, lighter_btc_pos
            else:
                self.logger.warning(f"✗ 持仓不对等!")
                self.logger.warning(f"  Paradex: {'空头' if paradex_is_short else ('多头' if paradex_size > 0 else '无')} {paradex_size} BTC")
                self.logger.warning(f"  Lighter: {'多头' if lighter_is_long else ('空头' if lighter_size > 0 else '无')} {lighter_size} BTC")
                self.logger.warning(f"  数量差异: {abs(paradex_size - lighter_size)} BTC (允许误差: {tolerance} BTC)")
                return False, paradex_btc_pos, lighter_btc_pos
                
        except Exception as e:
            self.logger.error(f"检查持仓对等性失败: {e}")
            import traceback
            traceback.print_exc()
            # 出错时返回不对等，触发关闭持仓
            return False, None, None
    
    async def execute_trading_strategy(self):
        """执行交易策略 - 无限循环"""
        loop_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 5  # 连续失败5次后停止
        
        while True:
            if self.shutting_down or shutdown_event.is_set():
                self.logger.info("检测到退出标记，停止交易循环")
                break
            # 资金费率黑窗期处理：如在窗口内则取消挂单并暂停至窗口结束
            if await self.pause_for_funding_window():
                continue
            loop_count += 1
            update_account_status(self.account_name, 'loop_count', loop_count)
            try:
                # 每5次循环或开始新循环时刷新token，确保token不会过期
                if loop_count % 5 == 1 or loop_count == 1:
                    if self.shutting_down or shutdown_event.is_set():
                        break
                    await self.refresh_jwt_token()
                
                self.logger.info("=" * 60)
                self.logger.info(f"=== 开始执行第 {loop_count} 次交易循环 ===")
                self.logger.info("=" * 60)
                
                wait_time = int(self.account_info.get('wait_time', 20))
                
                # 更新余额和持仓
                try:
                    paradex_balance = await self.get_paradex_balance()
                    lighter_balance = await self.get_lighter_balance()
                    update_account_status(self.account_name, 'current_balance_paradex', paradex_balance)
                    update_account_status(self.account_name, 'current_balance_lighter', lighter_balance)
                    await self.update_positions_status()
                except Exception as e:
                    self.logger.error(f"更新余额失败: {e}")
                
                # 步骤2: Paradex挂多单，成交后Lighter开空单
                update_account_status(self.account_name, 'wait_status', '挂多单中...')
                self.logger.info("=== 步骤2: Paradex挂多单 ===")
                # 下单前再次检查黑窗期
                if await self.pause_for_funding_window():
                    continue
                long_success = await self.execute_long_order_with_retry()
                
                if not long_success:
                    self.logger.error("多单策略失败")
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures or self.shutting_down or shutdown_event.is_set():
                        self.logger.error(f"连续失败{max_consecutive_failures}次或退出标记，停止交易")
                        break
                    self.logger.info(f"等待30秒后重试...")
                    await asyncio.sleep(30)
                    continue
                
                # 在Lighter开空单
                update_account_status(self.account_name, 'wait_status', '在Lighter开空单')
                self.logger.info("=== Paradex多单成交，在Lighter开空单 ===")
                if await self.pause_for_funding_window():
                    continue
                await self.create_lighter_market_order(is_long=False)
                
                # 等待一小段时间让订单执行完成
                await asyncio.sleep(2)
                
                # 检查持仓是否对等
                if self.shutting_down or shutdown_event.is_set():
                    break
                update_account_status(self.account_name, 'wait_status', '检查持仓对等...')
                is_balanced, _, _ = await self.check_positions_balanced()
                
                if not is_balanced:
                    self.logger.warning("持仓不对等，关闭所有仓位并重新开始循环")
                    update_account_status(self.account_name, 'wait_status', '持仓不对等，关闭仓位...')
                    await self.close_all_lighter_positions()
                    await self.close_all_paradex_positions()
                    self.logger.info("已关闭所有仓位，重新开始循环")
                    update_account_status(self.account_name, 'wait_status', '重新开始循环')
                    await asyncio.sleep(2)
                    continue  # 重新开始循环
                
                # 等待配置时间
                self.logger.info(f"持仓对等，开始等待 {wait_time} 秒...")
                update_account_status(self.account_name, 'wait_status', '等待中...')
                for i in range(wait_time, 0, -1):
                    # 倒计时过程中如进入黑窗期，立即处理
                    if await self.pause_for_funding_window():
                        break
                    update_account_status(self.account_name, 'countdown', str(i))
                    await asyncio.sleep(1)
                update_account_status(self.account_name, 'countdown', '')
                
                                # 步骤1: Paradex挂空单，成交后Lighter开多单
                update_account_status(self.account_name, 'wait_status', '挂空单中...')
                self.logger.info("=== 步骤1: Paradex挂空单 ===")
                if await self.pause_for_funding_window():
                    continue
                short_success = await self.execute_short_order_with_retry()
                
                if not short_success:
                    self.logger.error("空单策略失败")
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures or self.shutting_down or shutdown_event.is_set():
                        self.logger.error(f"连续失败{max_consecutive_failures}次或退出标记，停止交易")
                        break
                    self.logger.info(f"等待30秒后重试...")
                    await asyncio.sleep(30)
                    continue
                
                consecutive_failures = 0  # 重置失败计数
                
                # 在Lighter开多单
                update_account_status(self.account_name, 'wait_status', '在Lighter开多单')
                self.logger.info("=== Paradex空单成交，在Lighter开多单 ===")
                if await self.pause_for_funding_window():
                    continue
                await self.create_lighter_market_order(is_long=True)

                
                await asyncio.sleep(1)
                await self.update_positions_status()
                
                # 关闭所有持仓
                update_account_status(self.account_name, 'wait_status', '关闭持仓中...')
                self.logger.info("=== 关闭所有持仓 ===")
                await self.close_all_lighter_positions()
                await self.close_all_paradex_positions()
                await asyncio.sleep(1)
                await self.update_positions_status()
                
                self.logger.info("=" * 60)
                
                # 两个交易之间的间隔
                update_account_status(self.account_name, 'wait_status', '准备下一轮')
                await asyncio.sleep(5)
                
            except Exception as e:
                consecutive_failures += 1
                self.logger.error(f"执行交易策略失败 (第{loop_count}次循环): {e}")
                import traceback
                traceback.print_exc()
                
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error(f"连续失败{max_consecutive_failures}次，停止交易")
                    break
                
                self.logger.info(f"等待30秒后重试...")
                await asyncio.sleep(30)
        
        self.logger.info(f"交易循环结束，共执行了 {loop_count} 次循环")
    
    async def get_paradex_balance(self):
        """获取Paradex余额"""
        try:
            balances = await fetch_tokens(PARADEX_HTTP_URL, self.jwt_token)
            
            # 如果没有余额，返回0
            if not balances:
                return 0.0
            
            # 查找USDC余额
            for balance in balances:
                token = balance.get("token", "")
                
                # 尝试多个字段名：size, balance, amount
                amount = None
                for field in ["size", "balance", "amount"]:
                    if field in balance:
                        amount = balance.get(field, "0")
                        break
                
                if amount is None:
                    continue
                
                # 尝试多种方式查找USDC
                if "USDC" in token or "usdc" in token.lower() or "bridged_usdc" in token.lower():
                    try:
                        amount_float = float(amount)
                        return amount_float
                    except (ValueError, TypeError):
                        return 0.0
            
            # 如果没有找到USDC，返回0
            return 0.0
        except Exception as e:
            self.logger.error(f"获取Paradex余额失败: {e}")
            return 0.0
    
    async def get_lighter_balance(self):
        """获取Lighter余额"""
        try:
            from lighter.api.account_api import AccountApi
            account_api = AccountApi(self.lighter_client.api_client)
            
            account_info = await account_api.account(
                by="index",
                value=str(self.account_info['account_index'])
            )
            
            if hasattr(account_info, 'accounts') and account_info.accounts:
                account = account_info.accounts[0]
                # 返回总资产价值
                return float(account.total_asset_value)
            
            return 0.0
        except Exception as e:
            self.logger.error(f"获取Lighter余额失败: {e}")
            return 0.0
    
    async def token_refresh_loop(self):
        """Token自动刷新循环"""
        while True:
            try:
                # 每90秒刷新一次token（JWT token通常有效期是10分钟，提前刷新）
                await asyncio.sleep(90)
                if getattr(self, 'shutting_down', False) or shutdown_event.is_set():
                    break
                self.logger.info("定时刷新JWT token...")
                await self.refresh_jwt_token()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Token刷新失败: {e}")
                await asyncio.sleep(10)  # 失败后等待10秒再重试
    
    async def run(self):
        """运行交易器"""
        try:
            # 初始化
            await self.initialize_paradex()
            await self.initialize_lighter()
            
            # 启动Token自动刷新任务
            self.token_refresh_task = asyncio.create_task(self.token_refresh_loop())
            self.logger.info("已启动Token自动刷新线程")
            
            # 执行交易策略
            await self.execute_trading_strategy()
            
        except Exception as e:
            self.logger.error(f"运行失败: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 标记退出，阻止后续下单/请求
            self.shutting_down = True
            
            # 退出前：取消挂单并关闭持仓
            try:
                await self.cancel_all_orders()
            except Exception as e:
                self.logger.warning(f"取消Paradex挂单失败: {e}")
            try:
                await self.cancel_lighter_orders()
            except Exception as e:
                self.logger.warning(f"取消Lighter挂单失败: {e}")
            try:
                await self.close_all_paradex_positions()
            except Exception as e:
                self.logger.warning(f"关闭Paradex持仓失败: {e}")
            try:
                await self.close_all_lighter_positions()
            except Exception as e:
                self.logger.warning(f"关闭Lighter持仓失败: {e}")
            
            # 停止Token刷新任务
            if self.token_refresh_task:
                self.token_refresh_task.cancel()
                try:
                    await self.token_refresh_task
                except asyncio.CancelledError:
                    pass
            
            # 清理资源
            if self.lighter_client:
                try:
                    await self.lighter_client.close()
                except:
                    pass


def read_accounts_from_csv(csv_file="examples/accounts.csv"):
    """从CSV文件读取账户配置"""
    accounts = []
    
    if not os.path.exists(csv_file):
        logging.error(f"找不到CSV文件: {csv_file}")
        return accounts
    
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            accounts.append(row)
    
    return accounts


async def run_trader_in_async(account_info):
    """在异步环境中运行交易器"""
    trader = AccountTrader(account_info)
    await trader.run()


def run_trader_in_thread(account_info):
    """在新线程中运行交易器"""
    logging.info(f"启动交易器线程: {account_info.get('description', 'Unknown')}")
    
    # 创建新的事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(run_trader_in_async(account_info))
    except Exception as e:
        logging.error(f"线程运行失败: {e}")
    finally:
        loop.close()
    
    logging.info(f"交易器线程完成: {account_info.get('description', 'Unknown')}")


def main():
    """主函数"""
    # 读取账户配置
    accounts = read_accounts_from_csv()
    
    if not accounts:
        logging.error("没有找到账户配置")
        return
    
    logging.info(f"找到 {len(accounts)} 个账户配置")
    
    # 启动状态监控线程
    monitor_thread = threading.Thread(target=run_status_monitor, daemon=True)
    monitor_thread.start()
    
    # 为每个账户创建线程
    threads = []
    for account_info in accounts:
        thread = threading.Thread(
            target=run_trader_in_thread,
            args=(account_info,),
            name=f"Trader-{account_info.get('description', 'Unknown')}"
        )
        thread.start()
        threads.append(thread)
        logging.info(f"启动交易器线程: {account_info.get('description', 'Unknown')}")
        
        # 添加短暂延迟，避免同时启动
        time.sleep(10)
    
    # 等待所有线程完成
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        logging.info("收到中断信号，通知各线程退出并等待清理...")
        shutdown_event.set()
        # 再次等待线程自然结束（执行各自的finally清理）
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    print("=" * 60)
    print("多线程BTC交易脚本")
    print("=" * 60)
    
    main()

