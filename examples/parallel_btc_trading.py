#!/usr/bin/env python3
"""
多线程交易脚本（支持多个交易对）

从CSV读取配置，每组账号对应一条线程，执行以下逻辑：
1. paradex获取当前价格，挂限价空单（当前ask价格），监控成交
2. 如果10秒不成交，重新获取价格并重新挂单
3. 成交后，市价开多单lighter
4. 等待配置时间
5. paradex获取当前价格，挂限价多单（当前bid价格），监控成交
6. 如果10秒不成交，重新获取价格并重新挂单
7. 成交后，市价开空单lighter

CSV文件需要包含以下列：
- account_index: Lighter账户索引
- api_key_index: API密钥索引
- api_key_private_key: API密钥私钥
- paradex_eth_key: Paradex以太坊私钥
- description: 账户描述
- wait_time: 等待时间（秒）
- market: 交易对名称（如 BTC-USD-PERP）
- order_size: 订单大小（如 0.001）
- market_index: Lighter市场索引（如 1）
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

# 配置日志 - 文件输出详细日志，控制台完全不输出日志（只显示表格）
log_filename = f"trading_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# 创建文件处理器（DEBUG级别）
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)  # 文件记录所有日志
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d | [%(threadName)s] %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# 不创建控制台处理器，完全隐藏所有日志（只显示表格）
# 所有日志只写入文件

# 配置根日志记录器（只使用文件处理器）
logging.basicConfig(
    level=logging.DEBUG,  # 根级别设为DEBUG
    handlers=[file_handler]  # 只使用文件处理器，不输出到控制台
)

logger = logging.getLogger(__name__)
logger.info(f"日志文件已创建: {log_filename}")

# 配置参数
PARADEX_HTTP_URL = "https://api.prod.paradex.trade/v1"
LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
# 默认值（如果CSV中没有指定，使用这些默认值）
DEFAULT_MARKET = "BTC-USD-PERP"
DEFAULT_ORDER_SIZE = Decimal("0.001")
DEFAULT_MARKET_INDEX = 1
MAX_WAIT_TIME = 10  # 10秒超时
MAX_RETRIES = 5  # 最大重试次数

# 全局状态字典，用于UI显示
account_status = defaultdict(dict)
account_status_lock = threading.Lock()

# 全局退出事件：Ctrl+C 时置位，让各线程优雅退出并执行清理
shutdown_event = threading.Event()

# 全局代理配置字典（按账户名存储）
account_proxy_config = {}
account_proxy_lock = threading.Lock()

# 创建带代理的aiohttp ClientSession的辅助函数
async def create_proxy_session(account_name=None):
    """创建带代理的aiohttp ClientSession"""
    import aiohttp
    proxy_url = None
    if account_name:
        with account_proxy_lock:
            proxy_url = account_proxy_config.get(account_name)
    
    if proxy_url:
        return aiohttp.ClientSession(connector=aiohttp.TCPConnector())
    else:
        return aiohttp.ClientSession()


def update_account_status(account_name, key, value):
    """更新账户状态"""
    with account_status_lock:
        account_status[account_name][key] = value


def format_status_table():
    """格式化状态表格 - 使用rich库，返回表格对象或None"""
    import os
    from datetime import datetime
    
    with account_status_lock:
        if not account_status:
            return None
        
        # 使用rich创建表格
        try:
            from rich.console import Console
            from rich.table import Table
            from rich.text import Text
            
            console = Console()
            table = Table(title=f"交易监控面板 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                         show_header=True, header_style="bold cyan", 
                         title_style="bold yellow", box=None)
            
            # 使用固定列宽，不自适应屏幕大小
            table.add_column("账号", justify="left", width=18, no_wrap=True)
            table.add_column("交易对", justify="center", width=12, no_wrap=True)
            table.add_column("初始余额(P/L)", justify="right", width=28, no_wrap=True)
            table.add_column("当前余额(P/L)", justify="right", width=28, no_wrap=True)
            table.add_column("持仓(P/L)", justify="center", width=24, no_wrap=True)
            table.add_column("磨损", justify="right", width=12, no_wrap=True)
            table.add_column("循环", justify="center", width=8, no_wrap=True)
            table.add_column("状态", justify="center", width=20, no_wrap=True)
            
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
                market = status.get('market', 'N/A')
                
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
                if loss > 0:
                    loss_str = f"-{loss:.4f}"
                    loss_style = "red"
                elif loss < 0:
                    loss_str = f"+{abs(loss):.4f}"
                    loss_style = "green"
                else:
                    loss_str = "0.0000"
                    loss_style = None
                
                # 格式化余额显示，保留4位小数
                init_total = f"{init_bal_p:.4f}/{init_bal_l:.4f}"
                curr_total = f"{curr_bal_p:.4f}/{curr_bal_l:.4f}"
                
                # 添加行，带样式
                from rich.text import Text
                row_items = [
                    account_name,
                    market,
                    init_total,
                    curr_total,
                    position_total,
                    Text(loss_str, style=loss_style) if loss_style else loss_str,
                    str(loop_count),
                    status_display
                ]
                table.add_row(*row_items)
            
            return table
            
        except ImportError:
            # 如果rich未安装，使用简单表格
            print("=" * 120)
            print(f"交易监控面板 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 120)
            
            # 表格头部 - 使用固定列宽
            col_widths = [18, 12, 28, 28, 24, 12, 8, 20]
            col_names = ["账号", "交易对", "初始余额(P/L)", "当前余额(P/L)", "持仓(P/L)", "磨损", "循环", "状态"]
            header_line = "┌" + "┐".join("─" * w for w in col_widths) + "┐"
            sep_line = "├" + "┤".join("─" * w for w in col_widths) + "┤"
            footer_line = "└" + "┘".join("─" * w for w in col_widths) + "┘"
            
            print(header_line)
            header_row = "│".join(name.center(w) for name, w in zip(col_names, col_widths))
            print(f"│{header_row}│")
            print(sep_line)
            
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
                market = status.get('market', 'N/A')
                
                # 格式化余额显示，保留4位小数
                init_total = f"{init_bal_p:.4f}/{init_bal_l:.4f}"
                curr_total = f"{curr_bal_p:.4f}/{curr_bal_l:.4f}"
                position_total = f"{paradex_pos}/{lighter_pos}"
                
                status_display = wait_status
                if countdown:
                    status_display = f"{countdown}秒"
                
                # 计算磨损
                init_sum = init_bal_p + init_bal_l
                curr_sum = curr_bal_p + curr_bal_l
                loss = init_sum - curr_sum
                loss_str = f"-{loss:.4f}" if loss > 0 else f"+{abs(loss):.4f}" if loss < 0 else "0.0000"
                
                # 格式化每列数据
                col_widths = [18, 12, 28, 28, 24, 12, 8, 20]
                row_data = [
                    account_name[:17].ljust(17),
                    market.center(11),
                    init_total.rjust(27),
                    curr_total.rjust(27),
                    position_total.center(23),
                    loss_str.rjust(11),
                    str(loop_count).center(7),
                    status_display[:19].center(19)
                ]
                
                row_line = "│".join(data.center(w) if i in [1, 4, 6, 7] else data.ljust(w) if i == 0 else data.rjust(w) 
                                   for i, (data, w) in enumerate(zip(row_data, col_widths)))
                print(f"│{row_line}│")
            
            # 打印表格底部
            col_widths = [18, 12, 28, 28, 24, 12, 8, 20]
            footer_line = "└" + "┘".join("─" * w for w in col_widths) + "┘"
            print(footer_line)


def run_status_monitor():
    """运行状态监控 - 每2秒更新一次表格"""
    from rich.live import Live
    from rich.console import Console
    
    console = Console()
    
    # 使用Live实现实时更新，避免屏幕滚动
    try:
        # 清屏一次，然后使用Live更新（screen=True会保留屏幕，只更新Live区域）
        try:
            os.system('clear' if os.name != 'nt' else 'cls')
        except:
            pass
        
        with Live(console=console, refresh_per_second=0.5, screen=True, vertical_overflow="visible", redirect_stderr=False) as live:
            while True:
                try:
                    # 获取表格并更新（不清屏，让Live自动处理）
                    table = format_status_table()
                    if table:
                        live.update(table)
                    else:
                        live.update("暂无账户数据...")
                    
                    time.sleep(2)
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    # 使用print而不是logging，避免显示在控制台
                    # logging.error会输出到控制台，而我们已经设置了WARNING级别
                    pass
                    time.sleep(2)
    except Exception:
        # 如果Live失败，回退到简单模式（清屏后打印）
        import os
        while True:
            try:
                try:
                    os.system('clear' if os.name != 'nt' else 'cls')
                except:
                    pass
                format_status_table()
                time.sleep(2)
            except KeyboardInterrupt:
                break
            except Exception as e:
                # 静默处理错误，不输出日志
                time.sleep(2)
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
        
        # 从account_info读取市场参数，如果没有则使用默认值
        self.market = account_info.get('market', DEFAULT_MARKET).strip()
        try:
            self.order_size = Decimal(str(account_info.get('order_size', DEFAULT_ORDER_SIZE)))
        except (ValueError, TypeError):
            self.order_size = DEFAULT_ORDER_SIZE
            self.logger.warning(f"无效的order_size值，使用默认值: {DEFAULT_ORDER_SIZE}")
        
        # 从account_info读取代理配置
        proxy_url = account_info.get('proxy_url', '').strip()
        self.proxy_url = proxy_url if proxy_url else None
        if self.proxy_url:
            self.logger.info(f"代理配置: {self.proxy_url}")
        else:
            self.logger.info("未配置代理，使用直连")
        
        # 初始化market_index相关属性（将在initialize_lighter中设置）
        self.market_index = None
        self.size_decimals = None
        self._csv_market_index = None  # 保留此属性以兼容错误处理逻辑
        
        # 生成客户端ID（基于市场名称）
        market_symbol = self.market.split('-')[0].lower()  # 从BTC-USD-PERP提取BTC
        self.client_id_short = f"{market_symbol}-short-order"
        self.client_id_long = f"{market_symbol}-long-order"
        
        self.logger.info(f"交易对配置: market={self.market}, order_size={self.order_size}")
        
        # 初始化状态
        update_account_status(self.account_name, 'wait_status', '初始化中')
        update_account_status(self.account_name, 'loop_count', 0)
        update_account_status(self.account_name, 'market', self.market)
        
        # 为每个资金费率时间点随机生成前后偏移量（5-50分钟），在程序运行期间固定
        # funding_windows 格式: {时间点(分钟): (前偏移(分钟), 后偏移(分钟))}
        self.funding_windows = {}
        funding_times = [0, 8 * 60, 16 * 60]  # 00:00, 08:00, 16:00
        # funding_times = [0, 8 * 60]  # 00:00, 08:00, 16:00
        for ft in funding_times:
            before_offset = random.randint(5, 50)  # 前偏移 5-50 分钟
            after_offset = random.randint(5, 50)   # 后偏移 5-50 分钟
            self.funding_windows[ft] = (before_offset, after_offset)
            self.logger.info(f"资金费率时间点 {ft//60:02d}:{ft%60:02d} 窗口设置为: 前{before_offset}分钟, 后{after_offset}分钟")
        
    def _seconds_remaining_in_funding_window(self) -> int:
        """判断是否处于资金费率窗口（北京时间 00:00/08:00/16:00 前后随机5-50分钟）。
        返回：处于窗口则返回距离窗口结束的秒数；否则返回0。
        """
        try:
            now_utc = datetime.now(timezone.utc)
            now_cst = now_utc.astimezone(timezone(timedelta(hours=8)))
            minutes = now_cst.hour * 60 + now_cst.minute
            day_minutes = 24 * 60

            for funding_time, (before_offset, after_offset) in self.funding_windows.items():
                start = (funding_time - before_offset) % day_minutes
                end = (funding_time + after_offset) % day_minutes

                if start <= end:
                    in_window = (start <= minutes <= end)
                    remaining_min = end - minutes
                else:
                    # 跨天窗口
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
        
        # 如果配置了代理，设置全局代理配置（用于Paradex API调用）
        if self.proxy_url:
            self.logger.info(f"为Paradex配置代理: {self.proxy_url}")
            with account_proxy_lock:
                account_proxy_config[self.account_name] = self.proxy_url
        
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
    
    async def get_market_index_from_api(self):
        """通过API查询market_index - 根据Paradex market名称自动查找对应的Lighter market_id"""
        try:
            self.logger.info(f"正在查询 {self.market} 对应的Lighter market_index...")
            
            # 从Paradex market名称提取基础资产（如BTC-USD-PERP -> BTC）
            base_asset = self.market.split('-')[0].upper()
            
            # 创建临时API客户端用于查询
            from lighter.configuration import Configuration
            temp_config = Configuration(host=LIGHTER_URL)
            if self.proxy_url:
                temp_config.proxy = self.proxy_url
            
            temp_api_client = lighter.ApiClient(configuration=temp_config)
            account_api = lighter.AccountApi(temp_api_client)
            order_api = lighter.OrderApi(temp_api_client)
            
            try:
                # 方法1: 通过账户持仓信息获取所有市场，精确匹配symbol（最准确）
                try:
                    account_info = await account_api.account(
                        by="index",
                        value=str(self.account_info['account_index'])
                    )
                    
                    if hasattr(account_info, 'accounts') and account_info.accounts:
                        account = account_info.accounts[0]
                        if hasattr(account, 'positions') and account.positions:
                            self.logger.info(f"从账户持仓信息中查找 {base_asset} 对应的 market_id...")
                            
                            # 遍历所有持仓，查找精确匹配的symbol（避免ETH匹配到ETHFI）
                            for pos in account.positions:
                                if hasattr(pos, 'symbol') and hasattr(pos, 'market_id'):
                                    pos_symbol = pos.symbol.upper()
                                    market_id = pos.market_id
                                    
                                    # 精确匹配：symbol必须完全相等（不能使用in，避免ETH匹配到ETHFI）
                                    if pos_symbol == base_asset:
                                        self.logger.info(f"✓ 找到精确匹配：symbol={pos.symbol}, market_id={market_id}")
                                        # 验证market_id是否有效
                                        try:
                                            order_book = await order_api.order_book_orders(market_id=market_id, limit=1)
                                            if order_book and (not hasattr(order_book, 'code') or order_book.code == 200):
                                                self.logger.info(f"✓ 验证成功！{self.market} 对应 market_index={market_id}")
                                                return market_id
                                        except Exception as e:
                                            self.logger.warning(f"market_id={market_id} 验证失败: {e}")
                except Exception as e:
                    self.logger.warning(f"通过账户持仓信息查询失败: {e}")
                
                # 方法2: 通过order_books API查询所有市场，精确匹配symbol
                try:
                    order_books_response = await order_api.order_books()
                    # order_books返回的是OrderBooks对象，包含order_books列表
                    if order_books_response and hasattr(order_books_response, 'order_books'):
                        order_books_list = order_books_response.order_books
                        if order_books_list:
                            self.logger.info(f"从订单簿详情中查找 {base_asset} 对应的 market_id...")
                            self.logger.info(f"Lighter上共有 {len(order_books_list)} 个市场")
                            
                            # 记录所有可用市场（用于调试）
                            available_symbols = []
                            for detail in order_books_list:
                                if hasattr(detail, 'symbol') and hasattr(detail, 'market_id'):
                                    detail_symbol = detail.symbol.upper()
                                    market_id = detail.market_id
                                    available_symbols.append(f"{detail.symbol}({market_id})")
                                    
                                    # 精确匹配：symbol必须完全相等
                                    if detail_symbol == base_asset:
                                        self.logger.info(f"✓ 从订单簿详情找到精确匹配：symbol={detail.symbol}, market_id={market_id}")
                                        return market_id
                            
                            # 如果没有找到匹配，记录所有可用市场
                            self.logger.warning(f"⚠️ 未找到 {base_asset} 匹配的市场")
                            self.logger.info(f"Lighter上可用的市场symbol列表（前30个）: {', '.join(available_symbols[:30])}")
                            if len(available_symbols) > 30:
                                self.logger.info(f"... 还有 {len(available_symbols) - 30} 个市场")
                                # 尝试查找类似的symbol（包含base_asset的）
                                similar_symbols = [s for s in available_symbols if base_asset in s.upper()]
                                if similar_symbols:
                                    self.logger.info(f"⚠️ 找到包含 '{base_asset}' 的类似市场: {', '.join(similar_symbols)}")
                        else:
                            self.logger.warning(f"order_books返回的列表为空")
                    else:
                        self.logger.warning(f"order_books返回结果格式不正确")
                except Exception as e:
                    self.logger.warning(f"通过订单簿详情查询失败: {e}")
                    import traceback
                    self.logger.debug(traceback.format_exc())
                    
            finally:
                await temp_api_client.close()
            
            # 如果所有API查询方法都失败
            base_asset = self.market.split('-')[0].upper()
            self.logger.error(f"❌ 无法通过API查询到 {self.market} 对应的 market_index")
            self.logger.error(f"可能的原因：")
            self.logger.error(f"  1. {base_asset} 交易对在Lighter上不存在")
            self.logger.error(f"  2. Lighter上的symbol名称与Paradex不同（例如：Lighter可能是 {base_asset}USDT 而不是 {base_asset}）")
            self.logger.error(f"  3. 该交易对尚未在Lighter上架")
            self.logger.error(f"请检查：")
            self.logger.error(f"  - Lighter是否支持 {self.market} 交易对")
            self.logger.error(f"  - 访问 Lighter API 查看所有可用市场")
            
            if self._csv_market_index is not None:
                self.logger.warning(f"⚠️ 使用CSV中指定的 market_index: {self._csv_market_index}")
                return self._csv_market_index
            else:
                raise Exception(f"无法确定 {self.market} 的 market_index，该交易对可能在Lighter上不存在")
                
        except Exception as e:
            self.logger.error(f"查询 market_index 失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            base_asset = self.market.split('-')[0].upper()
            # 如果查询失败且有CSV备用值，使用它
            if self._csv_market_index is not None:
                self.logger.warning(f"⚠️ API查询异常，使用CSV中指定的 market_index: {self._csv_market_index}")
                return self._csv_market_index
            else:
                self.logger.error(f"❌ 无法确定 {self.market} 的 market_index")
                self.logger.error(f"可能的原因：{base_asset} 交易对在Lighter上不存在或symbol名称不同")
                raise Exception(f"无法确定 {self.market} 的 market_index: {e}")
    
    async def initialize_lighter(self):
        """初始化Lighter客户端"""
        update_account_status(self.account_name, 'wait_status', '初始化Lighter...')
        self.logger.info("开始初始化Lighter...")
        
        # 使用ApiNonceManager以每次都从API获取最新nonce
        import lighter.nonce_manager as nm
        from lighter.configuration import Configuration
        
        # 创建配置对象
        lighter_config = Configuration(host=LIGHTER_URL)
        
        # 如果配置了代理，设置代理（用于查询）
        if self.proxy_url:
            self.logger.info(f"为Lighter配置代理: {self.proxy_url}")
            lighter_config.proxy = self.proxy_url
        
        # 创建临时API客户端用于查询
        temp_api_client = lighter.ApiClient(configuration=lighter_config)
        
        # 先通过API查询market_index
        self.market_index = await self.get_market_index_from_api()
        self.logger.info(f"✓ 确定 market_index: {self.market_index} (对应 {self.market})")
        
        # 获取市场的size_decimals用于订单大小转换
        try:
            order_api = lighter.OrderApi(temp_api_client)
            order_book_details_response = await order_api.order_book_details(market_id=self.market_index)
            
            # order_book_details返回的是OrderBookDetails对象，包含order_book_details列表
            if order_book_details_response and hasattr(order_book_details_response, 'order_book_details'):
                order_book_details_list = order_book_details_response.order_book_details
                
                # 从列表中查找匹配market_id的项
                matched_detail = None
                for detail in order_book_details_list:
                    if hasattr(detail, 'market_id') and detail.market_id == self.market_index:
                        matched_detail = detail
                        break
                
                # 如果没有找到匹配的，使用第一个（如果提供了market_id，通常只返回一个）
                if matched_detail is None and order_book_details_list:
                    matched_detail = order_book_details_list[0]
                    self.logger.warning(f"⚠️ 未找到完全匹配的market_id={self.market_index}，使用第一个详情项")
                
                if matched_detail:
                    # 优先使用 size_decimals，如果没有则使用 supported_size_decimals
                    size_decimals_value = None
                    if hasattr(matched_detail, 'size_decimals'):
                        try:
                            size_decimals_value = matched_detail.size_decimals
                            if size_decimals_value is not None:
                                self.size_decimals = size_decimals_value
                                self.logger.info(f"✓ 从 size_decimals 获取: {self.size_decimals}")
                        except Exception as e:
                            self.logger.warning(f"读取 size_decimals 失败: {e}")
                    
                    if size_decimals_value is None and hasattr(matched_detail, 'supported_size_decimals'):
                        try:
                            size_decimals_value = matched_detail.supported_size_decimals
                            if size_decimals_value is not None:
                                self.size_decimals = size_decimals_value
                                self.logger.info(f"✓ 从 supported_size_decimals 获取: {self.size_decimals}")
                        except Exception as e:
                            self.logger.warning(f"读取 supported_size_decimals 失败: {e}")
                    
                    # 记录找到的详情信息
                    self.logger.info(f"订单簿详情: symbol={getattr(matched_detail, 'symbol', 'N/A')}, "
                                   f"market_id={getattr(matched_detail, 'market_id', 'N/A')}, "
                                   f"size_decimals={getattr(matched_detail, 'size_decimals', 'N/A')}, "
                                   f"supported_size_decimals={getattr(matched_detail, 'supported_size_decimals', 'N/A')}")
                else:
                    size_decimals_value = None
                
                # 如果还是获取不到，根据市场类型使用默认值
                if size_decimals_value is None:
                    base_asset = self.market.split('-')[0].upper()
                    if base_asset == 'BTC':
                        self.size_decimals = 5  # BTC使用5位小数（100000）
                    else:
                        self.size_decimals = 4  # 其他市场默认4位小数（10000）
                    self.logger.warning(f"⚠️ 无法从API获取size_decimals，根据市场类型使用默认值: {self.size_decimals}")
                else:
                    self.logger.info(f"✓ 获取市场 {self.market} (market_index={self.market_index}) 的 size_decimals: {self.size_decimals}")
            else:
                # 根据市场类型使用不同的默认值
                base_asset = self.market.split('-')[0].upper()
                if base_asset == 'BTC':
                    self.size_decimals = 5  # BTC使用5位小数（100000）
                else:
                    self.size_decimals = 4  # 其他市场默认4位小数（10000）
                self.logger.warning(f"⚠️ order_book_details返回空结果，根据市场类型使用默认值: {self.size_decimals}")
        except Exception as e:
            # 根据市场类型使用不同的默认值
            base_asset = self.market.split('-')[0].upper()
            if base_asset == 'BTC':
                self.size_decimals = 5  # BTC使用5位小数（100000）
            else:
                self.size_decimals = 4  # 其他市场默认4位小数（10000）
            self.logger.error(f"❌ 获取size_decimals失败: {e}，根据市场类型使用默认值: {self.size_decimals}")
            import traceback
            self.logger.error(traceback.format_exc())
        
        # 关闭临时API客户端
        await temp_api_client.close()
        
        # 如果配置了代理，设置代理
        if self.proxy_url:
            self.logger.info(f"为Lighter配置代理: {self.proxy_url}")
            lighter_config.proxy = self.proxy_url
            # 如果需要代理认证，可以设置proxy_headers
            # lighter_config.proxy_headers = {"Proxy-Authorization": "Basic ..."}
        
        # 创建SignerClient
        self.lighter_client = lighter.SignerClient(
            url=LIGHTER_URL,
            private_key=self.account_info['api_key_private_key'],
            account_index=int(self.account_info['account_index']),
            api_key_index=int(self.account_info['api_key_index']),
            nonce_management_type=nm.NonceManagerType.API,  # 使用API nonce管理器
        )
        
        # 如果配置了代理，替换api_client为带代理配置的客户端
        if self.proxy_url:
            api_client = lighter.ApiClient(configuration=lighter_config)
            self.lighter_client.api_client = api_client
            self.lighter_client.tx_api = lighter.TransactionApi(api_client)
            self.lighter_client.order_api = lighter.OrderApi(api_client)
            
            # 重新初始化nonce_manager（使用新的api_client）
            self.lighter_client.nonce_manager = nm.nonce_manager_factory(
                nonce_manager_type=nm.NonceManagerType.API,
                account_index=int(self.account_info['account_index']),
                api_client=api_client,
                start_api_key=int(self.account_info['api_key_index']),
                end_api_key=int(self.account_info['api_key_index']),
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
    
    async def _get_bbo_with_proxy(self, paradex_http_url: str, market: str):
        """带代理的get_bbo包装函数"""
        import aiohttp
        path = f"/bbo/{market}"
        url = paradex_http_url + path
        headers = {"Accept": "application/json"}
        
        # 获取代理配置
        proxy_url = None
        with account_proxy_lock:
            proxy_url = account_proxy_config.get(self.account_name)
        
        async with aiohttp.ClientSession() as session:
            kwargs = {"headers": headers}
            if proxy_url:
                kwargs["proxy"] = proxy_url
            async with session.get(url, **kwargs) as response:
                status_code = response.status
                data = await response.json()
                if status_code != 200:
                    self.logger.error(f"Unable to [GET] {path}")
                    self.logger.error(f"Status Code: {status_code}")
                    self.logger.error(f"Response Text: {data}")
                return data
    
    async def get_current_price(self, order_side=None):
        """获取当前价格"""
        try:
            self.logger.info(f"获取{self.market}价格...")
            
            # 设置超时
            try:
                # 使用带代理的get_bbo
                bbo = await asyncio.wait_for(
                    self._get_bbo_with_proxy(PARADEX_HTTP_URL, self.market),
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
                self.logger.info(f"获取{self.market}价格 ({price_type}): {price_float}")
                return price_float
            
            self.logger.error("无法获取有效价格")
            return None
            
        except Exception as e:
            self.logger.error(f"获取{self.market}价格失败: {e}")
            return None
    
    async def create_limit_order(self, side: OrderSide, size: Decimal, price: Decimal, client_id: str):
        """创建限价单"""
        try:
            order = Order(
                market=self.market,
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
                self.logger.error(f"多次尝试后仍无法获取{self.market}价格")
                await asyncio.sleep(0.1)
                continue
            
            # 3. 创建空单（使用ask价格）
            short_price = Decimal(str(current_price))
            self.logger.info(f"当前价格: {current_price}, 空单价格: {short_price}")
            
            short_success = await self.create_limit_order(
                OrderSide.Sell, 
                self.order_size, 
                short_price,
                f"{self.client_id_short}-attempt-{attempt + 1}"
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
                self.logger.error(f"多次尝试后仍无法获取{self.market}价格")
                await asyncio.sleep(2)
                continue
            
            # 3. 创建多单（使用bid价格）
            long_price = Decimal(str(current_price))
            self.logger.info(f"当前价格: {current_price}, 多单价格: {long_price}")
            
            long_success = await self.create_limit_order(
                OrderSide.Buy, 
                self.order_size, 
                long_price,
                f"{self.client_id_long}-attempt-{attempt + 1}"
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
                self.logger.info(f"========== 在Lighter创建{'多头' if is_long else '空头'}市价单 (尝试 {attempt + 1}/{max_retries}) ==========")
                self.logger.info(f"交易对: {self.market}, 市场索引: {self.market_index}, 订单大小: {self.order_size}")
                
                # 转换订单大小 (数量转为基础单位)
                # 根据市场的size_decimals动态计算base_amount
                # base_amount = order_size * (10 ** size_decimals)
                # 例如：如果size_decimals=4，则 0.01 ETH = 0.01 * 10000 = 100 base_amount
                size_scale = 10 ** self.size_decimals
                base_amount = int(self.order_size * size_scale)
                self.logger.info(f"订单大小转换: {self.order_size} {self.market.split('-')[0]} (size_decimals={self.size_decimals}, scale={size_scale}) -> {base_amount} 最小单位")
                
                # 验证base_amount是否合理（至少为1）
                if base_amount < 1:
                    self.logger.warning(f"订单大小转换后为0或负数，使用最小值1")
                    base_amount = 1
                
                # 使用时间戳和随机数生成唯一索引
                client_order_index = (int(time.time() * 1000) * 1000 + attempt * 100 + random.randint(1, 99)) % 1000000
                self.logger.info(f"客户端订单索引: {client_order_index}")
                
                # ApiNonceManager会每次从API获取最新nonce，无需手动刷新
                if hasattr(self.lighter_client.nonce_manager, 'nonce'):
                    current_nonce = self.lighter_client.nonce_manager.nonce
                    self.logger.info(f"当前nonce: {current_nonce}")
                    self.logger.info(f"账户信息: account_index={self.account_info.get('account_index')}, api_key_index={self.account_info.get('api_key_index')}")
                else:
                    self.logger.warning("无法获取nonce信息，nonce_manager没有nonce属性")
                
                # 使用限制滑点的市价单函数
                self.logger.info(f"调用 create_market_order_limited_slippage:")
                self.logger.info(f"  - market_index: {self.market_index}")
                self.logger.info(f"  - client_order_index: {client_order_index}")
                self.logger.info(f"  - base_amount: {base_amount}")
                self.logger.info(f"  - max_slippage: 0.05 (5%)")
                self.logger.info(f"  - is_ask: {not is_long} ({'卖出' if not is_long else '买入'})")
                
                tx = await self.lighter_client.create_market_order_limited_slippage(
                    market_index=self.market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    max_slippage=0.05,  # 最大滑点5%
                    is_ask=not is_long,  # 多头时是bid (买入)，空头时是ask (卖出)
                )
                
                # 详细记录返回结果
                self.logger.info(f"Lighter API返回结果: {tx}")
                if tx:
                    self.logger.info(f"返回结果类型: {type(tx)}")
                    self.logger.info(f"返回结果长度: {len(tx) if isinstance(tx, (list, tuple)) else 'N/A'}")
                    if len(tx) >= 2:
                        self.logger.info(f"响应代码: {tx[1].code if hasattr(tx[1], 'code') else 'N/A'}")
                        self.logger.info(f"响应详情: {tx[1]}")
                    if len(tx) >= 3:
                        self.logger.info(f"错误信息: {tx[2]}")
                
                # 检查是否成功
                if tx and len(tx) >= 2 and tx[1] and hasattr(tx[1], 'code') and tx[1].code == 200:
                    self.logger.info(f"✓ Lighter市价单创建成功！")
                    self.logger.info(f"完整返回结果: {tx}")
                    return True
                else:
                    error_msg = tx[2] if tx and len(tx) > 2 else "Unknown error"
                    error_code = tx[1].code if tx and len(tx) >= 2 and hasattr(tx[1], 'code') else "N/A"
                    
                    # 尝试从错误信息中提取错误代码
                    error_code_extracted = None
                    error_msg_clean = str(error_msg)
                    import re
                    code_match = re.search(r'code=(\d+)', error_msg_clean)
                    if code_match:
                        error_code_extracted = int(code_match.group(1))
                        message_match = re.search(r"message='([^']+)'", error_msg_clean)
                        if message_match:
                            error_msg_clean = message_match.group(1)
                    
                    # 使用提取的错误代码（如果存在）
                    if error_code_extracted is not None:
                        error_code = error_code_extracted
                    
                    self.logger.error(f"✗ Lighter市价单创建失败！")
                    self.logger.error(f"  错误代码: {error_code}")
                    self.logger.error(f"  错误信息: {error_msg_clean}")
                    if error_code_extracted != error_code:
                        self.logger.error(f"  原始错误信息: {error_msg}")
                    self.logger.error(f"  完整响应: {tx}")
                    
                    # 地区限制错误（20558）- 不应该重试
                    if error_code == 20558 or "restricted jurisdiction" in str(error_msg).lower():
                        self.logger.error(f"")
                        self.logger.error(f"  ⚠️  地区限制错误！")
                        self.logger.error(f"  当前IP地址所在的地区无法访问Lighter服务。")
                        self.logger.error(f"  解决方案：")
                        self.logger.error(f"  1. 使用VPN或代理服务器连接到允许的地区")
                        self.logger.error(f"  2. 联系Lighter支持了解详情: https://lighter.xyz/terms")
                        self.logger.error(f"  3. 检查网络连接和代理设置")
                        self.logger.error(f"")
                        # 地区限制错误不应该重试，直接返回False
                        return False
                    
                    # Nonce错误 - 可以重试
                    if "invalid nonce" in str(error_msg).lower() or error_code == "N/A" and "nonce" in str(error_msg).lower():
                        self.logger.error(f"  ⚠️  Nonce错误！后端期望的nonce与本地不匹配")
                        self.logger.error(f"  - API Key索引: {self.account_info.get('api_key_index', 0)}")
                        self.logger.error(f"  - 账户索引: {self.account_info.get('account_index', 0)}")
                        
                        # 等待更长的时间让pending交易完成，并强制刷新nonce
                        if attempt < max_retries - 1:
                            self.logger.warning(f"  等待pending交易完成（10秒）后重试...")
                            api_key_idx = int(self.account_info.get('api_key_index', 0))
                            self.lighter_client.nonce_manager.hard_refresh_nonce(api_key_idx)
                            await asyncio.sleep(10)  # 等待10秒
                            continue
                        else:
                            self.logger.error(f"  已达到最大重试次数，停止重试")
                            return False
                    
                    # 其他错误 - 根据错误类型决定是否重试
                    if attempt < max_retries - 1:
                        # 某些临时错误可以重试
                        retryable_errors = [500, 502, 503, 504, 429]  # 服务器错误和限流错误
                        if error_code in retryable_errors or "temporary" in str(error_msg).lower():
                            self.logger.warning(f"  临时错误，等待后重试...")
                            await asyncio.sleep(2)
                            continue
                    
                    # 不可重试的错误或已达到最大重试次数
                    if attempt == max_retries - 1:
                        self.logger.error(f"  已达到最大重试次数，停止重试")
                    return False
                
            except Exception as e:
                error_str = str(e)
                self.logger.error(f"✗ 创建Lighter市价单异常 (尝试 {attempt + 1}/{max_retries}): {error_str}")
                self.logger.error(f"  异常类型: {type(e).__name__}")
                import traceback
                self.logger.error(f"  完整堆栈:")
                self.logger.error(traceback.format_exc())
                
                # 检查是否是地区限制错误
                if "20558" in error_str or "restricted jurisdiction" in error_str.lower():
                    self.logger.error(f"")
                    self.logger.error(f"  ⚠️  地区限制错误！")
                    self.logger.error(f"  当前IP地址所在的地区无法访问Lighter服务。")
                    self.logger.error(f"  解决方案：")
                    self.logger.error(f"  1. 使用VPN或代理服务器连接到允许的地区")
                    self.logger.error(f"  2. 联系Lighter支持了解详情: https://lighter.xyz/terms")
                    self.logger.error(f"  3. 检查网络连接和代理设置")
                    self.logger.error(f"")
                    # 地区限制错误不应该重试，直接返回False
                    return False
                
                # Nonce错误 - 可以重试
                if "invalid nonce" in error_str.lower() or "nonce" in error_str.lower():
                    if attempt < max_retries - 1:
                        self.logger.warning(f"  ⚠️  Nonce相关错误，等待pending交易完成...")
                        
                        # 等待pending交易完成
                        await asyncio.sleep(3)
                        continue
                
                # 其他错误
                if attempt == max_retries - 1:
                    self.logger.error(f"  最后一次尝试失败，停止重试")
                
                return False
        
        self.logger.error(f"✗ Lighter市价单创建在{max_retries}次尝试后全部失败")
        return False
    
    async def get_lighter_positions(self):
        """获取Lighter当前持仓"""
        try:
            self.logger.info("获取Lighter当前持仓...")
            
            # 检查lighter_client是否已初始化
            if self.lighter_client is None:
                self.logger.warning("Lighter客户端未初始化，无法获取持仓")
                return []
            
            # 创建AccountApi实例
            from lighter.api.account_api import AccountApi
            account_api = AccountApi(self.lighter_client.api_client)
            
            # 使用 "index" 而不是 "account_index"
            account_info = await account_api.account(
                by="index",
                value=str(self.account_info['account_index'])
            )
            
            self.logger.info(f"账户信息: {account_info}")
            
            # 从Paradex market名称提取基础资产（如BTC-USD-PERP -> BTC）
            base_asset = self.market.split('-')[0].upper()
            
            # 从日志可以看到账户信息在 accounts 数组中
            if hasattr(account_info, 'accounts') and account_info.accounts:
                account = account_info.accounts[0]  # 取第一个账户
                if hasattr(account, 'positions') and account.positions:
                    self.logger.info(f"总持仓数: {len(account.positions)}")
                    market_positions = []
                    for i, pos in enumerate(account.positions):
                        market_id = getattr(pos, 'market_id', None)
                        symbol = getattr(pos, 'symbol', '').upper()
                        position = getattr(pos, 'position', 'N/A')
                        sign = getattr(pos, 'sign', 'N/A')
                        
                        self.logger.info(f"持仓 {i}: market_id={market_id}, symbol={symbol}, position={position}, sign={sign}")
                        
                        # 精确匹配：只匹配symbol完全相等的持仓（避免ETH匹配到ETHFI）
                        if hasattr(pos, 'market_id') and hasattr(pos, 'symbol'):
                            if symbol == base_asset and pos.market_id == self.market_index:
                                market_positions.append(pos)
                                self.logger.info(f"✓ 找到精确匹配的{self.market}持仓: market_id={pos.market_id}, symbol={symbol}, position={pos.position}, sign={pos.sign}")
                            elif symbol == base_asset:
                                # symbol匹配但market_id不匹配，记录警告
                                self.logger.warning(f"⚠️  symbol={symbol} 匹配，但market_id={pos.market_id}与配置的market_index={self.market_index}不一致")
                            elif pos.market_id == self.market_index:
                                # market_id匹配但symbol不匹配，记录警告
                                self.logger.warning(f"⚠️  market_id={pos.market_id} 匹配，但symbol={symbol}与预期的{base_asset}不一致")
                    
                    self.logger.info(f"找到 {len(market_positions)} 个{self.market}持仓（精确匹配）")
                    return market_positions
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
            position_amount = abs(float(str(position.position)))  # 转换为数量
            position_size = int(position_amount * 100000)  # 转换为最小单位
            # 如果有正仓位但换算后为0，兜底为1个最小单位
            if position_amount > 0 and position_size == 0:
                position_size = 1
            is_long = position.sign == 1  # sign: 1 for Long, -1 for Short
            close_is_ask = is_long  # 如果是多头，需要卖出（ask）；如果是空头，需要买入（bid）
            
            market_symbol = self.market.split('-')[0]
            self.logger.info(f"平仓: {'多头' if is_long else '空头'}, 数量: {position_amount} {market_symbol} ({position_size} 最小单位)")
            
            # 使用市价单平仓（reduce_only=True 防止误加仓）
            base_amount = position_size
            client_order_index = (int(time.time() * 1000) * 1000 + random.randint(1, 99)) % 1000000
            
            tx = await self.lighter_client.create_market_order_limited_slippage(
                market_index=self.market_index,
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
            
            # 过滤当前市场持仓
            market_positions = []
            for pos in positions:
                if pos.get('market') == self.market:
                    market_positions.append(pos)
            
            if market_positions:
                self.logger.info(f"找到 {len(market_positions)} 个{self.market}持仓")
            else:
                self.logger.info(f"没有{self.market}持仓")
            return market_positions
            
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
                market=self.market,
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
            
            # 提取当前市场持仓
            paradex_market_pos = None
            for pos in paradex_positions:
                if pos.get('market') == self.market:
                    paradex_market_pos = pos
                    break
            
            lighter_market_pos = None
            if lighter_positions and len(lighter_positions) > 0:
                lighter_market_pos = lighter_positions[0]
            
            # 解析持仓数量
            paradex_size = 0.0
            paradex_is_short = False
            if paradex_market_pos:
                try:
                    raw_size = paradex_market_pos.get('size', 0)
                    paradex_size = abs(float(raw_size))
                    paradex_is_short = float(raw_size) < 0
                except Exception as e:
                    self.logger.error(f"更新状态: 解析Paradex持仓数量失败: {e}")
                    paradex_size = 0.0
            
            lighter_size = 0.0
            lighter_is_long = False
            if lighter_market_pos:
                try:
                    position_value = getattr(lighter_market_pos, 'position', None)
                    sign_value = getattr(lighter_market_pos, 'sign', None)
                    if position_value is not None:
                        # position字段已经是单位的字符串，直接转换为float
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
            self.logger.info(f"Paradex持仓查询结果: {len(paradex_positions)} 个{self.market}持仓")
            
            self.logger.info("正在获取Lighter持仓...")
            lighter_positions = await self.get_lighter_positions()
            self.logger.info(f"Lighter持仓查询结果: {len(lighter_positions)} 个{self.market}持仓")
            
            # 提取当前市场持仓
            paradex_market_pos = None
            for pos in paradex_positions:
                if pos.get('market') == self.market:
                    paradex_market_pos = pos
                    self.logger.info(f"找到Paradex {self.market}持仓: {pos}")
                    break
            
            lighter_market_pos = None
            if lighter_positions and len(lighter_positions) > 0:
                lighter_market_pos = lighter_positions[0]  # 取第一个持仓
                self.logger.info(f"找到Lighter {self.market}持仓对象: position={getattr(lighter_market_pos, 'position', 'N/A')}, sign={getattr(lighter_market_pos, 'sign', 'N/A')}")
            else:
                self.logger.info(f"Lighter持仓列表为空或没有{self.market}持仓")
            
            market_symbol = self.market.split('-')[0]
            
            # 解析持仓数量
            paradex_size = 0.0
            paradex_is_short = False
            if paradex_market_pos:
                try:
                    raw_size = paradex_market_pos.get('size', 0)
                    paradex_size = abs(float(raw_size))
                    paradex_is_short = float(raw_size) < 0
                    self.logger.info(f"Paradex持仓: {'空头' if paradex_is_short else '多头'} {paradex_size} {market_symbol} (原始值: {raw_size})")
                except Exception as e:
                    self.logger.error(f"解析Paradex持仓数量失败: {e}, 持仓数据: {paradex_market_pos}")
                    paradex_size = 0.0
            else:
                self.logger.info(f"Paradex无{self.market}持仓")
            
            lighter_size = 0.0
            lighter_is_long = False
            if lighter_market_pos:
                try:
                    position_value = getattr(lighter_market_pos, 'position', None)
                    sign_value = getattr(lighter_market_pos, 'sign', None)
                    self.logger.info(f"Lighter持仓原始值: position={position_value}, sign={sign_value}")
                    
                    if position_value is not None:
                        # position字段已经是单位的字符串，直接转换为float
                        lighter_size = abs(float(str(position_value)))
                        lighter_is_long = (sign_value == 1) if sign_value is not None else False
                        self.logger.info(f"Lighter持仓: {'多头' if lighter_is_long else '空头'} {lighter_size} {market_symbol}")
                    else:
                        self.logger.warning("Lighter持仓position属性为None")
                except Exception as e:
                    self.logger.error(f"解析Lighter持仓数量失败: {e}, 持仓对象: {lighter_market_pos}")
                    import traceback
                    traceback.print_exc()
                    lighter_size = 0.0
            else:
                self.logger.info(f"Lighter无{self.market}持仓")
            
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
                self.logger.info(f"✓ 持仓对等: Paradex多头 {paradex_size} {market_symbol} = Lighter空头 {lighter_size} {market_symbol}")
                return True, paradex_market_pos, lighter_market_pos
            elif paradex_is_short and lighter_is_long and abs(paradex_size - lighter_size) < tolerance:
                # Paradex空头 + Lighter多头
                self.logger.info(f"✓ 持仓对等: Paradex空头 {paradex_size} {market_symbol} = Lighter多头 {lighter_size} {market_symbol}")
                return True, paradex_market_pos, lighter_market_pos
            else:
                self.logger.warning(f"✗ 持仓不对等!")
                self.logger.warning(f"  Paradex: {'空头' if paradex_is_short else ('多头' if paradex_size > 0 else '无')} {paradex_size} {market_symbol}")
                self.logger.warning(f"  Lighter: {'多头' if lighter_is_long else ('空头' if lighter_size > 0 else '无')} {lighter_size} {market_symbol}")
                self.logger.warning(f"  数量差异: {abs(paradex_size - lighter_size)} {market_symbol} (允许误差: {tolerance} {market_symbol})")
                return False, paradex_market_pos, lighter_market_pos
                
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
    """从CSV文件读取账户配置，自动检测编码"""
    accounts = []
    
    if not os.path.exists(csv_file):
        logging.error(f"找不到CSV文件: {csv_file}")
        return accounts
    
    # 尝试多种编码方式读取CSV文件
    encodings = ['utf-8', 'utf-8-sig', 'gbk', 'gb2312', 'gb18030', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(csv_file, 'r', encoding=encoding) as file:
                reader = csv.DictReader(file)
                for row in reader:
                    accounts.append(row)
            
            logging.info(f"成功使用 {encoding} 编码读取CSV文件: {csv_file}, 读取到 {len(accounts)} 个账户")
            return accounts
            
        except UnicodeDecodeError:
            # 编码不匹配，尝试下一个
            continue
        except Exception as e:
            # 其他错误（如CSV格式错误），记录并继续尝试
            logging.warning(f"使用 {encoding} 编码读取CSV时出错: {e}")
            continue
    
    # 如果所有编码都失败，尝试使用错误处理方式
    logging.warning("所有编码尝试失败，使用错误处理方式读取...")
    try:
        with open(csv_file, 'r', encoding='utf-8', errors='replace') as file:
            reader = csv.DictReader(file)
            for row in reader:
                accounts.append(row)
        logging.warning(f"使用错误处理方式读取到 {len(accounts)} 个账户（部分字符可能显示异常）")
        return accounts
    except Exception as e:
        logging.error(f"读取CSV文件失败: {e}")
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
    
    # 启动UI状态监控线程
    monitor_thread = threading.Thread(target=run_status_monitor, daemon=True)
    monitor_thread.start()
    logging.info("UI状态监控已启动，每2秒刷新一次")
    
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
    print("多线程交易脚本")
    print("=" * 60)
    
    main()

