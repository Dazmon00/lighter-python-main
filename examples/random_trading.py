import asyncio
import logging
import lighter
import csv
import random
import time
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
import ssl

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 配置SSL上下文，减少SSL错误
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

BASE_URL = "https://mainnet.zklighter.elliot.ai"

# 账户循环次数统计
account_loop_counts = {}
# 账户状态信息
account_status = {}

console = Console()

def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]

def load_accounts_from_csv(csv_file="examples/accounts.csv"):
    """从CSV文件加载账户信息"""
    accounts = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                accounts.append({
                    'account_index': int(row['account_index']),
                    'api_key_index': int(row['api_key_index']),
                    'api_key_private_key': row['api_key_private_key'],
                    'description': row['description']
                })
        print(f"成功加载 {len(accounts)} 个账户")
        return accounts
    except Exception as e:
        print(f"加载CSV文件失败: {e}")
        return []

def save_trading_log(account_info, action, result, timestamp):
    """保存交易日志到CSV文件"""
    log_file = "examples/trading_log.csv"
    try:
        with open(log_file, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # 如果是新文件，写入表头
            if file.tell() == 0:
                writer.writerow(['timestamp', 'account_index', 'api_key_index', 'description', 'action', 'result', 'details'])
            
            writer.writerow([
                timestamp,
                account_info['account_index'],
                account_info['api_key_index'],
                account_info['description'],
                action,
                result,
                str(result) if result else "None"
            ])
    except Exception as e:
        print(f"保存交易日志失败: {e}")

async def get_account_balance(account_info):
    """获取账户余额"""
    api_client = None
    try:
        # 创建独立的ApiClient来获取账户信息
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        
        # 使用AccountApi获取真实账户信息
        from lighter.api.account_api import AccountApi
        account_api = AccountApi(api_client)
        
        # 通过账户索引获取账户信息
        account_data = await account_api.account(
            by="index", 
            value=str(account_info['account_index'])
        )
        
        # 提取账户信息
        if hasattr(account_data, 'accounts') and account_data.accounts:
            account = account_data.accounts[0]  # 获取第一个账户
            
            # 获取抵押品余额
            collateral = getattr(account, 'collateral', 0)
            
            # 确保collateral是数字类型
            try:
                if isinstance(collateral, str):
                    collateral = float(collateral)
                return f"{collateral:.2f} USDC"
            except (ValueError, TypeError):
                return f"{collateral} USDC"
        else:
            return "未找到"
            
    except Exception as e:
        return f"获取失败: {str(e)[:20]}..."
    finally:
        if api_client:
            try:
                await api_client.close()
            except:
                pass

async def get_account_volume(account_info):
    """获取账户交易量"""
    api_client = None
    try:
        # 创建独立的ApiClient来获取账户信息
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        
        # 使用AccountApi获取真实账户信息
        from lighter.api.account_api import AccountApi
        account_api = AccountApi(api_client)
        
        # 通过账户索引获取账户信息
        account_data = await account_api.account(
            by="index", 
            value=str(account_info['account_index'])
        )
        
        # 提取账户信息
        if hasattr(account_data, 'accounts') and account_data.accounts:
            account = account_data.accounts[0]  # 获取第一个账户
            
            # 使用total_asset_value作为交易量
            total_asset_value = getattr(account, 'total_asset_value', 0)
            if total_asset_value and total_asset_value != 0:
                if isinstance(total_asset_value, str):
                    total_asset_value = float(total_asset_value)
                return f"{total_asset_value:.2f} USDC"
            else:
                return "0.00 USDC"
        else:
            return "未找到"
            
    except Exception as e:
        return f"获取失败: {str(e)[:20]}..."
    finally:
        if api_client:
            try:
                await api_client.close()
            except:
                pass

def create_status_table(accounts):
    """创建状态表格"""
    table = Table(title="账户交易状态", show_header=True, header_style="bold green")
    table.add_column("账户", style="bold white", no_wrap=True)
    table.add_column("余额", style="bold white")
    table.add_column("循环次数", style="bold white")
    table.add_column("交易量", style="bold white")
    table.add_column("状态", style="bold white")
    
    for account in accounts:
        account_key = f"{account['description']}_{account['account_index']}"
        loop_count = account_loop_counts.get(account_key, 0)
        status = account_status.get(account_key, "等待中")
        
        table.add_row(
            account['description'],
            account_status.get(f"{account_key}_balance", "获取中..."),
            str(loop_count),
            account_status.get(f"{account_key}_volume", "获取中..."),
            status
        )
    
    return table

def update_account_status(account_info, balance, status, volume=None):
    """更新账户状态"""
    account_key = f"{account_info['description']}_{account_info['account_index']}"
    account_status[f"{account_key}_balance"] = balance
    account_status[account_key] = status
    if volume is not None:
        account_status[f"{account_key}_volume"] = volume

async def retry_operation(operation, max_retries=3, delay=1):
    """重试机制"""
    for attempt in range(max_retries):
        try:
            return await operation()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            logging.warning(f"操作失败，第{attempt + 1}次重试: {e}")
            await asyncio.sleep(delay * (attempt + 1))

async def create_orders_like_original(client, account_info, loop_count):
    """按照原始脚本逻辑创建买单和卖单"""
    # 更新账户循环次数
    account_key = f"{account_info['description']}_{account_info['account_index']}"
    account_loop_counts[account_key] = account_loop_counts.get(account_key, 0) + 1
    
    # 更新状态为交易中
    update_account_status(account_info, "交易中...", "交易中")
    
    # 创建0.01 BTC的市价买单
    buy_success = False
    try:
        tx = await client.create_market_order_limited_slippage(
            market_index=1,  # BTC/USDC 市场索引
            client_order_index=100 + loop_count,  # 动态索引，避免重复
            base_amount=100,  # 0.01 BTC = 100 最小单位
            max_slippage=0.05,  # 最大滑点5%
            is_ask=False,  # 买单
        )
        if tx is not None:
            buy_success = True
            save_trading_log(account_info, "买单", "成功", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            save_trading_log(account_info, "买单", "失败", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        save_trading_log(account_info, "买单", f"异常: {e}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 创建0.01 BTC的市价卖单
    sell_success = False
    try:
        tx = await client.create_market_order_limited_slippage(
            market_index=1,  # BTC/USDC 市场索引
            client_order_index=200 + loop_count,  # 动态索引，避免重复
            base_amount=100,  # 0.01 BTC = 100 最小单位
            max_slippage=0.05,  # 最大滑点5%
            is_ask=True,  # 卖单
        )
        if tx is not None:
            sell_success = True
            save_trading_log(account_info, "卖单", "成功", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            save_trading_log(account_info, "卖单", "失败", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        save_trading_log(account_info, "卖单", f"异常: {e}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # 如果买卖都成功，查询账户余额和交易额
    if buy_success and sell_success:
        balance = await get_account_balance(account_info)
        volume = await get_account_volume(account_info)
        update_account_status(account_info, balance, "交易成功", volume)
    else:
        update_account_status(account_info, "获取失败", "交易失败")

async def main():
    # 加载账户信息
    accounts = load_accounts_from_csv()
    if not accounts:
        print("没有可用的账户，程序退出")
        return
    
    loop_count = 0
    
    # 初始化所有账户状态
    for account in accounts:
        account_key = f"{account['description']}_{account['account_index']}"
        account_status[account_key] = "等待中"
        account_status[f"{account_key}_balance"] = "获取中..."
        account_status[f"{account_key}_volume"] = "获取中..."
    
    try:
        with Live(create_status_table(accounts), refresh_per_second=4) as live:
            while True:
                loop_count += 1
                
                # 随机选择一个账户
                selected_account = random.choice(accounts)
                
                # 创建客户端
                client = lighter.SignerClient(
                    url=BASE_URL,
                    private_key=selected_account['api_key_private_key'],
                    account_index=selected_account['account_index'],
                    api_key_index=selected_account['api_key_index'],
                )
                
                # 检查客户端
                try:
                    err = client.check_client()
                    if err is not None:
                        update_account_status(selected_account, "连接失败", "连接失败")
                        await client.close()
                        live.update(create_status_table(accounts))
                        await asyncio.sleep(2.5)
                        continue
                except Exception as e:
                    logging.error(f"客户端检查异常: {e}")
                    update_account_status(selected_account, "连接异常", "连接异常")
                    try:
                        await client.close()
                    except:
                        pass
                    live.update(create_status_table(accounts))
                    await asyncio.sleep(2.5)
                    continue
                
                # 创建买单和卖单（按照原始脚本逻辑）
                try:
                    await retry_operation(lambda: create_orders_like_original(client, selected_account, loop_count))
                except Exception as e:
                    logging.error(f"交易执行异常: {e}")
                    update_account_status(selected_account, "交易异常", "交易异常")
                
                # 创建认证令牌
                try:
                    auth, err = client.create_auth_token_with_expiry(lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY)
                    if err is not None:
                        update_account_status(selected_account, "认证失败", "认证失败")
                except Exception as e:
                    logging.error(f"认证令牌异常: {e}")
                    update_account_status(selected_account, "认证异常", "认证异常")
                
                # 关闭客户端
                try:
                    await client.close()
                except Exception as e:
                    logging.error(f"关闭客户端异常: {e}")
                
                # 更新表格显示
                live.update(create_status_table(accounts))
                
                # 等待2.5秒
                await asyncio.sleep(2.5)
                
    except KeyboardInterrupt:
        console.print("\n[bold red]收到中断信号，程序停止。[/bold red]")
    except Exception as e:
        console.print(f"\n[bold red]发生错误: {e}[/bold red]")
    finally:
        console.print("[bold green]程序结束。[/bold green]")

if __name__ == "__main__":
    asyncio.run(main())
