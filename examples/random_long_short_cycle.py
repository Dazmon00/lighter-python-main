import asyncio
import contextlib
import csv
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import lighter
from rich.live import Live
from rich.table import Table


class ClientPool:
    def __init__(self):
        self._clients: Dict[str, lighter.SignerClient] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._ready: Dict[str, bool] = {}
        self._init_lock = asyncio.Lock()

    @staticmethod
    def _account_key(account_info: Dict[str, str]) -> str:
        return f"{account_info['account_index']}_{account_info['api_key_index']}"

    async def get_client(self, account_info: Dict[str, str]) -> Optional[Tuple[lighter.SignerClient, asyncio.Lock]]:
        key = self._account_key(account_info)

        if key not in self._clients:
            async with self._init_lock:
                if key not in self._clients:
                    client = build_client(account_info)
                    self._clients[key] = client
                    self._locks[key] = asyncio.Lock()
                    self._ready[key] = False

        client = self._clients[key]
        lock = self._locks[key]

        if not self._ready[key]:
            async with lock:
                if not self._ready[key]:
                    desc = f"{account_info['description']}[{account_info['account_index']}]"
                    if not await ensure_client_ready(client, desc):
                        return None
                    self._ready[key] = True

        return client, lock

    async def close_all(self) -> None:
        for client in self._clients.values():
            try:
                await client.close()
            except Exception:
                pass

BASE_URL = "https://mainnet.zklighter.elliot.ai"
BTC_MARKET_INDEX = 1

# Lighter BTC 的最小单位换算：1 BTC = 100000 基础单位
BTC_POSITION_SCALE = 100_000
TRADE_SIZE_BTC = 0.001
TRADE_BASE_AMOUNT = int(TRADE_SIZE_BTC * BTC_POSITION_SCALE)


@dataclass
class AccountState:
    account_info: Dict[str, str]
    initial_collateral: float = 0.0
    current_collateral: float = 0.0
    wear: float = 0.0
    position_btc: float = 0.0
    last_collateral: Optional[float] = None
    loop_count: int = 0


account_states: Dict[str, AccountState] = {}
total_initial_collateral: float = 0.0


LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "random_long_short_cycle.log")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)


def load_accounts_from_csv(csv_file: str = "examples/accounts.csv") -> List[Dict[str, str]]:
    accounts: List[Dict[str, str]] = []
    try:
        with open(csv_file, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                accounts.append({
                    "account_index": int(row["account_index"]),
                    "api_key_index": int(row["api_key_index"]),
                    "api_key_private_key": row["api_key_private_key"],
                    "description": row.get("description", "") or f"Account-{row['account_index']}",
                })
    except Exception as exc:
        logging.error("加载账户CSV失败: %s", exc)

    return accounts


def build_client(account_info: Dict[str, str]) -> lighter.SignerClient:
    return lighter.SignerClient(
        url=BASE_URL,
        private_key=account_info["api_key_private_key"],
        account_index=account_info["account_index"],
        api_key_index=account_info["api_key_index"],
    )


def next_client_order_index(prefix: int) -> int:
    # 时间戳乘以1000后截取6位，尽量避免重复
    return (int(time.time() * 1000) * 100 + prefix) % 1_000_000


async def ensure_client_ready(client: lighter.SignerClient, account_desc: str) -> bool:
    try:
        err = client.check_client()
        if err is not None:
            logging.error("%s 客户端检查失败: %s", account_desc, err)
            return False
        return True
    except Exception as exc:
        logging.exception("%s 客户端检查异常", account_desc)
        return False


async def submit_market_order(
    client_pool: ClientPool,
    account_info: Dict[str, str],
    is_long: bool,
    base_amount: int,
    reduce_only: bool = False,
    record_loop: bool = False,
) -> bool:
    desc = f"{account_info['description']}[{account_info['account_index']}]"

    try:
        client_lock = await client_pool.get_client(account_info)
        if client_lock is None:
            logging.error("%s 获取客户端失败，无法提交订单", desc)
            return False

        client, lock = client_lock

        async with lock:
            client_order_index = next_client_order_index(prefix=1 if is_long else 2)
            logging.info(
                "%s 创建%s单: base_amount=%s, client_order_index=%s",
                desc,
                "多" if is_long else "空",
                base_amount,
                client_order_index,
            )

            tx = await client.create_market_order_limited_slippage(
                market_index=BTC_MARKET_INDEX,
                client_order_index=client_order_index,
                base_amount=base_amount,
                max_slippage=0.05,
                is_ask=not is_long,
                reduce_only=reduce_only,
            )

        if tx and len(tx) >= 2 and getattr(tx[1], "code", None) == 200:
            logging.info("%s %s单提交成功", desc, "多" if is_long else "空")
            if record_loop:
                account_key = ClientPool._account_key(account_info)
                state = account_states.get(account_key)
                if state is not None:
                    state.loop_count += 1
            return True

        logging.warning("%s %s单返回异常: %s", desc, "多" if is_long else "空", tx)
        return False

    except Exception:
        logging.exception("%s %s单提交失败", desc, "多" if is_long else "空")
        return False


async def fetch_account_data(client: lighter.SignerClient, account_index: int):
    from lighter.api.account_api import AccountApi

    account_api = AccountApi(client.api_client)
    return await account_api.account(by="index", value=str(account_index))


async def fetch_account_positions(client: lighter.SignerClient, account_index: int):
    account_data = await fetch_account_data(client, account_index)

    if hasattr(account_data, "accounts") and account_data.accounts:
        account = account_data.accounts[0]
        return getattr(account, "positions", []) or []
    return []


async def fetch_positions_with_retry(
    client: lighter.SignerClient,
    account_index: int,
    retries: int = 3,
    delay: float = 1.0,
):
    positions = await fetch_account_positions(client, account_index)
    attempt = 0
    while not positions and attempt < retries - 1:
        await asyncio.sleep(delay)
        attempt += 1
        positions = await fetch_account_positions(client, account_index)
    return positions


async def fetch_account_collateral(client: lighter.SignerClient, account_index: int) -> Optional[float]:
    account_data = await fetch_account_data(client, account_index)

    if hasattr(account_data, "accounts") and account_data.accounts:
        account = account_data.accounts[0]
        collateral = getattr(account, "collateral", None)
        if collateral is None:
            return None
        try:
            return float(collateral)
        except (TypeError, ValueError):
            logging.warning("账户 %s 抵押品解析失败: %s", account_index, collateral)
            return None
    return None


def position_to_base_amount(position) -> Optional[int]:
    try:
        size_btc = abs(float(str(position.position)))
        base_amount = int(size_btc * BTC_POSITION_SCALE)
        return base_amount if base_amount > 0 else None
    except Exception:
        return None


async def close_positions_for_account(client_pool: ClientPool, account_info: Dict[str, str]) -> None:
    desc = f"{account_info['description']}[{account_info['account_index']}]"

    try:
        client_lock = await client_pool.get_client(account_info)
        if client_lock is None:
            logging.error("%s 获取客户端失败，无法平仓", desc)
            return

        client, lock = client_lock

        async with lock:
            positions = await fetch_positions_with_retry(client, account_info["account_index"], retries=4, delay=1.0)

        if not positions:
            logging.info("%s 没有持仓需要关闭", desc)
            return

        closed = 0
        for pos in positions:
            amt = position_to_base_amount(pos)
            if not amt:
                continue

            is_long = getattr(pos, "sign", 0) == 1
            order_ok = await submit_market_order(
                client_pool,
                account_info,
                is_long=not is_long,  # 平多要开空，平空要开多
                base_amount=amt,
                reduce_only=True,
            )

            if order_ok:
                closed += 1
            await asyncio.sleep(0.5)

        if closed:
            logging.info("%s 已尝试关闭 %s 个持仓", desc, closed)
    except Exception:
        logging.exception("%s 平仓失败", desc)
    finally:
        pass


async def close_positions_for_all(client_pool: ClientPool, accounts: List[Dict[str, str]]) -> None:
    logging.info("检查所有账户持仓并尝试关闭...")
    for account in accounts:
        await close_positions_for_account(client_pool, account)


async def trading_loop(client_pool: ClientPool, accounts: List[Dict[str, str]], shared_state: Dict[str, float]) -> None:
    loop_count = 0
    while True:
        loop_count += 1
        if len(accounts) > 1:
            long_account, short_account = random.sample(accounts, 2)
        else:
            long_account = short_account = accounts[0]

        await submit_market_order(
            client_pool,
            long_account,
            is_long=True,
            base_amount=TRADE_BASE_AMOUNT,
            record_loop=True,
        )
        await submit_market_order(
            client_pool,
            short_account,
            is_long=False,
            base_amount=TRADE_BASE_AMOUNT,
            record_loop=True,
        )

        sleep_seconds = random.randint(30, 8888)
        shared_state["next_cycle_deadline"] = time.time() + sleep_seconds
        await asyncio.sleep(sleep_seconds)

        if loop_count % 3 == 0:
            await close_positions_for_all(client_pool, accounts)


async def snapshot_account_status(client_pool: ClientPool, accounts: List[Dict[str, str]]) -> Dict[str, Dict[str, float]]:
    statuses: Dict[str, Dict[str, float]] = {}
    for account in accounts:
        desc = f"{account['description']}[{account['account_index']}]"
        client_lock = await client_pool.get_client(account)
        if client_lock is None:
            logging.error("%s 获取客户端失败，无法获取余额", desc)
            continue

        client, lock = client_lock

        async with lock:
            account_data = await fetch_account_data(client, account["account_index"])

        account_key = ClientPool._account_key(account)
        if hasattr(account_data, "accounts") and account_data.accounts:
            detailed = account_data.accounts[0]
            collateral_raw = getattr(detailed, "collateral", None)
            positions = getattr(detailed, "positions", []) or []

            collateral_value: Optional[float] = None
            if collateral_raw is not None:
                try:
                    collateral_value = float(collateral_raw)
                except (TypeError, ValueError):
                    logging.warning("%s 抵押品解析失败: %s", desc, collateral_raw)

            position_total = 0.0
            for pos in positions:
                try:
                    size = float(str(getattr(pos, "position", "0")))
                    sign = getattr(pos, "sign", 1)
                    position_total += size if sign >= 0 else -size
                except (TypeError, ValueError):
                    continue

            statuses[account_key] = {
                "collateral": collateral_value if collateral_value is not None else 0.0,
                "position": position_total,
            }
        else:
            logging.warning("%s 未能获取账户详细信息", desc)
    return statuses


async def balance_monitor(
    client_pool: ClientPool,
    accounts: List[Dict[str, str]],
    initial_collaterals: Dict[str, float],
    interval_seconds: int = 20,
) -> None:
    logging.info("余额监控启动，初始账户总余额: %.2f USDC", total_initial_collateral)

    try:
        while True:
            current_status = await snapshot_account_status(client_pool, accounts)
            current_collaterals = {key: value.get("collateral", 0.0) for key, value in current_status.items()}
            current_total = sum(current_collaterals.values())
            wear = total_initial_collateral - current_total
            logging.info("当前账户总余额: %.2f USDC，累计磨损: %.2f USDC", current_total, wear)

            for account in accounts:
                account_key = ClientPool._account_key(account)
                if account_key in current_status:
                    status = current_status[account_key]
                    collateral_value = status.get("collateral", 0.0)
                    position_value = status.get("position", 0.0)

                    state = account_states.get(account_key)
                    if state is not None:
                        previous_collateral = state.current_collateral if state.current_collateral is not None else state.initial_collateral
                        state.current_collateral = collateral_value
                        state.position_btc = position_value
                        state.wear = state.initial_collateral - collateral_value
                        state.last_collateral = collateral_value
                    # 详细账户日志降级为调试级别，避免终端噪音
                    if account_key in initial_collaterals:
                        diff = initial_collaterals[account_key] - collateral_value
                        desc = f"{account['description']}[{account['account_index']}]"
                        logging.debug("%s 余额: %.2f USDC，磨损: %.2f USDC", desc, collateral_value, diff)

            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        logging.info("余额监控任务已取消")
        raise
    except Exception:
        logging.exception("余额监控任务异常")


def create_status_table(shared_state: Dict[str, float]) -> Table:
    table = Table(title="账户实时状态", show_header=True, header_style="bold green")
    table.add_column("账号", style="bold white", no_wrap=True)
    table.add_column("持仓", justify="right")
    table.add_column("初始金额", justify="right")
    table.add_column("当前金额", justify="right")
    table.add_column("磨损", justify="right")
    table.add_column("总磨损", justify="right")
    table.add_column("循环次数", justify="right")
    table.add_column("倒计时", justify="right")

    countdown_seconds = max(0, int(shared_state.get("next_cycle_deadline", time.time()) - time.time()))
    current_total_collateral = sum(state.current_collateral for state in account_states.values())
    total_wear_value = max(0.0, total_initial_collateral - current_total_collateral)
    total_wear_str = f"{total_wear_value:.2f}"

    for account_key in sorted(account_states.keys()):
        state = account_states[account_key]
        account = state.account_info
        desc = f"{account['description']}[{account['account_index']}]"

        position_str = f"{state.position_btc:+.4f} BTC"
        initial_str = f"{state.initial_collateral:.2f}"
        current_str = f"{state.current_collateral:.2f}"
        wear_str = f"{state.wear:.2f}"
        loop_count_str = str(state.loop_count)
        countdown_str = f"{countdown_seconds}s"

        table.add_row(
            desc,
            position_str,
            initial_str,
            current_str,
            wear_str,
            total_wear_str,
            loop_count_str,
            countdown_str,
        )

    return table


async def ui_update_loop(live: Live, shared_state: Dict[str, float], refresh_interval: float = 1.0) -> None:
    try:
        while True:
            live.update(create_status_table(shared_state))
            await asyncio.sleep(refresh_interval)
    except asyncio.CancelledError:
        raise


async def main():
    accounts = load_accounts_from_csv()
    if not accounts:
        logging.error("未加载到账户信息，程序退出")
        return

    client_pool = ClientPool()
    initial_status = await snapshot_account_status(client_pool, accounts)
    initial_collaterals = {key: value.get("collateral", 0.0) for key, value in initial_status.items()}
    for account in accounts:
        key = ClientPool._account_key(account)
        status = initial_status.get(key, {})
        initial_value = status.get("collateral", 0.0)
        position_value = status.get("position", 0.0)
        account_states[key] = AccountState(
            account_info=account,
            initial_collateral=initial_value,
            current_collateral=initial_value,
            wear=0.0,
            position_btc=position_value,
            last_collateral=initial_value,
        )

    global total_initial_collateral
    total_initial_collateral = sum(state.initial_collateral for state in account_states.values())
    logging.info("全账户初始总余额: %.2f USDC", total_initial_collateral)

    shared_state: Dict[str, float] = {"next_cycle_deadline": time.time()}
    monitor_task: Optional[asyncio.Task] = None
    ui_task: Optional[asyncio.Task] = None

    if initial_collaterals:
        monitor_task = asyncio.create_task(
            balance_monitor(client_pool, accounts, initial_collaterals, interval_seconds=3)
        )
    else:
        logging.warning("初始余额获取失败，余额监控任务未启动")

    try:
        with Live(create_status_table(shared_state), refresh_per_second=4, screen=False) as live:
            ui_task = asyncio.create_task(ui_update_loop(live, shared_state))
            try:
                await trading_loop(client_pool, accounts, shared_state)
            except KeyboardInterrupt:
                logging.info("收到中断信号，开始清理持仓")
            finally:
                if ui_task:
                    ui_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ui_task
    finally:
        if monitor_task:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        await close_positions_for_all(client_pool, accounts)
        await client_pool.close_all()
        logging.info("程序结束")


if __name__ == "__main__":
    asyncio.run(main())

