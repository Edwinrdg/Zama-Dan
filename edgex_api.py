#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
aster交易所API封装

提供与aster交易所交互的功能，包括获取价格、资金费率、下单、查询仓位等
"""

import time
import json
import base64
import asyncio
import traceback
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple, Union
import urllib.parse

import httpx
import websockets
import logging

import hmac
import hashlib
import uuid

from funding_arbitrage_bot.exchanges.base_exchange import BaseExchange
from funding_arbitrage_bot.utils import helpers
from starkware.crypto.signature.signature import sign

from web3 import Web3
from eth_hash.auto import keccak  # 꼭 이걸 써야 함
from starkware.crypto.signature.fast_pedersen_hash import pedersen_hash
from starkware.crypto.signature.signature import sign, ec_mult, verify, ALPHA, FIELD_PRIME, EC_GEN
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP


class EdgexAPI(BaseExchange):
    """aster交易所API封装类"""

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            base_url: str = "https://pro.edgex.exchange",
            ws_url: str = "wss://quote.edgex.exchange",
            logger: Optional[logging.Logger] = None,
            config: Optional[Dict] = None
    ):
        """
        初始化gate API

        Args:
            api_key: API密钥
            api_secret: API密钥对应的密钥
            base_url: REST API基础URL
            ws_url: WebSocket API URL
            logger: 日志记录器，如果为None则使用默认日志记录器
            config: 配置信息，包含交易对精度等设置
        """
        self.logger = logger or logging.getLogger(__name__)
        self.api_key = api_key.strip()

        # 保存原始的API密钥
        self.api_secret = api_secret.strip()

        self.base_url = base_url
        self.ws_url = ws_url

        # 保存配置信息
        self.config = config or {}

        # 价格和资金费率缓存
        self.prices = {}
        self.funding_rates = {}
        # 24小时交易量缓存
        self.volumes = {}
        # 24小时成交数缓存
        self.trade_counts = {}

        # 新的数据结构，格式为 {"BTC": {"symbol": "BTCUSDT", "funding_rate": "0.111", "funding_rate_timestamp": "xxxx", "price": 1, "price_timestamp": "xxxx"}}
        self.coin_data = {}

        # HTTP客户端
        self.http_client = httpx.AsyncClient(timeout=10.0)

        # WebSocket连接
        self.ws = None
        self.ws_connected = False
        self.ws_task = None

        # 默认请求超时时间
        self.default_window = 5000  # 5秒

        # 价格日志输出控制
        self.last_price_log = {}
        self.price_log_interval = 300  # 每5分钟记录一次价格

        self.price_coins = []

        self.depth_data = {}

        self.ws_trade = None
        self.ws_trade_connected = False
        self.ws_trade_task = None
        self.ws_trade_pending_message = {}
        self.ws_trade_pending_futures = {}  # 新增：id -> Future
        self.contract_map = {}  # {"BTC":"123123"}
        self._init_contract_map()
        self.last_contract_ask1_bid1 = {}  # 上一次存储的ask1、bid1， {"BTC": {"ask1_size": 1, "ask1_price": 12, "bid1_size": 123, "bid1_price": 123 }}

        self.account_id = config.get("exchanges").get("edgex").get("account_id")
        self.market_info = {}
        self._init_market_info()

    def _init_contract_map(self):
        pass
        with httpx.Client() as client:
            res = client.get('https://pro.edgex.exchange/api/v1/public/meta/getMetaData')
            contract_list = res.json()['data']['contractList']
            self.contract_map = {x['contractName']: x['contractId'] for x in contract_list}
            print(f'edge-x，contract初始化:{self.contract_map}')

    def _init_market_info(self):
        url = f"{self.base_url}/api/v1/public/meta/getMetaData"
        with httpx.Client() as session:
            resp = session.get(url)
            if resp.status_code != 200:
                # print(f"[get_meta_data] HTTP {resp.status}")
                return None
            res = resp.json()
            data = res.get("data", {})
            meta = data
            contract_list = data.get("contractList", [])

            for contract in contract_list:
                name = contract["contractName"]
                self.market_info[name] = {
                    "contract": contract,
                    "meta": meta,
                    "contractId": contract["contractId"],
                    "tickSize": contract["tickSize"],
                    "stepSize": contract["stepSize"],
                    "minOrderSize": contract["minOrderSize"],
                    "maxOrderSize": contract["maxOrderSize"],
                    "defaultTakerFeeRate": contract["defaultTakerFeeRate"],
                }

            return contract_list

    def create_ed25519_sign(self, request: dict):
        # Create signature payload from params
        pass

    async def close(self):
        """关闭API连接"""
        if self.http_client:
            await self.http_client.aclose()

        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass

    def gen_sign(self, origin_data: dict):
        pass

    def gen_sign_rest(self, params: dict) -> str:
        """
        使用 HMAC-SHA256 算法生成币安API签名

        Args:
            params (dict): 请求参数

        Returns:
            str: 十六进制格式的签名
        """
        pass

    async def _make_signed_request(
            self,
            instruction: str,
            method: str,
            endpoint: str,
            params: Optional[Dict] = None,
            data: Optional[Dict] = None
    ) -> Dict:
        """
        发送带签名的API请求
        todo  sign参数问题暂时废弃
        Args:
            instruction: API指令名称
            method: HTTP方法（GET、POST等）
            endpoint: API端点（不包含基础URL）
            params: URL参数
            data: 请求体数据

        Returns:
            API响应数据
        """
        pass

    async def get_price(self, symbol: str) -> dict:
        pass

    async def get_funding_rate(self, symbol: str) -> dict:
        pass

    def format_price(self, symbol, price, config, market_info):
        trading_pair_config = None
        for pair in config.get("trading_pairs", []):
            if pair.get("symbol") == symbol:
                trading_pair_config = pair
                break
        price_precision = 2
        tick_size = '0.01'
        if trading_pair_config:
            price_precision = int(trading_pair_config.get("price_precision", 2))
            tick_size = str(trading_pair_config.get("tick_size", '0.01'))
        else:
            info = market_info.get(symbol)
            if info:
                tick_size = str(info['tickSize'])
                price_precision = len(tick_size.split('.')[-1].rstrip('0'))
        price = Decimal(str(price)).quantize(Decimal(tick_size), rounding=ROUND_HALF_UP)
        formatted_price = f"{price:.{price_precision}f}".rstrip('0').rstrip('.') if '.' in f"{price:.{price_precision}f}" else f"{price:.{price_precision}f}"
        return formatted_price

    def format_size(self, symbol, size, config, market_info):
        trading_pair_config = None
        for pair in config.get("trading_pairs", []):
            if pair.get("symbol") == symbol:
                trading_pair_config = pair
                break
        size_precision = 3
        step_size = 0.001
        if trading_pair_config:
            size_precision = int(trading_pair_config.get("size_precision", 3))
            step_size = float(trading_pair_config.get("step_size", 0.001))
        else:
            info = market_info.get(symbol)
            if info:
                size_precision = len(str(info['stepSize']).split('.')[-1].rstrip('0'))
                step_size = float(info['stepSize'])
        size = float(size)
        size = round(size / step_size) * step_size
        size = round(size, size_precision)
        formatted_size = f"{size:.{size_precision}f}".rstrip('0').rstrip('.') if '.' in f"{size:.{size_precision}f}" else f"{size:.{size_precision}f}"
        return formatted_size

    def _edegx_sign(self, method: str, path: str, params: dict = None, timestamp=None):
        K_MODULUS = int("0800000000000010ffffffffffffffffb781126dcae7b2321e66a241adc64d2f", 16)
        if not timestamp:
            timestamp = str(int(time.time() * 1000))
        # 直接对所有实际传递的参数签名（body/params），不区分接口
        sign_params = params or {}
        sorted_items = sorted(sign_params.items())
        param_str = "&".join(f"{k}={v}" for k, v in sorted_items)
        message = timestamp + method + path + param_str
        msg_bytes = message.encode("utf-8")
        msg_hash = int.from_bytes(keccak(msg_bytes), "big")
        msg_hash = msg_hash % K_MODULUS  # FIELD_PRIME
        private_key_int = int(self.api_secret, 16)
        r, s = sign(msg_hash, private_key_int)
        _, y = ec_mult(private_key_int, EC_GEN, ALPHA, FIELD_PRIME)
        y_hex = y.to_bytes(32, "big").hex()
        stark_signature = r.to_bytes(32, "big").hex() + s.to_bytes(32, "big").hex() + y_hex
        return stark_signature, timestamp

    async def place_order_rest(self,
                               symbol: str,
                               side: str,
                               order_type: str = "MARKET",
                               size: float = None,
                               price: float = None,
                               tif: str = "gtc",
                               is_close=False):
        """
        Edgex下单接口（REST方式），自动支持L2参数
        """
        LIMIT_ORDER_WITH_FEES = 3
        try:
            path = "/api/v1/private/order/createOrder"
            contract_id = self.contract_map.get(helpers.get_edgex_symbol(symbol))
            tif_map = {
                'gtc': 'GOOD_TIL_CANCEL',
                'ioc': 'IMMEDIATE_OR_CANCEL'
            }

            contract_info = self.market_info[helpers.get_edgex_symbol(symbol)]
            contract_id = contract_info['contractId']
            resolution = Decimal(int(contract_info['contract']['starkExResolution'], 16))
            fee_rate = Decimal(contract_info['defaultTakerFeeRate'])

            is_buy = side.upper() == 'BUY'

            client_order_id = str(uuid.uuid4())
            l2_nonce = int(hashlib.sha256(client_order_id.encode()).hexdigest()[:8], 16)
            l2_expire_time = str(int(time.time() * 1000) + 14 * 24 * 60 * 60 * 1000)
            expire_time = str(int(l2_expire_time) - 10 * 24 * 60 * 60 * 1000)

            tick_size = str(contract_info['tickSize'])
            step_size = str(contract_info['stepSize'])

            # 价格处理
            price = Decimal(price).quantize(Decimal(tick_size), rounding=ROUND_HALF_UP)
            # 数量处理
            size = Decimal(size)
            size = self.round_step_size(size, step_size)

            l2Size = Decimal(size)

            # l2Value 必须大于等于 price * l2Size，且为 step_size 的整数倍
            l2_step_size = Decimal('0.000001')  # Edgex特殊要求
            raw_l2_value = price * l2Size
            l2Value = (raw_l2_value / l2_step_size).to_integral_value(rounding=ROUND_UP) * l2_step_size
            l2Value = l2Value.quantize(l2_step_size, rounding=ROUND_UP)

            amt_synth = int((Decimal(size) * resolution).to_integral_value())
            amt_coll = int((l2Value * Decimal("1e6")).to_integral_value())
            amt_fee = int((l2Value * fee_rate * Decimal("1e6")).to_integral_value())
            expire_ts = int(int(l2_expire_time) / (1000 * 60 * 60))

            asset_id_synth = int(contract_info['contract']['starkExSyntheticAssetId'], 16)
            asset_id_coll = int(contract_info['meta']['global']['starkExCollateralCoin']['starkExAssetId'], 16)

            # L2 order hash
            h = pedersen_hash(asset_id_coll if is_buy else asset_id_synth,
                              asset_id_synth if is_buy else asset_id_coll)
            h = pedersen_hash(h, asset_id_coll)
            packed_0 = (amt_coll if is_buy else amt_synth)
            packed_0 = (packed_0 << 64) + (amt_synth if is_buy else amt_coll)
            packed_0 = (packed_0 << 64) + amt_fee
            packed_0 = (packed_0 << 32) + l2_nonce
            h = pedersen_hash(h, packed_0)
            packed_1 = LIMIT_ORDER_WITH_FEES
            pid = int(self.account_id)
            packed_1 = (packed_1 << 64) + pid
            packed_1 = (packed_1 << 64) + pid
            packed_1 = (packed_1 << 64) + pid
            packed_1 = (packed_1 << 32) + expire_ts
            packed_1 = (packed_1 << 17)
            h = pedersen_hash(h, packed_1)
            private_key_int = int(self.api_secret, 16)
            r, s = sign(h, private_key_int)
            l2_signature = r.to_bytes(32, "big").hex() + s.to_bytes(32, "big").hex()

            # 组装body
            body = {
                "accountId": self.account_id,
                "contractId": contract_id,
                "price": str(price if order_type.upper() != 'MARKET' else 0),
                "size": str(size),
                "type": order_type.upper(),
                "timeInForce": tif_map[tif.lower()],
                "side": side.upper(),
                "reduceOnly": 'false',
                "clientOrderId": client_order_id,
                "expireTime": expire_time,
                "l2Nonce": str(l2_nonce),
                "l2Value": str(l2Value),
                "l2Size": str(size),
                "l2LimitFee": str((l2Value * fee_rate).quantize(Decimal("1.000000"))),
                "l2ExpireTime": l2_expire_time,
                "l2Signature": l2_signature
            }
            print(body)
            signature, timestamp = self._edegx_sign('POST', path, params=body)
            headers = {
                'X-edgeX-Api-Timestamp': timestamp,
                'X-edgeX-Api-Signature': signature,
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            url = f"{self.base_url}{path}"
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=body)
                order_res = {
                    "success": False,
                    "order_id": None,
                    "error": None,
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "price": price,
                    "type": order_type,
                    "raw_response": None
                }
                if response.status_code == 200:
                    res_json = response.json()
                    order_res["raw_response"] = res_json
                    if res_json.get("code") == "SUCCESS":
                        order_id = res_json.get("data", {}).get("orderId")
                        order_res["success"] = True
                        order_res["order_id"] = order_id
                        return order_res
                    else:
                        order_res["error"] = res_json.get("msg")
                        return order_res
                else:
                    try:
                        order_res["raw_response"] = response.json()
                    except Exception:
                        order_res["raw_response"] = response.text
                    order_res["error"] = order_res["raw_response"]
                    return order_res
        except Exception as e:
            print(f"Edgex下单异常: {e}")
            self.logger.error(f"Edgex下单异常: {e}")
            return {
                "success": False,
                "order_id": None,
                "error": str(e),
                "symbol": symbol,
                "side": side,
                "size": size,
                "price": price,
                "type": order_type,
                "raw_response": str(e)
            }

    async def place_order(
            self,
            symbol: str,
            side: str,
            order_type: str = "MARKET",
            size: float = None,
            price: float = None,
            tif: str = "gtc",
            is_close=False
    ) -> dict:
        """
        aster下单接口
        """
        return await self.place_order_rest(symbol=symbol, side=side, order_type=order_type, size=size, price=price,
                                           tif=tif, is_close=False)

    async def get_order_status(self, order_id: str = None, symbol: str = None) -> Dict:
        """查询订单状态（使用新签名）"""
        try:
            path = "/api/v1/private/order/getOrderById"
            params = {
                "accountId": self.account_id,
                "orderIdList": order_id
            }

            # 生成签名
            signature, timestamp = self._edegx_sign('GET', path, params=params)

            # 新增请求头规范
            headers = {
                'X-edgeX-Api-Timestamp': timestamp,
                'X-edgeX-Api-Signature': signature,
                'Content-Type': 'application/json',  # 必须添加
                'User-Agent': 'Mozilla/5.0'  # 防止被反爬
            }

            # 使用与官方SDK相同的请求构造方式
            url = f"{self.base_url}{path}?{urllib.parse.urlencode(params, doseq=True, quote_via=urllib.parse.quote_plus)}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)
                status_res = {
                    'success': False,
                    'total_size': '',
                    'price': '',
                    'left_size': '',
                    'status': 'unknown',
                    'finish_status': None,
                    'raw_response': None
                }
                if response.status_code == 200:
                    res_json = response.json()
                    status_res['raw_response'] = res_json
                    if res_json.get('code') == 'SUCCESS' and res_json.get('data'):
                        order = res_json['data'][0]
                        status_res['success'] = True
                        total_size = float(order.get('size', 0))
                        executed_qty = float(order.get('cumFillSize', 0))
                        left_size = total_size - executed_qty
                        price = float(order.get('price', 0))
                        status_res['total_size'] = total_size
                        status_res['left_size'] = left_size
                        status_res['price'] = price
                        # 状态适配
                        order_status = order.get('status', '').upper()
                        if order_status == 'FILLED':
                            status_res['status'] = 'finished'
                            status_res['finish_status'] = 'filled'
                        elif order_status == 'OPEN':
                            status_res['status'] = 'open'
                        elif order_status == 'CANCELED':
                            status_res['status'] = 'cancelled'
                            status_res['finish_status'] = 'cancelled'
                        else:
                            status_res['status'] = order_status.lower() or 'unknown'
                    return status_res
                else:
                    try:
                        status_res['raw_response'] = response.json()
                    except Exception:
                        status_res['raw_response'] = response.text
                    return status_res
        except Exception as e:
            self.logger.error(f"Edgex查单异常: {e}")
            return {
                'success': False,
                'total_size': '',
                'price': '',
                'left_size': '',
                'status': 'unknown',
                'finish_status': 'error',
                'raw_response': str(e),
                'error': str(e)
            }

    async def get_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        获取当前所有持仓，返回结构与aster_api一致，带Edgex签名
        """
        try:
            url = f"{self.base_url}/api/v1/private/account/getAccountAsset"
            params = {"accountId": self.account_id}
            # 生成签名
            signature, timestamp = self._edegx_sign('GET', '/api/v1/private/account/getAccountAsset', params=params)
            headers = {
                'X-edgeX-Api-Timestamp': timestamp,
                'X-edgeX-Api-Signature': signature,
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(url, params=params, headers=headers)
                print('edgeX持仓：', res.text)
                if res.status_code != 200:
                    self.logger.error(f"Edgex获取持仓失败: {res.text}")
                    return {}
                data = res.json()
                if not data or data.get('code') != 'SUCCESS':
                    self.logger.error(f"Edgex获取持仓返回失败: {data}")
                    return {}
                pos_list = data.get('data', {}).get('positionList', [])
                pos_asset_list = data.get('data', {}).get('positionAssetList', [])
                contract_map = {v['contractId']: k for k, v in self.market_info.items()}
                positions = {}
                for pos, asset in zip(pos_list, pos_asset_list):
                    contract_id = pos.get('contractId')
                    symbol = None
                    # 反查symbol
                    for k, v in self.market_info.items():
                        if v['contractId'] == contract_id:
                            symbol = k
                            break
                    if not symbol:
                        continue
                    open_size = float(pos.get('openSize', 0))
                    open_value = float(pos.get('openValue', 0))
                    if open_size == 0:
                        continue
                    side = 'BUY' if open_size > 0 else 'SELL'
                    # entry_price用openValue/openSize，且保证为正值
                    entry_price = abs(open_value / open_size) if open_size else 0.0
                    mark_price = float(asset.get('positionValue', 0))
                    unrealized_pnl = float(asset.get('unrealizePnl', 0))
                    liquidation_price = float(asset.get('liquidatePrice', 0))
                    leverage = float(asset.get('maxLeverage', 0))
                    base_symbol = symbol.split("USDT")[
                        0] if "USDT" in symbol else symbol
                    positions[base_symbol] = {
                        "symbol": symbol,
                        "side": side,
                        "size": abs(open_size),
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "unrealized_pnl": unrealized_pnl,
                        "liquidation_price": liquidation_price,
                        "leverage": leverage,
                        "position_side": "",
                        "raw": {"pos": pos, "asset": asset}
                    }

                print(f'edgeX: 当前positions:{positions}')
                return positions
        except Exception as e:
            self.logger.error(f"Edgex获取持仓失败: {e}")
            return {}

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """
        获取单一symbol的持仓，返回结构与aster_api一致
        """
        all_positions = await self.get_positions()
        for k, v in all_positions.items():
            if symbol in (k, k.replace("USDT", "")):
                return v
        return None

    async def close_position_rest(self,
                                  symbol: str,
                                  side: str,
                                  order_type: str = "MARKET",
                                  size: float = None,
                                  price: float = None,
                                  tif: str = "gtc",
                                  reduce_only: bool = True,
                                  close_position: bool = False,
                                  **kwargs) -> dict:
        """
        Edgex平仓接口（REST方式，独立实现，风格参考binance_api/aster_api）
        """
        LIMIT_ORDER_WITH_FEES = 3
        try:
            path = "/api/v1/private/order/createOrder"
            contract_info = self.market_info[helpers.get_edgex_symbol(symbol)]
            contract_id = contract_info['contractId']

            tif_map = {
                'gtc': 'GOOD_TIL_CANCEL',
                'ioc': 'IMMEDIATE_OR_CANCEL'
            }

            contract_info = self.market_info[helpers.get_edgex_symbol(symbol)]
            contract_id = contract_info['contractId']
            resolution = Decimal(int(contract_info['contract']['starkExResolution'], 16))
            fee_rate = Decimal(contract_info['defaultTakerFeeRate'])

            is_buy = side.upper() == 'BUY'

            client_order_id = str(uuid.uuid4())
            l2_nonce = int(hashlib.sha256(client_order_id.encode()).hexdigest()[:8], 16)
            l2_expire_time = str(int(time.time() * 1000) + 14 * 24 * 60 * 60 * 1000)
            expire_time = str(int(l2_expire_time) - 10 * 24 * 60 * 60 * 1000)

            tick_size = str(contract_info['tickSize'])
            step_size = str(contract_info['stepSize'])

            if order_type == "MARKET":
                tif = "ioc"
                price = tick_size

            # 价格处理
            price = Decimal(price).quantize(Decimal(tick_size), rounding=ROUND_HALF_UP)
            # 数量处理
            size = Decimal(size)
            size = self.round_step_size(size, step_size)

            l2Size = Decimal(size)

            # l2Value 必须大于等于 price * l2Size，且为 step_size 的整数倍
            l2_step_size = Decimal('0.000001')  # Edgex特殊要求
            raw_l2_value = price * l2Size
            l2Value = (raw_l2_value / l2_step_size).to_integral_value(rounding=ROUND_UP) * l2_step_size
            l2Value = l2Value.quantize(l2_step_size, rounding=ROUND_UP)

            amt_synth = int((Decimal(size) * resolution).to_integral_value())
            amt_coll = int((l2Value * Decimal("1e6")).to_integral_value())
            amt_fee = int((l2Value * fee_rate * Decimal("1e6")).to_integral_value())
            expire_ts = int(int(l2_expire_time) / (1000 * 60 * 60))

            asset_id_synth = int(contract_info['contract']['starkExSyntheticAssetId'], 16)
            asset_id_coll = int(contract_info['meta']['global']['starkExCollateralCoin']['starkExAssetId'], 16)

            # L2 order hash
            h = pedersen_hash(asset_id_coll if is_buy else asset_id_synth,
                              asset_id_synth if is_buy else asset_id_coll)
            h = pedersen_hash(h, asset_id_coll)
            packed_0 = (amt_coll if is_buy else amt_synth)
            packed_0 = (packed_0 << 64) + (amt_synth if is_buy else amt_coll)
            packed_0 = (packed_0 << 64) + amt_fee
            packed_0 = (packed_0 << 32) + l2_nonce
            h = pedersen_hash(h, packed_0)
            packed_1 = LIMIT_ORDER_WITH_FEES
            pid = int(self.account_id)
            packed_1 = (packed_1 << 64) + pid
            packed_1 = (packed_1 << 64) + pid
            packed_1 = (packed_1 << 64) + pid
            packed_1 = (packed_1 << 32) + expire_ts
            packed_1 = (packed_1 << 17)
            h = pedersen_hash(h, packed_1)
            private_key_int = int(self.api_secret, 16)
            r, s = sign(h, private_key_int)
            l2_signature = r.to_bytes(32, "big").hex() + s.to_bytes(32, "big").hex()

            # 组装body
            body = {
                "accountId": self.account_id,
                "contractId": contract_id,
                "price": str(price if order_type.upper() != 'MARKET' else 0),
                "size": str(size),
                "type": order_type.upper(),
                "timeInForce": tif_map[tif.lower()],
                "side": side.upper(),
                "reduceOnly": 'true',
                "clientOrderId": client_order_id,
                "expireTime": expire_time,
                "l2Nonce": str(l2_nonce),
                "l2Value": str(l2Value),
                "l2Size": str(size),
                "l2LimitFee": str((l2Value * fee_rate).quantize(Decimal("1.000000"))),
                "l2ExpireTime": l2_expire_time,
                "l2Signature": l2_signature
            }

            if close_position:
                body["closePosition"] = "true"
            # 额外参数
            for k, v in kwargs.items():
                body[k] = v
            signature, timestamp = self._edegx_sign('POST', path, params=body)
            headers = {
                'X-edgeX-Api-Timestamp': timestamp,
                'X-edgeX-Api-Signature': signature,
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            url = f"{self.base_url}{path}"
            print(f'edgex-平仓order:{body}')
            self.logger.info(f'edgex-平仓order:{body}')
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=body)
                close_res = {
                    'success': False,
                    'order_id': None,
                    'raw_response': None,
                    'error': None,
                    'symbol': symbol,
                    'side': side,
                    'size': size,
                    'price': price,
                    'type': order_type,
                    'status': 'error'
                }
                if response.status_code == 200:
                    res_json = response.json()
                    close_res['raw_response'] = res_json
                    if res_json.get('code') == 'SUCCESS':
                        order_id = res_json.get('data', {}).get('orderId')
                        close_res['success'] = True
                        close_res['order_id'] = order_id
                        close_res['status'] = 'success'
                        return close_res
                    else:
                        close_res['error'] = res_json.get('msg')
                        return close_res
                else:
                    try:
                        close_res['raw_response'] = response.json()
                    except Exception:
                        close_res['raw_response'] = response.text
                    close_res['error'] = close_res['raw_response']
                    return close_res
        except Exception as e:
            print(f"Edgex平仓异常错误信息: {str(e)}")
            print(f"Edgex平仓异常错误类型: {type(e).__name__}")
            print(f"Edgex平仓异常错误行数: {traceback.extract_tb(e.__traceback__)[-1].lineno}")
            self.logger.error(f"Edgex平仓异常错误信息: {str(e)}")
            self.logger.error(f"Edgex平仓异常错误类型: {type(e).__name__}")
            self.logger.error(f"Edgex平仓异常错误行数: {traceback.extract_tb(e.__traceback__)[-1].lineno}")
            self.logger.error(f"Edgex平仓异常Edgex平仓异常: {e}")

            return {
                'success': False,
                'order_id': None,
                'raw_response': None,
                'error': str(e),
                'symbol': symbol,
                'side': side,
                'size': size,
                'price': price,
                'type': order_type,
                'status': 'error'
            }

    async def start_ws_price_stream(self):
        """
        启动WebSocket价格数据流
        """
        if self.ws_task:
            return

        self.ws_task = asyncio.create_task(self._ws_price_listener())

    async def _ws_price_listener(self):
        """
        WebSocket价格数据监听器（edgex专用）
        """
        while True:
            try:
                async with websockets.connect("wss://quote.edgex.exchange/api/v1/public/ws") as ws:
                    self.ws = ws
                    self.ws_connected = True
                    self.logger.info("edgeX WebSocket已连接")

                    # 获取永续合约交易对列表
                    symbols = await self._get_perp_symbols()
                    # 构建订阅参数，既订阅bookTicker，也订阅markPrice@1s
                    for symbol in symbols:
                        subscribe_params = f'depth.{symbol}.15'

                        subscribe_msg = {
                            "type": "subscribe",
                            "channel": subscribe_params,

                        }

                        # 发送订阅请求
                        await ws.send(json.dumps(subscribe_msg))
                    self.logger.info(f"已向edgeX发送订阅请求，共 {len(symbols)} 个交易对")
                    print(f"已向edgeX发送订阅请求，共 {len(symbols)} 个交易对")
                    # 接收和处理消息
                    while True:
                        message = await ws.recv()
                        data_json = json.loads(message)
                        # 只处理depth事件
                        channel = data_json.get('channel')
                        if str(channel).startswith("depth."):
                            if data_json.get('type') == 'quote-event':

                                # 如果dataType为Snapshot则使用该数据的ask1\bid1, 并更新缓存。  如果dataType为changed 没有ask1/bid1，就用上一次缓存的
                                content = data_json.get('content', {})
                                data_type = content.get('dataType')

                                data_list = content.get('data')
                                data = data_list[0]
                                contract_name = data.get('contractName')
                                base_symbol = contract_name.split("USDT")[
                                    0] if "USDT" in contract_name else contract_name
                                contract_id = data.get('contractId')
                                asks = data.get('asks')
                                bids = data.get('bids')
                                if data_type == 'Snapshot':
                                    ask1 = asks[0]
                                    bid1 = bids[0]
                                    ask1_price = ask1['price']
                                    ask1_size = ask1['size']
                                    bid1_price = bid1['price']
                                    bid1_size = bid1['size']
                                    self.last_contract_ask1_bid1[base_symbol] = {
                                        "ask1_price": ask1_price,
                                        "ask1_size": ask1_size,
                                        "bid1_price": bid1_price,
                                        "bid1_size": bid1_size
                                    }

                                elif data_type == 'changed':
                                    if len(asks) > 0:
                                        ask1 = asks[0]
                                        ask1_price = ask1['price']
                                        ask1_size = ask1['size']
                                    else:
                                        ask1_price = self.last_contract_ask1_bid1[base_symbol].get("ask1_price")
                                        ask1_size = self.last_contract_ask1_bid1[base_symbol].get("ask1_size")
                                    if len(bids) > 0:
                                        bid1 = bids[0]
                                        bid1_price = bid1['price']
                                        bid1_size = bid1['size']
                                    else:
                                        bid1_price = self.last_contract_ask1_bid1[base_symbol].get("bid1_price")
                                        bid1_size = self.last_contract_ask1_bid1[base_symbol].get("bid1_size")

                                    self.last_contract_ask1_bid1[base_symbol] = {
                                        "ask1_price": ask1_price,
                                        "ask1_size": ask1_size,
                                        "bid1_price": bid1_price,
                                        "bid1_size": bid1_size
                                    }
                                timestamp = time.time()
                                self.logger.debug(
                                    f"edgeX深度更新: {base_symbol} 卖一价={ask1_price} 卖一量={ask1_size} 买一价={bid1_price} 买一量={bid1_size}")
                                if hasattr(self, "data_manager") and self.data_manager:
                                    self.data_manager.update_price(base_symbol, 'edgex', ask1_price, ask1_size,
                                                                   bid1_price, bid1_size, [], [], timestamp)

                        # # 处理资金费率事件 markPriceUpdate
                        # elif data_json.get("e") == "markPriceUpdate":
                        #     symbol = data_json.get("s", "")
                        #     base_symbol = symbol.split("USDT")[0] if "USDT" in symbol else symbol
                        #     funding_rate = data_json.get("r")
                        #     event_time = time.time()
                        #     next_funding_time = data_json.get("T")
                        #     # 写入 self.funding_rates
                        #     self.funding_rates[symbol] = funding_rate
                        #     # 写入 self.coin_data
                        #     if base_symbol not in self.coin_data:
                        #         self.coin_data[base_symbol] = {
                        #             "symbol": symbol,
                        #             "funding_rate": None,
                        #             "funding_rate_timestamp": None,
                        #             "price": None,
                        #             "price_timestamp": None
                        #         }
                        #     self.coin_data[base_symbol]["funding_rate"] = funding_rate
                        #     self.coin_data[base_symbol]["funding_rate_timestamp"] = event_time
                        #     self.logger.debug(
                        #         f"Aster资金费率更新: {base_symbol} 资金费率={funding_rate} 时间={event_time}")
                        #     # 如果有 data_manager，则同步推送资金费率
                        #     if hasattr(self, "data_manager") and self.data_manager:
                        #         self.data_manager.update_funding_rate(base_symbol, 'aster', funding_rate, event_time)
            except Exception as e:
                print(e)
                self.ws_connected = False
                self.logger.error(f"Aster WebSocket错误: {e}")

    async def _ws_trade_task(self):
        pass

    async def _get_perp_symbols(self) -> List[str]:
        """
        获取所有永续合约交易对
        todo 是否从API获取全量

        Returns:
            交易对列表
        """
        # 是否从API获取全量
        print(f'basic，price_coins:{self.price_coins}')
        default_symbols = []
        for symbol in self.price_coins:
            bn_symbol = helpers.get_edgex_symbol(symbol)
            default_symbols.append(bn_symbol)
        perp_symbols = []
        print(f'edgeX: default: {default_symbols}')
        for symbol in default_symbols:
            # 确保符号格式与get_binance_symbol函数一致
            perp_symbol = self.contract_map.get(symbol)
            perp_symbols.append(perp_symbol)
        print(f'edgeX: 永续合约交易对: {perp_symbols}')
        return perp_symbols

    async def get_volume(self, symbol: str) -> Optional[float]:
        """
        获取指定交易对的24小时交易量

        Args:
            symbol: 交易对，如 "BTCUSDT"

        Returns:
            24小时交易量，如果无法获取则返回None
        """
        pass

    async def get_trade_count(self, symbol: str) -> Optional[int]:
        """
        获取指定交易对的24小时成交数

        Args:
            symbol: 交易对，如 "BTCUSDT"

        Returns:
            24小时成交数，如果无法获取则返回None
        """
        pass

    async def get_all_funding_rates(self) -> Dict[str, float]:
        """
        批量获取所有交易对的资金费率

        Returns:
            资金费率字典，格式为 {symbol: funding_rate}
        """
        pass

    async def get_depth(self, symbol) -> Optional[Dict]:
        """
        获取指定币种的深度数据（与Aster一致）
        Args:
            symbol: 交易对，如 "BTC"
        Returns:
            深度数据字典，包含 bids 和 asks，如果获取失败则返回 None
        """
        if symbol in self.depth_data:
            depth_data = self.depth_data[symbol]
            current_time = time.time()
            last_timestamp = depth_data.get("timestamp", 0)
            if current_time - last_timestamp < 60:
                return depth_data
        await self.get_depths_rest(symbol)
        if symbol in self.depth_data:
            return self.depth_data[symbol]
        return None

    async def get_depths_rest(self, symbol: str):
        """
        通过REST API获取指定交易对的深度数据（与Aster一致）
        Args:
            symbol: 交易对，如 "BTC"
        """
        try:
            contract_id = self.contract_map.get(helpers.get_edgex_symbol(symbol))
            url = f'https://pro.edgex.exchange/api/v1/public/quote/getDepth?contractId={contract_id}&level=15'
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(url)
                if res.status_code != 200:
                    self.logger.error(f"Edgex获取深度数据失败: {res.text}")
                    return
                data = res.json()
                if not data or data.get('code') != 'SUCCESS':
                    self.logger.error(f"Edgex获取深度数据返回失败: {data}")
                    return
                data_list = data.get('data', [])
                if not data_list:
                    self.logger.warning(f"Edgex深度数据为空: {data}")
                    return
                d = data_list[0]
                # bids/asks: list of dicts {price, size}
                bids = [[float(x['price']), float(x['size'])] for x in d.get('bids', [])]
                asks = [[float(x['price']), float(x['size'])] for x in d.get('asks', [])]
                self.depth_data[symbol] = {
                    "bids": bids,
                    "asks": asks,
                    "timestamp": time.time(),
                    "contractId": d.get('contractId'),
                    "contractName": d.get('contractName'),
                    "level": d.get('level'),
                    "depthType": d.get('depthType'),
                }
                self.logger.debug(f"Edgex已获取{symbol}深度数据: {len(bids)}买单, {len(asks)}卖单")
        except Exception as e:
            self.logger.error(f"Edgex REST获取{symbol}深度数据时出错: {str(e)}")
            print(f"Edgex REST获取{symbol}深度数据时出错: {str(e)}")

    async def get_depths(self):
        """
        获取所有已订阅币种的深度数据（与Aster一致）
        Returns:
            所有币种的深度数据字典
        """
        return self.depth_data

    async def update_depths(self):
        """
        更新所有已订阅币种的深度数据（与Aster一致）
        Returns:
            更新后的深度数据字典
        """
        tasks = [self.get_depths_rest(symbol) for symbol in self.price_coins]
        await asyncio.gather(*tasks)
        return self.depth_data

    async def get_klines(self, symbol, interval='1m', limit=50):
        """
        获取Edgex交易所K线数据，返回结构与Aster一致
        Args:
            symbol: 交易对，如 "BTCUSDT"
            interval: K线周期，如 '1m', '5m', '1h' 等
            limit: 返回K线数量
        Returns:
            K线数据列表，每个元素为dict，包含timestamp, open, high, low, close, volume
        """
        # 时间map :
        kline_type_map = {
            '1m': "MINUTE_1",
            '5m': "MINUTE_5",
            '15m': 'MINUTE_15',
        }
        contract_id = self.contract_map.get(helpers.get_edgex_symbol(symbol))
        try:
            url = f'https://pro.edgex.exchange/api/v1/public/quote/getKline?contractId={contract_id}&klineType={kline_type_map[interval]}&priceType=LAST_PRICE&size={limit}'
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(url)
                if res.status_code != 200:
                    self.logger.error(f"Aster获取K线数据失败: {res.text}")
                    return None
                data = res.json()
                if not data or data.get('code') != 'SUCCESS':
                    self.logger.error(f"Edgex获取K线数据返回失败: {data}")
                    return None
                data_list = data.get('data', {}).get('dataList', [])
                klines = []
                for k in data_list:
                    klines.append({
                        "timestamp": int(k.get("klineTime", 0)),
                        "open": float(k.get("open", 0)),
                        "high": float(k.get("high", 0)),
                        "low": float(k.get("low", 0)),
                        "close": float(k.get("close", 0)),
                        "volume": float(k.get("size", 0)),
                    })
                return klines
        except Exception as e:
            self.logger.error(f"Aster获取K线数据失败: {e}")
            return None

    async def cancel_order(self, symbol: str, order_id: str) -> Dict:
        """
        取消订单

        Args:
            symbol: 交易对，如 BTC_USDC_PERP
            order_id: 订单ID

        Returns:
            取消订单响应
        """
        return await self.cancel_order_rest(symbol=symbol, order_id=order_id)

    async def cancel_order_rest(self, symbol: str, order_id: str = None, orig_client_order_id: str = None) -> Dict:
        """
        取消订单（Edgex新API，POST /api/v1/private/order/cancelOrderById）
        Args:
            symbol: 币种，如 "BTC"
            order_id: 订单ID
        Returns:
            取消订单响应
        """
        try:
            account_id = self.account_id
            if not account_id:
                raise ValueError('accountId must be set in EdgexAPI (as self.account_id or in config)')
            if not order_id:
                return {
                    'success': False,
                    'order_id': order_id,
                    'raw_response': None,
                    'error': 'orderId must be provided',
                    'symbol': symbol,
                    'status': 'error'
                }
            path = "/api/v1/private/order/cancelOrderById"
            # 1. 签名参数
            params = {
                "accountId": str(account_id),
                "orderIdList": str(order_id)  # 这里是字符串，单个时直接 str(order_id)
            }
            # 2. 请求体
            body = {
                "accountId": str(account_id),
                "orderIdList": [str(order_id)]  # 这里是数组
            }
            # 3. 生成签名
            signature, timestamp = self._edegx_sign('POST', path, params=params)
            headers = {
                'X-edgeX-Api-Timestamp': timestamp,
                'X-edgeX-Api-Signature': signature,
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            url = f"{self.base_url}{path}"
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=body)
                cancel_res = {
                    'success': False,
                    'order_id': order_id,
                    'raw_response': None,
                    'error': None,
                    'symbol': symbol,
                    'status': 'error'
                }
                if response.status_code == 200:
                    res_json = response.json()
                    cancel_res['raw_response'] = res_json
                    if res_json.get('code') == 'SUCCESS':
                        cancel_map = res_json.get('data', {}).get('cancelResultMap', {})
                        cancel_status = cancel_map.get(str(order_id), 'FAILED')
                        cancel_res['success'] = (cancel_status == 'SUCCESS')
                        cancel_res['status'] = cancel_status.lower()
                        cancel_res['order_id'] = order_id
                        return cancel_res
                    else:
                        cancel_res['error'] = res_json.get('msg')
                        return cancel_res
                else:
                    try:
                        cancel_res['raw_response'] = response.json()
                    except Exception:
                        cancel_res['raw_response'] = response.text
                    cancel_res['error'] = cancel_res['raw_response']
                    return cancel_res
        except Exception as e:
            self.logger.error(f"Edgex取消订单异常: {e}")
            return {
                'success': False,
                'order_id': order_id,
                'raw_response': None,
                'error': str(e),
                'symbol': symbol,
                'status': 'error'
            }

    def round_step_size(self, value: Decimal, step_size: str) -> Decimal:
        step = Decimal(step_size)
        precision = abs(step.as_tuple().exponent)
        return value.quantize(step, rounding=ROUND_DOWN)

    async def close_position(self, symbol: str, size: float = None, order_type: str = "LIMIT", price: float = None, side: str = None) -> dict:
        """
        Edgex平仓接口，风格参考binance_api/aster_api
        side ：持仓方向（long为BUY, short为SELL）
        """
        # 方向反向
        cur_side = side
        side = "BUY" if cur_side == "SELL" else "SELL"
        if order_type == "MARKET":
            if self.last_contract_ask1_bid1 is not None and symbol in self.last_contract_ask1_bid1:
                price = max(float(self.last_contract_ask1_bid1[symbol]['ask1_price']), float(self.last_contract_ask1_bid1[symbol]['bid1_price']))

    # 价格参数处理
        try:
            price_val = float(price) if price not in (None, "") else None
        except Exception:
            price_val = None
        # 限价单时格式化价格
        if order_type == "LIMIT" and price_val is not None:
            price_val = self.format_price(symbol, price_val, self.config, self.market_info)
        self.logger.info(f"Edgex平仓， 当前持仓:{symbol}: {size}, 持仓方向: {cur_side}，平仓方向: {side}, order_type:{order_type}, price:{price}, price_val:{price_val}")
        print(f"Edgex平仓， 当前持仓:{symbol}: {size}, 持仓方向: {cur_side}，平仓方向: {side}, order_type:{order_type}, price:{price}, price_val:{price_val}")
        return await self.close_position_rest(symbol=symbol, side=side, order_type=order_type, price=price_val, size=size)
