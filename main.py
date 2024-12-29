import asyncio
import json
import hmac
import base64
import requests
import logging
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass
from decimal import Decimal
import traceback
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Market:
    symbol: str
    perp_asset_id: int
    spot_asset_id: int
    funding_rate: float = 0.0
    mark_price: float = 0.0
    size_decimals: int = 8

class HyperliquidFundingArb:
    def __init__(self,
                 min_funding_rate: float = 0.05,  # 5% annualized 
                 position_size_usd: float = 1000,
                 max_slippage: float = 0.001):
        
        # Load API credentials
        self.hl_key = os.getenv('HYPERLIQUID_KEY')
        self.hl_secret = os.getenv('HYPERLIQUID_SECRET')
        
        if not self.hl_key or not self.hl_secret:
            raise ValueError("HYPERLIQUID_KEY and HYPERLIQUID_SECRET must be set in .env file")
            
        self.base_url = "https://api.hyperliquid.xyz"
        self.min_funding_rate = min_funding_rate
        self.position_size_usd = position_size_usd
        self.max_slippage = max_slippage
        self.active_positions = {}
        
        # Initialize markets cache
        self.markets = {}
        self.initialize_markets()

    def initialize_markets(self):
        """Initialize market information with correct asset IDs"""
        try:
            # Get perp metadata
            perp_meta = self._make_request('POST', '/info', {'type': 'meta'})
            
            # Get spot metadata
            spot_meta = self._make_request('POST', '/info', {'type': 'spotMeta'})
            
            # Map markets
            for perp_idx, perp in enumerate(perp_meta['universe']):
                symbol = perp['name']
                spot_info = None
                
                # Find corresponding spot market
                for spot in spot_meta['universe']:
                    if spot['name'].split('/')[0] == symbol:
                        spot_info = spot
                        break
                
                if spot_info:
                    self.markets[symbol] = Market(
                        symbol=symbol,
                        perp_asset_id=perp_idx,
                        spot_asset_id=10000 + spot_info['index'],
                        size_decimals=perp['szDecimals']
                    )
            
            logger.info(f"Initialized {len(self.markets)} markets")
            
        except Exception as e:
            logger.error(f"Error initializing markets: {str(e)}")
            raise

    async def get_funding_opportunities(self) -> List[Dict]:
        """Get all markets and their funding rates"""
        try:
            # Get market contexts
            contexts = self._make_request('POST', '/info', {'type': 'metaAndAssetCtxs'})
            
            opportunities = []
            
            # Process each market
            for idx, ctx in enumerate(contexts[1]):
                symbol = contexts[0]['universe'][idx]['name']
                if symbol not in self.markets:
                    continue
                
                market = self.markets[symbol]
                
                # Update market data
                market.funding_rate = float(ctx['funding']) * 365 * 100  # Annualize funding rate
                market.mark_price = float(ctx['markPx'])
                
                opportunities.append({
                    'symbol': symbol,
                    'funding_rate': market.funding_rate,
                    'mark_price': market.mark_price,
                    'market': market
                })

            # Sort by absolute funding rate
            opportunities.sort(key=lambda x: abs(x['funding_rate']), reverse=True)
            return opportunities

        except Exception as e:
            logger.error(f"Error fetching funding opportunities: {str(e)}")
            return []

    async def execute_funding_arb(self, market: Market, funding_rate: float):
        """Execute the funding arbitrage for a specific market"""
        try:
            # Calculate position sizes
            token_amount = self.position_size_usd / market.mark_price
            formatted_amount = f"{token_amount:.{market.size_decimals}f}"
            
            logger.info(f"Attempting funding arb for {market.symbol}")
            logger.info(f"Funding Rate: {funding_rate:.2f}% APR")
            logger.info(f"Trade size: {formatted_amount} {market.symbol} (${self.position_size_usd:.2f})")

            # Place spot long order
            spot_order = self._place_order(
                asset_id=market.spot_asset_id,
                is_buy=True,
                price=str(market.mark_price * (1 + self.max_slippage)),
                size=formatted_amount
            )
            
            if not spot_order.get('status') == 'ok':
                logger.error("Failed to place spot order")
                return False

            # Place perp short order
            perp_order = self._place_order(
                asset_id=market.perp_asset_id,
                is_buy=False,
                price=str(market.mark_price * (1 - self.max_slippage)),
                size=formatted_amount
            )

            if not perp_order.get('status') == 'ok':
                logger.error("Failed to place perp order")
                # Try to close spot position
                await self._emergency_close(market.spot_asset_id, formatted_amount, True)
                return False

            # Record the position
            self.active_positions[market.symbol] = {
                'entry_time': datetime.now(),
                'funding_rate': funding_rate,
                'mark_price': market.mark_price,
                'amount': token_amount,
                'market': market
            }

            logger.info(f"Successfully opened funding arbitrage position for {market.symbol}")
            return True

        except Exception as e:
            logger.error(f"Error executing funding arbitrage: {str(e)}")
            return False

    def _place_order(self, asset_id: int, is_buy: bool, price: str, size: str) -> Dict:
        """Place an order on Hyperliquid"""
        try:
            order = {
                "a": asset_id,
                "b": is_buy,
                "p": price,
                "s": size,
                "r": False,
                "t": {"limit": {"tif": "Gtc"}}
            }

            payload = {
                "action": {
                    "type": "order",
                    "orders": [order],
                    "grouping": "na"
                },
                "nonce": int(time.time() * 1000),
                "signature": self._get_signature()
            }

            response = requests.post(
                f"{self.base_url}/exchange",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            return response.json()

        except Exception as e:
            logger.error(f"Error placing order: {str(e)}")
            return None

    async def _emergency_close(self, asset_id: int, size: str, is_buy: bool):
        """Emergency position closure"""
        try:
            close_order = self._place_order(
                asset_id=asset_id,
                is_buy=not is_buy,  # Opposite of original order
                price="0",  # Market order
                size=size
            )
            return close_order and close_order.get('status') == 'ok'
        except Exception as e:
            logger.error(f"Error in emergency close: {str(e)}")
            return False

    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Make request to Hyperliquid API"""
        try:
            url = f"{self.base_url}{endpoint}"
            headers = {"Content-Type": "application/json"}

            if method == 'GET':
                response = requests.get(url, headers=headers)
            else:
                response = requests.post(url, headers=headers, json=data)

            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise

    def _get_signature(self) -> str:
        """Generate API signature"""
        # Implementation needed based on your authentication method
        pass

async def main():
    try:
        bot = HyperliquidFundingArb(
            min_funding_rate=0.05,  # 5% APR minimum
            position_size_usd=1000,  # $1000 per trade
            max_slippage=0.001  # 0.1% max slippage
        )
        
        while True:
            try:
                opportunities = await bot.get_funding_opportunities()
                
                if not opportunities:
                    logger.info("No opportunities found")
                    await asyncio.sleep(60)
                    continue

                logger.info("\nTop funding rate opportunities:")
                for opp in opportunities[:5]:
                    logger.info(f"{opp['symbol']}: {opp['funding_rate']:.2f}% APR")

                # Check for new opportunities
                for opp in opportunities:
                    if abs(opp['funding_rate']) >= bot.min_funding_rate:
                        if opp['symbol'] not in bot.active_positions:
                            logger.info(f"\nFound opportunity: {opp['symbol']} with {opp['funding_rate']:.2f}% APR funding rate")
                            
                            success = await bot.execute_funding_arb(
                                market=opp['market'],
                                funding_rate=opp['funding_rate']
                            )
                            
                            if success:
                                logger.info(f"Successfully executed funding arbitrage for {opp['symbol']}")
                            else:
                                logger.error(f"Failed to execute funding arbitrage for {opp['symbol']}")

            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                
            await asyncio.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())