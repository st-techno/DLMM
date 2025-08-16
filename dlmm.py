import threading
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Callable

# Setup logger
logger = logging.getLogger("DYNAMITE_DLMM")
logger.setLevel(logging.INFO)

@dataclass
class LiquidityBin:
    bin_id: int
    lower_bound: float
    upper_bound: float
    liquidity: float
    lp_shares: Dict[str, float] = field(default_factory=dict)  # LP address -> shares

    def adjust_liquidity(self, delta: float):
        logger.info(f"Adjusting liquidity in bin {self.bin_id} by {delta}")
        self.liquidity += delta
        if self.liquidity < 0:
            raise ValueError("Liquidity cannot be negative")

@dataclass
class LPAccount:
    address: str
    total_shares: float = 0.0
    bin_positions: Dict[int, float] = field(default_factory=dict)  # bin_id -> shares
    rewards: float = 0.0

class DYNAMITE_DLMM:
    def __init__(
        self,
        bins: List[LiquidityBin],
        base_factor: float,
        bin_step: float,
        volatility_source: Callable[[], float],
        fee_hook: Callable[[float, float], float] = None,
    ):
        self.bins = {bin.bin_id: bin for bin in bins}
        self.base_factor = base_factor
        self.bin_step = bin_step
        self.lp_accounts: Dict[str, LPAccount] = {}
        self.lock = threading.Lock()
        self.volatility_source = volatility_source
        self.fee_hook = fee_hook if fee_hook else self.default_fee_hook

    def default_fee_hook(self, price, volatility):
        # Advanced fee logic for institutional LPs
        # Example: nonlinear volatility impact
        base_fee = self.base_factor * self.bin_step
        variable_fee = self.bin_step * (volatility**1.25)
        return max(base_fee + variable_fee, 0.0001)

    def find_bin(self, price):
        for bin in self.bins.values():
            if bin.lower_bound <= price < bin.upper_bound:
                return bin
        return None

    def swap(self, price, amount, aggressor='taker'):
        with self.lock:
            bin = self.find_bin(price)
            if bin is None:
                logger.error("Swap failed: No bin found for price")
                raise Exception("Price out of range (no liquidity bin found)")
            if bin.liquidity < amount:
                logger.error("Swap failed: Insufficient liquidity")
                raise Exception("Insufficient liquidity in bin")
            volatility = self.volatility_source()
            fee = self.fee_hook(price, volatility)
            total_out = amount - fee
            bin.adjust_liquidity(-amount)
            logger.info(f"Swap executed: price={price}, amount={amount}, fee={fee}, total_out={total_out}")
            self._accrue_fees(bin, fee)
            return {"filled": amount, "fee": fee, "received": total_out, "bin_id": bin.bin_id}

    def add_liquidity(self, lp_address, bin_id, amount):
        with self.lock:
            bin = self.bins.get(bin_id)
            if not bin:
                raise Exception("Invalid bin_id")
            bin.adjust_liquidity(amount)
            if lp_address not in self.lp_accounts:
                self.lp_accounts[lp_address] = LPAccount(lp_address)
            lp = self.lp_accounts[lp_address]
            lp.total_shares += amount
            lp.bin_positions[bin_id] = lp.bin_positions.get(bin_id, 0) + amount
            bin.lp_shares[lp_address] = bin.lp_shares.get(lp_address, 0) + amount
            logger.info(f"Liquidity added: LP {lp_address}, bin {bin_id}, amount {amount}")

    def remove_liquidity(self, lp_address, bin_id, amount):
        with self.lock:
            bin = self.bins.get(bin_id)
            if not bin:
                raise Exception("Invalid bin_id")
            if lp_address not in self.lp_accounts or bin.lp_shares.get(lp_address, 0) < amount:
                raise Exception("LP does not have enough shares in this bin")
            bin.adjust_liquidity(-amount)
            lp = self.lp_accounts[lp_address]
            lp.total_shares -= amount
            lp.bin_positions[bin_id] -= amount
            bin.lp_shares[lp_address] -= amount
            logger.info(f"Liquidity removed: LP {lp_address}, bin {bin_id}, amount {amount}")

    def _accrue_fees(self, bin: LiquidityBin, fee: float):
        # Simple pro-rata distribution to LPs in bin
        total_shares = sum(bin.lp_shares.values())
        for lp_address, shares in bin.lp_shares.items():
            lp = self.lp_accounts[lp_address]
            lp_share = (shares / total_shares) if total_shares != 0 else 0
            lp.rewards += fee * lp_share
            logger.info(f"Accrued fees: LP {lp_address} in bin {bin.bin_id} receives {fee * lp_share}")

    def get_lp_summary(self, lp_address):
        lp = self.lp_accounts.get(lp_address)
        if not lp:
            raise Exception("LP not found")
        return {
            "address": lp.address,
            "total_shares": lp.total_shares,
            "bin_positions": lp.bin_positions,
            "rewards": lp.rewards
        }

    def reallocate_liquidity(self, reallocation_strategy: Callable[['DYNAMITE_DLMM'], None]):
        # Allows user-provided strategies: e.g. shift liquidity to bins with rising volatility/volume
        with self.lock:
            reallocation_strategy(self)
            logger.info("Liquidity reallocation completed using custom strategy.")

# --- Example usage setup ---

def mock_volatility():
    # Integrate with institutional-grade price/volatility feed here
    import random
    return random.uniform(0.01, 0.2)

def institutional_bin_reallocation(dlmm: DYNAMITE_DLMM):
    # Example: Move excess liquidity from bins with low volume to bins with highest volatility
    vol = dlmm.volatility_source()
    # In production, integrate with analytics/volume data feed
    high_vol_bin = max(dlmm.bins.values(), key=lambda b: b.upper_bound - b.lower_bound)
    for bin in dlmm.bins.values():
        if bin != high_vol_bin and bin.liquidity > 100_000:
            transfer = bin.liquidity * 0.1
            bin.adjust_liquidity(-transfer)
            high_vol_bin.adjust_liquidity(transfer)
            logger.info(f"Reallocated {transfer} liquidity from bin {bin.bin_id} to {high_vol_bin.bin_id}")

# Initialization
bins = [
    LiquidityBin(bin_id=1, lower_bound=0.0, upper_bound=1.0, liquidity=500_000),
    LiquidityBin(bin_id=2, lower_bound=1.0, upper_bound=2.0, liquidity=500_000),
    LiquidityBin(bin_id=3, lower_bound=2.0, upper_bound=3.0, liquidity=500_000),
]
dlmm = DYNAMITE_DLMM(
    bins=bins,
    base_factor=0.0005,
    bin_step=0.05,
    volatility_source=mock_volatility
)

# Sample LP operation
dlmm.add_liquidity("LP001", 1, 100_000)
dlmm.swap(price=1.5, amount=10_000)
dlmm.reallocate_liquidity(institutional_bin_reallocation)
summary = dlmm.get_lp_summary("LP001")
print("LP Summary:", summary)
