#!/usr/bin/env python3
"""
Store rotation manager for multi-store scraping.
Rotates through configured store IDs to capture regional pricing variations.
"""

import random
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class StoreConfig:
    """Store configuration with location metadata."""
    id: str
    name: str
    city: str
    province: str

    @classmethod
    def from_dict(cls, data: Dict) -> 'StoreConfig':
        """Create StoreConfig from dictionary."""
        return cls(
            id=str(data['id']),
            name=data.get('name', f"Store {data['id']}"),
            city=data.get('city', ''),
            province=data.get('province', '')
        )

    def __str__(self) -> str:
        return f"{self.name} ({self.city}, {self.province}) - ID: {self.id}"


# Default Alberta Sobeys stores
# These are example store IDs - actual IDs should be verified from Sobeys website
DEFAULT_ALBERTA_STORES = [
    {"id": "0320", "name": "Sobeys Airdrie", "city": "Airdrie", "province": "AB"},
    {"id": "0315", "name": "Sobeys Shawnessy", "city": "Calgary", "province": "AB"},
    {"id": "0348", "name": "Sobeys Signal Hill", "city": "Calgary", "province": "AB"},
    {"id": "0325", "name": "Sobeys Crowfoot", "city": "Calgary", "province": "AB"},
    {"id": "0521", "name": "Sobeys Riverbend", "city": "Edmonton", "province": "AB"},
    {"id": "0515", "name": "Sobeys Summerside", "city": "Edmonton", "province": "AB"},
    {"id": "0530", "name": "Sobeys Windermere", "city": "Edmonton", "province": "AB"},
    {"id": "0535", "name": "Sobeys St. Albert", "city": "St. Albert", "province": "AB"},
]


class StoreRotator:
    """
    Manages rotation through multiple store IDs for regional pricing capture.

    Features:
    - Rotate through stores for each query
    - Support different rotation modes (all, sample, single)
    - Track store metadata (city, province)
    - Filter by province
    """

    def __init__(self, stores_config: List[Dict] = None, rotation_mode: str = 'all'):
        """
        Initialize store rotator.

        Args:
            stores_config: List of store configuration dicts. If None, uses default Alberta stores.
            rotation_mode: How to select stores for each query:
                - 'all': Use all configured stores
                - 'sample': Random subset of stores
                - 'single': Use only the first store
        """
        if stores_config:
            self.stores = [StoreConfig.from_dict(s) for s in stores_config]
        else:
            logging.info("No stores configured, using default Alberta stores")
            self.stores = [StoreConfig.from_dict(s) for s in DEFAULT_ALBERTA_STORES]

        self.rotation_mode = rotation_mode
        self.current_index = 0

        if not self.stores:
            logging.warning("No stores configured, adding default Airdrie store")
            self.stores = [StoreConfig(id="0320", name="Sobeys Airdrie", city="Airdrie", province="AB")]

        logging.info(f"StoreRotator initialized with {len(self.stores)} stores in mode '{rotation_mode}'")
        for store in self.stores:
            logging.debug(f"  - {store}")

    def get_current_store(self) -> StoreConfig:
        """Get current store configuration."""
        return self.stores[self.current_index]

    def get_all_stores(self) -> List[StoreConfig]:
        """Get all configured stores."""
        return self.stores

    def get_stores_for_query(self, sample_size: int = None) -> List[StoreConfig]:
        """
        Get stores to use for a query based on rotation mode.

        Args:
            sample_size: For 'sample' mode, how many stores to use (default: half)

        Returns:
            List of stores to query
        """
        if self.rotation_mode == 'single':
            return [self.stores[0]]

        elif self.rotation_mode == 'sample':
            if sample_size is None:
                sample_size = max(1, len(self.stores) // 2)
            return random.sample(self.stores, min(sample_size, len(self.stores)))

        else:  # 'all'
            return self.stores

    def get_stores_by_city(self, city: str) -> List[StoreConfig]:
        """
        Get all stores in a specific city.

        Args:
            city: City name (case-insensitive)

        Returns:
            List of stores in that city
        """
        city_lower = city.lower()
        return [s for s in self.stores if s.city.lower() == city_lower]

    def get_stores_by_province(self, province: str) -> List[StoreConfig]:
        """
        Get all stores in a specific province.

        Args:
            province: Province code (e.g., 'AB', 'ON')

        Returns:
            List of stores in that province
        """
        province_upper = province.upper()
        return [s for s in self.stores if s.province.upper() == province_upper]

    def rotate(self):
        """Rotate to next store (for single-store sequential access)."""
        old_index = self.current_index
        self.current_index = (self.current_index + 1) % len(self.stores)
        logging.debug(f"Rotated from store {old_index} to {self.current_index}: {self.stores[self.current_index].name}")

    def reset(self):
        """Reset to first store."""
        self.current_index = 0

    def get_unique_cities(self) -> List[str]:
        """Get list of unique cities with configured stores."""
        return list(set(s.city for s in self.stores))

    def get_unique_provinces(self) -> List[str]:
        """Get list of unique provinces with configured stores."""
        return list(set(s.province for s in self.stores))

    def get_status(self) -> Dict:
        """
        Get current rotator status.

        Returns:
            Dict with status information
        """
        return {
            'total_stores': len(self.stores),
            'rotation_mode': self.rotation_mode,
            'current_index': self.current_index,
            'current_store': str(self.get_current_store()),
            'cities': self.get_unique_cities(),
            'provinces': self.get_unique_provinces()
        }
