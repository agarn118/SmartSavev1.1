#!/usr/bin/env python3
"""
Debug script to examine __NEXT_DATA__ structure from Real Canadian Superstore.
This helps diagnose extraction issues.
"""

import json
import requests
from bs4 import BeautifulSoup


def fetch_and_examine_next_data(url):
    """Fetch a page and examine its __NEXT_DATA__ structure."""

    print(f"Fetching: {url}")
    print("-" * 80)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'lxml')

    # Find __NEXT_DATA__
    next_data_script = soup.find('script', id='__NEXT_DATA__')

    if not next_data_script:
        print("ERROR: No __NEXT_DATA__ script found!")
        return

    print("Found __NEXT_DATA__ script")

    # Parse JSON
    data = json.loads(next_data_script.string)

    # Navigate structure
    print("\n1. TOP-LEVEL KEYS:")
    print(json.dumps(list(data.keys()), indent=2))

    # Check props
    if 'props' in data:
        print("\n2. data['props'] KEYS:")
        print(json.dumps(list(data['props'].keys()), indent=2))

        if 'pageProps' in data['props']:
            print("\n3. data['props']['pageProps'] KEYS:")
            page_props = data['props']['pageProps']
            print(json.dumps(list(page_props.keys()), indent=2))

            # Check for initialSearchData
            if 'initialSearchData' in page_props:
                print("\n4. data['props']['pageProps']['initialSearchData'] KEYS:")
                search_data = page_props['initialSearchData']
                print(json.dumps(list(search_data.keys()), indent=2))

                # Check for layout
                if 'layout' in search_data:
                    print("\n5. data['props']['pageProps']['initialSearchData']['layout'] KEYS:")
                    layout = search_data['layout']
                    print(json.dumps(list(layout.keys()), indent=2))

                    # Check sections
                    if 'sections' in layout:
                        sections = layout['sections']
                        print(f"\n6. Found sections in layout (type: {type(sections)})")

                        if isinstance(sections, dict):
                            print(f"   Sections is a dict with keys: {list(sections.keys())}")

                            # Check if mainContentCollection exists
                            if 'mainContentCollection' in sections:
                                print(f"\n   Checking sections['mainContentCollection']...")
                                main_content = sections['mainContentCollection']
                                print(f"   Type: {type(main_content)}")

                                if isinstance(main_content, dict):
                                    print(f"   Keys: {list(main_content.keys())}")

                                    if 'components' in main_content:
                                        components = main_content['components']
                                        print(f"\n7. Found {len(components)} components in sections.mainContentCollection")

                                        # Examine each component
                                        for i, component in enumerate(components):
                                            print(f"\n   Component [{i}] keys: {list(component.keys())}")
                                            if 'data' in component:
                                                data_keys = list(component['data'].keys())
                                                print(f"   Component [{i}]['data'] keys: {data_keys}")

                                                if 'productTiles' in component['data']:
                                                    product_tiles = component['data']['productTiles']
                                                    print(f"\n   >>> FOUND {len(product_tiles)} products in sections.mainContentCollection.components[{i}].data.productTiles!")

                                                    # Show first product
                                                    if product_tiles:
                                                        print(f"\n8. FIRST PRODUCT EXAMPLE:")
                                                        print(json.dumps(product_tiles[0], indent=2))

                                                    return product_tiles
                        elif isinstance(sections, list):
                            # Old structure: sections is a list
                            print(f"\n6. Found {len(sections)} sections in layout (as list)")

                            # Examine each section
                            for i, section in enumerate(sections):
                                if isinstance(section, dict) and 'components' in section:
                                    components = section['components']
                                    print(f"   Section [{i}] has {len(components)} components")

                                    # Examine each component
                                    for j, component in enumerate(components):
                                        print(f"      Component [{i}][{j}] keys: {list(component.keys())}")
                                        if 'data' in component:
                                            data_keys = list(component['data'].keys())
                                            print(f"      Component [{i}][{j}]['data'] keys: {data_keys}")

                                            if 'productTiles' in component['data']:
                                                product_tiles = component['data']['productTiles']
                                                print(f"\n      >>> FOUND {len(product_tiles)} products!")

                                                if product_tiles:
                                                    print(f"\n7. FIRST PRODUCT EXAMPLE:")
                                                    print(json.dumps(product_tiles[0], indent=2))

                                                return product_tiles

                    # Check mainContentCollection (legacy path)
                    if 'mainContentCollection' in layout:
                        print("\n6. LEGACY: data['props']['pageProps']['initialSearchData']['layout']['mainContentCollection'] KEYS:")
                        main_content = layout['mainContentCollection']
                        print(json.dumps(list(main_content.keys()), indent=2))

                        # Check components
                        if 'components' in main_content:
                            components = main_content['components']
                            print(f"\n7. Found {len(components)} components in mainContentCollection")

                            # Examine each component
                            for i, component in enumerate(components):
                                print(f"\n   Component [{i}] keys: {list(component.keys())}")
                                if 'data' in component:
                                    print(f"   Component [{i}]['data'] keys: {list(component['data'].keys())}")
                                    if 'productTiles' in component['data']:
                                        product_tiles = component['data']['productTiles']
                                        print(f"   Component [{i}]['data']['productTiles']: FOUND {len(product_tiles)} products!")

                                        # Show first product
                                        if product_tiles:
                                            print(f"\n8. FIRST PRODUCT EXAMPLE:")
                                            print(json.dumps(product_tiles[0], indent=2))

                                        return product_tiles

                # OLD PATH: Check if products key exists (legacy)
                if 'products' in search_data:
                    print("\n   LEGACY PATH: 'products' key found directly in initialSearchData")
                    print(f"   Found {len(search_data['products'])} products via legacy path")

    print("\n" + "=" * 80)
    print("CONCLUSION: Could not find products in expected locations")
    print("=" * 80)


if __name__ == '__main__':
    # Test URL
    test_url = "https://www.realcanadiansuperstore.ca/search?search-bar=milk&page=1"

    print("="*80)
    print("DEBUG: Real Canadian Superstore __NEXT_DATA__ Structure")
    print("="*80)
    print()

    products = fetch_and_examine_next_data(test_url)

    if products:
        print("\n" + "="*80)
        print(f"SUCCESS: Found {len(products)} products!")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("FAILURE: No products found")
        print("="*80)
