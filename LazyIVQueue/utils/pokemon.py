import json
import asyncio
import aiohttp
from LazyIVQueue.utils.logger import logger
import os
from .pokemon_names import POKEMON_NAMES

_pokemon_names = {}

async def load_pokemon_names():
    global _pokemon_names
    cache_path = os.path.join(os.path.dirname(__file__), "..", "config", "pokemon.json")
    
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                _pokemon_names = json.load(f)
            logger.info(f"Loaded {len(_pokemon_names)} Pokemon names from cache.")
            return
        except Exception as e:
            logger.warning(f"Failed to load pokemon names from cache: {e}")

    try:
        logger.info("Downloading Pokemon names from WatWowMap...")
        async with aiohttp.ClientSession() as session:
            async with session.get("https://raw.githubusercontent.com/WatWowMap/Masterfile-Generator/master/master-latest-poracle.json") as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    for key, val in data.get("monsters", {}).items():
                        _pokemon_names[key] = val.get("name", "Unknown")
                    
                    try:
                        with open(cache_path, "w") as f:
                            json.dump(_pokemon_names, f)
                    except Exception as e:
                        logger.warning(f"Could not save pokemon.json cache: {e}")
                        
                    logger.info(f"Downloaded and cached {len(_pokemon_names)} Pokemon names.")
                else:
                    logger.error(f"Failed to download pokemon names, status: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to download pokemon names: {e}")

def get_pokemon_name(pokemon_id: int, form: int = None) -> str:
    # First check network loaded names
    if _pokemon_names:
        if form is not None:
            key = f"{pokemon_id}_{form}"
            if key in _pokemon_names:
                return _pokemon_names[key]
        
        key_zero = f"{pokemon_id}_0"
        if key_zero in _pokemon_names:
            return _pokemon_names[key_zero]
            
        key_str = str(pokemon_id)
        if key_str in _pokemon_names:
            return _pokemon_names[key_str]
            
    # Fallback to local hardcoded list
    return POKEMON_NAMES.get(pokemon_id, str(pokemon_id))
