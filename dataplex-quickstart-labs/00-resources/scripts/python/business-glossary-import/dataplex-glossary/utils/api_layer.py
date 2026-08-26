"""API Layer for Dataplex Glossary Operations."""

import os
import sys
import threading
import time
from typing import Dict, List, Optional

import requests
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from . import api_call_utils, logging_utils
from .api_call_utils import fetch_api_response
from .constants import (
    CLOUD_RESOURCE_MANAGER_BASE_URL,
    DATAPLEX_BASE_URL,
    ENTRY_NAME_PATTERN,
    EXCLUDED_LOCATIONS,
    PAGE_SIZE,
    PROJECT_PATTERN,
    API_CALL_DELAY_SECONDS,
    TERM_NAME_PATTERN,
)

from .error import *
from .retry_utils import execute_with_retry, is_retryable_google_api_error

logger = logging_utils.get_logger()

_locations_cache: Dict[str, List[str]] = {}
_glossary_cache: Dict[str, Dict] = {}
_term_cache: Dict[str, Dict] = {}
_project_glossaries_cache: Dict[str, List[Dict]] = {}
_glossary_terms_map_cache: Dict[str, Dict[str, str]] = {}
_fqn_to_entry_cache: Dict[str, Dict] = {}
_entry_to_fqn_cache: Dict[str, str] = {}


def clear_caches():
    """Clear all in-memory caches (useful between batch runs and in unit tests)."""
    global _locations_cache, _glossary_cache, _term_cache, _project_glossaries_cache
    global _glossary_terms_map_cache, _fqn_to_entry_cache, _entry_to_fqn_cache
    _locations_cache.clear()
    _glossary_cache.clear()
    _term_cache.clear()
    _project_glossaries_cache.clear()
    _glossary_terms_map_cache.clear()
    _fqn_to_entry_cache.clear()
    _entry_to_fqn_cache.clear()


# Global throttle lock for lookupEntryLinks API calls.
# Ensures a minimum delay of API_CALL_DELAY_SECONDS (240ms) between
# consecutive calls across all threads, keeping within the 500 QPM quota.
_entry_links_throttle_lock = threading.Lock()
_last_entry_links_call_time = 0.0


def initialize_locations_cache(user_project: str) -> List[str]:
    """Pre-fetch and cache the list of supported locations.
    
    Args:
        user_project: The project ID to fetch locations for.
    
    Returns:
        List of all supported location IDs.
    """
    all_locations = list_supported_locations(user_project)
    logger.debug(f"Initialized locations cache with {len(all_locations)} locations")
    return all_locations


def authenticate_dataplex() -> build:
    """Authenticate with Dataplex API."""
    logger.debug("Authenticating with Dataplex API using Application Default Credentials...")
    try:
        creds, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        service = build('dataplex', 'v1', credentials=creds, cache_discovery=False)
        logger.debug("Dataplex API service built successfully")
        return service
    except Exception as e:
        logger.error(f"Dataplex auth error: {e}")
        raise DataplexAPIError(f"Dataplex auth error: {e}")


def list_glossary_terms(dataplex_service: build, glossary_name: str) -> List[Dict]:
    """Lists terms from a Dataplex glossary with pagination support."""
    all_terms = []
    logger.debug(f"Request: glossaries.terms.list(parent={glossary_name})")
    
    terms_request = dataplex_service.projects().locations().glossaries().terms().list(
        parent=glossary_name, pageSize=1000
    )
    
    while terms_request:
        try:
            page_response = execute_with_retry(
                terms_request.execute, 
                f"List glossary terms for {glossary_name}"
            )
        except Exception as terms_error:
            raise DataplexAPIError(f"Error while listing glossary terms for {glossary_name}: {terms_error}")

        all_terms.extend(page_response.get('terms', []))
        terms_request = dataplex_service.projects().locations().glossaries().terms().list_next(
            terms_request, page_response
        )
    
    logger.debug(f"Response: {len(all_terms)} terms for {glossary_name}")
    return all_terms

def parse_entry_name(entry_name: str) -> tuple:
    """Parse a full entry resource name to extract project, location, entry group, and entry ID."""
    match = ENTRY_NAME_PATTERN.match(entry_name)
    if not match:
        logger.error(f"Invalid entry name format: {entry_name}")
        raise InvalidEntryIdFormatError(f"Invalid entry name format: {entry_name}")
    return (
        match.group('project_id'),
        match.group('location_id'),
        match.group('entry_group'),
        match.group('entry_id'),
    )


def _throttle_entry_links_call():
    """Enforce minimum delay between consecutive lookupEntryLinks API calls.
    
    With API_CALL_DELAY_SECONDS=0.24s and 5 threads, each thread effectively
    waits ~1.2s, yielding ~250 QPM — safely within the 500 QPM quota.
    """
    global _last_entry_links_call_time
    with _entry_links_throttle_lock:
        now = time.time()
        elapsed = now - _last_entry_links_call_time
        if elapsed < API_CALL_DELAY_SECONDS:
            time.sleep(API_CALL_DELAY_SECONDS - elapsed)
        _last_entry_links_call_time = time.time()


def _fetch_entry_links_page(
    term_entry_name: str, 
    project_id: str, 
    location_id: str, 
    billing_project: str, 
    page_token: str = None
) -> tuple:
    """Fetch a single page of entry links. Returns (entry_links, next_page_token, error_msg).
    
    Applies throttling to stay within the 500 QPM lookupEntryLinks quota.
    """
    _throttle_entry_links_call()
    lookup_url = build_entry_link_lookup_url(term_entry_name, project_id, location_id, page_token=page_token)
    
    api_response = fetch_api_response(
        method=requests.get,
        url=lookup_url,
        project_id=billing_project
    )
    
    if api_response.get('error_msg'):
        return [], None, api_response['error_msg']
    
    response_data = api_response.get('json', {})
    page_entry_links = response_data.get('entryLinks', [])
    next_page_token = response_data.get('nextPageToken')
    
    return page_entry_links, next_page_token, None


def lookup_entry_links_for_term(
    term_entry_name: str, 
    billing_project: str,
    location: Optional[str] = None
) -> Optional[List[Dict]]:
    """Looks up EntryLinks for a glossary term with pagination."""
    try:
        project_id, entry_location, _, _ = parse_entry_name(term_entry_name)
        target_location = location if location else entry_location        
        all_entry_links = []
        current_page_token = None
        
        while True:
            page_links, next_token, error_message = _fetch_entry_links_page(
                term_entry_name, project_id, target_location, billing_project, current_page_token
            )            
            if error_message:
                logger.error(f"Error looking up entry links at {target_location} for {term_entry_name}: {error_message}")
                break            
            if page_links:
                all_entry_links.extend(page_links)            
            current_page_token = next_token
            if not current_page_token:
                break
        
        logger.debug(f"Entry links found for {term_entry_name} in {target_location}: {all_entry_links}")
        return all_entry_links if all_entry_links else None
        
    except Exception as lookup_error:
        loc = target_location if 'target_location' in dir() else location or 'unknown'
        logger.error(f"Error while looking up entry links for {term_entry_name} in {loc}: {lookup_error}")
        return None


def build_entry_link_lookup_url(
    term_entry_name: str, 
    project_id: str, 
    location_id: str, 
    page_size: int = PAGE_SIZE,
    page_token: Optional[str] = None
) -> str:
    """Builds the lookupEntryLinks API URL with pagination parameters."""
    url = f"{DATAPLEX_BASE_URL}/projects/{project_id}/locations/{location_id}:lookupEntryLinks?entry={term_entry_name}&pageSize={page_size}"
    if page_token:
        url += f"&pageToken={page_token}"
    return url

def lookup_entry(dataplex_service: build, entry_name: str, project_location_name: str) -> Optional[Dict]:
    """Looks up an entry using the Dataplex API."""
    logger.debug(f"Request: lookupEntry(entry={entry_name}, location={project_location_name})")
    try:
        request = dataplex_service.projects().locations().lookupEntry(
            name=project_location_name, entry=entry_name, view="ALL"
        )
        response = execute_with_retry(request.execute, f"Lookup entry {entry_name}")
        logger.debug(f"Response: found entry {response.get('name', 'N/A')}")
        return response
    except HttpError as e:
        status_code = e.resp.status if hasattr(e, 'resp') else None
        if status_code == 404:
            return None
        if status_code in (401, 403):
            logger.warning(f"Permission denied for entry {entry_name}")
            return None
        raise


def _get_project_url(project_id: str) -> str:
    """Builds the Cloud Resource Manager project URL."""
    return f"{CLOUD_RESOURCE_MANAGER_BASE_URL}/projects/{project_id}"


def _fetch_project_info(project_id: str, user_project: str) -> dict:
    """Calls the Cloud Resource Manager API and returns the project JSON payload."""
    url = _get_project_url(project_id)
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        raise DataplexAPIError(f"Failed to fetch project info for '{project_id}': {response['error_msg']}")
    return response.get("json", {})


def _extract_project_number_from_info(project_info: dict) -> str:
    """Extracts the numeric project number from the project info 'name' field."""
    name = project_info.get("name", "")
    match = PROJECT_PATTERN.search(name)
    if match:
        return match.group('project_number')
    raise DataplexAPIError(f"Project number not found in project info: {project_info}")


def get_project_number(project_id: str, user_project: str) -> str:
    """Fetches the project number from the project ID (composed from smaller helpers)."""
    project_info = _fetch_project_info(project_id, user_project)
    return _extract_project_number_from_info(project_info)


def list_supported_locations(billing_project: str, dataplex_service=None, force_refresh: bool = False) -> List[str]:
    """Lists all supported Dataplex locations for a project with caching."""
    global _locations_cache
    
    if not force_refresh and billing_project in _locations_cache:
        return _locations_cache[billing_project]
    
    if dataplex_service is None:
        dataplex_service = authenticate_dataplex()
    
    try:
        parent_resource = f"projects/{billing_project}"
        logger.debug(f"Request: locations.list(name={parent_resource})")
        
        locations_request = dataplex_service.projects().locations().list(name=parent_resource)
        response = execute_with_retry(locations_request.execute, f"List locations for {billing_project}")
        locations = [loc.get('locationId') for loc in response.get('locations', []) if loc.get('locationId')]
        
        _locations_cache[billing_project] = locations
        logger.debug(f"Response: {len(locations)} locations for {billing_project}")
        return locations
        
    except Exception as locations_error:
        logger.error(f"Error while listing supported locations: {locations_error}")
        raise DataplexAPIError(f"Error while listing supported locations: {locations_error}")


def resolve_regions_to_query(location: str, user_project: str) -> List[str]:
    """Resolve location to regions for entry link queries.
    
    For 'global' location, returns all supported locations for fanout.
    For specific locations, returns just that location.
    """
    if location.lower() == "global":
        return [location for location in list_supported_locations(user_project) if location not in EXCLUDED_LOCATIONS]
    return [location]


def get_glossary(dataplex_service: build, glossary_name: str) -> Dict:
    """Fetch a glossary resource by name with in-memory caching."""
    if glossary_name in _glossary_cache:
        return _glossary_cache[glossary_name]
    logger.debug(f"Request: glossaries.get(name={glossary_name})")
    try:
        request = dataplex_service.projects().locations().glossaries().get(name=glossary_name)
        response = execute_with_retry(request.execute, f"Get glossary {glossary_name}")
        _glossary_cache[glossary_name] = response
        return response
    except Exception as e:
        logger.error(f"Error fetching glossary {glossary_name}: {e}")
        raise DataplexAPIError(f"Error fetching glossary {glossary_name}: {e}")


def get_term(dataplex_service: build, term_name: str) -> Dict:
    """Fetch a term resource by name with in-memory caching."""
    if term_name in _term_cache:
        return _term_cache[term_name]
    logger.debug(f"Request: glossaries.terms.get(name={term_name})")
    try:
        request = dataplex_service.projects().locations().glossaries().terms().get(name=term_name)
        response = execute_with_retry(request.execute, f"Get term {term_name}")
        _term_cache[term_name] = response
        return response
    except Exception as e:
        logger.error(f"Error fetching term {term_name}: {e}")
        raise DataplexAPIError(f"Error fetching term {term_name}: {e}")


def list_glossaries(dataplex_service: build, parent: str) -> List[Dict]:
    """Lists all glossaries under a project/location with pagination and in-memory caching."""
    if parent in _project_glossaries_cache:
        return _project_glossaries_cache[parent]

    all_glossaries = []
    logger.debug(f"Request: glossaries.list(parent={parent})")
    try:
        request = dataplex_service.projects().locations().glossaries().list(
            parent=parent, pageSize=1000
        )
        while request:
            response = execute_with_retry(request.execute, f"List glossaries for {parent}")
            all_glossaries.extend(response.get('glossaries', []))
            request = dataplex_service.projects().locations().glossaries().list_next(request, response)

        _project_glossaries_cache[parent] = all_glossaries
        for g in all_glossaries:
            if g.get('name'):
                _glossary_cache[g['name']] = g
        return all_glossaries
    except Exception as e:
        logger.error(f"Error listing glossaries for {parent}: {e}")
        raise DataplexAPIError(f"Error listing glossaries for {parent}: {e}")


def resolve_term_entry_to_display_identifier(dataplex_service: build, term_entry_name: str) -> str:
    """Resolves a Dataplex term entry resource name into '<project>.<location>.<glossaryDisplayName>.<termDisplayName>'."""
    from utils import business_glossary_utils
    term_resource_name = business_glossary_utils.extract_term_resource_from_entry_name(term_entry_name)

    match = TERM_NAME_PATTERN.match(term_resource_name)
    if not match:
        raise InvalidTermNameError(f"Invalid term resource name: {term_resource_name}")

    inner_project = match.group('project_id')
    location_id = match.group('location_id')
    glossary_id = match.group('glossary_id')

    # Prefer outer project ID if it is alphanumeric (not pure numeric digits)
    outer_project, _, _, _ = parse_entry_name(term_entry_name)
    project_id = outer_project if (outer_project and not outer_project.isdigit()) else inner_project

    glossary_resource_name = f"projects/{inner_project}/locations/{location_id}/glossaries/{glossary_id}"
    glossary = get_glossary(dataplex_service, glossary_resource_name)
    term = get_term(dataplex_service, term_resource_name)

    glossary_display_name = glossary.get('displayName') or glossary_id
    term_display_name = term.get('displayName') or match.group('term_id')

    return business_glossary_utils.format_term_display_identifier(
        project_id, location_id, glossary_display_name, term_display_name
    )


def get_entry_fqn(dataplex_service: build, entry_resource_name: str, user_project: str) -> str:
    """Resolve an entry resource name to its Fully Qualified Name (FQN) with caching."""
    if entry_resource_name in _entry_to_fqn_cache:
        return _entry_to_fqn_cache[entry_resource_name]

    project_id, location_id, entry_group, entry_id = parse_entry_name(entry_resource_name)
    parent = f"projects/{user_project}/locations/{location_id}"
    entry_dict = lookup_entry(dataplex_service, entry_resource_name, parent)
    if not entry_dict:
        # Fallback to project's own location
        entry_dict = lookup_entry(dataplex_service, entry_resource_name, f"projects/{project_id}/locations/{location_id}")

    fqn = entry_dict.get("fullyQualifiedName") if entry_dict else None
    if not fqn:
        logger.warning(f"Could not retrieve fullyQualifiedName for entry {entry_resource_name}, falling back to entry name.")
        fqn = entry_resource_name

    _entry_to_fqn_cache[entry_resource_name] = fqn
    return fqn


def lookup_term_by_display_identifier(dataplex_service: build, identifier: str, user_project: str = "") -> str:
    """Resolves a human-readable term identifier to a Dataplex term entry resource name."""
    from utils import business_glossary_utils
    parsed = business_glossary_utils.parse_term_display_identifier(identifier)
    parent_loc = f"projects/{parsed.project_id}/locations/{parsed.location}"

    glossaries = list_glossaries(dataplex_service, parent_loc)
    matched_glossary = None
    for g in glossaries:
        if (g.get('displayName') or '').strip().lower() == parsed.glossary_display_name.lower():
            matched_glossary = g
            break
        # Also check if user passed glossary ID
        glossary_id = g.get('name', '').split('/')[-1]
        if glossary_id.lower() == parsed.glossary_display_name.lower():
            matched_glossary = g
            break

    if not matched_glossary:
        raise GlossaryNotFoundError(
            f"Glossary '{parsed.glossary_display_name}' not found in project '{parsed.project_id}' location '{parsed.location}'"
        )

    glossary_name = matched_glossary['name']

    # Load terms for glossary (cached)
    if glossary_name not in _glossary_terms_map_cache:
        terms = list_glossary_terms(dataplex_service, glossary_name)
        terms_map = {}
        for t in terms:
            t_name = t.get('name', '')
            t_display = (t.get('displayName') or '').strip().lower()
            t_id = t_name.split('/')[-1].lower()
            if t_display:
                terms_map[t_display] = t_name
            if t_id:
                terms_map[t_id] = t_name
        _glossary_terms_map_cache[glossary_name] = terms_map

    terms_map = _glossary_terms_map_cache[glossary_name]
    target_term_key = parsed.term_display_name.strip().lower()
    if target_term_key not in terms_map:
        raise TermNotFoundError(
            f"Term '{parsed.term_display_name}' not found in glossary '{parsed.glossary_display_name}' ({glossary_name})"
        )

    term_resource_name = terms_map[target_term_key]
    try:
        project_number = get_project_number(parsed.project_id, user_project or parsed.project_id)
    except Exception:
        project_number = parsed.project_id
    return business_glossary_utils.generate_entry_name_from_term_name(term_resource_name, project_number=project_number)


def lookup_entry_by_fqn(
    dataplex_service: build, fqn: str, user_project: str, location: str = "global"
) -> Dict:
    """Looks up a Dataplex entry by its Fully Qualified Name (FQN) with caching."""
    if fqn in _fqn_to_entry_cache:
        return _fqn_to_entry_cache[fqn]

    parent = f"projects/{user_project}/locations/{location}"
    entry_dict = lookup_entry(dataplex_service, entry_name=fqn, project_location_name=parent)
    if not entry_dict:
        raise EntryFQNNotFoundError(f"Entry with FQN '{fqn}' not found in Dataplex under project '{user_project}'")

    _fqn_to_entry_cache[fqn] = entry_dict
    if entry_dict.get('name') and entry_dict.get('fullyQualifiedName'):
        _entry_to_fqn_cache[entry_dict['name']] = entry_dict['fullyQualifiedName']
    return entry_dict