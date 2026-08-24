"""
Data models for Dataplex Glossary Import/Export operations.

These dataclasses provide type-safe representations of Dataplex resources
and eliminate the need for fragile dictionary access patterns.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class EntryReference:
    """Represents a source or target reference within an EntryLink."""
    name: str
    path: str = ""
    type: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntryReference':
        """Create an EntryReference from a dictionary."""
        return cls(
            name=data.get('name', ''),
            path=data.get('path', ''),
            type=data.get('type')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        result = {'name': self.name}
        if self.path:
            result['path'] = self.path
        if self.type:
            result['type'] = self.type
        return result


@dataclass
class EntryLink:
    """Represents a complete EntryLink for import/export operations."""
    name: str
    entryLinkType: str
    entryReferences: List[EntryReference] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntryLink':
        """Create EntryLink from a dictionary."""
        # Handle both nested and flat dictionary formats
        entry_data = data.get('entryLink', data)
        refs = [EntryReference.from_dict(ref) for ref in entry_data.get('entryReferences', [])]
        return cls(
            name=entry_data.get('name', ''),
            entryLinkType=entry_data.get('entryLinkType', ''),
            entryReferences=refs
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'entryLink': {
                'name': self.name,
                'entryLinkType': self.entryLinkType,
                'entryReferences': [ref.to_dict() for ref in self.entryReferences]
            }
        }


@dataclass
class ParsedTermIdentifier:
    """Represents a parsed human-readable term identifier."""
    project_id: str
    location: str
    glossary_display_name: str
    term_display_name: str


@dataclass
class SpreadsheetRow:
    """Represents a row from the EntryLink import spreadsheet."""
    entry_link_type: str = ""
    source: str = ""
    target: str = ""
    column: str = ""

    def __init__(
        self,
        entry_link_type: str = "",
        source: str = "",
        target: str = "",
        column: str = "",
        source_entry: Optional[str] = None,
        target_entry: Optional[str] = None,
        source_path: Optional[str] = None,
    ):
        self.entry_link_type = entry_link_type
        self.source = source_entry if source_entry is not None else source
        self.target = target_entry if target_entry is not None else target
        self.column = source_path if source_path is not None else column

    @property
    def source_entry(self) -> str:
        return self.source

    @property
    def target_entry(self) -> str:
        return self.target

    @property
    def source_path(self) -> str:
        return self.column

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'SpreadsheetRow':
        """Create a SpreadsheetRow from a dictionary supporting new and legacy keys."""
        link_type = (
            data.get('entry_link_type') or data.get('Entry link type') or data.get('entryLinkType', '')
        ).strip()
        source = (
            data.get('source') or data.get('Source') or data.get('source_entry') or data.get('sourceEntry', '')
        ).strip()
        target = (
            data.get('target') or data.get('Target') or data.get('target_entry') or data.get('targetEntry', '')
        ).strip()
        column = (
            data.get('column') or data.get('Column') or data.get('source_path') or data.get('sourcePath', '')
        ).strip()
        return cls(
            entry_link_type=link_type,
            source=source,
            target=target,
            column=column
        )


