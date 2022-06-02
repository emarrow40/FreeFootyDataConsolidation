from dataclasses import dataclass

@dataclass
class ClubData:
    """Dataclass to store club site scraping results"""
    name: str
    id: int
    league: str
    url: str
    site: str

@dataclass
class PlayerData:
    """Dataclass to store player site scraping results"""
    name: str
    id: int
    team: str
    url: str
    site: str