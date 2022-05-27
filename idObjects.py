from dataclasses import dataclass

@dataclass
class ClubData:
    name: str
    id: int
    league: str
    url: str
    site: str

@dataclass
class PlayerData:
    name: str
    id: int
    team: str
    url: str
    site: str