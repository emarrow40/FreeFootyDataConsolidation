from dataclasses import dataclass

@dataclass
class HttpHeaders:
    """Dataclass to store appropriate headers for a given site"""
    cookie: str
    origin: str
    referer: str
    sec_ch_ua: str = '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"'
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36'

    def header_dict(self):
        headers = self.__dict__
        return {key.replace('_', '-'): value for key, value in headers.items() if value}