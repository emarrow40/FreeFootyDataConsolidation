"""Helpers to manage request headers for site scrapers"""

agent_headers = {
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
}

def set_headers(agent_headers=agent_headers, **kwargs):
    """Sets headers for web scraping tasks"""
    return agent_headers | kwargs