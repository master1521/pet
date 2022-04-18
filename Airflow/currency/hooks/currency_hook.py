from airflow.hooks.base import BaseHook

class CurrencyHook(BaseHook):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_currency(self, date, base, symbols) -> float:
        """ Выгружает курс валюты за день
        :param base: Базовая валюта
        :param symbols: Другая валюта относительно базовой (Например евро к рублю)
        """
        import requests

        params = {
            'start_date': date,
            'end_date': date,
            'base': base,
            'symbols': symbols,
            'format': 'json'
        }
        url = 'https://api.exchangerate.host/timeseries'
        response = requests.get(url, params=params)
        return response.json()['rates'][date]['RUB']
