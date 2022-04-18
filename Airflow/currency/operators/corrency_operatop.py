from airflow.models.baseoperator import BaseOperator
from ..hooks.currency_hook import CurrencyHook

class CurrencyOperator(BaseOperator):
    def __init__(self, base: str = 'USD', symbols: str = 'RUB', **kwargs):
        super().__init__(**kwargs)
        self.base = base
        self.symbols = symbols

    def execute(self, context):
        currency_hook = CurrencyHook()
        rate = currency_hook.get_currency(date=context['ds'], base=self.base, symbols=self.symbols)
        return rate
